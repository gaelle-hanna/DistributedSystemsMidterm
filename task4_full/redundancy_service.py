import hashlib
import os
import threading
import time
from contextlib import asynccontextmanager

import httpx
import psycopg
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from zfec import Decoder, Encoder

DB_URL = os.getenv('DATABASE_URL', 'postgresql://storage:storagepass@postgres:5432/storage_db')
METADATA_URL = os.getenv('METADATA_URL', 'http://metadata:8000')
NODE_URLS = {f'node{i}': os.getenv(f'NODE{i}_URL', f'http://node{i}:8000') for i in range(1, 7)}
CHUNK_SIZE = 4 * 1024 * 1024
SCRUB_SECONDS = int(os.getenv('SCRUB_SECONDS', '60'))
lock = threading.RLock()
metrics = {'repair_bytes_read': 0, 'repair_bytes_written': 0, 'scrub_bytes_read': 0, 'scrub_checks': 0, 'scrub_corrupt': 0, 'repair_operations': 0}
mode_metrics = {mode: dict.fromkeys(metrics, 0) for mode in ('replication', 'erasure')}


def record_metric(mode, name, amount=1):
    metrics[name] += amount
    mode_metrics[mode][name] += amount


def db():
    return psycopg.connect(DB_URL)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def initialize():
    for attempt in range(30):
        try:
            with db() as conn:
                conn.execute('CREATE TABLE IF NOT EXISTS redundancy_buckets (name TEXT PRIMARY KEY, mode TEXT NOT NULL, replicas INTEGER NOT NULL, write_quorum INTEGER NOT NULL, read_quorum INTEGER NOT NULL)')
                conn.execute('CREATE TABLE IF NOT EXISTS redundancy_objects (bucket TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL, size BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(bucket,key,version))')
                conn.execute('CREATE TABLE IF NOT EXISTS redundancy_chunks (bucket TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL, position INTEGER NOT NULL, chunk_hash TEXT NOT NULL, chunk_size INTEGER NOT NULL, PRIMARY KEY(bucket,key,version,position), FOREIGN KEY(bucket,key,version) REFERENCES redundancy_objects(bucket,key,version) ON DELETE CASCADE)')
                conn.execute('CREATE TABLE IF NOT EXISTS redundancy_shards (bucket TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL, position INTEGER NOT NULL, shard_index INTEGER NOT NULL, node_id TEXT NOT NULL, shard_hash TEXT NOT NULL, shard_size INTEGER NOT NULL, PRIMARY KEY(bucket,key,version,position,shard_index,node_id), FOREIGN KEY(bucket,key,version,position) REFERENCES redundancy_chunks(bucket,key,version,position) ON DELETE CASCADE)')
            return
        except psycopg.OperationalError:
            if attempt == 29:
                raise
            time.sleep(2)


class BucketConfig(BaseModel):
    mode: str = 'replication'
    replicas: int = Field(default=3, ge=1, le=6)
    write_quorum: int = Field(default=2, ge=1, le=6)
    read_quorum: int = Field(default=2, ge=1, le=6)


def bucket_config(conn, bucket):
    row = conn.execute('SELECT mode, replicas, write_quorum, read_quorum FROM redundancy_buckets WHERE name=%s', (bucket,)).fetchone()
    if not row:
        raise HTTPException(404, 'Bucket not configured; create it using PUT /buckets/{bucket}')
    return dict(zip(('mode', 'replicas', 'write_quorum', 'read_quorum'), row))


def available_nodes():
    try:
        with httpx.Client(timeout=4) as client:
            response = client.get(f'{METADATA_URL}/nodes')
            response.raise_for_status()
            return {item['node_id'] for item in response.json()['nodes'] if item['status'] == 'healthy' and item['node_id'] in NODE_URLS}
    except (httpx.HTTPError, KeyError, ValueError):
        return set()


def ranked_nodes(chunk_hash, live):
    return sorted(live, key=lambda n: hashlib.sha256(f'{chunk_hash}:{n}'.encode()).digest(), reverse=True)


def encode_shards(data, mode, replicas):
    if mode == 'replication':
        return [data] * replicas
    size = (len(data) + 3) // 4
    padded = data.ljust(size * 4, b'\0')
    return list(Encoder(4, 6).encode([padded[i * size:(i + 1) * size] for i in range(4)]))


def get_piece(client, node, piece_hash, mode=None, counter='repair_bytes_read'):
    try:
        response = client.get(f'{NODE_URLS[node]}/chunks/{piece_hash}')
        if response.status_code != 200:
            return None
        content = response.content
        if mode is not None:
            record_metric(mode, counter, len(content))
        return content if digest(content) == piece_hash else None
    except httpx.HTTPError:
        return None


def put_piece(client, node, content):
    piece_hash = digest(content)
    try:
        response = client.put(f'{NODE_URLS[node]}/chunks/{piece_hash}', content=content)
        response.raise_for_status()
        return piece_hash
    except httpx.HTTPError:
        return None


def recover_chunk(conn, client, bucket, key, version, position, mode, replicas, read_quorum, live, measure_repair=False):
    entry = conn.execute('SELECT chunk_hash, chunk_size FROM redundancy_chunks WHERE bucket=%s AND key=%s AND version=%s AND position=%s', (bucket, key, version, position)).fetchone()
    if entry is None:
        raise HTTPException(404, 'Chunk metadata missing')
    chunk_hash, chunk_size = entry
    rows = conn.execute('SELECT shard_index,node_id,shard_hash FROM redundancy_shards WHERE bucket=%s AND key=%s AND version=%s AND position=%s ORDER BY shard_index', (bucket, key, version, position)).fetchall()
    found = {}
    healthy_replicas = 0
    for index, node, piece_hash in rows:
        if node not in live:
            continue
        data = get_piece(client, node, piece_hash, mode if measure_repair else None)
        if data is not None:
            healthy_replicas += 1
            found.setdefault(index, data)
            if (mode == 'replication' and healthy_replicas >= read_quorum) or (mode == 'erasure' and len(found) >= 4):
                break
    if mode == 'replication':
        if healthy_replicas < read_quorum:
            raise HTTPException(503, f'Read quorum unavailable: {healthy_replicas}/{read_quorum}')
        data = next(iter(found.values()))
    else:
        if len(found) < 4:
            raise HTTPException(503, f'Insufficient erasure shards: {len(found)}/4')
        selected = sorted(found)[:4]
        blocks = Decoder(4, 6).decode([found[i] for i in selected], selected)
        data = b''.join(blocks)[:chunk_size]
    if digest(data) != chunk_hash:
        raise HTTPException(409, 'Recovered chunk failed SHA-256')
    return data


def repair_once():
    live = available_nodes()
    if not live:
        return
    with lock, db() as conn, httpx.Client(timeout=25) as client:
        chunks = conn.execute('SELECT c.bucket,c.key,c.version,c.position,c.chunk_hash,c.chunk_size,b.mode,b.replicas,b.read_quorum FROM redundancy_chunks c JOIN redundancy_buckets b ON b.name=c.bucket').fetchall()
        for bucket, key, version, position, chunk_hash, chunk_size, mode, replicas, rq in chunks:
            desired = replicas if mode == 'replication' else 6
            rows = conn.execute('SELECT shard_index,node_id,shard_hash FROM redundancy_shards WHERE bucket=%s AND key=%s AND version=%s AND position=%s', (bucket,key,version,position)).fetchall()
            good = {}
            used = set()
            for index,node,piece_hash in rows:
                piece = get_piece(client,node,piece_hash,mode,'scrub_bytes_read') if node in live else None
                record_metric(mode, 'scrub_checks')
                if piece is not None:
                    good[index] = (node,piece_hash)
                    used.add(node)
                else:
                    if node in live:
                        record_metric(mode, 'scrub_corrupt')
            if len(good) == desired or len(used) == len(live):
                continue
            try:
                data = recover_chunk(conn,client,bucket,key,version,position,mode,replicas,1 if mode=='replication' else rq,live,measure_repair=True)
            except HTTPException:
                continue
            shards = encode_shards(data,mode,replicas)
            missing = [i for i in range(desired) if i not in good]
            for index in missing:
                options = [n for n in ranked_nodes(chunk_hash,live) if n not in used]
                if not options:
                    continue
                payload = shards[index]
                node = options[0]
                result = put_piece(client,node,payload)
                if result is None:
                    continue
                conn.execute('DELETE FROM redundancy_shards WHERE bucket=%s AND key=%s AND version=%s AND position=%s AND shard_index=%s', (bucket,key,version,position,index))
                conn.execute('INSERT INTO redundancy_shards VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', (bucket,key,version,position,index,node,result,len(payload)))
                used.add(node)
                record_metric(mode, 'repair_bytes_written', len(payload))
                record_metric(mode, 'repair_operations')


def background_loop(stop):
    while not stop.wait(SCRUB_SECONDS):
        try:
            repair_once()
        except Exception as exc:
            print(f'Repair/scrub error: {exc}', flush=True)


@asynccontextmanager
async def lifespan(app):
    initialize()
    stop = threading.Event()
    worker = threading.Thread(target=background_loop, args=(stop,), daemon=True)
    worker.start()
    yield
    stop.set()
    worker.join(timeout=2)


app = FastAPI(title='Redundancy and Recovery Service', lifespan=lifespan)


@app.put('/buckets/{bucket}')
def configure_bucket(bucket: str, config: BucketConfig):
    if config.mode not in ('replication','erasure'):
        raise HTTPException(400,'mode must be replication or erasure')
    if config.mode == 'replication' and (config.write_quorum > config.replicas or config.read_quorum > config.replicas or config.write_quorum + config.read_quorum <= config.replicas):
        raise HTTPException(400,'Require W <= N, R <= N and W + R > N')
    with lock, db() as conn:
        exists = conn.execute('SELECT 1 FROM redundancy_objects WHERE bucket=%s LIMIT 1',(bucket,)).fetchone()
        if exists:
            raise HTTPException(409,'Cannot change configuration of nonempty bucket')
        conn.execute('INSERT INTO redundancy_buckets VALUES (%s,%s,%s,%s,%s) ON CONFLICT(name) DO UPDATE SET mode=EXCLUDED.mode,replicas=EXCLUDED.replicas,write_quorum=EXCLUDED.write_quorum,read_quorum=EXCLUDED.read_quorum', (bucket,config.mode,config.replicas,config.write_quorum,config.read_quorum))
    return {'bucket':bucket, **config.model_dump(), 'erasure_scheme':'4+2' if config.mode=='erasure' else None}


@app.get('/buckets')
def list_buckets():
    with db() as conn:
        rows = conn.execute('SELECT name,mode,replicas,write_quorum,read_quorum FROM redundancy_buckets ORDER BY name').fetchall()
    return {'buckets':[dict(zip(('name','mode','replicas','write_quorum','read_quorum'),row)) for row in rows]}


@app.put('/objects/{bucket}/{key:path}')
def upload(bucket: str, key: str, file: UploadFile = File(...)):
    with lock, db() as conn, httpx.Client(timeout=90) as client:
        config = bucket_config(conn,bucket)
        mode,replicas,wq = config['mode'],config['replicas'],config['write_quorum']
        live = available_nodes()
        minimum = wq if mode == 'replication' else 6
        if len(live) < minimum:
            raise HTTPException(503,f'Need {minimum} healthy nodes; found {len(live)}')
        version = conn.execute('SELECT COALESCE(MAX(version),0)+1 FROM redundancy_objects WHERE bucket=%s AND key=%s',(bucket,key)).fetchone()[0]
        conn.execute('INSERT INTO redundancy_objects(bucket,key,version,size) VALUES (%s,%s,%s,0)',(bucket,key,version))
        total = 0
        position = 0
        physical = 0
        try:
            while True:
                chunk = file.file.read(CHUNK_SIZE)
                if not chunk:
                    break
                chunk_hash = digest(chunk)
                pieces = encode_shards(chunk,mode,replicas)
                targets = ranked_nodes(chunk_hash,live)[:len(pieces)]
                successful = []
                for index,(node,payload) in enumerate(zip(targets,pieces)):
                    piece_hash = put_piece(client,node,payload)
                    if piece_hash:
                        successful.append((index,node,piece_hash,len(payload)))
                minimum = wq if mode == 'replication' else 6
                if len(successful) < minimum:
                    raise HTTPException(503,f'Write quorum unmet: {len(successful)}/{minimum}')
                conn.execute('INSERT INTO redundancy_chunks VALUES (%s,%s,%s,%s,%s,%s)',(bucket,key,version,position,chunk_hash,len(chunk)))
                for index,node,piece_hash,size in successful:
                    conn.execute('INSERT INTO redundancy_shards VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',(bucket,key,version,position,index,node,piece_hash,size))
                    physical += size
                total += len(chunk)
                position += 1
            conn.execute('UPDATE redundancy_objects SET size=%s WHERE bucket=%s AND key=%s AND version=%s',(total,bucket,key,version))
        finally:
            file.file.close()
    return {'bucket':bucket,'key':key,'version':version,'mode':mode,'size':total,'chunks':position,'physical_bytes_written':physical,'write_quorum':wq if mode=='replication' else '6/6'}


@app.get('/objects/{bucket}/{key:path}')
def download(bucket: str, key: str, version: int | None = None):
    with db() as conn:
        config = bucket_config(conn,bucket)
        row = conn.execute('SELECT version,size FROM redundancy_objects WHERE bucket=%s AND key=%s AND (%s::integer IS NULL OR version=%s) ORDER BY version DESC LIMIT 1',(bucket,key,version,version)).fetchone()
        if row is None:
            raise HTTPException(404,'Object not found')
        selected_version,size = row
        positions = [r[0] for r in conn.execute('SELECT position FROM redundancy_chunks WHERE bucket=%s AND key=%s AND version=%s ORDER BY position',(bucket,key,selected_version)).fetchall()]
    def stream():
        with db() as conn, httpx.Client(timeout=90) as client:
            for position in positions:
                yield recover_chunk(conn,client,bucket,key,selected_version,position,config['mode'],config['replicas'],config['read_quorum'],available_nodes())
    return StreamingResponse(stream(),media_type='application/octet-stream',headers={'X-Object-Version':str(selected_version),'Content-Length':str(size)})


@app.get('/objects')
def list_objects():
    with db() as conn:
        rows = conn.execute('SELECT bucket,key,version,size,created_at FROM redundancy_objects ORDER BY bucket,key,version').fetchall()
    return {'objects':[{'bucket':b,'key':k,'version':v,'size':s,'created_at':t.isoformat()} for b,k,v,s,t in rows]}


@app.get('/stats')
def stats():
    with db() as conn:
        rows = conn.execute('SELECT b.mode,COALESCE(SUM(c.chunk_size),0) AS logical,COALESCE(SUM((SELECT SUM(s.shard_size) FROM redundancy_shards s WHERE s.bucket=c.bucket AND s.key=c.key AND s.version=c.version AND s.position=c.position)),0) AS physical FROM redundancy_chunks c JOIN redundancy_buckets b ON b.name=c.bucket GROUP BY b.mode').fetchall()
    return {'modes':[{'mode':mode,'logical_bytes':int(logical),'recorded_shard_bytes':int(physical),'overhead_ratio':round(float(physical)/float(logical),3) if logical else 0} for mode,logical,physical in rows], 'traffic_counters':dict(metrics),'traffic_by_mode':{mode:dict(values) for mode,values in mode_metrics.items()},'healthy_nodes':sorted(available_nodes())}


@app.post('/repair')
def repair():
    repair_once()
    return {'status':'repair_and_scrub_completed','traffic_counters':dict(metrics),'traffic_by_mode':{mode:dict(values) for mode,values in mode_metrics.items()}}


@app.get('/health')
def health():
    return {'status':'healthy','healthy_nodes':len(available_nodes())}
