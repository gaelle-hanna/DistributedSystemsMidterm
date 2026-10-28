
import hashlib
import os
import threading
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import httpx
import psycopg
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field

from chunking import split_file
from placement import STORAGE_NODES, ring

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://storage:storagepass@localhost:5432/storage_db"
)

HEARTBEAT_TIMEOUT = 15
nodes = {}
node_lock = threading.Lock()
object_lock = threading.Lock()


class Heartbeat(BaseModel):
    node_id: str = Field(min_length=1)
    free_space: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


def database():
    return psycopg.connect(DATABASE_URL)


def initialize_database():
    for attempt in range(30):
        try:
            with database() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS objects (
                        bucket TEXT NOT NULL,
                        key TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        size BIGINT NOT NULL,
                        mode TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (bucket, key, version)
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS chunks (
                        hash TEXT PRIMARY KEY,
                        size BIGINT NOT NULL,
                        node_id TEXT NOT NULL,
                        ref_count BIGINT NOT NULL
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS object_chunks (
                        bucket TEXT NOT NULL,
                        key TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        position INTEGER NOT NULL,
                        chunk_hash TEXT NOT NULL REFERENCES chunks(hash),
                        PRIMARY KEY (bucket, key, version, position),
                        FOREIGN KEY (bucket, key, version)
                        REFERENCES objects(bucket, key, version)
                        ON DELETE CASCADE
                    )
                """)

            return

        except psycopg.OperationalError:
            if attempt == 29:
                raise
            time.sleep(2)


@asynccontextmanager
async def lifespan(app):
    initialize_database()
    yield


app = FastAPI(
    title="Distributed Storage Metadata and Placement Service",
    lifespan=lifespan
)


@app.get("/health")
def health():
    return {"status": "healthy", "database": "postgresql"}


@app.post("/heartbeat")
def receive_heartbeat(heartbeat: Heartbeat):
    with node_lock:
        nodes[heartbeat.node_id] = {
            "node_id": heartbeat.node_id,
            "free_space": heartbeat.free_space,
            "chunk_count": heartbeat.chunk_count,
            "last_seen": datetime.now(timezone.utc)
        }

    return {
        "status": "received",
        "node_id": heartbeat.node_id
    }


@app.get("/nodes")
def list_nodes():
    now = datetime.now(timezone.utc)

    with node_lock:
        result = []

        for node in sorted(nodes.values(), key=lambda n: n["node_id"]):
            age = (now - node["last_seen"]).total_seconds()

            result.append({
                "node_id": node["node_id"],
                "free_space": node["free_space"],
                "chunk_count": node["chunk_count"],
                "last_seen": node["last_seen"].isoformat(),
                "status": "healthy" if age <= HEARTBEAT_TIMEOUT else "offline"
            })

    return {
        "total_nodes": len(result),
        "nodes": result
    }


@app.get("/placement")
def placement(chunk_hash: str):
    if len(chunk_hash) != 64:
        raise HTTPException(status_code=400, detail="Invalid chunk hash")

    try:
        int(chunk_hash, 16)
        node_id = ring.get_node(chunk_hash.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid chunk hash")

    return {
        "hash": chunk_hash.lower(),
        "node_id": node_id,
        "node_url": STORAGE_NODES[node_id]
    }


@app.get("/objects")
def list_objects(bucket: str | None = None):
    with database() as conn:
        if bucket:
            rows = conn.execute("""
                SELECT bucket, key, version, size, mode, created_at
                FROM objects
                WHERE bucket = %s
                ORDER BY bucket, key, version
            """, (bucket,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT bucket, key, version, size, mode, created_at
                FROM objects
                ORDER BY bucket, key, version
            """).fetchall()

    return {
        "objects": [
            {
                "bucket": row[0],
                "key": row[1],
                "version": row[2],
                "size": row[3],
                "mode": row[4],
                "created_at": row[5].isoformat()
            }
            for row in rows
        ]
    }


def find_object(conn, bucket, key, version):
    if version is None:
        row = conn.execute("""
            SELECT bucket, key, version, size, mode, created_at
            FROM objects
            WHERE bucket = %s AND key = %s
            ORDER BY version DESC
            LIMIT 1
        """, (bucket, key)).fetchone()
    else:
        row = conn.execute("""
            SELECT bucket, key, version, size, mode, created_at
            FROM objects
            WHERE bucket = %s AND key = %s AND version = %s
        """, (bucket, key, version)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Object not found")

    chunk_rows = conn.execute("""
        SELECT oc.chunk_hash, c.node_id, c.size
        FROM object_chunks oc
        JOIN chunks c ON oc.chunk_hash = c.hash
        WHERE oc.bucket = %s
          AND oc.key = %s
          AND oc.version = %s
        ORDER BY oc.position
    """, (bucket, key, row[2])).fetchall()

    return row, chunk_rows


@app.put("/objects/{bucket}/{key:path}")
def put_object(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    mode: Literal["fixed", "cdc"] = Query("fixed")
):
    if not bucket or not key:
        raise HTTPException(status_code=400, detail="Invalid object name")

    chunk_hashes = []
    total_size = 0
    new_chunks = 0
    reused_chunks = 0

    with object_lock:
        with database() as conn:
            with httpx.Client(timeout=120) as client:
                try:
                    for chunk in split_file(file.file, mode):
                        chunk_hash = hashlib.sha256(chunk).hexdigest()
                        chunk_size = len(chunk)

                        existing = conn.execute("""
                            SELECT hash FROM chunks WHERE hash = %s
                        """, (chunk_hash,)).fetchone()

                        if existing is None:
                            node_id = ring.get_node(chunk_hash)
                            url = (
                                f"{STORAGE_NODES[node_id]}"
                                f"/chunks/{chunk_hash}"
                            )

                            response = client.put(
                                url,
                                content=chunk,
                                headers={
                                    "Content-Type": "application/octet-stream"
                                }
                            )
                            response.raise_for_status()

                            conn.execute("""
                                INSERT INTO chunks
                                (hash, size, node_id, ref_count)
                                VALUES (%s, %s, %s, 0)
                            """, (
                                chunk_hash,
                                chunk_size,
                                node_id
                            ))

                            new_chunks += 1
                        else:
                            reused_chunks += 1

                        chunk_hashes.append(chunk_hash)
                        total_size += chunk_size

                    row = conn.execute("""
                        SELECT COALESCE(MAX(version), 0) + 1
                        FROM objects
                        WHERE bucket = %s AND key = %s
                    """, (bucket, key)).fetchone()

                    version = row[0]

                    conn.execute("""
                        INSERT INTO objects
                        (bucket, key, version, size, mode)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        bucket,
                        key,
                        version,
                        total_size,
                        mode
                    ))

                    for position, chunk_hash in enumerate(chunk_hashes):
                        conn.execute("""
                            INSERT INTO object_chunks
                            (bucket, key, version, position, chunk_hash)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            bucket,
                            key,
                            version,
                            position,
                            chunk_hash
                        ))

                    counts = Counter(chunk_hashes)

                    for chunk_hash, count in counts.items():
                        conn.execute("""
                            UPDATE chunks
                            SET ref_count = ref_count + %s
                            WHERE hash = %s
                        """, (count, chunk_hash))

                except httpx.HTTPError as error:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Storage node request failed: {error}"
                    )
                finally:
                    file.file.close()

    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": total_size,
        "mode": mode,
        "chunk_count": len(chunk_hashes),
        "new_chunks": new_chunks,
        "reused_chunks": reused_chunks,
        "chunk_hashes": chunk_hashes
    }


@app.get("/objects/{bucket}/{key:path}")
def get_object(
    bucket: str,
    key: str,
    version: int | None = None,
    metadata: bool = False
):
    with database() as conn:
        row, chunks = find_object(conn, bucket, key, version)

    result = {
        "bucket": row[0],
        "key": row[1],
        "version": row[2],
        "size": row[3],
        "mode": row[4],
        "created_at": row[5].isoformat(),
        "chunk_hashes": [chunk[0] for chunk in chunks],
        "chunk_nodes": [chunk[1] for chunk in chunks]
    }

    if metadata:
        return result

    output = bytearray()

    with httpx.Client(timeout=120) as client:
        for chunk_hash, node_id, size in chunks:
            url = f"{STORAGE_NODES[node_id]}/chunks/{chunk_hash}"

            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.HTTPError as error:
                raise HTTPException(
                    status_code=502,
                    detail=f"Chunk retrieval failed: {error}"
                )

            data = response.content

            if hashlib.sha256(data).hexdigest() != chunk_hash:
                raise HTTPException(
                    status_code=409,
                    detail="Chunk integrity verification failed"
                )

            output.extend(data)

    return Response(
        content=bytes(output),
        media_type="application/octet-stream",
        headers={
            "X-Object-Version": str(row[2]),
            "X-Chunk-Count": str(len(chunks))
        }
    )


@app.delete("/objects/{bucket}/{key:path}")
def delete_object(
    bucket: str,
    key: str,
    version: int | None = None
):
    cleanup = []

    with object_lock:
        with database() as conn:
            row, chunk_rows = find_object(
                conn, bucket, key, version
            )

            counts = Counter(chunk[0] for chunk in chunk_rows)

            conn.execute("""
                DELETE FROM objects
                WHERE bucket = %s AND key = %s AND version = %s
            """, (bucket, key, row[2]))

            for chunk_hash, count in counts.items():
                conn.execute("""
                    UPDATE chunks
                    SET ref_count = ref_count - %s
                    WHERE hash = %s
                """, (count, chunk_hash))

            rows = conn.execute("""
                SELECT hash, node_id
                FROM chunks
                WHERE ref_count <= 0
            """).fetchall()

            cleanup.extend(rows)

            for chunk_hash, node_id in cleanup:
                conn.execute("""
                    DELETE FROM chunks WHERE hash = %s
                """, (chunk_hash,))

    with httpx.Client(timeout=30) as client:
        for chunk_hash, node_id in cleanup:
            try:
                client.delete(
                    f"{STORAGE_NODES[node_id]}/chunks/{chunk_hash}"
                )
            except httpx.HTTPError:
                pass

    return {
        "bucket": bucket,
        "key": key,
        "deleted_version": row[2],
        "unreferenced_chunks": len(cleanup),
        "status": "deleted"
    }


@app.get("/chunks/metadata")
def chunk_metadata():
    with database() as conn:
        rows = conn.execute("""
            SELECT hash, size, node_id, ref_count
            FROM chunks
            ORDER BY hash
        """).fetchall()

    return {
        "total_unique_chunks": len(rows),
        "chunks": [
            {
                "hash": row[0],
                "size": row[1],
                "node_id": row[2],
                "ref_count": row[3]
            }
            for row in rows
        ]
    }
