import argparse
import csv
import json
import random
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx

URL = 'http://localhost:8200'
RESULTS = Path(__file__).resolve().parent / 'benchmark_results'


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def checked_json(response):
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise httpx.HTTPStatusError(
            f'{exc}; API response: {response.text}',
            request=exc.request,
            response=exc.response,
        ) from exc
    return response.json()


def make_file(path, size, seed):
    rng = random.Random(seed)
    with path.open('wb') as file:
        while size:
            count = min(size, 1024 * 1024)
            file.write(rng.randbytes(count))
            size -= count


def run_workload(mode, bucket, name, count, minimum, maximum, client):
    start = time.perf_counter()
    logical = 0
    actual = 0
    with tempfile.TemporaryDirectory(prefix='redundancy-benchmark-') as directory:
        path = Path(directory) / 'sample.bin'
        for i in range(count):
            size = random.Random(1000 + i).randint(minimum, maximum)
            make_file(path, size, i)
            with path.open('rb') as source:
                result = checked_json(client.put(
                    f'/objects/{bucket}/{name}-{i}',
                    files={'file': (f'{name}-{i}.bin', source, 'application/octet-stream')},
                ))
            if result['size'] != size or result['mode'] != mode:
                raise RuntimeError(f'Unexpected upload response: {result}')
            logical += size
            actual += result['physical_bytes_written']
            if (i + 1) % 100 == 0 or i + 1 == count:
                print(f'{mode} {name}: {i + 1}/{count}', flush=True)
    return {
        'mode': mode,
        'bucket': bucket,
        'workload': name,
        'files': count,
        'logical_bytes': logical,
        'written_shard_bytes': actual,
        'storage_overhead': round(actual / logical, 3) if logical else 0,
        'upload_seconds': round(time.perf_counter() - start, 2),
    }


def save_results(report, csv_path, json_path):
    json_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    with csv_path.open('w', newline='', encoding='utf-8') as file:
        fields = list(report['workloads'][0]) if report['workloads'] else [
            'mode', 'bucket', 'workload', 'files', 'logical_bytes',
            'written_shard_bytes', 'storage_overhead', 'upload_seconds',
        ]
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report['workloads'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--small-count', type=positive_int, default=5000)
    parser.add_argument('--large-count', type=positive_int, default=10)
    parser.add_argument('--large-mb', type=positive_int, default=100)
    parser.add_argument('--url', default=URL)
    parser.add_argument('--results-dir', type=Path, default=RESULTS)
    args = parser.parse_args()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex
    buckets = {mode: f'{mode}-benchmark-{run_id}' for mode in ('replication', 'erasure')}
    settings = {
        'replication': {'mode': 'replication', 'replicas': 3, 'write_quorum': 2, 'read_quorum': 2},
        'erasure': {'mode': 'erasure'},
    }
    results = args.results_dir.resolve()
    results.mkdir(parents=True, exist_ok=True)
    csv_path = results / f'redundancy_results_{run_id}.csv'
    json_path = results / f'redundancy_results_{run_id}.json'
    report = {
        'run_id': run_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'script': str(Path(__file__).resolve()),
        'api_url': args.url,
        'parameters': {'small_count': args.small_count, 'large_count': args.large_count, 'large_mb': args.large_mb},
        'buckets': buckets,
        'status': 'running',
        'workloads': [],
        'stats_scope': 'Entire service, including pre-existing data',
    }
    print(f'Script: {report["script"]}\nPython: {sys.executable}\nAPI: {args.url}', flush=True)
    save_results(report, csv_path, json_path)
    try:
        with httpx.Client(base_url=args.url.rstrip('/'), timeout=600) as client:
            health = checked_json(client.get('/health'))
            if health['healthy_nodes'] < 6:
                raise RuntimeError('Both workloads require six healthy nodes for complete 3-copy and 4+2 storage')
            for mode, bucket in buckets.items():
                checked_json(client.put(f'/buckets/{bucket}', json=settings[mode]))
                print(f'Created bucket: {bucket}', flush=True)
            for mode, bucket in buckets.items():
                for name, count, minimum, maximum in (
                    ('small', args.small_count, 1024, 50 * 1024),
                    ('large', args.large_count, args.large_mb * 1024 * 1024, args.large_mb * 1024 * 1024),
                ):
                    report['workloads'].append(run_workload(mode, bucket, name, count, minimum, maximum, client))
                    save_results(report, csv_path, json_path)
            report['stats'] = checked_json(client.get('/stats'))
        report['status'] = 'completed'
    except Exception as exc:
        report['status'] = 'failed'
        report['error'] = str(exc)
        raise
    finally:
        save_results(report, csv_path, json_path)
        print(f'CSV results: {csv_path}\nJSON results: {json_path}', flush=True)


if __name__ == '__main__':
    main()
