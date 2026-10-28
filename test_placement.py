
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from placement import ConsistentHashRing

RESULTS_DIR = Path("benchmark_results")
RESULTS_DIR.mkdir(exist_ok=True)

TOTAL_CHUNKS = 5000
VIRTUAL_NODES = 100

original_nodes = ["node1", "node2", "node3", "node4"]

chunk_hashes = [
    hashlib.sha256(f"test-chunk-{i}".encode()).hexdigest()
    for i in range(TOTAL_CHUNKS)
]


def get_placements(nodes):
    ring = ConsistentHashRing(nodes, VIRTUAL_NODES)

    return {
        chunk_hash: ring.get_node(chunk_hash)
        for chunk_hash in chunk_hashes
    }


def compare(name, previous, current):
    moved = sum(
        1
        for chunk_hash in chunk_hashes
        if previous[chunk_hash] != current[chunk_hash]
    )

    percentage = moved / TOTAL_CHUNKS * 100

    return {
        "test": name,
        "total_chunks": TOTAL_CHUNKS,
        "moved_chunks": moved,
        "unchanged_chunks": TOTAL_CHUNKS - moved,
        "movement_percentage": round(percentage, 2)
    }


original = get_placements(original_nodes)

added = get_placements(
    original_nodes + ["node5"]
)

removed = get_placements(
    ["node1", "node3", "node4"]
)

results = [
    compare("Add node5", original, added),
    compare("Remove node2", original, removed)
]

report = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "virtual_nodes": VIRTUAL_NODES,
    "original_nodes": original_nodes,
    "results": results
}

with open(
    RESULTS_DIR / "placement_results.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(report, file, indent=2)

with open(
    RESULTS_DIR / "placement_results.csv",
    "w",
    newline="",
    encoding="utf-8"
) as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "test",
            "total_chunks",
            "moved_chunks",
            "unchanged_chunks",
            "movement_percentage"
        ]
    )

    writer.writeheader()
    writer.writerows(results)

print("Consistent Hashing Placement Test")
print(f"Total chunks: {TOTAL_CHUNKS}")
print(f"Virtual nodes per storage node: {VIRTUAL_NODES}")

for result in results:
    print()
    print(result["test"])
    print(f"Moved chunks: {result['moved_chunks']}")
    print(f"Unchanged chunks: {result['unchanged_chunks']}")
    print(f"Movement percentage: {result['movement_percentage']}%")

print()
print("Results saved in benchmark_results/")
