
import bisect
import hashlib


def hash_value(value):
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    def __init__(self, nodes=None, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.positions = []
        self.owners = []
        self.nodes = set()

        for node in nodes or []:
            self.add_node(node)

    def add_node(self, node):
        if node in self.nodes:
            return

        self.nodes.add(node)

        for index in range(self.virtual_nodes):
            position = hash_value(f"{node}:{index}")
            location = bisect.bisect_left(self.positions, position)

            self.positions.insert(location, position)
            self.owners.insert(location, node)

    def remove_node(self, node):
        if node not in self.nodes:
            return

        self.nodes.remove(node)

        kept = [
            (position, owner)
            for position, owner in zip(self.positions, self.owners)
            if owner != node
        ]

        self.positions = [position for position, owner in kept]
        self.owners = [owner for position, owner in kept]

    def get_node(self, chunk_hash):
        if not self.positions:
            raise ValueError("No storage nodes available")

        position = int(chunk_hash, 16)
        index = bisect.bisect_left(self.positions, position)

        if index == len(self.positions):
            index = 0

        return self.owners[index]

    def get_nodes(self):
        return sorted(self.nodes)


STORAGE_NODES = {
    "node1": "http://node1:8000",
    "node2": "http://node2:8000",
    "node3": "http://node3:8000",
    "node4": "http://node4:8000",
}

ring = ConsistentHashRing(STORAGE_NODES.keys())
