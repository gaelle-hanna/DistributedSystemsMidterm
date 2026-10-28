
import hashlib

FIXED_SIZE = 4 * 1024 * 1024
CDC_MIN_SIZE = 1 * 1024 * 1024
CDC_AVG_SIZE = 4 * 1024 * 1024
CDC_MAX_SIZE = 8 * 1024 * 1024

GEAR = [
    int.from_bytes(hashlib.sha256(bytes([i])).digest()[:8], "big")
    for i in range(256)
]

MASK64 = (1 << 64) - 1


def fixed_chunks(file):
    while True:
        data = file.read(FIXED_SIZE)
        if not data:
            break
        yield data


def cdc_chunks(file):
    buffer = bytearray()
    rolling_hash = 0
    mask = CDC_AVG_SIZE - 1

    while True:
        block = file.read(64 * 1024)

        if not block:
            break

        for byte in block:
            buffer.append(byte)
            rolling_hash = (
                (rolling_hash << 1) + GEAR[byte]
            ) & MASK64

            length = len(buffer)

            if length < CDC_MIN_SIZE:
                continue

            if (rolling_hash & mask) == 0 or length >= CDC_MAX_SIZE:
                yield bytes(buffer)
                buffer.clear()
                rolling_hash = 0

    if buffer:
        yield bytes(buffer)


def split_file(file, mode="fixed"):
    if mode == "fixed":
        return fixed_chunks(file)

    if mode == "cdc":
        return cdc_chunks(file)

    raise ValueError("Unsupported chunking mode")
