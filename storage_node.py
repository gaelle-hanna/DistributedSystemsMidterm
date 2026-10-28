
import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

NODE_ID = os.getenv("NODE_ID", "node1")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "storage_nodes/node1"))
METADATA_URL = os.getenv("METADATA_URL", "http://localhost:8100")
HEARTBEAT_INTERVAL = 5

STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def validate_hash(chunk_hash):
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")


def get_chunk_path(chunk_hash):
    validate_hash(chunk_hash)
    return STORAGE_DIR / chunk_hash


def calculate_hash(path):
    sha256 = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            data = file.read(1024 * 1024)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()


def get_chunk_count():
    return sum(
        1
        for file in STORAGE_DIR.iterdir()
        if file.is_file() and re.fullmatch(r"[0-9a-f]{64}", file.name)
    )


def get_free_space():
    return shutil.disk_usage(STORAGE_DIR).free


async def send_heartbeats():
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                await client.post(
                    f"{METADATA_URL}/heartbeat",
                    json={
                        "node_id": NODE_ID,
                        "free_space": get_free_space(),
                        "chunk_count": get_chunk_count(),
                    },
                )
            except (httpx.RequestError, OSError) as error:
                print(f"Heartbeat failed: {error}")

            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(send_heartbeats())

    yield

    task.cancel()

    with suppress(asyncio.CancelledError):
        await task


app = FastAPI(title=f"Storage Node {NODE_ID}", lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "node_id": NODE_ID,
        "status": "healthy",
        "free_space": get_free_space(),
        "chunk_count": get_chunk_count(),
    }


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    chunk_path = get_chunk_path(chunk_hash)

    descriptor, temporary_path = tempfile.mkstemp(dir=STORAGE_DIR)

    sha256 = hashlib.sha256()
    size = 0

    try:
        with os.fdopen(descriptor, "wb") as file:
            async for data in request.stream():
                if data:
                    file.write(data)
                    sha256.update(data)
                    size += len(data)

        if sha256.hexdigest() != chunk_hash:
            raise HTTPException(
                status_code=400,
                detail="SHA-256 hash mismatch",
            )

        if chunk_path.exists():
            if calculate_hash(chunk_path) == chunk_hash:
                return {
                    "node_id": NODE_ID,
                    "hash": chunk_hash,
                    "size": chunk_path.stat().st_size,
                    "status": "already_exists",
                }

        os.replace(temporary_path, chunk_path)

        return {
            "node_id": NODE_ID,
            "hash": chunk_hash,
            "size": size,
            "status": "stored",
        }

    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    chunk_path = get_chunk_path(chunk_hash)

    if not chunk_path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    if calculate_hash(chunk_path) != chunk_hash:
        raise HTTPException(status_code=409, detail="Corrupted chunk")

    return FileResponse(
        chunk_path,
        media_type="application/octet-stream",
        headers={"X-Chunk-Hash": chunk_hash},
    )


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    chunk_path = get_chunk_path(chunk_hash)

    if not chunk_path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    if calculate_hash(chunk_path) != chunk_hash:
        raise HTTPException(status_code=409, detail="Corrupted chunk")

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(chunk_path.stat().st_size),
            "X-Chunk-Hash": chunk_hash,
        },
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    chunk_path = get_chunk_path(chunk_hash)

    if not chunk_path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    chunk_path.unlink()

    return {
        "node_id": NODE_ID,
        "hash": chunk_hash,
        "status": "deleted",
    }
