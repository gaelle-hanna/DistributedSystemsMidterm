
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_node.py .
COPY metadata_service.py .
COPY chunking.py .
COPY placement.py .

EXPOSE 8000

CMD ["uvicorn", "storage_node:app", "--host", "0.0.0.0", "--port", "8000"]
