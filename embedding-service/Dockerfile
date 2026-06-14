FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# CPU-only torch keeps the image ~1.5 GB instead of ~5 GB
RUN pip install --no-cache-dir torch==2.4.1+cpu --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt

# Pre-download model into the image so first startup is instant
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

COPY main.py .

EXPOSE 8083
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD curl -f http://localhost:8083/health || exit 1

CMD ["python", "main.py"]
