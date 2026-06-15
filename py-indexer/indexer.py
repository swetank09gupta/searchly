"""
Searchly Python Indexer
-----------------------
Kafka consumer that reads IndexingEvent messages and writes to OpenSearch:
  - Full document  → documents-{tenantId} / documents-shared  (BM25 keyword search)
  - Text chunks    → chunks-{tenantId}   / chunks-shared      (kNN vector search)

Embedding failures are non-fatal: the doc stays keyword-searchable.
confluent-kafka processes one message at a time — no 500-record pre-fetch batch,
no ZGC allocation stall, no JVM Keep-Alive-Timer OOM.
"""

import json
import logging
import os
import signal
import sys
import time

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("indexer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP",       "kafka:9092")
OS_HOST         = os.environ.get("OPENSEARCH_HOST",        "opensearch")
OS_PORT         = int(os.environ.get("OPENSEARCH_PORT",    "9200"))
OS_BASE         = f"http://{OS_HOST}:{OS_PORT}"
EMBED_URL       = os.environ.get("EMBEDDING_SERVICE_URL",  "http://embedding-service:8083") + "/embed"

CHUNK_CHARS     = 1500
OVERLAP_CHARS   = 200
EMBED_BATCH     = 50
MAX_EMBED_CHARS = 750_000
VECTOR_DIM      = 384


# ── Chunking ──────────────────────────────────────────────────────────────────

def _last_sentence_boundary(text, from_pos, to_pos):
    for i in range(to_pos - 1, from_pos - 1, -1):
        if text[i] in ".?!":
            return i + 1
    return -1


def chunk_text(text):
    if not text or not text.strip():
        return []
    t = text.strip()
    chunks, start = [], 0
    while start < len(t):
        end = min(start + CHUNK_CHARS, len(t))
        if end < len(t):
            boundary = _last_sentence_boundary(t, start + CHUNK_CHARS // 2, end)
            if boundary > 0:
                end = boundary
        piece = t[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= len(t):
            break
        start = end - OVERLAP_CHARS
        if start <= 0:
            break
    return chunks


# ── Embedding ─────────────────────────────────────────────────────────────────

def embed(texts):
    """Return one float-vector per text, or [] on any error."""
    all_vecs = []
    for i in range(0, len(texts), EMBED_BATCH):
        batch = texts[i: i + EMBED_BATCH]
        try:
            resp = requests.post(EMBED_URL, json={"texts": batch}, timeout=30)
            resp.raise_for_status()
            all_vecs.extend(resp.json()["vectors"])
        except Exception as exc:
            log.warning("Embedding failed (doc will be keyword-only): %s", exc)
            return []
    return all_vecs


# ── OpenSearch ────────────────────────────────────────────────────────────────

_ensured_doc_indices   = set()
_ensured_chunk_indices = set()

_DOC_SETTINGS = {
    "settings": {"index": {"number_of_shards": "3", "number_of_replicas": "0"}}
}

_CHUNK_MAPPING = {
    "settings": {
        "index": {"knn": True, "number_of_shards": "3", "number_of_replicas": "0"}
    },
    "mappings": {
        "properties": {
            "embedding": {
                "type": "knn_vector",
                "dimension": VECTOR_DIM,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    "parameters": {"m": 16, "ef_construction": 128},
                },
            },
            "chunk_text":  {"type": "text"},
            "tenant_id":   {"type": "keyword"},
            "doc_id":      {"type": "keyword"},
            "title":       {"type": "text"},
            "chunk_index": {"type": "integer"},
            "created_at":  {"type": "date"},
        }
    },
}


def _index_exists(index):
    try:
        return requests.head(f"{OS_BASE}/{index}", timeout=5).status_code == 200
    except Exception:
        return False


def _ensure_doc_index(index):
    if index in _ensured_doc_indices:
        return
    if not _index_exists(index):
        r = requests.put(f"{OS_BASE}/{index}", json=_DOC_SETTINGS, timeout=10)
        if r.status_code not in (200, 201):
            log.warning("Create doc index %s -> %s: %s", index, r.status_code, r.text[:200])
        else:
            log.info("Created document index: %s", index)
    _ensured_doc_indices.add(index)


def _ensure_chunk_index(index):
    if index in _ensured_chunk_indices:
        return
    if not _index_exists(index):
        r = requests.put(f"{OS_BASE}/{index}", json=_CHUNK_MAPPING, timeout=10)
        if r.status_code not in (200, 201):
            log.warning("Create chunk index %s -> %s: %s", index, r.status_code, r.text[:200])
        else:
            log.info("Created k-NN chunk index: %s", index)
    _ensured_chunk_indices.add(index)


def os_put(url, doc):
    r = requests.put(url, json=doc, timeout=10)
    if r.status_code >= 300:
        log.warning("PUT %s -> %s: %s", url, r.status_code, r.text[:200])


# ── Event processing ──────────────────────────────────────────────────────────

def _doc_index(tier, tenant_id):
    return f"documents-{tenant_id}" if tier == "ENTERPRISE" else "documents-shared"


def _chunk_index(tier, tenant_id):
    return f"chunks-{tenant_id}" if tier == "ENTERPRISE" else "chunks-shared"


def process(event):
    doc_id     = event.get("docId") or ""
    tenant_id  = event.get("tenantId") or ""
    tier       = event.get("tier") or "SHARED"
    title      = event.get("title") or ""
    content    = event.get("content") or ""
    metadata   = event.get("metadata") or {}
    created_at = event.get("createdAt")  # int epoch-ms or None; never coerce to str

    doc_idx   = _doc_index(tier, tenant_id)
    chunk_idx = _chunk_index(tier, tenant_id)

    # 1. Full-document index (keyword / BM25 search)
    _ensure_doc_index(doc_idx)
    os_put(
        f"{OS_BASE}/{doc_idx}/_doc/{doc_id}?routing={tenant_id}",
        {"tenant_id": tenant_id, "title": title, "content": content,
         "metadata": metadata, "created_at": created_at},
    )
    log.info("Indexed doc %s in %s", doc_id, doc_idx)

    # 2. Chunk -> embed -> k-NN index
    if len(content) > MAX_EMBED_CHARS:
        log.warning("Doc %s content truncated %d -> %d chars", doc_id, len(content), MAX_EMBED_CHARS)
        content = content[:MAX_EMBED_CHARS]

    full_text = (title + "\n\n" + content) if title else content
    chunks = chunk_text(full_text)
    if not chunks:
        return

    log.info("Embedding %d chunks for doc %s", len(chunks), doc_id)
    vectors = embed(chunks)
    if not vectors:
        log.warning("No vectors for %s — skipping chunk index", doc_id)
        return

    _ensure_chunk_index(chunk_idx)
    for i, (chunk_str, vec) in enumerate(zip(chunks, vectors)):
        os_put(
            f"{OS_BASE}/{chunk_idx}/_doc/{doc_id}-chunk-{i}?routing={tenant_id}",
            {"doc_id": doc_id, "chunk_index": i, "tenant_id": tenant_id,
             "title": title, "chunk_text": chunk_str, "embedding": vec,
             "metadata": metadata, "created_at": created_at},
        )

    log.info("Indexed %d chunks for doc %s in %s", len(chunks), doc_id, chunk_idx)


# ── Kafka consumer ────────────────────────────────────────────────────────────

def _wait_for_opensearch(retries=30, delay=5.0):
    for i in range(retries):
        try:
            r = requests.get(f"{OS_BASE}/_cluster/health", timeout=5)
            if r.status_code == 200:
                log.info("OpenSearch ready (status=%s)", r.json().get("status"))
                return
        except Exception:
            pass
        log.info("Waiting for OpenSearch... (%d/%d)", i + 1, retries)
        time.sleep(delay)
    log.warning("OpenSearch not ready after %d tries — continuing anyway", retries)


_shutdown = False


def _handle_term(sig, _frame):
    global _shutdown
    log.info("Received signal %s — shutting down gracefully", sig)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_term)


def main():
    log.info("Searchly Python indexer starting")
    log.info("  kafka=%s  opensearch=%s  embed=%s", KAFKA_BOOTSTRAP, OS_BASE, EMBED_URL)

    _wait_for_opensearch()

    # kafka-python is pure-Python; messages are Python bytes objects freed immediately
    # by reference counting, avoiding the librdkafka C-heap backlog OOM issue.
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id="indexer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        max_poll_interval_ms=300_000,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=3_000,
        max_partition_fetch_bytes=1_048_576,   # 1 MB per partition per fetch
        fetch_max_bytes=10_485_760,             # 10 MB total per fetch response
        consumer_timeout_ms=-1,                # block until message or signal
        metadata_max_age_ms=10_000,            # refresh topic list every 10s
    )
    consumer.subscribe(
        pattern=r"^indexing\.(shared|enterprise\..+)$",
    )
    log.info("Subscribed to indexing.shared + indexing.enterprise.*")

    try:
        while not _shutdown:
            # poll() returns a dict of {TopicPartition: [ConsumerRecord]}
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        event = json.loads(msg.value.decode("utf-8"))
                        process(event)
                    except Exception as exc:
                        log.error(
                            "Failed processing %s[%d]@%d: %s",
                            tp.topic, tp.partition, msg.offset, exc,
                            exc_info=True,
                        )
    except KeyboardInterrupt:
        log.info("Interrupted — shutting down")
    except Exception as exc:
        log.error("Unhandled consumer loop exception: %s", exc, exc_info=True)
    finally:
        log.info("Consumer closing...")
        try:
            consumer.close()
        except Exception as exc:
            log.error("Error during consumer.close(): %s", exc)
        log.info("Consumer closed")


if __name__ == "__main__":
    main()
