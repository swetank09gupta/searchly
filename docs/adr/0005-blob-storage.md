# ADR 0005: S3-Compatible Blob Storage

**Status:** Accepted
**Date:** 2026-06-03

## Context

Documents include PDFs, DOCX, and other binaries up to ~50 MB. Storing raw bytes in OpenSearch bloats the heap and JVM GC; passing them through Kafka inflates broker disk and network. We need a cheap, durable, encryption-capable object store.

## Decision

Store raw documents in an **S3-compatible blob store**: **MinIO** locally (docker-compose) and **AWS S3** (or GCS) in production. The same SDK and API work against both.

## Consequences

**Positive**
- Decouples large binaries from index and queue.
- Server-side encryption (SSE-KMS) and versioning available natively.
- Presigned PUT/GET URLs avoid proxying bytes through the API.
- MinIO gives identical local-dev experience; no behavioral surprises in production.
- Cross-region replication is a configuration change, not a code change.

**Negative**
- Extra dependency to operate locally (MinIO container).
- Eventual consistency for some S3 operations (now strongly consistent for read-after-write since 2020, so largely a non-issue).

**Data flow**
- `POST /documents`: API generates a presigned PUT URL, client uploads directly to MinIO/S3, API persists metadata + blob URI in Postgres + emits Kafka event with `{doc_id, tenant_id, blob_uri, checksum}`.
- Indexer fetches the blob from MinIO/S3, runs Tika, indexes extracted text.
