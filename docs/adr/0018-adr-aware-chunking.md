# ADR 0018: ADR-Aware Document Chunking

**Status:** Accepted
**Date:** 2026-06-14
**Layer:** Intelligence Agent (connectors/sync.py)

## Context

Standard RAG chunking splits documents into fixed-size chunks (e.g. 512 tokens) with overlap.
This works well for long Jira descriptions or Confluence body pages where a user query is likely
to match a sub-section.

Architecture Decision Records (ADRs), Low-Level Design documents (LLDs), and similar design
artefacts have a different structure:

- They are **short** — typically 500–3,000 words, almost always under 12,000 characters.
- They are **structurally interdependent** — sections like Context, Decision, Consequences, and
  Alternatives Considered cannot be understood in isolation. An "Alternatives Considered" section
  without the "Decision" it rejected is meaningless.
- Queries **span the whole document** — "what was the rationale for X?" or "what alternatives
  were considered for Y?" need the full ADR in context, not a fragment.

The same applies to LLDs, HLDs, design docs, RFC-style proposals, and architecture runbooks.

## Decision

Files detected as ADR/architecture documents use a **structure-preserving chunking strategy**
instead of standard fixed-size chunking.

**Detection** (`_doc_type(rel_path)` in `sync.py`):

Path-based patterns (matched on the normalised file path):
- `/adr/`, `/adrs/`, `/decisions/`, `/rfcs/`, `/proposals/`
- `docs/adr/`, `docs/decisions/`, `docs/architecture/`
- `/architecture/` as a path segment

Stem-based patterns (matched on the filename stem):
- Contains `architecture`, `lld`, `hld`, `design-doc`
- Starts with `adr-`, `rfc-`
- Equals `decisions`

**Chunking rules:**
- `doc_type in ("adr", "architecture")` AND size < 12,000 chars → **single chunk** (no split)
- `doc_type in ("adr", "architecture")` AND size ≥ 12,000 chars → **6,000-char chunks**, no overlap
- All other files → **standard chunking** (2,000-char chunks, 200-char overlap)

**Metadata:** All chunks get `doc_type: "adr"` or `doc_type: "architecture"` in OpenSearch,
enabling filtered queries like `GET /search?doc_type=adr`.

## Consequences

**Positive**
- Architecture questions get complete, coherent context — Context, Decision, and Consequences
  always appear together in retrieval.
- For most ADRs (< 12,000 chars), zero overhead — one embed call, one OpenSearch document.
- `doc_type` metadata enables future UI filtering ("show only ADRs").

**Negative**
- Whole-document chunks (up to 12,000 chars ≈ ~3,000 tokens) are large. Not a problem for
  modern LLMs with 32k+ context windows; may consume more of the context budget for 3b models.
- Detection regex may miss non-standard naming (`decision-log.md`, `technical-design.md`).
  These files are chunked normally. The regex is extensible without re-indexing all content.

**Neutral**
- The 12,000-char threshold was chosen as a safe ceiling for typical ADR/LLD files; adjust
  if your org's design docs are consistently longer.
- Confluence architecture pages may exceed 12,000 chars (long LLD pages); the 6,000-char path
  handles them.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| Standard chunking for all files | Splits ADRs at arbitrary positions; loses decision rationale |
| Separate "ADR index" in OpenSearch | Adds operational complexity; cross-index RRF fusion is harder |
| Smaller chunk size for ADRs | Still splits; the goal is to preserve whole-document context |
| Require ADR frontmatter/tags | Requires repo owners to maintain tags; ADRs rarely have frontmatter |
