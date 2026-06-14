# ADR 0015: Self-Hosted LLM via Ollama (on CPU)

**Status:** Accepted
**Date:** 2026-06-14
**Layer:** Intelligence Agent

## Context

The intelligence agent needs to generate natural-language answers from retrieved context (RAG).
The alternatives were:

1. **External LLM API** (OpenAI GPT-4o, Anthropic Claude, Google Gemini)
2. **Self-hosted LLM** via Ollama running a quantised open-weights model on CPU
3. **Template-based answers** without LLM generation

Key constraints:
- **Data confidentiality** — the knowledge base contains internal engineering docs, incident
  tickets, architecture decisions. Sending this context to an external API is a data-egress risk.
- **Cost** — the system is meant to run continuously on existing infrastructure without
  per-query billing.
- **Hardware** — target deployment is a standard developer laptop or on-prem server with no GPU.
- **Quality** — answers need to be coherent and contextually accurate for engineering Q&A,
  not state-of-the-art creative writing.

## Decision

Use **Ollama** to serve a quantised open-weights LLM on CPU inside Docker.

- Default model: `llama3.2:3b` (3 billion parameters, 4-bit quantised, ~2 GB RAM)
- Runtime: Ollama in a dedicated container (`deploy/ollama/`)
- Access: HTTP at `http://ollama:11434/api/generate` from the agent container
- First run: model is pulled automatically by `deploy/ollama/start.sh`
- Configurable: change `OLLAMA_MODEL` in `.env` to swap models without code changes

## Consequences

**Positive**
- **Zero data egress** — all inference happens on-prem inside Docker network.
- **Zero per-query cost** — only electricity and RAM.
- **No API key management** — one fewer credential to rotate or audit.
- **Works offline** — once model is pulled, no internet access required.
- **Compliance-friendly** — data never leaves the machine.

**Negative**
- **Latency** — ~20–40s per query on CPU (vs ~1s for GPT-4o). Acceptable for async chat
  interface; not suitable for real-time embedded UIs.
- **Quality ceiling** — 3b parameters underperforms larger models on complex multi-step
  reasoning. For straightforward Q&A over retrieved context it is sufficient.
- **Startup time** — first boot pulls the model; subsequent boots use Docker volume cache.
- **No reliable function calling** — live tools are invoked by the agent framework, not
  delegated to the LLM, because small models are inconsistent at tool-use.

**Neutral**
- Swapping to a larger model requires only changing `OLLAMA_MODEL` in `.env`. No code changes.
- Adding a GPU replaces CPU inference transparently — Ollama detects CUDA/Metal automatically.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| OpenAI GPT-4o | Data egress; per-query cost; requires internet access |
| Anthropic Claude API | Same concerns as GPT-4o |
| Larger local model (8b+) | Higher RAM requirement; quality gap small for Q&A over retrieved context |
| Template answers (no LLM) | Cannot synthesise across multiple retrieved chunks; brittle |
