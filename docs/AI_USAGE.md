# AI Tool Usage

This assessment was built with the assistance of **Claude Code** (Anthropic) as the primary AI pair-programmer. Per the assignment guidelines, AI assistance was encouraged.

## How AI was used

- **Requirement decomposition:** I asked Claude to read the assignment PDF and produce a structured checklist of mandatory items, NFRs, and bonuses — used to scope the work and avoid missing requirements.
- **Stack selection dialog:** Iterated with Claude on stack choices (Java vs alternatives, Maven vs Gradle, OpenSearch vs Postgres FTS, Kafka vs RabbitMQ) — each choice was discussed with explicit trade-offs and committed to in `DECISIONS.md`.
- **Architecture brainstorming:** Used Claude to pressure-test the design across cross-cutting concerns I explicitly raised: API gateway, RBAC, tenant tiering / noisy-neighbor, distributed tracing, security at all layers, K8s vs Docker, blob storage. Each concern produced a section in the architecture and production-readiness docs.
- **Documentation drafting:** Architecture, production-readiness, decisions, and experience-showcase templates were drafted by Claude under my direction and reviewed/edited by me.
- **Code scaffolding:** Maven multi-module skeleton, Spring Boot config, docker-compose, sample DTOs, and security filters were generated and reviewed.

## What I did, not the AI

- **All architectural decisions** (hybrid tenant isolation, Kafka topic tiering, RBAC model, rolling-window rate limit) — Claude offered options; I picked.
- **Trade-off framing and prioritization** — what to build vs what to document for production-readiness.
- **Experience showcase** — drawn from my own work; AI helped shape phrasing.
- **Verification** of any factual claims (licensing, library versions, OpenSearch behaviour).

## Why this matters

The assignment is partly about *how I work*, not just *what I produce*. Using AI to accelerate scaffolding and prose, while keeping decisions and verification in human hands, is the workflow I use day-to-day on real systems. Hallucinated APIs, deprecated patterns, and shallow trade-off analysis are the failure modes — I guarded against them by being explicit about constraints and forcing options into a written decisions log.
