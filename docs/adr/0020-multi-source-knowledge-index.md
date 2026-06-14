# ADR 0020: Multi-Source Knowledge Indexing (Jira + Confluence + GitHub)

**Status:** Accepted
**Date:** 2026-06-14
**Layer:** Intelligence Agent (connectors/sync.py)

## Context

Institutional engineering knowledge is distributed across three sources:

| Source | Content | Access |
|---|---|---|
| **Jira** | Bug reports, feature specs, incident tickets, acceptance criteria, comments | Atlassian REST API |
| **Confluence** | Architecture docs, LLDs, HLDs, ADRs, runbooks, onboarding guides, design decisions | Atlassian REST API |
| **GitHub** | Source code, inline documentation, config files, migration history | HTTPS clone |

A fourth source — **Elasticsearch logs** — is handled separately at query time via Mode A
bastion-kubectl (see ADR 0017), not pre-indexed, because log data is ephemeral and real-time.

No single source answers all engineering questions. Typical queries need:
- The Jira ticket that introduced a feature (context + acceptance criteria)
- The Confluence LLD describing the design (architecture + rationale)
- The source file that implements it (exact behaviour)

## Decision

Index all three sources into a single OpenSearch `chunks` index with rich metadata tagging,
using source-specific fetchers that share a common rate-limited HTTP client.

**Configurable scope** (set in `connectors/.env`):
- `JIRA_PROJECTS` — comma-separated project keys; blank = all projects
- `CONFLUENCE_SPACES` — comma-separated space keys; blank = all spaces
- `github_org` + `repos` in `products.yml` — which repos to clone

**Common metadata fields** on every chunk:

| Field | Example values |
|---|---|
| `source` | `jira`, `confluence`, `github`, `customer_state` |
| `product` | product name from `products.yml` |
| `doc_type` | `adr`, `architecture`, `ticket`, `code`, `runbook` |
| `customer` | (optional) environment ID if content is customer-specific |
| `space` | Confluence space key or Jira project key |
| `repo` | GitHub repo name |
| `url` | Deep link back to original source |
| `updated_at` | ISO timestamp of last modification |

**Confluence recursive fetching:** Child pages fetched recursively to depth 8 via
`GET /wiki/rest/api/content/{id}/child/page`. Ensures nested architecture docs are fully indexed.

**Jira issue content:** Title + description + all comments concatenated and chunked together
to preserve context. Resolution and status included as metadata.

**Rate limiting:** All fetchers share `_api_get()` — 150ms inter-request pacing, `Retry-After`
honoured on 429, exponential backoff on 5xx. Stays within standard Atlassian API limits.

## Consequences

**Positive**
- **Single query interface** — one OpenSearch index; no fan-out across sources at query time.
- **Cross-source answers** — a question about a feature retrieves chunks from the Confluence
  LLD, the Jira ticket that introduced it, and the Python/Java source. The LLM synthesises.
- **Deep link citations** — every chunk has a `url` field so the LLM can cite the source.
- **Recursive Confluence** — design docs are typically nested; without depth recursion, most
  content would be missed.
- **Easy to extend** — add Jira projects or Confluence spaces by editing `.env`. Add repos by
  editing `products.yml`. No code changes.

**Negative**
- **Initial sync time** — first index of a large org (many projects + spaces + repos) takes
  20–60 minutes. Subsequent incremental syncs are much faster.
- **Eventual consistency** — Jira comments added between sync cycles are not reflected until
  the next run. Not a problem for architecture queries; may matter for incident tracking.
- **GitHub PAT required** — private repos need a PAT. Without one, only public repos are indexed.
- **Atlassian rate limits** — at 150ms pacing (~6 req/s), stays within Atlassian's documented
  10 req/s limit. Increase pacing delay if limits tighten.

**Neutral**
- `skip_repos` in `products.yml` excludes forks, marketing sites, and binary-heavy repos.
- Future sources (Slack export, Google Drive, Notion, PagerDuty) can be added as new fetcher
  classes following the same pattern.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| Jira only | Misses design decisions in Confluence and exact implementation in code |
| Confluence only | Misses live ticket context and code-level details |
| GitHub only | Misses issue history and documented design rationale |
| Separate indices per source | Complicates cross-source queries; multi-index RRF fusion is harder |
| Real-time indexing via webhooks | Requires public-facing endpoints; 4h lag is acceptable for knowledge Q&A |
