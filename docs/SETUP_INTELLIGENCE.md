# Intelligence Agent — Setup Guide

> For the generic Searchly search platform setup (Java services, Keycloak, etc.) see [../SETUP.md](../SETUP.md).

---

## Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| Docker | 24.x | Engine + compose v2 |
| Disk | 35 GB free | OpenSearch data + model weights + repo clones |
| RAM | 8 GB | OpenSearch 4 GB + Ollama 2 GB + services 2 GB |
| CPU | 4 cores | Ollama runs on CPU; more cores = faster inference |
| Network | SSH access to bastion (optional) | Required only for live ES log queries |

---

## First-time setup

### 1. Clone the repo

```bash
git clone <this-repo>
cd searchly
```

### 2. Configure credentials

```bash
cp connectors/.env.example connectors/.env
nano connectors/.env
```

Fill in:
- `JIRA_URL`, `JIRA_EMAIL`, `JIRA_TOKEN` — Atlassian API token
  (generate at https://id.atlassian.com/manage-profile/security/api-tokens)
- `CONFLUENCE_TOKEN` — same token as `JIRA_TOKEN`
- `GIT_TOKEN` — GitHub PAT with `repo` scope
  (generate at https://github.com/settings/tokens → Classic → repo)
- `JIRA_PROJECTS` — comma-separated project keys to index, e.g. `ENG,OPS,INFRA`
  (leave blank to index all projects)
- `CONFLUENCE_SPACES` — comma-separated space keys, e.g. `ARCH,DEV,OPS`
  (leave blank to index all spaces)

### 3. Configure what to index

```bash
nano connectors/products.yml
```

- Set `github_org: your-org` (your GitHub organisation or user)
- Under `products:`, add your services with their repo names and pod prefixes
- Add repo names to skip under `skip_repos:`

### 4. Add environments (optional — for live log queries)

```bash
nano connectors/customers.yml
```

Copy one of the commented example blocks and fill in:
- `k8s_bastion` — SSH jump host that can reach your clusters (`user@host`)
- `k8s_context` — kubectl context name on the bastion
- `k8s_namespace` — k8s namespace where your application pods run
- `pod_map` — pod name prefix → product name mapping

Skip this step if you only want knowledge-base search (no live log queries).

### 5. Start everything

```bash
./start.sh
```

This will:
1. Check Docker is running
2. Build all Docker images
3. Start OpenSearch, PostgreSQL, Redis, Ollama, embedding service, agent
4. Pull the LLM model (~2 GB, first run only)
5. Wait for all services to become healthy
6. Print the URL to the chat UI

**First boot:** 5–10 minutes (image builds + model pull).
**Subsequent starts:** ~30 seconds (images cached, model cached in volume).

---

## First sync

After startup, the sync cron starts automatically. First full sync time depends on volume:

- Small org (< 5 Jira projects, < 20 repos): ~5 minutes
- Medium org (10 projects, 50 repos): ~15-20 minutes
- Large org (20+ projects, 100+ repos): ~40-60 minutes

```bash
# Watch sync progress
./start.sh --logs

# Or trigger manually
./start.sh --force-sync
```

After first sync, incremental syncs run every 4 hours:
- Only Jira issues updated since the last run are re-fetched
- Only Confluence pages changed since the last run are re-fetched
- Only repos/branches with new commits are re-cloned (SHA check, ~100ms per repo)
- **New repos** added to the org are auto-discovered and indexed on the next cycle
- **New branches** matching a `GIT_BRANCHES` pattern are auto-discovered on the next cycle

---

## Verification

```bash
# Chat UI
open http://localhost:8084

# Check indexed document counts
curl -s "http://localhost:9200/_cat/indices?v&h=index,docs.count,store.size"

# Check all services healthy
./start.sh --status

# Test search directly
curl "http://localhost:8081/api/v1/search?q=architecture&tenant=default" \
  -H "X-Tenant-Id: default"

# Test the agent API
curl -X POST http://localhost:8084/api/v1/agent/answer \
  -H "Content-Type: application/json" \
  -d '{"query": "how does our caching strategy work?"}'
```

---

## Adding a new environment

### Option A: Edit customers.yml

```yaml
customers:
  - id: acme-prod
    name: "Acme Corp — Production"
    env: prod
    products:
      - backend
      - platform
    k8s_bastion:   deploy@your-bastion.example.com
    k8s_context:   acme-prod
    k8s_namespace: production
    pod_map:
      api-server:   backend
      worker:       backend
      frontend:     platform
```

Restart the agent or it picks up on the next sync cycle.

### Option B: API (no restart)

```bash
# Register
curl -X POST http://localhost:8084/api/v1/customers \
  -H "X-Api-Key: <api-key>" \
  -H "Content-Type: application/json" \
  -d '{"id": "acme-prod", "name": "Acme Corp — Prod", "products": ["backend"]}'

# Add environment (Mode A — no ES password stored)
curl -X POST http://localhost:8084/api/v1/customers/acme-prod/environments/prod \
  -H "X-Api-Key: <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "k8s_bastion":   "deploy@your-bastion.example.com",
    "k8s_context":   "acme-prod",
    "k8s_namespace": "production",
    "pod_map": {"api-server": "backend", "worker": "backend"}
  }'
```

### Finding the right k8s_namespace for ES queries

If live log queries return 0 results with your configured namespace:

1. Open Kibana for the environment
2. In Discover, look at the `kubernetes.namespace_name` field values
3. The namespace where your application logs appear is the value to set

The agent also retries automatically without the namespace filter on 0 hits and surfaces a
warning in the response.

---

## Configuration reference

### `connectors/.env`

| Variable | Default | Description |
|---|---|---|
| `JIRA_URL` | — | Jira base URL (e.g. `https://your-org.atlassian.net`) |
| `JIRA_EMAIL` | — | Your Atlassian account email |
| `JIRA_TOKEN` | — | Atlassian API token |
| `JIRA_PROJECTS` | (blank = all) | Comma-separated project keys to index |
| `JIRA_MAX_RESULTS` | `10000` | Max issues per project |
| `CONFLUENCE_URL` | — | Confluence base URL |
| `CONFLUENCE_TOKEN` | — | Atlassian API token (same as JIRA_TOKEN) |
| `CONFLUENCE_SPACES` | (blank = all) | Comma-separated space keys to index |
| `CONFLUENCE_MAX_RESULTS` | `2000` | Max pages per space |
| `GIT_TOKEN` | — | GitHub PAT (Classic, `repo` scope) — required for private repos |
| `GIT_BRANCHES` | (blank = default branch only) | Extra branches to index. Supports globs: `develop,release/*,hotfix/*`. New branches matching a glob are auto-discovered each cycle. |
| `GIT_INCLUDE_EXTENSIONS` | `.py,.java,.md,...` | File types to index |
| `GIT_MAX_FILE_KB` | `500` | Skip files larger than this |

### `.env` (root)

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_MODEL` | `llama3.2:3b` | LLM model. Change to `llama3.1:8b` for higher quality (5 GB RAM) |
| `SSH_KEY_PATH` | `~/.ssh/id_rsa` | SSH private key for bastion access |
| `AGENT_API_KEY` | (auto-generated) | Agent API key — retrieve from logs on first boot |
| `SYNC_FULL_INTERVAL_HOURS` | `4` | Jira + Confluence + repos sync frequency |
| `SYNC_CUSTOMER_INTERVAL_MIN` | `60` | Environment state sync frequency |

---

## Troubleshooting

### "Ollama not ready" timeout on first start

Model download takes 2–5 minutes. If `./start.sh` times out:

```bash
# Check download progress
docker logs searchly-ollama-1 -f
# Wait for completion, then:
./start.sh
```

### Live log query returns 0 results

```bash
# 1. Verify SSH access to bastion
ssh <bastion-host> "kubectl get nodes"

# 2. Check the kubectl context exists on the bastion
ssh <bastion-host> "kubectl config get-contexts"

# 3. Verify the ES secret exists
ssh <bastion-host> \
  "kubectl get secret <es-secret> -n elastic-system"

# 4. Check Kibana → Discover → kubernetes.namespace_name
#    Update k8s_namespace in customers.yml accordingly
```

### Sync not picking up new content after config change

```bash
# Force full re-sync (ignores SHA state)
./start.sh --force-sync

# Or repos only
docker compose -f deploy/docker-compose.yml \
  run --rm connectors python sync.py --only repos --force
```

### LLM answers are slow

Expected: llama3.2:3b on CPU generates at ~10–20 tokens/second; a typical answer takes 20–40s.

Options to improve:
1. Smaller/faster model: set `OLLAMA_MODEL=tinyllama` in `.env`
2. Add a GPU — Ollama detects CUDA/Metal automatically, no code changes needed
3. Use a hosted LLM API: replace the Ollama call in `agent.py` with an OpenAI-compatible endpoint

### OpenSearch disk full

```bash
df -h
docker system df
docker system prune -f
```

---

## Port map

| Port | Service |
|---|---|
| 8084 | Intelligence Agent + Chat UI |
| 8081 | Search API (BM25 + kNN) |
| 9200 | OpenSearch |
| 8083 | Embedding service |
| 11434 | Ollama LLM |
| 5432 | PostgreSQL |
| 6379 | Redis |
| 8080 | Gateway (optional) |
