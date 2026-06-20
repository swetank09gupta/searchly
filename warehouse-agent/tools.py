"""
Warehouse Agent Tools — tenant-isolated live data queries.

All tools receive `customer_obj` injected by agent.py at runtime.
The LLM never supplies credentials — only intent (service name, time range, etc.).

Log architecture:
  Pods have minimal rolling logs (last ~100 lines).
  Full logs are shipped via Logstash → Elasticsearch.
  The Kibana UI at https://<env>-logviewer.greymatter.greyorange.com/app/discover
  sits in front of that ES cluster.

  We query Elasticsearch directly via its REST API.
  Each customer env config has:
    elastic_url      — Elasticsearch endpoint
    elastic_index    — index pattern (e.g. logstash-*)
    elastic_api_key  — API key (or elastic_user + elastic_password)
    elastic_fields   — optional field name overrides

  get_logs falls back to kubectl logs when:
    - no elastic_url is configured for this env, OR
    - Elasticsearch returns an error

Tools:
  get_logs              — query Elasticsearch logs (fallback: kubectl logs)
  get_deployment_state  — currently deployed image tags (kubectl)
  get_pod_status        — pod health / crash loops (kubectl)
  list_log_indices      — discover ES indices when index pattern is unknown
  search_knowledge      — BM25 + kNN in Searchly (Jira, Confluence, code)
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
from typing import Any

import httpx

import elastic_logs
from elastic_logs import query_logs_via_bastion, query_logs, list_indices_via_bastion, list_indices

log = logging.getLogger(__name__)


# ─── kubectl helper (still used for deployment state + pod status) ────────────

def _kubectl(customer_obj: dict, *args: str, timeout: int = 30) -> str:
    bastion   = customer_obj.get("k8s_bastion", "")
    context   = customer_obj.get("k8s_context", "")
    namespace = customer_obj.get("k8s_namespace", "default")

    kubectl_cmd = ["kubectl"]
    if context:
        kubectl_cmd += ["--context", context]
    kubectl_cmd += ["--namespace", namespace]
    kubectl_cmd += list(args)

    cmd = (
        ["ssh", "-o", "StrictHostKeyChecking=no",
         "-o", "BatchMode=yes",
         "-o", "ConnectTimeout=10",
         bastion, " ".join(kubectl_cmd)]
        if bastion else kubectl_cmd
    )

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            log.warning("kubectl stderr: %s", r.stderr[:300])
        return r.stdout
    except subprocess.TimeoutExpired:
        log.warning("kubectl timed out (%ss)", timeout)
        return ""
    except Exception as exc:
        log.warning("kubectl error: %s", exc)
        return ""


async def _kubectl_async(customer_obj: dict, *args: str, timeout: int = 30) -> str:
    return await asyncio.to_thread(_kubectl, customer_obj, *args, timeout=timeout)


def _find_pods(customer_obj: dict, pod_prefix: str) -> list[str]:
    raw = _kubectl(customer_obj, "get", "pods", "-o",
                   "jsonpath={.items[*].metadata.name}")
    return [p for p in raw.split() if p.startswith(pod_prefix)] if raw else []


# ─── Fallback: kubectl logs ───────────────────────────────────────────────────

async def _kubectl_logs_fallback(
    customer_obj: dict,
    pod_prefix:   str,
    tail_lines:   int = 200,
    grep:         str | None = None,
) -> dict[str, Any]:
    """Last resort: kubectl logs when Elasticsearch is not configured."""
    pods = await asyncio.to_thread(_find_pods, customer_obj, pod_prefix)
    if not pods:
        return {
            "lines":   [],
            "source":  "kubectl (fallback)",
            "message": (
                f"No pods found matching '{pod_prefix}'. "
                "Note: pod logs are minimal — configure elastic_url in the env "
                "to query the full log store."
            ),
        }

    all_lines: list[str] = []
    for pod in pods[:3]:
        raw = await asyncio.to_thread(
            _kubectl, customer_obj,
            "logs", pod, "--tail", str(tail_lines), "--timestamps"
        )
        lines = raw.splitlines()
        if grep:
            lines = [l for l in lines if grep.lower() in l.lower()]
        all_lines.extend(f"[{pod}] {l}" for l in lines)

    return {
        "lines":   all_lines[-400:],
        "source":  "kubectl (fallback — configure elastic_url for full logs)",
        "pods":    pods[:3],
        "warning": (
            "Pods contain only minimal rolling logs. "
            "Full logs are in Elasticsearch — set elastic_url in the env config."
        ),
    }


# ─── Tools ───────────────────────────────────────────────────────────────────

async def get_logs(
    customer_obj: dict,
    service:      str | None = None,
    minutes:      int = 30,
    level:        str | None = None,
    grep:         str | None = None,
    max_lines:    int = 300,
) -> dict[str, Any]:
    """
    Fetch logs from the customer's Elasticsearch log store.

    Full logs are shipped from pods → Filebeat/Logstash → Elasticsearch.
    Two ES query modes, selected automatically:

    MODE A (bastion-kubectl) — used when no elastic_url is configured.
      Fetches the ES password at runtime from the k8s Secret via bastion SSH,
      then execs a curl inside a Filebeat pod. Zero credentials stored anywhere.
      Works for ALL GreyOrange ECK deployments with only k8s_bastion configured.

    MODE B (direct HTTP) — used when elastic_url is set in env config.
      Queries ES REST API directly using stored credentials.

    Falls back to kubectl pod logs only as last resort (minimal rolling buffer).

    Args:
      service   — service/pod name prefix: "operator-backend", "pick-assist",
                  "mission-manager", "greymatter", "il-server", "redis", etc.
      minutes   — look-back window (default 30, max practical: 1440 = 24h)
      level     — filter by level: "ERROR", "WARN", "INFO", "DEBUG"
      grep      — free-text search in the message field
      max_lines — max lines to return (default 300)
    """
    namespace   = customer_obj.get("k8s_namespace", "default")
    has_elastic = bool(customer_obj.get("elastic_url"))
    has_bastion = bool(customer_obj.get("k8s_bastion"))

    # ── Mode A: bastion-kubectl (zero credential storage) ─────────────────────
    if not has_elastic and has_bastion:
        log.info(
            "ES mode A (bastion-kubectl): bastion=%s",
            customer_obj.get("k8s_bastion"),
        )
        result = await elastic_logs.query_logs_via_bastion(
            env_cfg   = customer_obj,
            namespace = namespace,
            service   = service,
            minutes   = minutes,
            level     = level,
            grep      = grep,
            max_hits  = max_lines,
        )
        if result.get("error") and not result.get("lines"):
            log.warning("Bastion ES failed (%s), trying kubectl pod logs", result["error"])
            fallback = await _kubectl_logs_fallback(
                customer_obj, service or "", tail_lines=150, grep=grep
            )
            fallback["es_error"] = result["error"]
            return fallback
        return result

    # ── Mode B: direct HTTP (elastic_url + credentials) ───────────────────────
    if has_elastic:
        result = await elastic_logs.query_logs(
            env_cfg   = customer_obj,
            namespace = namespace,
            service   = service,
            minutes   = minutes,
            level     = level,
            grep      = grep,
            max_hits  = max_lines,
        )
        if result.get("error") and not result.get("lines"):
            log.warning("ES direct failed (%s), trying kubectl pod logs", result["error"])
            fallback = await _kubectl_logs_fallback(
                customer_obj, service or "", tail_lines=150, grep=grep
            )
            fallback["es_error"] = result["error"]
            return fallback
        return result

    # ── Last resort: kubectl pod logs ─────────────────────────────────────────
    log.info("No ES configured — falling back to kubectl pod logs")
    return await _kubectl_logs_fallback(customer_obj, service or "",
                                        tail_lines=200, grep=grep)


async def get_deployment_state(customer_obj: dict) -> dict[str, Any]:
    """
    Return the currently deployed image tag (version) for every service.
    Queries kubectl — this is fast and always current.
    """
    raw = await _kubectl_async(
        customer_obj,
        "get", "deployments",
        "-o", "custom-columns="
              "NAME:.metadata.name,"
              "IMAGE:.spec.template.spec.containers[0].image,"
              "READY:.status.readyReplicas,"
              "DESIRED:.spec.replicas",
    )
    if not raw:
        return {"deployments": [], "message": "Could not reach cluster via kubectl"}

    deployments: list[dict] = []
    for line in raw.strip().splitlines()[1:]:   # skip header
        parts = line.split()
        if len(parts) >= 2:
            image   = parts[1]
            version = image.split(":")[-1] if ":" in image else "latest"
            deployments.append({
                "name":    parts[0],
                "image":   image,
                "version": version,
                "ready":   parts[2] if len(parts) >= 3 else "?",
                "desired": parts[3] if len(parts) >= 4 else "?",
            })
    return {"deployments": deployments}


async def get_pod_status(customer_obj: dict) -> dict[str, Any]:
    """Pod health: running/pending/crashloop, restart counts."""
    raw = await _kubectl_async(
        customer_obj,
        "get", "pods",
        "-o", "custom-columns="
              "NAME:.metadata.name,"
              "STATUS:.status.phase,"
              "READY:.status.containerStatuses[0].ready,"
              "RESTARTS:.status.containerStatuses[0].restartCount",
    )
    if not raw:
        return {"pods": [], "message": "Could not reach cluster via kubectl"}

    pods: list[dict] = []
    for line in raw.strip().splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2:
            restarts = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            pods.append({
                "name":     parts[0],
                "status":   parts[1],
                "ready":    parts[2] if len(parts) > 2 else "?",
                "restarts": restarts,
            })
    return {
        "pods":      pods,
        "unhealthy": [p for p in pods
                      if p["status"] != "Running" or p["restarts"] > 5],
    }


async def list_log_indices(customer_obj: dict) -> dict[str, Any]:
    """
    List all Elasticsearch indices in the customer's log cluster.
    Use this when get_logs returns 'index not found' or you're not sure
    which index pattern to use. Then retry get_logs with the correct index.

    Works in both modes:
      - Mode A (bastion-kubectl): no elastic_url needed, uses bastion SSH
      - Mode B (direct HTTP): requires elastic_url + credentials
    """
    if customer_obj.get("elastic_url"):
        return await elastic_logs.list_indices(customer_obj)
    if customer_obj.get("k8s_bastion"):
        return await elastic_logs.list_indices_via_bastion(customer_obj)
    return {"error": "No Elasticsearch URL or bastion configured for this environment."}


async def query_kg(
    entity_type:  str,
    entity_id:    str,
    searchly_url: str,
    tenant:       str,
    depth:        int = 3,
) -> dict[str, Any]:
    """
    Traverse the knowledge graph from a given entity and return all related entities.

    Use this after finding a Jira issue key or PR ID to discover:
      - Which PRs fixed a Jira ticket     (jira_issue --[fixed_by]--> pull_request)
      - Which service a PR touches        (pull_request --[merges_into]--> service)
      - Which version is running per env  (deployment --[runs]--> service)
      - Which Jira tickets a PR references (pull_request --[references]--> jira_issue)

    entity_type: "jira_issue" | "pull_request" | "service" | "deployment" | "customer"
    entity_id:   e.g. "AES-891", "pick-assist#234", "pick-assist", "sams-club-atlanta/prod"
    depth:       how many hops to traverse (1–5, default 3)
    """
    headers = {"X-Tenant-ID": tenant, "X-Tenant-Tier": "SHARED"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{searchly_url}/api/v1/kg/traverse/{entity_type}/{entity_id}",
                params={"depth": min(depth, 5)},
                headers=headers,
            )
        if resp.status_code == 200:
            nodes = resp.json()
            # Group by entity_type for readability
            by_type: dict[str, list] = {}
            for node in nodes:
                t = node.get("entity_type", "unknown")
                by_type.setdefault(t, []).append({
                    "id":    node.get("entity_id"),
                    "name":  node.get("name"),
                    "depth": node.get("depth"),
                })
            return {"nodes": by_type, "total": len(nodes)}
        return {"nodes": {}, "error": f"HTTP {resp.status_code}"}
    except Exception as exc:
        log.warning("query_kg failed: %s", exc)
        return {"nodes": {}, "error": str(exc)}


async def search_knowledge(
    query:        str,
    searchly_url: str,
    tenant:       str,
    customer_id:  str | None = None,
    product:      str | None = None,
) -> dict[str, Any]:
    """Search Jira tickets, Confluence docs, and code via Searchly."""
    # hits_only=true skips Ollama synthesis — the agent does its own synthesis.
    # rag_only=true still needed to prevent the circular call loop (agent → search-api → agent).
    params: dict[str, str] = {"q": query, "size": "5", "rag_only": "true", "hits_only": "true"}
    if customer_id:
        params["customer"] = customer_id
    if product:
        params["product"] = product
    headers = {"X-Tenant-ID": tenant, "X-Tenant-Tier": "SHARED"}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{searchly_url}/api/v1/search",
                params=params, headers=headers,
            )
        if resp.status_code != 200:
            return {"hits": [], "error": f"HTTP {resp.status_code}"}
        data = resp.json()
        return {
            "hits": [
                {"title":    h.get("title", ""),
                 "snippet":  (h.get("highlights") or [""])[0],
                 "metadata": h.get("metadata", {})}
                for h in data.get("hits", [])[:5]
            ],
            "answer": data.get("answer"),
        }
    except Exception as exc:
        log.warning("search_knowledge failed: %s", exc)
        return {"hits": [], "error": str(exc)}


# ─── Tool registry ────────────────────────────────────────────────────────────

TOOL_REGISTRY: dict[str, Any] = {
    "get_logs":             get_logs,
    "get_deployment_state": get_deployment_state,
    "get_pod_status":       get_pod_status,
    "list_log_indices":     list_log_indices,
    "search_knowledge":     search_knowledge,
    "query_kg":             query_kg,
}

# Ollama tool-calling schema.
# customer_obj is NOT listed — it's injected by agent.py, not the LLM.
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_logs",
            "description": (
                "Fetch logs from the customer's Elasticsearch log store. "
                "Full logs are shipped from pods → Logstash → Elasticsearch — "
                "this is the primary log source, not kubectl. "
                "Use for: errors, exceptions, slow queries, allocation issues, "
                "robot not responding, order stuck. "
                "Always try with level='ERROR' first, then widen if needed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "service": {
                        "type": "string",
                        "description": (
                            "Service / pod name prefix to filter on. "
                            "Examples: 'operator-backend', 'pick-assist', "
                            "'mission-manager', 'greymatter', 'il-server', "
                            "'redis', 'postgres'. "
                            "Omit to get logs from ALL services in the namespace."
                        ),
                    },
                    "minutes": {
                        "type": "integer",
                        "default": 30,
                        "description": (
                            "How far back to look in minutes. "
                            "30 for recent issues, 60-120 for the last hour, "
                            "up to 1440 for today."
                        ),
                    },
                    "level": {
                        "type": "string",
                        "enum": ["ERROR", "WARN", "INFO", "DEBUG"],
                        "description": (
                            "Filter by log level. Start with ERROR for fault diagnosis. "
                            "Omit to get all levels."
                        ),
                    },
                    "grep": {
                        "type": "string",
                        "description": (
                            "Free-text search in log messages. "
                            "Examples: 'robot 42', 'ORD-9182', 'timeout', "
                            "'allocat', 'mission', 'Hungarian'."
                        ),
                    },
                    "max_lines": {
                        "type": "integer",
                        "default": 300,
                        "description": "Maximum log lines to return.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_deployment_state",
            "description": (
                "Get the currently deployed image tag (= version) of every service. "
                "Use when the question is about what version is running, "
                "recent upgrades, or to check if a known bug fix is deployed."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pod_status",
            "description": (
                "Get health and readiness of all pods: running/pending/crashloop, "
                "restart counts. Use first when a service is down or behaving strangely."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_log_indices",
            "description": (
                "List all Elasticsearch indices in the customer's log cluster. "
                "Use when get_logs returns 'index not found' or you're not sure "
                "which index pattern to use. Then retry get_logs with the correct index."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge",
            "description": (
                "Search Jira tickets, Confluence documentation, and source code. "
                "Use to find: known bugs, design explanations, release notes, "
                "architectural context. Not for live cluster data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query":   {"type": "string"},
                    "product": {
                        "type": "string",
                        "description": "Narrow to a product: pick-assist, greymatter, intralogistics, etc.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_kg",
            "description": (
                "Traverse the knowledge graph from a known entity to find related entities. "
                "Use AFTER search_knowledge returns a Jira key or PR ID to discover: "
                "which PR fixed a ticket (jira_issue→fixed_by→pull_request), "
                "which service a PR touches (pull_request→merges_into→service), "
                "which version is running per env (deployment→runs→service). "
                "This is pre-computed — use it instead of asking the LLM to guess correlations."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "description": "jira_issue | pull_request | service | deployment | customer",
                    },
                    "entity_id": {
                        "type": "string",
                        "description": "e.g. 'AES-891', 'pick-assist#234', 'sams-club-atlanta/prod'",
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Traversal depth 1–5. Default 3.",
                    },
                },
                "required": ["entity_type", "entity_id"],
            },
        },
    },
]
