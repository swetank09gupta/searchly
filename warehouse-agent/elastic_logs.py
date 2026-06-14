"""
Elasticsearch Log Client — queries customer log stores.

GreyOrange ships pod logs via Filebeat → Logstash → Elasticsearch.
Each env has its own ES cluster (ECK — Elastic Cloud on Kubernetes).

─── Two query modes ────────────────────────────────────────────────────────────

MODE A: Bastion-kubectl (zero credential storage — DEFAULT for GreyOrange envs)
  The ES password lives in a k8s Secret. We fetch it at query time via the
  bastion SSH + kubectl, then exec a curl inside a Filebeat pod that already
  has network access to the ES ClusterIP.

  Required in env config:
    k8s_bastion    — already present for kubectl access
    (optional) elastic_k8s_ns     — default: "elastic-system"
    (optional) elastic_k8s_secret — default: "gm-elasticsearch-es-elastic-user"
    (optional) elastic_k8s_svc    — default: "gm-elasticsearch-es-http"
    (optional) elastic_index      — default: "filebeat-*"

  Zero credentials stored anywhere. Password fetched fresh each query from the
  cluster secret that Kubernetes already manages.

MODE B: Direct HTTP (external ES URL + stored credentials)
  Used when ES is exposed externally with an API key or basic auth.
  Requires: elastic_url + (elastic_api_key OR elastic_user+elastic_password)

─── Selecting mode ─────────────────────────────────────────────────────────────
  • If elastic_url is configured → Mode B (direct HTTP)
  • Else if k8s_bastion is set   → Mode A (bastion-kubectl, auto-detects ECK)
  • Otherwise                    → falls back to kubectl logs (minimal)

─── Field names ─────────────────────────────────────────────────────────────────
Default (ECS + Filebeat kubernetes.* metadata):
  @timestamp                 — event time
  message                    — the log line
  log.level                  — severity
  kubernetes.namespace_name  — k8s namespace
  kubernetes.pod_name        — pod name
  kubernetes.container_name  — container name
  kubernetes.labels.app      — app label

Override per-env in elastic_fields:
  {"namespace": "kubernetes.namespace", "level": "log_level"}
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import subprocess
from typing import Any

import httpx

log = logging.getLogger(__name__)

# ─── Default field names ──────────────────────────────────────────────────────

DEFAULT_FIELDS = {
    "timestamp":   "@timestamp",
    "message":     "message",
    "level":       "log.level",
    "namespace":   "kubernetes.namespace_name",
    "pod":         "kubernetes.pod_name",
    "container":   "kubernetes.container_name",
    "app_label":   "kubernetes.labels.app",
}


def _resolve_fields(env_cfg: dict) -> dict[str, str]:
    """Merge env-specific field overrides over defaults."""
    overrides = env_cfg.get("elastic_fields") or {}
    return {**DEFAULT_FIELDS, **overrides}


def _auth_headers(env_cfg: dict) -> dict[str, str]:
    """Build auth headers for Mode B (direct HTTP)."""
    api_key  = env_cfg.get("elastic_api_key", "")
    user     = env_cfg.get("elastic_user", "")
    password = env_cfg.get("elastic_password", "")
    if api_key:
        return {"Authorization": f"ApiKey {api_key}"}
    if user and password:
        creds = base64.b64encode(f"{user}:{password}".encode()).decode()
        return {"Authorization": f"Basic {creds}"}
    return {}


def _build_query(
    fields:    dict[str, str],
    namespace: str | None,
    service:   str | None,
    minutes:   int,
    level:     str | None,
    grep:      str | None,
    max_hits:  int,
) -> dict[str, Any]:
    """Build an Elasticsearch bool query."""
    ts_field  = fields["timestamp"]
    msg_field = fields["message"]
    ns_field  = fields["namespace"]
    pod_field = fields["pod"]
    lvl_field = fields["level"]

    filters: list[dict] = [
        {"range": {ts_field: {"gte": f"now-{minutes}m", "lte": "now"}}},
    ]
    if namespace:
        filters.append({"term": {ns_field: namespace}})
    if service:
        filters.append({"prefix": {pod_field: service}})
    if level:
        filters.append({"term": {lvl_field: level.upper()}})

    must: list[dict] = []
    if grep:
        must.append({"match": {msg_field: {"query": grep, "operator": "and"}}})

    source_fields = [
        ts_field, msg_field, lvl_field,
        pod_field, fields["container"], fields["app_label"],
    ]

    return {
        "query": {"bool": {"filter": filters, **({"must": must} if must else {})}},
        "sort":    [{ts_field: {"order": "desc"}}],
        "_source": source_fields,
        "size":    max_hits,
    }


def _fmt_hit(hit: dict, fields: dict[str, str]) -> str:
    src    = hit.get("_source", {})
    ts     = src.get(fields["timestamp"], "")[:23]
    lvl    = src.get(fields["level"], "")
    pod    = src.get(fields["pod"],   "")
    msg    = src.get(fields["message"], "")
    return f"{ts} {'['+lvl+'] ' if lvl else ''}{('['+pod+'] ') if pod else ''}{msg}"


def _parse_hits(data: dict, fields: dict[str, str]) -> tuple[int, list[str]]:
    hits_block  = data.get("hits", {})
    total_raw   = hits_block.get("total", {})
    total_count = (total_raw.get("value", 0)
                   if isinstance(total_raw, dict) else int(total_raw or 0))
    lines = [_fmt_hit(h, fields) for h in hits_block.get("hits", [])]
    return total_count, lines


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE A — Bastion-kubectl (zero credential storage)
# ═══════════════════════════════════════════════════════════════════════════════

# Shell script templates that run on the bastion via SSH.
# The query body is base64-encoded to avoid shell quoting issues.
# ES_CTX is always exported by the caller (empty string if no context).
# Placeholders use {name} format — all shell ${VAR} use plain $VAR to avoid
# conflicts with Python's str.format().

_BASTION_QUERY_SCRIPT = """\
export ES_CTX={ctx}
set -euo pipefail

CTX_FLAG=""
if [ -n "$ES_CTX" ]; then CTX_FLAG="--context=$ES_CTX"; fi

# Fetch ES password from k8s secret at runtime — never stored in config
ES_PASS=$(kubectl $CTX_FLAG \\
  get secret {secret} -n {es_ns} \\
  -o jsonpath='{{.data.elastic}}' | base64 -d)

# Find the best pod (Filebeat preferred — already has ES network access)
POD=$(kubectl $CTX_FLAG get pods -n {es_ns} \\
  -l 'common.k8s.elastic.co/type=beat' \\
  --no-headers -o jsonpath='{{.items[0].metadata.name}}' 2>/dev/null || true)

if [ -z "$POD" ]; then
  POD=$(kubectl $CTX_FLAG get pods -n {es_ns} \\
    --field-selector=status.phase=Running \\
    --no-headers -o jsonpath='{{.items[0].metadata.name}}' 2>/dev/null || true)
fi

if [ -z "$POD" ]; then
  echo '{{"error":"No running pod found in {es_ns}","hits":{{"hits":[],"total":{{"value":0}}}}}}'
  exit 0
fi

# Decode the query body from base64 (avoids shell quoting hell with JSON)
QUERY=$(echo "{query_b64}" | base64 -d)

# Exec curl inside the Filebeat pod against the ES ClusterIP service
kubectl $CTX_FLAG exec -n {es_ns} "$POD" -i -- \\
  curl -sf -u "elastic:$ES_PASS" \\
  "http://{svc}:9200/{index}/_search" \\
  -H 'Content-Type: application/json' \\
  --data-raw "$QUERY" 2>/dev/null \\
  || echo '{{"error":"curl inside pod failed","hits":{{"hits":[],"total":{{"value":0}}}}}}'
"""

_BASTION_INDICES_SCRIPT = """\
export ES_CTX={ctx}
set -euo pipefail

CTX_FLAG=""
if [ -n "$ES_CTX" ]; then CTX_FLAG="--context=$ES_CTX"; fi

ES_PASS=$(kubectl $CTX_FLAG get secret {secret} -n {es_ns} \\
  -o jsonpath='{{.data.elastic}}' | base64 -d)

POD=$(kubectl $CTX_FLAG get pods -n {es_ns} \\
  -l 'common.k8s.elastic.co/type=beat' \\
  --no-headers -o jsonpath='{{.items[0].metadata.name}}' 2>/dev/null || true)

if [ -z "$POD" ]; then
  POD=$(kubectl $CTX_FLAG get pods -n {es_ns} \\
    --field-selector=status.phase=Running \\
    --no-headers -o jsonpath='{{.items[0].metadata.name}}' 2>/dev/null || true)
fi

if [ -z "$POD" ]; then echo ""; exit 0; fi

kubectl $CTX_FLAG exec -n {es_ns} "$POD" -- \\
  curl -sf -u "elastic:$ES_PASS" \\
  "http://{svc}:9200/_cat/indices?v&h=index,docs.count,store.size&s=index" 2>/dev/null || true
"""


def _ssh_run(bastion: str, script: str, timeout: int = 45) -> str:
    """Run a shell script on the bastion via SSH, return stdout."""
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        bastion,
        "bash -s",
    ]
    try:
        r = subprocess.run(
            cmd,
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if r.returncode != 0:
            log.warning("Bastion script stderr: %s", r.stderr[:400])
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        log.warning("Bastion ES query timed out (%ds)", timeout)
        return ""
    except Exception as exc:
        log.warning("Bastion SSH error: %s", exc)
        return ""


async def query_logs_via_bastion(
    env_cfg:   dict,
    namespace: str | None,
    *,
    service:   str | None = None,
    minutes:   int = 30,
    level:     str | None = None,
    grep:      str | None = None,
    max_hits:  int = 300,
) -> dict[str, Any]:
    """
    MODE A — query ES via bastion SSH + kubectl exec.

    No credentials stored in config. The ES password is fetched at runtime from
    the k8s Secret that Kubernetes already manages (gm-elasticsearch-es-elastic-user
    by default). A curl is executed inside a Filebeat pod that already has
    network access to the ES ClusterIP service.

    env_cfg fields used:
      k8s_bastion        — SSH bastion (required)
      k8s_context        — kubectl context (optional)
      elastic_k8s_ns     — namespace where ES runs (default: elastic-system)
      elastic_k8s_secret — k8s Secret name (default: gm-elasticsearch-es-elastic-user)
      elastic_k8s_svc    — ES k8s Service name (default: gm-elasticsearch-es-http)
      elastic_index      — index pattern (default: filebeat-*)
      elastic_fields     — field name overrides (optional)
    """
    bastion   = env_cfg.get("k8s_bastion", "")
    context   = env_cfg.get("k8s_context", "")
    es_ns     = env_cfg.get("elastic_k8s_ns",     "elastic-system")
    es_secret = env_cfg.get("elastic_k8s_secret", "gm-elasticsearch-es-elastic-user")
    es_svc    = env_cfg.get("elastic_k8s_svc",    "gm-elasticsearch-es-http")
    es_index  = env_cfg.get("elastic_index",       "filebeat-*")

    if not bastion:
        return {"lines": [], "error": "No k8s_bastion configured for this environment."}

    fields = _resolve_fields(env_cfg)

    # Try namespace filter first; if 0 hits, retry without it
    for ns_attempt in [namespace, None]:
        body     = _build_query(fields, ns_attempt, service, minutes, level, grep, max_hits)
        body_b64 = base64.b64encode(json.dumps(body).encode()).decode()

        script = _BASTION_QUERY_SCRIPT.format(
            ctx       = context or "",
            secret    = es_secret,
            es_ns     = es_ns,
            svc       = es_svc,
            index     = es_index,
            query_b64 = body_b64,
        )

        log.info(
            "ES bastion query: bastion=%s  ns=%r  service=%s  level=%s  grep=%r",
            bastion, ns_attempt, service, level, grep,
        )

        raw = await asyncio.to_thread(_ssh_run, bastion, script, 50)

        if not raw:
            log.warning("Bastion ES query returned empty output")
            continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Bastion ES response is not JSON: %r", raw[:200])
            continue

        if "error" in data and "hits" not in data:
            return {"lines": [], "error": data["error"]}

        total_count, lines = _parse_hits(data, fields)
        ns_used = ns_attempt or "all"

        if total_count == 0 and ns_attempt is not None:
            log.info("ES bastion: 0 hits for namespace=%r — retrying without ns filter", ns_attempt)
            continue   # retry with ns_attempt=None

        log.info(
            "ES bastion: index=%s ns=%s → %d hits (returned %d), last %dmin",
            es_index, ns_used, total_count, len(lines), minutes,
        )
        return {
            "lines":      lines,
            "total":      total_count,
            "returned":   len(lines),
            "index":      es_index,
            "namespace":  ns_used,
            "time_range": f"last {minutes} minutes",
            "mode":       "bastion-kubectl",
        }

    return {
        "lines":   [],
        "total":   0,
        "returned": 0,
        "index":   es_index,
        "namespace": "unknown",
        "time_range": f"last {minutes} minutes",
        "mode":   "bastion-kubectl",
        "warning": (
            "Bastion ES query returned 0 hits. "
            "The ES cluster may have no recent logs for this namespace, "
            "or the index pattern may need updating. "
            f"Try list_log_indices to see what indices exist."
        ),
    }


async def list_indices_via_bastion(env_cfg: dict) -> dict[str, Any]:
    """Mode A version of list_indices — fetches via bastion kubectl exec."""
    bastion   = env_cfg.get("k8s_bastion", "")
    context   = env_cfg.get("k8s_context", "")
    es_ns     = env_cfg.get("elastic_k8s_ns",     "elastic-system")
    es_secret = env_cfg.get("elastic_k8s_secret", "gm-elasticsearch-es-elastic-user")
    es_svc    = env_cfg.get("elastic_k8s_svc",    "gm-elasticsearch-es-http")

    if not bastion:
        return {"error": "No k8s_bastion configured"}

    script = _BASTION_INDICES_SCRIPT.format(
        ctx    = context or "",
        secret = es_secret,
        es_ns  = es_ns,
        svc    = es_svc,
    )

    raw = await asyncio.to_thread(_ssh_run, bastion, script, 30)
    if not raw:
        return {"error": "Empty response from bastion — check kubectl access"}

    lines = [l for l in raw.splitlines() if l.strip()]
    return {"indices": lines, "mode": "bastion-kubectl"}


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE B — Direct HTTP (external ES URL + credentials)
# ═══════════════════════════════════════════════════════════════════════════════

async def query_logs(
    env_cfg:   dict,
    namespace: str | None,
    *,
    service:   str | None = None,
    minutes:   int = 30,
    level:     str | None = None,
    grep:      str | None = None,
    max_hits:  int = 300,
) -> dict[str, Any]:
    """
    MODE B — query ES via direct HTTP (external URL + stored credentials).

    Use when ES is exposed externally (e.g. via nginx ingress) with an API key
    or basic auth credentials stored in the env config.

    env_cfg fields:
      elastic_url         — ES REST endpoint (required)
      elastic_api_key     — API key auth (preferred)
      elastic_user        — basic auth username
      elastic_password    — basic auth password
      elastic_index       — index pattern (default: logstash-*)
      elastic_verify_ssl  — set False for self-signed certs
      elastic_fields      — field name overrides
    """
    elastic_url = (env_cfg.get("elastic_url") or "").rstrip("/")
    index       = env_cfg.get("elastic_index", "logstash-*")

    if not elastic_url:
        return {"lines": [], "error": "No elastic_url configured."}

    fields  = _resolve_fields(env_cfg)
    headers = {"Content-Type": "application/json", **_auth_headers(env_cfg)}
    url     = f"{elastic_url}/{index}/_search"

    async def _run(ns_filter: str | None) -> tuple[int, list[str]]:
        body = _build_query(fields, ns_filter, service, minutes, level, grep, max_hits)
        log.debug("ES direct query → %s  ns=%s", url, ns_filter)
        async with httpx.AsyncClient(
            timeout=20.0,
            verify=env_cfg.get("elastic_verify_ssl", True),
        ) as client:
            resp = await client.post(url, json=body, headers=headers)

        if resp.status_code == 404:
            raise ValueError(
                f"Index '{index}' not found. "
                f"Common patterns: logstash-*, filebeat-*, k8s-*"
            )
        if resp.status_code == 400:
            detail = resp.json().get("error", {}).get("reason", resp.text[:300])
            raise ValueError(
                f"ES query error (400): {detail}. "
                f"Field names may differ — check elastic_fields in env config."
            )
        resp.raise_for_status()
        return _parse_hits(resp.json(), fields)

    try:
        # First attempt: filter by namespace
        total_count, lines = await _run(namespace)

        ns_used = namespace or "all"
        if total_count == 0 and namespace:
            log.info("ES direct: 0 hits ns=%r — retrying without namespace filter", namespace)
            total_count, lines = await _run(None)
            ns_used = "all (namespace filter yielded 0 — update k8s_namespace in env)"

        log.info(
            "ES direct: index=%s ns=%s → %d hits (returned %d), last %dmin",
            index, ns_used, total_count, len(lines), minutes,
        )
        return {
            "lines":      lines,
            "total":      total_count,
            "returned":   len(lines),
            "index":      index,
            "namespace":  ns_used,
            "time_range": f"last {minutes} minutes",
            "mode":       "direct-http",
        }

    except ValueError as exc:
        return {"lines": [], "error": str(exc)}
    except httpx.ConnectError as exc:
        return {"lines": [], "error": f"Cannot reach ES at {elastic_url}: {exc}"}
    except httpx.TimeoutException:
        return {"lines": [], "error": f"ES query timed out ({elastic_url})"}
    except Exception as exc:
        log.warning("ES direct query failed: %s", exc)
        return {"lines": [], "error": str(exc)}


async def list_indices(env_cfg: dict) -> dict[str, Any]:
    """Mode B: list ES indices via direct HTTP."""
    elastic_url = (env_cfg.get("elastic_url") or "").rstrip("/")
    if not elastic_url:
        return {"error": "No elastic_url configured"}

    headers = _auth_headers(env_cfg)
    try:
        async with httpx.AsyncClient(
            timeout=10.0,
            verify=env_cfg.get("elastic_verify_ssl", True),
        ) as client:
            resp = await client.get(
                f"{elastic_url}/_cat/indices",
                params={"v": "", "h": "index,docs.count,store.size"},
                headers=headers,
            )
        resp.raise_for_status()
        lines = [l for l in resp.text.splitlines() if l.strip()]
        return {"indices": lines, "mode": "direct-http"}
    except Exception as exc:
        return {"error": str(exc)}
