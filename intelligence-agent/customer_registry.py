"""
Customer Registry — persistent, hot-reloadable, lifecycle-aware.

Customers progress through lifecycle stages:
  solution → dev → testing → staging → prod

At each stage, a new environment entry can be added (k8s cluster details).
Queries work at every stage — earlier stages just don't have live cluster data,
so they fall back to knowledge-only (RAG) answers.

Storage: a single JSON file (`customers_db.json`) mounted as a Docker volume
so it survives restarts and can be inspected/edited by hand if needed.

API shape (per customer):
  {
    "id":              "acme-corp",
    "name":            "Sam's Club — Atlanta DC",
    "lifecycle_stage": "prod",              # current highest reached stage
    "products":        ["pick-assist", "core-platform"],
    "notes":           "Multi-bot RRoLS flow",
    "environments": {
      "dev": {
        "k8s_bastion":   "user@host",
        "k8s_context":   "ctx-name",        # kubectl --context
        "k8s_namespace": "default",
        "pod_map": {                         # pod_prefix → product name
          "operator-backend": "pick-assist",
          "core-platform": "core-platform"
        }
      },
      "test":    { ... },
      "staging": { ... },
      "prod":    { ... }
    }
  }
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

LIFECYCLE_ORDER = ["solution", "dev", "testing", "staging", "prod"]


class CustomerRegistry:
    """
    Thread-safe, file-backed customer registry.

    Customers are added once (at solution phase) and environments are added
    progressively as each phase goes live.  No restart needed.
    """

    def __init__(self, db_path: str = "/app/data/customers_db.json"):
        self._path = Path(db_path)
        self._lock = threading.RLock()
        self._data: dict[str, dict] = {}
        self._load()

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load(self):
        """Load from disk; create empty db if not found."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            try:
                with open(self._path) as f:
                    raw = json.load(f)
                # Support both list format (from old customers.yml) and dict format
                if isinstance(raw, list):
                    self._data = {c["id"]: _normalize(c) for c in raw}
                else:
                    self._data = {k: _normalize(v) for k, v in raw.items()}
                log.info("Loaded %d customers from %s", len(self._data), self._path)
            except Exception as e:
                log.warning("Could not load customer DB: %s — starting empty", e)
                self._data = {}
        else:
            self._data = {}
            self._flush()

    def _flush(self):
        """Write current state to disk (caller must hold lock)."""
        try:
            tmp = self._path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(self._data, f, indent=2)
            tmp.replace(self._path)
        except Exception as e:
            log.error("Could not persist customer DB: %s", e)

    def import_yaml(self, yaml_path: str):
        """
        One-time import from a legacy customers.yml.
        Existing customers are NOT overwritten.
        """
        try:
            import yaml
            with open(yaml_path) as f:
                data = yaml.safe_load(f)
            customers = data.get("customers", [])
            with self._lock:
                imported = 0
                for c in customers:
                    cid = c.get("id")
                    if cid and cid not in self._data:
                        self._data[cid] = _normalize(c)
                        imported += 1
                if imported:
                    self._flush()
            log.info("Imported %d new customers from %s", imported, yaml_path)
        except Exception as e:
            log.warning("YAML import failed: %s", e)

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def list_customers(self) -> list[dict]:
        with self._lock:
            return [_public(v) for v in self._data.values()]

    def get(self, customer_id: str) -> dict | None:
        with self._lock:
            c = self._data.get(customer_id)
            return _public(c) if c else None

    def create(self, customer_id: str, name: str, products: list[str],
               lifecycle_stage: str = "solution", notes: str = "") -> dict:
        """
        Register a new customer.  Initially at lifecycle_stage='solution'
        with no environments configured — only knowledge queries work.
        """
        if lifecycle_stage not in LIFECYCLE_ORDER:
            raise ValueError(f"lifecycle_stage must be one of {LIFECYCLE_ORDER}")
        with self._lock:
            if customer_id in self._data:
                raise ValueError(f"Customer '{customer_id}' already exists")
            c = {
                "id":              customer_id,
                "name":            name,
                "lifecycle_stage": lifecycle_stage,
                "products":        products,
                "notes":           notes,
                "environments":    {},
            }
            self._data[customer_id] = c
            self._flush()
            log.info("Registered new customer: %s (%s)", customer_id, lifecycle_stage)
            return _public(c)

    def update(self, customer_id: str, **fields) -> dict:
        """Update top-level fields (name, notes, lifecycle_stage, products, aliases)."""
        allowed = {"name", "notes", "lifecycle_stage", "products", "aliases"}
        with self._lock:
            c = self._data.get(customer_id)
            if not c:
                raise KeyError(f"Customer not found: {customer_id!r}")
            for k, v in fields.items():
                if k in allowed:
                    c[k] = v
            self._flush()
            return _public(c)

    def delete(self, customer_id: str):
        with self._lock:
            if customer_id not in self._data:
                raise KeyError(f"Customer not found: {customer_id!r}")
            del self._data[customer_id]
            self._flush()

    # ── Environment management ─────────────────────────────────────────────

    def upsert_environment(self, customer_id: str, env: str,
                           k8s_bastion:      str  = "",
                           k8s_context:      str  = "",
                           k8s_namespace:    str  = "default",
                           pod_map:          dict | None = None,
                           # Mode A: bastion-kubectl (zero credential storage)
                           elastic_k8s_ns:     str  = "elastic-system",
                           elastic_k8s_secret: str  = "gm-elasticsearch-es-elastic-user",
                           elastic_k8s_svc:    str  = "gm-elasticsearch-es-http",
                           elastic_index:      str  = "filebeat-*",
                           elastic_fields:     dict | None = None,
                           # Mode B: direct HTTP (external URL + credentials)
                           elastic_url:        str  = "",
                           elastic_api_key:    str  = "",
                           elastic_user:       str  = "",
                           elastic_password:   str  = "",
                           elastic_verify_ssl: bool = True,
                           ) -> dict:
        """
        Add or update an environment for a customer.

        Stores both k8s cluster access details AND Elasticsearch log-store config.

        Logs flow: pod → Logstash → Elasticsearch → Kibana
        We query Elasticsearch directly for live log access.

        Kubernetes access:
          k8s_bastion   — SSH bastion for kubectl (user@host)
          k8s_context   — kubectl context name
          k8s_namespace — k8s namespace where customer pods run
          pod_map       — pod_prefix → product name hint

        Mode A — bastion-kubectl (zero credential storage, DEFAULT):
          The agent SSHes to the bastion, fetches the ES password at runtime
          from the k8s Secret, and execs curl inside a Filebeat pod.
          Works for all ECK deployments with only k8s_bastion set.
          elastic_k8s_ns     — namespace where ECK runs (default: elastic-system)
          elastic_k8s_secret — k8s Secret name (default: gm-elasticsearch-es-elastic-user)
          elastic_k8s_svc    — ES ClusterIP Service (default: gm-elasticsearch-es-http)
          elastic_index      — index pattern (default: filebeat-*)
          elastic_fields     — field name overrides if Logstash config differs

        Mode B — direct HTTP (only needed for externally-exposed ES):
          elastic_url        — external ES REST API endpoint
          elastic_api_key    — ES API key (preferred)
          elastic_user       — basic auth username
          elastic_password   — basic auth password
          elastic_verify_ssl — set False for self-signed certs

        Lifecycle auto-advances: upsert_environment(env='prod') on a 'staging'
        customer automatically advances their lifecycle_stage to 'prod'.
        """
        if env not in LIFECYCLE_ORDER[1:]:   # env must not be 'solution'
            raise ValueError(f"env must be one of {LIFECYCLE_ORDER[1:]}")
        with self._lock:
            c = self._data.get(customer_id)
            if not c:
                raise KeyError(f"Customer not found: {customer_id!r}")

            # Build env record — only store non-empty values to keep JSON clean
            env_record: dict = {
                "k8s_bastion":   k8s_bastion,
                "k8s_context":   k8s_context,
                "k8s_namespace": k8s_namespace,
                "pod_map":       pod_map or {},
            }

            # Mode A — bastion-kubectl ES config (stored always when bastion is set)
            if k8s_bastion:
                env_record["elastic_index"]      = elastic_index or "filebeat-*"
                # Only store non-default values to keep JSON clean
                if elastic_k8s_ns != "elastic-system":
                    env_record["elastic_k8s_ns"]     = elastic_k8s_ns
                if elastic_k8s_secret != "gm-elasticsearch-es-elastic-user":
                    env_record["elastic_k8s_secret"] = elastic_k8s_secret
                if elastic_k8s_svc != "gm-elasticsearch-es-http":
                    env_record["elastic_k8s_svc"]    = elastic_k8s_svc
                if elastic_fields:
                    env_record["elastic_fields"]     = elastic_fields

            # Mode B — direct HTTP (only if elastic_url is explicitly provided)
            if elastic_url:
                env_record["elastic_url"]       = elastic_url.rstrip("/")
                env_record["elastic_verify_ssl"] = elastic_verify_ssl
                if elastic_api_key:
                    env_record["elastic_api_key"] = elastic_api_key
                elif elastic_user:
                    env_record["elastic_user"]     = elastic_user
                    env_record["elastic_password"] = elastic_password

            # Merge with existing env (don't wipe fields the caller didn't provide)
            existing = c["environments"].get(env, {})
            c["environments"][env] = {**existing, **env_record}

            # Auto-advance lifecycle stage if this env is newer
            current_idx = LIFECYCLE_ORDER.index(c.get("lifecycle_stage", "solution"))
            new_idx     = LIFECYCLE_ORDER.index(env)
            if new_idx > current_idx:
                c["lifecycle_stage"] = env
                log.info("Customer %s advanced to stage: %s", customer_id, env)

            self._flush()
            return _public(c)

    def remove_environment(self, customer_id: str, env: str) -> dict:
        with self._lock:
            c = self._data.get(customer_id)
            if not c:
                raise KeyError(f"Customer not found: {customer_id!r}")
            c["environments"].pop(env, None)
            self._flush()
            return _public(c)

    # ── Query helpers ─────────────────────────────────────────────────────

    def resolve_env(self, customer_id: str, requested_env: str | None
                    ) -> tuple[dict, dict | None]:
        """
        Return (customer_record, env_config_or_None).

        If requested_env is None, returns the highest configured environment.
        If no environments are configured (solution phase), env_config is None.
        """
        c = self._data.get(customer_id)
        if not c:
            raise KeyError(f"Unknown customer: {customer_id!r}. "
                           f"Known: {list(self._data.keys())}")
        envs = c.get("environments", {})

        if requested_env:
            env_cfg = envs.get(requested_env)
            return _public(c), env_cfg  # None if env not yet configured

        # Pick highest configured env in lifecycle order
        for stage in reversed(LIFECYCLE_ORDER[1:]):
            if stage in envs:
                return _public(c), envs[stage]

        return _public(c), None  # solution-only customer


def _normalize(c: dict) -> dict:
    """Ensure a customer dict has all required fields."""
    # Handle old flat k8s_* key style from customers.yml
    envs = c.get("environments", {})
    if not envs:
        # Try to migrate old single-env flat format
        bastion   = c.get("k8s_bastion",   (c.get("k8s") or {}).get("bastion",   ""))
        context   = c.get("k8s_context",   (c.get("k8s") or {}).get("context",   ""))
        namespace = c.get("k8s_namespace", (c.get("k8s") or {}).get("namespace", "default"))
        pod_map   = c.get("pod_map", {})
        old_env   = c.get("env", "prod")
        if bastion or context:
            envs = {
                old_env: {
                    "k8s_bastion":   bastion,
                    "k8s_context":   context,
                    "k8s_namespace": namespace,
                    "pod_map":       pod_map,
                }
            }
    return {
        "id":              c.get("id", ""),
        "name":            c.get("name", ""),
        "lifecycle_stage": c.get("lifecycle_stage", c.get("env", "solution")),
        "products":        c.get("products", []),
        "notes":           c.get("notes", ""),
        "aliases":         c.get("aliases", []),
        "environments":    envs,
    }


def _public(c: dict) -> dict:
    """Return a copy safe to send to callers (no mutation risk)."""
    import copy
    return copy.deepcopy(c)


def lifecycle_label(stage: str) -> str:
    """Human-readable label for each lifecycle stage."""
    return {
        "solution": "Solution design (no live cluster yet)",
        "dev":      "Development",
        "testing":  "Testing / QA",
        "staging":  "Staging / UAT",
        "prod":     "Production",
    }.get(stage, stage)
