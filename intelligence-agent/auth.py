"""
API Key authentication and per-customer tenant isolation.

Keys are stored in {DATA_DIR}/auth_db.json alongside customers_db.json.

Key record:
  {
    "key":               "sk-go-...",
    "name":              "Searchly Admin",
    "allowed_customers": ["*"],        # "*" = all; or ["acme-corp", ...]
    "is_admin":          true,         # can create/delete other keys
    "created_at":        "2026-..."
  }

AUTH_ENABLED (env var, default "false"):
  false → all requests pass, key header ignored, UI hides key prompt
  true  → X-API-Key header required; resolved customer must be in allowed_customers

The admin key is seeded from ADMIN_API_KEY env var on first start.
If ADMIN_API_KEY is not set, a random key is generated and printed to the log once.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

AUTH_ENABLED: bool = os.getenv("AUTH_ENABLED", "false").lower() in ("true", "1", "yes")

# Sentinel returned when auth is disabled — grants all access
_ANON_KEY = {"name": "anonymous", "allowed_customers": ["*"], "is_admin": True}


class AuthDB:
    """
    Thread-safe JSON-backed API key store.

    Keys are never logged or returned in full after creation — the caller
    receives the plaintext key once (at creation time) and should store it.
    """

    def __init__(self, db_path: str):
        self._path = Path(db_path)
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db: dict = self._load()
        self._seed_admin()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text())
            except Exception as exc:
                log.warning("auth_db corrupted, resetting: %s", exc)
        return {"keys": []}

    def _save(self):
        """Write to disk atomically (write tmp → rename)."""
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._db, indent=2))
        tmp.replace(self._path)

    # ── Admin seed ────────────────────────────────────────────────────────────

    def _seed_admin(self):
        """Ensure at least one admin key exists."""
        admin_key = os.getenv("ADMIN_API_KEY", "")
        with self._lock:
            existing = {k["key"] for k in self._db["keys"]}
            if admin_key and admin_key not in existing:
                self._add_key(admin_key, "Admin (env)", ["*"], is_admin=True)
                log.info("Admin API key seeded from ADMIN_API_KEY env var")
            elif not self._db["keys"]:
                # No key at all — generate one and print it prominently
                generated = "sk-go-" + secrets.token_urlsafe(32)
                self._add_key(generated, "Auto-generated admin", ["*"], is_admin=True)
                log.warning(
                    "\n" + "=" * 60 + "\n"
                    "  GENERATED ADMIN API KEY (save this — shown only once):\n"
                    "  %s\n"
                    "  Set ADMIN_API_KEY in your .env to use a fixed key.\n"
                    + "=" * 60,
                    generated,
                )

    def _add_key(self, key: str, name: str, allowed_customers: list[str],
                 is_admin: bool = False):
        """Internal: add a key WITHOUT acquiring the lock (caller holds it)."""
        self._db["keys"].append({
            "key":               key,
            "name":              name,
            "allowed_customers": allowed_customers,
            "is_admin":          is_admin,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        })
        self._save()

    # ── Public API ────────────────────────────────────────────────────────────

    def validate(self, key: str | None) -> Optional[dict]:
        """
        Return the key record if valid, None if invalid.
        If AUTH_ENABLED=false, always returns the anonymous record.
        """
        if not AUTH_ENABLED:
            return _ANON_KEY
        if not key:
            return None
        with self._lock:
            for record in self._db["keys"]:
                if secrets.compare_digest(record["key"], key):
                    return record
        return None

    def is_customer_allowed(self, key_record: dict, customer_id: str | None) -> bool:
        """
        Return True if this key may access the given customer.

        None customer_id → True (clarification still in progress).
        """
        if not AUTH_ENABLED:
            return True
        if customer_id is None:
            return True
        allowed = key_record.get("allowed_customers", [])
        return "*" in allowed or customer_id in allowed

    def create_key(self, name: str, allowed_customers: list[str],
                   is_admin: bool = False) -> dict:
        """
        Create a new API key.
        Returns the FULL record including the plaintext key — this is the only
        time the full key is returned.
        """
        key = "sk-go-" + secrets.token_urlsafe(32)
        record = {
            "key":               key,
            "name":              name,
            "allowed_customers": allowed_customers,
            "is_admin":          is_admin,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._db["keys"].append(record)
            self._save()
        return record  # full key visible here only

    def list_keys(self) -> list[dict]:
        """Return all key records with the key value masked (first 12 chars + '...')."""
        with self._lock:
            return [
                {**r, "key": r["key"][:16] + "..."}
                for r in self._db["keys"]
            ]

    def delete_key(self, key_prefix: str):
        """
        Delete the key whose value starts with key_prefix.
        Raises KeyError if not found.
        Raises ValueError if trying to delete the last admin key.
        """
        with self._lock:
            match = next(
                (k for k in self._db["keys"] if k["key"].startswith(key_prefix)),
                None,
            )
            if not match:
                raise KeyError(f"No key with prefix {key_prefix!r}")
            admins = [k for k in self._db["keys"] if k.get("is_admin")]
            if match.get("is_admin") and len(admins) == 1:
                raise ValueError("Cannot delete the last admin key")
            self._db["keys"] = [k for k in self._db["keys"] if k is not match]
            self._save()
