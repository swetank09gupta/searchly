"""
Searchly Sync Scheduler

Two tracks — very different purposes:

  TRACK A — Deployment state snapshot (per customer, per env)
  ─────────────────────────────────────────────────────────────────────────────
  What:   `kubectl get deployments` → which image tag is running on which env
  When:   Every SYNC_DEPLOY_INTERVAL_MIN (default: 60 min)
  Why:    Stored in OpenSearch so the agent can answer "what version is in
          prod?" without a live kubectl call. Changes only on releases.
  Cost:   1 SSH call per configured env × number of customers.
          With 10 customers × 3 envs = 30 SSH calls. Takes ~30s total.

  NOT logs. Logs are never pre-indexed — they are fetched LIVE by the
  warehouse-agent's get_pod_logs tool when a user asks an operational
  question. This gives always-current data with zero storage cost.

  TRACK B — Shared knowledge (Jira + Confluence + GitHub repos)
  ─────────────────────────────────────────────────────────────────────────────
  What:   All Jira tickets, Confluence pages, code from 168+ repos.
  When:   Every SYNC_FULL_INTERVAL_HOURS (default: 4h). Incremental —
          git ls-remote checks HEAD before cloning; unchanged repos are
          skipped. Jira/Confluence: all issues/pages re-fetched (Atlassian
          doesn't expose a reliable delta API, but the 100-result pages
          are fast with the polite 150ms delay between calls).
  Cost:   First run: 1-3h (168 repo clones + Jira + Confluence).
          Subsequent runs: 5-15 min (only changed repos get cloned).

Startup sequence:
  1. Wait for search-api to be reachable.
  2. Sleep SYNC_STARTUP_WAIT_SEC (default 90s) — lets OpenSearch+indexer settle.
  3. Run Track B (full shared sync) immediately — builds the knowledge base.
  4. Run Track A (deployment state) immediately — seeds version info.
  5. Enter the two-track loop.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

SEARCHLY_URL      = os.getenv("SEARCHLY_URL",              "http://search-api:8081")
DEPLOY_INTERVAL_S = int(os.getenv("SYNC_DEPLOY_INTERVAL_MIN",  "60"))  * 60
FULL_INTERVAL_S   = int(os.getenv("SYNC_FULL_INTERVAL_HOURS",  "4"))   * 3600
STARTUP_WAIT_S    = int(os.getenv("SYNC_STARTUP_WAIT_SEC",     "90"))


def _wait_for_searchly():
    url = f"{SEARCHLY_URL}/actuator/health"
    log.info("Waiting for search-api at %s ...", url)
    while True:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                log.info("search-api ready (status %d)", r.status_code)
                return
        except Exception as exc:
            log.debug("Not ready yet: %s", exc)
        time.sleep(10)


def _run(label: str, *args: str) -> bool:
    """Run sync.py with the given args. Returns True on success."""
    cmd = [sys.executable, "sync.py", *args]
    log.info("▶ [%s] %s", label, " ".join(args))
    t0 = time.monotonic()
    r = subprocess.run(cmd, cwd=os.path.dirname(__file__) or ".")
    elapsed = time.monotonic() - t0
    if r.returncode == 0:
        log.info("◀ [%s] done in %.0fs", label, elapsed)
        return True
    else:
        log.warning("◀ [%s] exit=%d in %.0fs", label, r.returncode, elapsed)
        return False


def run():
    _wait_for_searchly()

    log.info("Waiting %ds for services to settle before first sync...", STARTUP_WAIT_S)
    time.sleep(STARTUP_WAIT_S)

    # ── Initial runs ──────────────────────────────────────────────────────────
    log.info("=== INITIAL: Full shared sync (Jira + Confluence + repos) ===")
    _run("shared", "--only", "shared")

    log.info("=== INITIAL: Deployment state (all customers, all envs) ===")
    _run("deploy", "--only", "all-customers-deploy")

    last_full   = time.monotonic()
    last_deploy = time.monotonic()

    log.info(
        "Scheduler running. Deploy state every %dmin, full sync every %dh.",
        DEPLOY_INTERVAL_S // 60,
        FULL_INTERVAL_S   // 3600,
    )

    TICK = 30  # wake up every 30s to check timers (responsive to SIGTERM)
    while True:
        time.sleep(TICK)
        now = time.monotonic()

        if now - last_deploy >= DEPLOY_INTERVAL_S:
            log.info("--- Deployment state refresh ---")
            _run("deploy", "--only", "all-customers-deploy")
            last_deploy = time.monotonic()

        if now - last_full >= FULL_INTERVAL_S:
            log.info("=== Full shared sync ===")
            _run("shared", "--only", "shared")
            last_full = time.monotonic()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Scheduler stopped.")
