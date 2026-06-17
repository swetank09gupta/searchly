"""
Searchly Connector — GreyOrange Multi-Product, Multi-Customer Intelligence

WHAT IT INDEXES
───────────────
  Shared knowledge (indexed once, all customers benefit):
    • All product repos (auto-discovered from GitHub org via products.yml)
    • Jira: AES, AE, GM, SRE, PA, PKE  (+ any others in JIRA_PROJECTS)
    • Confluence: CE, GME, DEV, GSP, AE, GRYMTTR  (+ CONFLUENCE_SPACES)

  Customer-specific knowledge (per customer, from customers.yml):
    • Live pod logs from their k8s cluster right now
    • Deployed versions (image tags) per product
    • Their env: prod / staging / dev

HOW SEARCH WORKS AFTER THIS
────────────────────────────
  GET /api/v1/search?q=why+is+operator+stuck&customer=sams-club-atlanta

  The LLM answer gets two layers of context:
    1. Shared: relevant code + Jira tickets + Confluence docs
    2. Customer: "they run GreyMatter v6.0.5 + OGA v2.3.1 in prod,
       operator-backend has 3 ERROR lines in the last 10 min, see AES-891
       which is a known allocation bug fixed in v2.3.2"

USAGE
─────
  python sync.py                           # sync everything
  python sync.py --only shared             # Jira + Confluence + all repos
  python sync.py --only repos              # code repos only
  python sync.py --only jira               # Jira only
  python sync.py --only confluence         # Confluence only
  python sync.py --only customer <id>      # logs + versions for one customer
  python sync.py --only all-customers      # logs + versions for ALL customers
  python sync.py --list-repos              # list repos from GitHub org (for products.yml)
  python sync.py --dry-run                 # print what would be indexed
"""

import argparse
import ast
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import logging

import time

import requests
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate-limited HTTP helper
# ---------------------------------------------------------------------------

def _api_get(url: str, *, auth=None, headers=None, params=None,
             timeout: int = 30,
             max_retries: int = 6,
             page_delay: float = 0.15,
             rate_limiter: "_RateLimiter | None" = None) -> requests.Response:
    """
    GET with automatic retry and rate-limit handling.

    Handles:
      - 429 Too Many Requests → respects Retry-After header (or waits 60s)
      - 5xx Server Error      → exponential back-off (2, 4, 8, 16, 32s + jitter)
      - Network errors        → same back-off as 5xx

    rate_limiter: when provided, its wait() replaces the fixed page_delay sleep.
    page_delay: minimum seconds to wait between every call (polite pacing).
                Atlassian Cloud allows ~10 req/s per token; 0.15s keeps us at ~6/s.
    """
    if rate_limiter is not None:
        rate_limiter.wait()
    else:
        time.sleep(page_delay)   # polite pacing — always wait before firing

    backoff = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, auth=auth, headers=headers,
                             params=params, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                raise
            wait = backoff + (backoff * 0.1 * attempt)
            log.warning("Network error (attempt %d/%d): %s — retry in %.0fs",
                        attempt, max_retries, exc, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "60"))
            retry_after = min(retry_after, 120)   # cap at 2 min
            log.warning("Rate limited (429) — waiting %ds (attempt %d/%d)",
                        retry_after, attempt, max_retries)
            time.sleep(retry_after + 1)
            continue

        if r.status_code >= 500 and attempt < max_retries:
            wait = backoff + (backoff * 0.1 * attempt)
            log.warning("Server error %d (attempt %d/%d) — retry in %.0fs",
                        r.status_code, attempt, max_retries, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        return r   # 2xx, 3xx, 4xx (non-429) — caller inspects status

    return r  # exhausted retries, return last response

SCRIPT_DIR = Path(__file__).parent

# State file persisted on the sync-state Docker volume (/data in container).
# Falls back to script dir for local runs outside Docker.
_STATE_DIR = Path(os.environ.get("SYNC_STATE_DIR", "/data"))
_STATE_DIR.mkdir(parents=True, exist_ok=True)
SYNC_STATE_FILE = _STATE_DIR / ".sync_state.json"


_STATE_LOCK = threading.Lock()


def _load_sync_state() -> dict:
    with _STATE_LOCK:
        if SYNC_STATE_FILE.exists():
            try:
                return json.loads(SYNC_STATE_FILE.read_text())
            except Exception:
                pass
        return {}


def _save_sync_state(state: dict):
    with _STATE_LOCK:
        try:
            SYNC_STATE_FILE.write_text(json.dumps(state, indent=2))
        except Exception as e:
            log.warning("Could not save sync state: %s", e)


def _update_state(key: str, value) -> None:
    """Atomically load → set key → save. Safe for concurrent workers."""
    with _STATE_LOCK:
        try:
            state = {}
            if SYNC_STATE_FILE.exists():
                try:
                    state = json.loads(SYNC_STATE_FILE.read_text())
                except Exception:
                    pass
            state[key] = value
            SYNC_STATE_FILE.write_text(json.dumps(state, indent=2))
        except Exception as e:
            log.warning("Could not update sync state key %s: %s", key, e)


class _RateLimiter:
    """Token-bucket rate limiter. Thread-safe."""

    def __init__(self, rate: float):
        self._rate = rate          # max requests per second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        min_interval = 1.0 / self._rate
        with self._lock:
            now = time.monotonic()
            gap = self._last + min_interval - now
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()


_ATLASSIAN_RL = _RateLimiter(7)   # Atlassian Cloud: ~7 req/s per token
_GITHUB_RL    = _RateLimiter(5)   # GitHub: ~5 req/s (conservative)

# CPU thresholds (as fraction of core count via os.getloadavg()[0] / ncpu).
# Override with env vars if the VM's services have a different baseline load.
_CPU_HIGH = float(os.environ.get("SYNC_CPU_HIGH", "0.80"))  # load/cores above → shed a worker
_CPU_LOW  = float(os.environ.get("SYNC_CPU_LOW",  "0.55"))  # load/cores below → add a worker


class _AdaptivePool:
    """
    Adaptive concurrency pool using os.getloadavg() (no extra deps).

    Starts at 1 worker.  A background thread samples CPU load every
    POLL_S seconds and adjusts the active slot count:
      - load/cores > cpu_high → reduce by 1 (down to min=1)
      - load/cores < cpu_low  → increase by 1 (up to max_workers)

    Mechanics:
      Increasing — release() on the semaphore immediately adds a slot.
      Decreasing — increment a drain counter; the next worker that
                   finishes absorbs its slot without returning it.
    This means a reduction takes effect as soon as one in-flight task
    completes, without interrupting running work.

    API rate limiters (_ATLASSIAN_RL / _GITHUB_RL) still cap throughput
    regardless of how many workers are running, so over-provisioning
    workers just means they queue behind the rate limiter — harmless.
    """

    POLL_S = 8.0   # CPU sample interval

    def __init__(self, name: str, max_workers: int = 8,
                 cpu_high: float = _CPU_HIGH, cpu_low: float = _CPU_LOW):
        self._name      = name
        self._max       = max_workers
        self._cpu_high  = cpu_high
        self._cpu_low   = cpu_low
        self._ncpu      = max(os.cpu_count() or 1, 1)
        self._target    = 1
        self._sem       = threading.Semaphore(1)   # starts at 1 slot
        self._drain     = 0
        self._lock      = threading.Lock()

    def _cpu_load(self) -> float:
        try:
            return os.getloadavg()[0] / self._ncpu
        except (AttributeError, OSError):
            return 0.5   # Windows / unknown — assume mid

    def _adjust(self) -> None:
        load = self._cpu_load()
        with self._lock:
            if load > self._cpu_high and self._target > 1:
                self._target -= 1
                self._drain  += 1
                log.info("AdaptivePool[%s]: load=%.0f%% → %d workers",
                         self._name, load * 100, self._target)
            elif load < self._cpu_low and self._target < self._max:
                self._target += 1
                self._sem.release()   # slot available immediately
                log.info("AdaptivePool[%s]: load=%.0f%% → %d workers",
                         self._name, load * 100, self._target)

    def _acquire(self) -> None:
        self._sem.acquire()

    def _release(self) -> None:
        with self._lock:
            if self._drain > 0:
                self._drain -= 1   # absorb slot — do not put it back
            else:
                self._sem.release()

    def map(self, fn, items: list) -> list:
        """
        Run fn over every item with adaptive concurrency.

        Uses ThreadPoolExecutor(max_workers=self._max) so at most max_workers
        OS threads ever exist — NOT one thread per item.  The semaphore gates
        actual concurrency from 1 up to max_workers as CPU load allows.
        """
        items = list(items)
        if not items:
            return []

        stop = threading.Event()

        def _watch():
            while not stop.wait(self.POLL_S):
                self._adjust()

        watcher = threading.Thread(target=_watch, daemon=True,
                                   name=f"pool-{self._name}-watcher")
        watcher.start()

        results = [None] * len(items)

        def _task(args):
            idx, item = args
            self._acquire()
            try:
                results[idx] = fn(item)
            except Exception as exc:
                log.error("AdaptivePool[%s] item %d failed: %s", self._name, idx, exc)
            finally:
                self._release()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max,
                thread_name_prefix=self._name) as pool:
            list(pool.map(_task, enumerate(items)))

        stop.set()
        return results


# ---------------------------------------------------------------------------
# Branch filtering + customer-env discovery
# ---------------------------------------------------------------------------

# Branches that carry meaningful, stable knowledge for the knowledge base.
# feature/*, dev/*, bugfix/*, and short-lived CI branches are excluded — they
# are orders of magnitude more numerous than signal branches and contain
# work-in-progress noise rather than stable knowledge.
_SIGNAL_BRANCH_PATTERNS = [
    "develop",
    "release/*",
    "release-*",
    "hotfix/*",
    "hotfix-*",
]

# Pre-compiled regex matching any branch name we will NOT index on app repos.
# Anything not matching a signal pattern and not being main/master/HEAD is noise.
_NOISE_BRANCH_RE = re.compile(
    r"^(feature|feat|dev|bugfix|fix|chore|deps|dependabot|renovate|ci|test|wip|tmp|temp)/",
    re.IGNORECASE,
)


def _signal_branches(extra: list[str]) -> list[str]:
    """
    Return the canonical signal-branch list merged with any caller-supplied extras.
    Always includes the hardcoded safe set; caller extras are appended if not already present.
    """
    base = list(_SIGNAL_BRANCH_PATTERNS)
    for p in extra:
        if p and p not in base:
            base.append(p)
    return base


# Known environment suffixes used in deployment repo branch names.
# Branch convention: {customer-id}-{env}
#   sams-club-prod          → customer=sams-club,         env=prod
#   sams-club-atlanta-prod  → customer=sams-club-atlanta,  env=prod
#   sodimac-colombia-staging → customer=sodimac-colombia,  env=staging
# Location is part of the customer ID, NOT a separate field.
_BRANCH_ENV_SUFFIXES = {
    "prod":        "prod",
    "production":  "prod",
    "staging":     "staging",
    "uat":         "staging",
    "preprod":     "staging",
    "pre":         "staging",   # handles "pre-prod" split as ["pre","prod"] edge case
    "dev":         "dev",
    "development": "dev",
    "testing":     "testing",
    "test":        "testing",
    "qa":          "testing",
}


def _parse_customer_branch(branch_name: str) -> tuple[str, str] | None:
    """
    Parse a deployment-repo branch name into (customer_id, env).

    Returns None if the branch doesn't look like a customer-env branch
    (e.g. main, develop, release/*, hotfix/* — those are code branches).
    """
    parts = branch_name.replace("/", "-").split("-")
    if len(parts) < 2:
        return None
    env = _BRANCH_ENV_SUFFIXES.get(parts[-1].lower())
    if not env:
        return None
    customer_id = "-".join(parts[:-1]).lower().strip("-")
    if not customer_id:
        return None
    return customer_id, env


def _customer_name_from_id(customer_id: str) -> str:
    """Turn a kebab-case ID into a display name: sams-club-atlanta → Sams Club Atlanta."""
    return " ".join(w.capitalize() for w in customer_id.replace("-", " ").split())


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Config:
    jira_url: str = ""
    jira_email: str = ""
    jira_token: str = ""
    jira_projects: list = field(default_factory=list)

    confluence_url: str = ""
    confluence_email: str = ""
    confluence_token: str = ""
    confluence_spaces: list = field(default_factory=list)

    github_token: str = ""
    github_org: str = ""
    # Signal branches indexed on every app repo (in addition to the default branch).
    # Hardcoded — we never want feature/* or dev/* branches (too many, too noisy).
    # The GIT_BRANCHES env var can ADD to this list but cannot remove from it.
    git_branches: list = field(default_factory=list)
    # Deployment/DevOps repos whose branches represent live customer environments.
    # All non-feature branches are indexed; branch names are parsed for customer discovery.
    # Example: greyorange/greymatter-deployment,greyorange/pick-assist-helm-charts
    devops_repos: list = field(default_factory=list)
    # Intelligence agent URL — used to auto-register customers/envs discovered
    # from deployment repo branches. Leave blank to skip auto-registration.
    agent_url: str = ""

    searchly_url: str = "http://localhost:8081"
    searchly_tenant: str = "default"
    searchly_user: str = "sync-bot"

    batch_size: int = 5
    atlassian_workers: int = 3   # parallel Jira projects / Confluence spaces
    github_workers: int = 1      # parallel GitHub repo indexing + KG (keep 1 to avoid OOM on clone)
    jira_max_results: int = 1000
    confluence_max_results: int = 500
    git_max_file_kb: int = 500
    git_include_extensions: set = field(default_factory=lambda: {
        ".py", ".java", ".md", ".yml", ".yaml", ".json", ".txt", ".sh", ".ts", ".js"
    })
    k8s_log_lines: int = 500
    dry_run: bool = False
    force: bool = False  # if True, ignore incremental state and re-index everything


def load_config(args) -> Config:
    def opt(k, default=""):
        return os.getenv(k, default).strip()
    def csv(k, arg_val=""):
        raw = arg_val or opt(k)
        return [x.strip() for x in raw.split(",") if x.strip()] if raw else []

    return Config(
        jira_url=opt("JIRA_URL").rstrip("/"),
        jira_email=opt("JIRA_EMAIL"),
        jira_token=opt("JIRA_TOKEN"),
        jira_projects=csv("JIRA_PROJECTS"),

        confluence_url=opt("CONFLUENCE_URL", opt("JIRA_URL")).rstrip("/"),
        confluence_email=opt("CONFLUENCE_EMAIL", opt("JIRA_EMAIL")),
        confluence_token=opt("CONFLUENCE_TOKEN", opt("JIRA_TOKEN")),
        confluence_spaces=csv("CONFLUENCE_SPACES"),

        github_token=opt("GIT_TOKEN"),
        github_org=opt("GITHUB_ORG"),
        git_branches=_signal_branches(csv("GIT_BRANCHES")),
        devops_repos=csv("DEVOPS_REPOS"),
        agent_url=opt("AGENT_URL", "http://intelligence-agent:8084").rstrip("/"),

        searchly_url=opt("SEARCHLY_URL", "http://localhost:8081").rstrip("/"),
        searchly_tenant=opt("SEARCHLY_TENANT", "default"),
        searchly_user=opt("SEARCHLY_USER", "sync-bot"),

        batch_size=int(opt("SYNC_BATCH_SIZE", "5")),
        atlassian_workers=int(opt("SYNC_ATLASSIAN_WORKERS", "3")),
        github_workers=int(opt("SYNC_GITHUB_WORKERS", "1")),
        jira_max_results=int(opt("JIRA_MAX_RESULTS", "1000")),
        confluence_max_results=int(opt("CONFLUENCE_MAX_RESULTS", "500")),
        git_max_file_kb=int(opt("GIT_MAX_FILE_KB", "500")),
        git_include_extensions=set(
            x.strip() for x in opt("GIT_INCLUDE_EXTENSIONS",
                ".py,.java,.md,.yml,.yaml,.json,.txt,.sh,.ts,.js").split(",") if x.strip()
        ),
        k8s_log_lines=int(opt("K8S_LOG_LINES", "500")),
        dry_run=args.dry_run,
        force=getattr(args, "force", False),
    )


def load_products() -> dict:
    path = SCRIPT_DIR / "products.yml"
    if not path.exists():
        log.warning("products.yml not found at %s", path)
        return {}
    return yaml.safe_load(path.read_text())


def load_customers() -> list:
    path = SCRIPT_DIR / "customers.yml"
    if not path.exists():
        log.warning("customers.yml not found at %s", path)
        return []
    data = yaml.safe_load(path.read_text())
    return data.get("customers", [])


# ---------------------------------------------------------------------------
# Document splitting
# ---------------------------------------------------------------------------

# Max chars per document posted to the API. Kafka messages that carry more
# than this cause the Java indexer to OOM during deserialization even before
# any application-level truncation can run.
# 80k chars ≈ 20k words — more than enough for any single coherent page section.
_DOC_SPLIT_CHARS = 80_000


def split_doc(base_doc: dict) -> list:
    """Split a document whose content exceeds _DOC_SPLIT_CHARS into multiple
    docs, each within the limit. The full content is preserved across parts.
    Parts are split on paragraph boundaries where possible.

    Each part inherits the base metadata and gets a '(part N/M)' title suffix
    so the RAG agent knows they belong together.
    """
    content = base_doc.get("content", "")
    if len(content) <= _DOC_SPLIT_CHARS:
        return [base_doc]

    # Split on double-newlines (paragraph boundaries) to avoid mid-sentence cuts
    paragraphs = content.split("\n\n")
    parts, current = [], []
    current_len = 0
    for para in paragraphs:
        if current_len + len(para) > _DOC_SPLIT_CHARS and current:
            parts.append("\n\n".join(current))
            current, current_len = [], 0
        current.append(para)
        current_len += len(para) + 2  # +2 for the \n\n separator
    if current:
        parts.append("\n\n".join(current))

    total = len(parts)
    base_title = base_doc["title"]
    docs = []
    for i, part_content in enumerate(parts, 1):
        doc = {**base_doc, "content": part_content,
               "title": f"{base_title} (part {i}/{total})"}
        docs.append(doc)
    return docs


# ---------------------------------------------------------------------------
# Searchly poster
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Knowledge Graph poster
# ---------------------------------------------------------------------------

class KgPoster:
    """Posts entities and relationships to the Searchly KG API (/api/v1/kg/*)."""

    def __init__(self, cfg: Config):
        self.entity_url       = f"{cfg.searchly_url}/api/v1/kg/entity"
        self.relationship_url = f"{cfg.searchly_url}/api/v1/kg/relationship"
        self.headers = {
            "Content-Type": "application/json",
            "X-Tenant-Id":  cfg.searchly_tenant,
            "X-User-Id":    cfg.searchly_user,
        }
        self.dry_run  = cfg.dry_run
        self._ok      = 0
        self._failed  = 0

    def upsert_entity(self, entity_type: str, entity_id: str,
                      name: str, properties: dict | None = None) -> bool:
        if self.dry_run:
            log.debug("[kg dry-run] entity %s/%s", entity_type, entity_id)
            self._ok += 1
            return True
        try:
            r = requests.post(self.entity_url, headers=self.headers, json={
                "entity_type": entity_type,
                "entity_id":   entity_id,
                "name":        name,
                "properties":  properties or {},
            }, timeout=10)
            if r.status_code in (200, 201):
                self._ok += 1
                return True
            log.warning("KG entity %s/%s HTTP %d: %s", entity_type, entity_id, r.status_code, r.text[:100])
            self._failed += 1
            return False
        except Exception as e:
            log.warning("KG entity error %s/%s: %s", entity_type, entity_id, e)
            self._failed += 1
            return False

    def upsert_relationship(self, from_type: str, from_id: str, relation: str,
                            to_type: str, to_id: str, properties: dict | None = None) -> bool:
        if self.dry_run:
            log.debug("[kg dry-run] rel %s/%s -[%s]-> %s/%s", from_type, from_id, relation, to_type, to_id)
            self._ok += 1
            return True
        try:
            r = requests.post(self.relationship_url, headers=self.headers, json={
                "from_type":  from_type,
                "from_id":    from_id,
                "relation":   relation,
                "to_type":    to_type,
                "to_id":      to_id,
                "properties": properties or {},
            }, timeout=10)
            if r.status_code in (200, 201):
                self._ok += 1
                return True
            log.warning("KG rel %s/%s -[%s]-> %s/%s HTTP %d", from_type, from_id, relation, to_type, to_id, r.status_code)
            self._failed += 1
            return False
        except Exception as e:
            log.warning("KG rel error: %s", e)
            self._failed += 1
            return False

    def summary(self):
        log.info("KG ── ok: %d  failed: %d", self._ok, self._failed)


# Extracts a GitHub PR number from a remote link URL.
# Handles: github.com/{org}/{repo}/pull/{number}
_GITHUB_PR_RE = re.compile(r"github\.com/[^/]+/([^/]+)/pull/(\d+)", re.IGNORECASE)


class SearchlyPoster:
    def __init__(self, cfg: Config):
        self.url = f"{cfg.searchly_url}/api/v1/documents"
        self.purge_url = f"{cfg.searchly_url}/api/v1/admin/sync/purge-stale"
        self.headers = {
            "Content-Type": "application/json",
            "X-Tenant-Id": cfg.searchly_tenant,
            "X-User-Id": cfg.searchly_user,
        }
        self.dry_run = cfg.dry_run
        self.ok = 0
        self.failed = 0

    def post(self, doc: dict) -> bool:
        if self.dry_run:
            log.info("[dry-run] %s", doc["title"][:80])
            self.ok += 1
            return True
        try:
            r = requests.post(self.url, headers=self.headers, json=doc, timeout=15)
            if r.status_code in (200, 201, 202):
                self.ok += 1
                return True
            log.warning("POST %d '%s': %s", r.status_code, doc["title"][:60], r.text[:200])
            self.failed += 1
            return False
        except Exception as e:
            log.warning("POST error '%s': %s", doc["title"][:60], e)
            self.failed += 1
            return False

    def post_batch(self, docs: list, workers: int = 5):
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(self.post, docs))

    def purge_stale(self, source_type: str, sync_started_at: str):
        """Delete source docs not seen since sync_started_at (tombstone support, P0.3)."""
        if self.dry_run:
            log.info("[dry-run] purge-stale source_type=%s cutoff=%s", source_type, sync_started_at)
            return
        try:
            r = requests.post(
                self.purge_url,
                headers=self.headers,
                json={"source_type": source_type, "sync_started_at": sync_started_at},
                timeout=60,
            )
            if r.status_code == 200:
                data = r.json()
                log.info("Purged %d stale %s docs", data.get("purged", 0), source_type)
            else:
                log.warning("purge-stale HTTP %d: %s", r.status_code, r.text[:200])
        except Exception as e:
            log.warning("purge-stale error: %s", e)

    def summary(self):
        log.info("Done ── indexed: %d  failed: %d", self.ok, self.failed)


# ---------------------------------------------------------------------------
# GitHub repo discovery
# ---------------------------------------------------------------------------

class GitHubDiscovery:
    """Lists all repos for a GitHub org, optionally filtered by topics or name patterns."""

    def __init__(self, org: str, token: str = ""):
        self.org = org
        self.headers = {"Accept": "application/vnd.github+json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def list_repos(self) -> list[dict]:
        """Returns list of {name, clone_url, topics, description}."""
        repos, page = [], 1
        while True:
            r = _api_get(
                f"https://api.github.com/orgs/{self.org}/repos",
                headers=self.headers,
                params={"per_page": 100, "page": page, "type": "all"},
                timeout=30,
            )
            # Warn if approaching GitHub rate limit (5000 req/h authenticated)
            remaining = r.headers.get("X-RateLimit-Remaining")
            if remaining and int(remaining) < 50:
                reset_at = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(0, reset_at - int(time.time())) + 5
                log.warning("GitHub rate limit low (%s remaining) — sleeping %ds", remaining, wait)
                time.sleep(wait)
            if r.status_code == 404:
                log.warning("GitHub org '%s' not found or no access", self.org)
                return []
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            repos.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return [{"name": r["name"], "clone_url": r["clone_url"],
                 "ssh_url": r["ssh_url"], "topics": r.get("topics", []),
                 "description": r.get("description", "")} for r in repos]

    def print_repos(self):
        repos = self.list_repos()
        print(f"\n{'Repo':<40} {'Topics'}")
        print("-" * 70)
        for r in sorted(repos, key=lambda x: x["name"]):
            print(f"{r['name']:<40} {', '.join(r['topics'])}")
        print(f"\nTotal: {len(repos)} repos")


# ---------------------------------------------------------------------------
# Code repo indexer
# ---------------------------------------------------------------------------

class RepoIndexer:
    """Clones repos from GitHub and indexes code files as searchly docs."""

    CHUNK_CHARS = 2000
    OVERLAP_CHARS = 200

    # Extensions indexed for low-priority repos (OSS forks, etc.)
    DOCS_ONLY_EXTS = {".md", ".rst", ".txt"}

    # Regex matching path components that indicate an ADR or architecture doc.
    # Checked against the full relative path (forward-slash normalised).
    ADR_PATHS = re.compile(
        r"(?:^|/)"
        r"(?:adr|adrs|decisions|rfcs|proposals|docs/adr|architecture)"
        r"(?:/|$)",
        re.IGNORECASE,
    )

    # Stem keywords that identify a file as an architecture / design doc.
    _ADR_STEM_RE = re.compile(
        r"(?:architecture|lld|hld|design[_-]doc|adr-|rfc[_-]|decisions)",
        re.IGNORECASE,
    )

    def _doc_type(self, rel_path: str) -> str | None:
        """Return 'adr' or 'architecture' if *rel_path* is a design/ADR doc, else None."""
        norm = rel_path.replace("\\", "/")
        if self.ADR_PATHS.search(norm):
            return "adr"
        stem = Path(rel_path).stem
        if self._ADR_STEM_RE.search(stem):
            return "architecture"
        return None

    def __init__(self, cfg: Config, products: dict):
        self.cfg = cfg
        self.products = products
        self.github_org = products.get("github_org", "")
        self.include_exts = cfg.git_include_extensions
        self.max_file_kb = cfg.git_max_file_kb
        self._skip_repos: set = set(products.get("skip_repos", []))
        self._sync_state: dict = {} if cfg.force else _load_sync_state()

    def _build_repo_product_map(self) -> dict[str, tuple[str, str]]:
        """
        Build a mapping of repo_name → (product_name, priority) from products.yml.

        Only covers repos explicitly listed under a product's `repos:` key.
        Repos not listed here get product="unclassified" and priority="high".
        """
        mapping: dict[str, tuple[str, str]] = {}
        for product_name, product_cfg in (self.products.get("products") or {}).items():
            priority = product_cfg.get("priority", "high")
            for repo_name in product_cfg.get("repos", []):
                mapping[repo_name] = (product_name, priority)
        return mapping

    def _discover_repos(self) -> list[dict]:
        """
        Auto-discover all repos in the GitHub org via the API.

        Returns list of {name, clone_url} dicts.
        Falls back to an empty list if no org/token is configured or the API
        call fails (e.g. rate limit, org not found).
        """
        if not self.github_org:
            return []
        token = self.cfg.github_token
        try:
            discovery = GitHubDiscovery(self.github_org, token=token)
            repos = discovery.list_repos()
            log.info("GitHub org '%s': discovered %d repos", self.github_org, len(repos))
            return repos
        except Exception as exc:
            log.warning("GitHub repo discovery failed: %s — falling back to products.yml only", exc)
            return []

    def index_all_products(self, poster: SearchlyPoster, workers: int = 1):
        """
        Index all repos, combining two sources:

        1. Auto-discovery — if `github_org` is set and `GIT_TOKEN` is configured,
           fetches ALL repos from the org via the GitHub API.  No manual listing
           required in products.yml.

        2. products.yml categorisation (optional) — if a repo appears under a
           product's `repos:` key, it is tagged with that product name and its
           priority setting.  Repos not listed in products.yml get
           product="unclassified" and are still indexed.

        Either source alone is sufficient:
          - github_org set, no products: indexes everything as "unclassified"
          - products with explicit repos, no github_org: indexes exactly those repos
          - both: org-wide discovery + correct product tags for listed repos

        Incremental: each repo's remote HEAD is checked before cloning.
        If the HEAD SHA matches .sync_state.json, the repo is skipped.
        Use --force to bypass and re-index everything.
        """
        # ── Step 1: build repo → product mapping from products.yml ───────────
        repo_product_map = self._build_repo_product_map()

        # ── Step 2: collect the full repo work list ───────────────────────────
        work: dict[str, tuple[str, str]] = {}   # repo_name → (product, priority)
        for repo_name, (product, priority) in repo_product_map.items():
            if repo_name not in self._skip_repos:
                work[repo_name] = (product, priority)

        for repo in self._discover_repos():
            name = repo["name"]
            if name in self._skip_repos:
                continue
            if name not in work:
                topics = repo.get("topics", [])
                product = topics[0] if topics else "unclassified"
                work[name] = (product, "high")

        if not work:
            log.warning(
                "No repos to index. Set github_org in products.yml or add repos: entries."
            )
            return

        log.info("Indexing %d repos total (%d from products.yml, %d auto-discovered) workers=%d",
                 len(work), len(repo_product_map),
                 len(work) - len(repo_product_map), workers)

        branch_patterns = self.cfg.git_branches
        log.info("Signal branch patterns: default + %s", branch_patterns)

        # ── Step 3: index each app repo (default branch + signal branches) ──────
        def _index_one_repo(item):
            repo_name, (product_name, priority) = item
            exts = self.include_exts if priority != "low" else self.DOCS_ONLY_EXTS
            clone_url = self._resolve_url(repo_name)
            auth_url  = self._auth_url(clone_url)

            docs, new_sha, state_key = self._index_repo(
                clone_url, repo_name, product_name, exts=exts, branch=None
            )
            if new_sha:
                _update_state(state_key, new_sha)
            if docs:
                poster.post_batch(docs, workers=self.cfg.batch_size)

            _GITHUB_RL.wait()
            for branch_name, _ in self._resolve_branches(auth_url, branch_patterns):
                docs, new_sha, state_key = self._index_repo(
                    clone_url, repo_name, product_name,
                    exts=exts, branch=branch_name
                )
                if new_sha:
                    _update_state(state_key, new_sha)
                if docs:
                    poster.post_batch(docs, workers=self.cfg.batch_size)

        _AdaptivePool("git", max_workers=workers).map(
            _index_one_repo, sorted(work.items()))

        # ── Step 4: DevOps / deployment repos ────────────────────────────────────
        if self.cfg.devops_repos:
            log.info("Syncing %d DevOps repos: %s", len(self.cfg.devops_repos),
                     self.cfg.devops_repos)
            # DevOps repos update state internally via _update_state
            self._index_devops_repos(self.cfg.devops_repos, poster, {})

    def _resolve_url(self, repo_name_or_url: str) -> str:
        if repo_name_or_url.startswith("http") or repo_name_or_url.startswith("git@"):
            return repo_name_or_url
        if self.github_org:
            return f"https://github.com/{self.github_org}/{repo_name_or_url}"
        return repo_name_or_url

    def _resolve_branches(self, auth_url: str, patterns: list[str]) -> list[tuple[str, str]]:
        """
        Resolve branch patterns against the remote via git ls-remote.

        Each pattern can be:
          "develop"    — exact branch name
          "release/*"  — glob, matches all release/v1.0, release/v2.0, etc.
          "hotfix/*"   — glob

        Returns a deduplicated list of (branch_name, sha) tuples.
        New branches matching a glob are discovered automatically on each sync.
        """
        found: dict[str, str] = {}
        for pattern in patterns:
            ref_pattern = f"refs/heads/{pattern}"
            try:
                r = subprocess.run(
                    ["git", "ls-remote", auth_url, ref_pattern],
                    capture_output=True, text=True, timeout=15
                )
                if r.returncode != 0 or not r.stdout.strip():
                    log.debug("  ls-remote: no match for %s", pattern)
                    continue
                for line in r.stdout.strip().splitlines():
                    parts = line.split()
                    if len(parts) == 2:
                        sha, ref = parts
                        branch_name = ref.replace("refs/heads/", "")
                        found[branch_name] = sha
            except Exception as exc:
                log.warning("ls-remote failed for pattern '%s': %s", pattern, exc)
        return list(found.items())

    def _list_all_branches(self, auth_url: str) -> list[tuple[str, str]]:
        """
        Return all branches in a remote repo as (branch_name, sha) tuples,
        excluding noise branches (feature/*, dev/*, dependabot/*, etc.).
        Used for DevOps repos where every non-noise branch may be a customer env.
        """
        try:
            r = subprocess.run(
                ["git", "ls-remote", "--heads", auth_url],
                capture_output=True, text=True, timeout=30
            )
            if r.returncode != 0:
                return []
            results = []
            for line in r.stdout.strip().splitlines():
                parts = line.split()
                if len(parts) != 2:
                    continue
                sha, ref = parts
                branch_name = ref.replace("refs/heads/", "")
                if not _NOISE_BRANCH_RE.match(branch_name):
                    results.append((branch_name, sha))
            return results
        except Exception as exc:
            log.warning("ls-remote --heads failed for %s: %s", auth_url, exc)
            return []

    def _index_devops_repos(self, repo_specs: list[str], poster, updated_state: dict = None):
        """
        Index DevOps/deployment repos.  Unlike app repos, every non-noise branch
        is indexed — each branch typically corresponds to one customer environment.

        repo_specs can be:
          - "repo-name"              → resolved via github_org
          - "org/repo-name"          → used as-is under github.com
          - "https://github.com/..."  → full URL

        Branch naming convention: {customer-id}-{env}
          sams-club-prod          → (sams-club, prod)
          sams-club-atlanta-prod  → (sams-club-atlanta, prod)
          sodimac-colombia-staging → (sodimac-colombia, staging)
        Location tokens (atlanta, colombia, …) are part of the customer ID.
        """
        agent_url = self.cfg.agent_url

        for spec in repo_specs:
            # Resolve to a clone URL
            if spec.startswith("http") or spec.startswith("git@"):
                clone_url = spec
            elif "/" in spec:
                clone_url = f"https://github.com/{spec}"
            elif self.github_org:
                clone_url = f"https://github.com/{self.github_org}/{spec}"
            else:
                log.warning("Cannot resolve devops repo '%s' — set GITHUB_ORG or use org/repo", spec)
                continue

            repo_name = clone_url.rstrip("/").split("/")[-1].removesuffix(".git")
            auth_url  = self._auth_url(clone_url)

            # Always index the default branch
            docs, new_sha, state_key = self._index_repo(
                clone_url, repo_name, "devops", exts=self.include_exts, branch=None
            )
            if new_sha:
                _update_state(state_key, new_sha)
            if docs:
                poster.post_batch(docs, workers=self.cfg.batch_size)

            # Index every non-noise branch + auto-register customers
            branches = self._list_all_branches(auth_url)
            log.info("DevOps repo %s: %d non-noise branches", repo_name, len(branches))
            for branch_name, _ in branches:
                parsed = _parse_customer_branch(branch_name)
                if parsed and agent_url:
                    customer_id, env = parsed
                    self._register_customer_env(agent_url, customer_id, env)

                docs, new_sha, state_key = self._index_repo(
                    clone_url, repo_name, "devops",
                    exts=self.include_exts, branch=branch_name
                )
                if new_sha:
                    _update_state(state_key, new_sha)
                if docs:
                    poster.post_batch(docs, workers=self.cfg.batch_size)

    def _register_customer_env(self, agent_url: str, customer_id: str, env: str):
        """
        Upsert a customer + environment into the intelligence agent registry.
        Both calls are idempotent — safe to call on every sync run.
        """
        customer_name = _customer_name_from_id(customer_id)
        lifecycle = env if env in ("prod", "staging", "dev", "testing") else "solution"

        # 1. Create customer (409 = already exists → fine)
        try:
            r = requests.post(
                f"{agent_url}/api/v1/customers",
                json={"id": customer_id, "name": customer_name, "lifecycle_stage": lifecycle},
                timeout=10,
            )
            if r.status_code not in (200, 201, 409):
                log.warning("Customer register %s → HTTP %s: %s", customer_id, r.status_code, r.text[:200])
        except Exception as exc:
            log.warning("Customer register %s failed: %s", customer_id, exc)
            return

        # 2. Upsert environment (cluster details filled later by ops)
        try:
            r = requests.post(
                f"{agent_url}/api/v1/customers/{customer_id}/environments/{env}",
                json={"lifecycle_stage": lifecycle},
                timeout=10,
            )
            if r.status_code not in (200, 201, 409):
                log.warning("Env register %s/%s → HTTP %s: %s", customer_id, env, r.status_code, r.text[:200])
            else:
                log.info("Registered customer env: %s / %s", customer_id, env)
        except Exception as exc:
            log.warning("Env register %s/%s failed: %s", customer_id, env, exc)

    def _remote_sha(self, auth_url: str, branch: str | None = None) -> str | None:
        """
        Return the SHA for a specific branch (or HEAD if branch is None/empty).
        One lightweight ls-remote call — no clone.
        """
        ref = f"refs/heads/{branch}" if branch else "HEAD"
        try:
            r = subprocess.run(
                ["git", "ls-remote", auth_url, ref],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout.strip().split()[0]
        except Exception:
            pass
        return None

    def _index_repo(self, clone_url: str, repo_name: str, product: str,
                    exts: set | None = None,
                    branch: str | None = None) -> tuple[list, str | None, str]:
        """
        Clone a repo (or a specific branch), walk files, return
        (docs, new_sha, state_key).

        state_key is  "repo_name"         for the default branch
                      "repo_name:branch"  for named branches
        Returns ([], None, state_key) when the branch is unchanged or clone fails.
        """
        if exts is None:
            exts = self.include_exts

        auth_url = self._auth_url(clone_url)
        state_key = repo_name if not branch else f"{repo_name}:{branch}"
        label    = repo_name if not branch else f"{repo_name}[{branch}]"

        # ── Incremental check ──────────────────────────────────────────────
        if not self.cfg.force:
            remote_sha = self._remote_sha(auth_url, branch)
            if remote_sha and self._sync_state.get(state_key) == remote_sha:
                log.info("  %s: unchanged (%s), skipping", label, remote_sha[:8])
                return [], None, state_key
        else:
            remote_sha = None

        # ── Clone ──────────────────────────────────────────────────────────
        def _rss() -> int:
            try:
                return int(Path("/proc/self/status").read_text().split("VmRSS:")[1].split()[0]) // 1024
            except Exception:
                return 0

        with tempfile.TemporaryDirectory() as tmp:
            clone_cmd = ["git", "clone", "--depth=1", "--single-branch"]
            if branch:
                clone_cmd += ["--branch", branch]
            clone_cmd += [auth_url, tmp]

            rss_before = _rss()
            log.info("Cloning %s ... (RSS=%dMB)", label, rss_before)
            result = subprocess.run(
                clone_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=180,
            )
            rss_after_clone = _rss()
            log.info("  %s: clone done RSS=%dMB (+%dMB)", label, rss_after_clone, rss_after_clone - rss_before)
            if result.returncode != 0:
                log.error("Clone failed for %s: %s", label, result.stderr[:200])
                return [], None, state_key

            # Actual SHA after clone (more reliable than ls-remote for default branch)
            head_result = subprocess.run(
                ["git", "-C", tmp, "rev-parse", "HEAD"],
                capture_output=True, text=True
            )
            actual_sha = head_result.stdout.strip() if head_result.returncode == 0 else remote_sha

            docs = self._walk(tmp, repo_name, product, exts=exts, branch=branch)
            rss_after_walk = _rss()
            log.info("  %s: walk done %d chunks RSS=%dMB (+%dMB)", label, len(docs), rss_after_walk, rss_after_walk - rss_after_clone)

        rss_final = _rss()
        log.info("  %s: %d chunks (%s) RSS=%dMB (total +%dMB)", label, len(docs), (actual_sha or "")[:8], rss_final, rss_final - rss_before)
        return docs, actual_sha, state_key

    def sync_kg_for_repo(self, repo_name: str, org: str, kg: "KgPoster",
                         max_prs: int = 200, since: str | None = None) -> int:
        """
        Fetch merged PRs for *repo_name* via GitHub API and upsert KG entities.

        Relationships created:
          pull_request  --[merges_into]--> service  (service = repo_name)
          pull_request  --[references]-->  jira_issue  (parsed from PR title/body)

        *since* is an ISO-8601 datetime string (e.g. "2024-01-15T10:00:00Z").  When
        provided, only PRs merged after that point are fetched.  On the first KG run
        for a repo, *since* is None so all historical PRs are backfilled.

        Called once per repo after document indexing.  Rate-limited to avoid
        exhausting the 5000 req/h GitHub quota.
        """
        if not self.cfg.github_token or not org:
            return 0

        headers = {
            "Accept":        "application/vnd.github+json",
            "Authorization": f"Bearer {self.cfg.github_token}",
        }
        service_id = repo_name
        kg.upsert_entity("service", service_id, repo_name, {"repo": repo_name})

        processed = 0
        page = 1
        jira_key_re = re.compile(r"\b([A-Z]{2,6}-\d{2,6})\b")
        while processed < max_prs:
            try:
                r = _api_get(
                    f"https://api.github.com/repos/{org}/{repo_name}/pulls",
                    headers=headers,
                    params={"state": "closed", "per_page": 50, "page": page},
                    timeout=20,
                    rate_limiter=_GITHUB_RL,
                )
                remaining = r.headers.get("X-RateLimit-Remaining", "999")
                if int(remaining) < 20:
                    log.warning("GitHub rate limit low (%s), stopping KG PR fetch", remaining)
                    break
                if r.status_code == 404:
                    break
                r.raise_for_status()
                prs = r.json()
            except Exception as e:
                log.debug("KG PR fetch %s/%s: %s", org, repo_name, e)
                break

            if not prs:
                break

            for pr in prs:
                if not pr.get("merged_at"):
                    continue  # closed but not merged
                # When backfilling incrementally, stop once we reach PRs older than cutoff
                if since and pr["merged_at"] < since:
                    processed = max_prs  # signal outer while to stop
                    break
                pr_number = str(pr["number"])
                pr_id     = f"{repo_name}#{pr_number}"
                pr_title  = pr.get("title", f"PR #{pr_number}")
                merged_sha = (pr.get("merge_commit_sha") or "")[:12]
                base_branch = (pr.get("base") or {}).get("ref", "")

                kg.upsert_entity("pull_request", pr_id, pr_title, {
                    "repo":        repo_name,
                    "pr_number":   pr_number,
                    "merged_at":   pr.get("merged_at", ""),
                    "merged_sha":  merged_sha,
                    "base_branch": base_branch,
                    "url":         pr.get("html_url", ""),
                })
                kg.upsert_relationship("pull_request", pr_id, "merges_into",
                                       "service", service_id,
                                       {"merged_sha": merged_sha, "branch": base_branch})

                # Parse Jira keys from PR title + body to create references
                text = f"{pr_title} {pr.get('body') or ''}"
                for jira_key in set(jira_key_re.findall(text)):
                    kg.upsert_entity("jira_issue", jira_key, jira_key, {})
                    kg.upsert_relationship("pull_request", pr_id, "references",
                                           "jira_issue", jira_key)

                processed += 1

            if len(prs) < 50:
                break
            page += 1

        log.info("KG GitHub %s/%s: %d merged PRs", org, repo_name, processed)
        return processed

    def _auth_url(self, url: str) -> str:
        if self.cfg.github_token and url.startswith("https://"):
            return url.replace("https://", f"https://oauth2:{self.cfg.github_token}@")
        return url

    def _walk(self, root: str, repo_name: str, product: str,
              exts: set | None = None, branch: str | None = None) -> list:
        docs = []
        root_path = Path(root)
        skip_dirs = {".git", "__pycache__", "node_modules", ".gradle", "target",
                     "build", "dist", ".idea", ".vscode", "venv"}
        _exts = exts if exts is not None else self.include_exts
        for path in root_path.rglob("*"):
            if path.is_dir():
                continue
            if any(part in skip_dirs for part in path.parts):
                continue
            if path.suffix.lower() not in _exts:
                continue
            if path.stat().st_size > self.max_file_kb * 1024:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                rel = str(path.relative_to(root_path))
                dt = self._doc_type(rel)
                if dt:
                    # ADR / architecture docs: single doc when small, else 6000-char
                    # chunks with no overlap so context windows stay coherent.
                    if len(text) < 12000:
                        chunks = [text]
                    else:
                        chunks = [text[i:i + 6000]
                                  for i in range(0, len(text), 6000)]
                else:
                    chunks = self._chunk_code(text, path.suffix.lower())
                for i, chunk in enumerate(chunks):
                    meta: dict = {
                        "source":    "git",
                        "source_id": f"{repo_name}:{rel}",   # tombstone tracking (P0.3)
                        "product":   product,
                        "repo":      repo_name,
                        "file_path": rel,
                        "language":  _lang(path.suffix.lower()),
                        "chunk_index": i,
                        # no customer tag — shared knowledge
                    }
                    if branch:
                        meta["branch"] = branch
                    if dt:
                        meta["doc_type"] = dt
                    branch_label = f"@{branch}" if branch else ""
                    docs.append({
                        "title": f"[{product}/{repo_name}{branch_label}] {rel}"
                                 + (f" (part {i+1})" if len(chunks) > 1 else ""),
                        "content": chunk,
                        "metadata": meta,
                    })
            except Exception as e:
                log.debug("Skip %s: %s", path, e)
        return docs

    def _chunk_code(self, text: str, ext: str) -> list:
        if ext == ".py":
            chunks = self._chunk_python(text)
            if chunks:
                return chunks
        elif ext == ".java":
            chunks = self._chunk_java(text)
            if chunks:
                return chunks
        return self._chunk_text(text)

    def _chunk_python(self, text: str) -> list:
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return []
        lines = text.splitlines(keepends=True)
        boundaries = sorted(
            node.lineno - 1 for node in ast.walk(tree)
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and node.col_offset == 0
        )
        if not boundaries:
            return []
        boundaries = [0] + boundaries + [len(lines)]
        chunks = []
        for i in range(len(boundaries) - 1):
            chunk = "".join(lines[boundaries[i]:boundaries[i+1]]).strip()
            if chunk:
                chunks.append(chunk)
        return _merge_small(chunks)

    def _chunk_java(self, text: str) -> list:
        pattern = re.compile(
            r'(?:^|\n)(?:(?:public|private|protected|static|final|abstract)\s+)*'
            r'(?:class|interface|enum|@interface)\s+\w+',
            re.MULTILINE
        )
        positions = [m.start() for m in pattern.finditer(text)]
        if len(positions) < 1:
            return []
        positions.append(len(text))
        chunks = []
        for i in range(len(positions) - 1):
            chunk = text[positions[i]:positions[i+1]].strip()
            if chunk:
                chunks.append(chunk)
        return _merge_small(chunks)

    def _chunk_text(self, text: str) -> list:
        chunks, start = [], 0
        while start < len(text):
            end = min(start + self.CHUNK_CHARS, len(text))
            if end < len(text):
                b = text.rfind("\n", start + self.CHUNK_CHARS // 2, end)
                if b > 0:
                    end = b
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            start = end - self.OVERLAP_CHARS
            if start <= 0 or start >= len(text):
                break
        return chunks


# ---------------------------------------------------------------------------
# Customer deployment state (what version is running, per env)
# ---------------------------------------------------------------------------
#
# POD LOGS ARE NOT STORED HERE.
# Logs are a live stream — they are fetched on-demand by the warehouse-agent's
# get_pod_logs tool at query time (always current, zero storage cost).
# Only deployment state (image tags = which version is running) is indexed,
# because versions change rarely (once per release) and are useful for
# searches like "what version of pick-assist is in prod?".

class CustomerDeployFetcher:
    """
    Fetches deployment state (image tags) for ONE customer across ALL their
    configured environments.

    One OpenSearch document per (customer, env), e.g.:
      "[DeploymentState] Sams Club Atlanta — prod
       operator-backend: v2.3.1 (3/3 pods)
       greymatter: v6.0.5 (2/2 pods)"

    Handles the new multi-env registry format:
      environments:
        dev:     { k8s_bastion: ..., k8s_context: ..., k8s_namespace: ... }
        staging: { k8s_bastion: ..., k8s_context: ..., k8s_namespace: ... }
        prod:    { k8s_bastion: ..., k8s_context: ..., k8s_namespace: ... }

    AND the legacy flat format from customers.yml:
      k8s_bastion: ..., k8s_context: ..., k8s_namespace: ...
    """

    def __init__(self, customer: dict, cfg: Config):
        self.customer      = customer
        self.cfg           = cfg
        self.customer_id   = customer["id"]
        self.customer_name = customer["name"]
        self.products      = customer.get("products", [])

    def fetch_all_envs(self) -> list:
        """Returns one deployment-state document per configured environment."""
        docs = []

        # ── New registry format: environments: {dev: {...}, prod: {...}} ─────
        envs = self.customer.get("environments", {})
        if envs:
            for env_name, env_cfg in envs.items():
                bastion   = env_cfg.get("k8s_bastion", "")
                namespace = env_cfg.get("k8s_namespace", "default")
                context   = env_cfg.get("k8s_context", "")
                if not bastion:
                    log.debug("%s/%s: no k8s_bastion, skipping",
                              self.customer_id, env_name)
                    continue
                doc = self._fetch_one_env(env_name, bastion, namespace, context)
                if doc:
                    docs.append(doc)
            return docs

        # ── Legacy flat keys ─────────────────────────────────────────────────
        k8s = self.customer.get("k8s", {})
        bastion   = self.customer.get("k8s_bastion",   k8s.get("bastion",   ""))
        namespace = self.customer.get("k8s_namespace", k8s.get("namespace", "default"))
        context   = self.customer.get("k8s_context",   k8s.get("context",   ""))
        env_name  = self.customer.get("env", "prod")
        if not bastion:
            log.debug("%s: no k8s_bastion, skipping", self.customer_id)
            return []
        doc = self._fetch_one_env(env_name, bastion, namespace, context)
        if doc:
            docs.append(doc)
        return docs

    def _fetch_one_env(self, env_name: str,
                       bastion: str, namespace: str, context: str) -> Optional[dict]:
        ctx_flag = f"--context={context}" if context else ""
        jsonpath = (
            "{range .items[*]}"
            "{.metadata.name}\t"
            "{.spec.template.spec.containers[0].image}\t"
            "{.status.readyReplicas}/{.status.replicas}\n"
            "{end}"
        )
        cmd = f"kubectl {ctx_flag} -n {namespace} get deployments -o jsonpath='{jsonpath}'"
        out = self._ssh(bastion, cmd)
        if not out:
            log.warning("%s/%s: no output from kubectl", self.customer_id, env_name)
            return None

        summary_lines = []
        for line in (l.strip() for l in out.strip().splitlines() if l.strip()):
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            name    = parts[0]
            image   = parts[1]
            ready   = parts[2] if len(parts) > 2 else "?/?"
            version = image.split(":")[-1] if ":" in image else "unknown"
            summary_lines.append(f"  {name}: {version}  ({ready} pods)")

        if not summary_lines:
            return None

        content = (
            f"Customer: {self.customer_name}\n"
            f"Environment: {env_name}\n"
            f"Products: {', '.join(self.products)}\n"
            f"Namespace: {namespace}\n"
            f"Notes: {self.customer.get('notes', '')}\n\n"
            "DEPLOYED VERSIONS (kubectl get deployments):\n"
            + "\n".join(summary_lines)
            + "\n\nNote: live pod logs are fetched on-demand by the agent "
              "at query time — not stored here."
        )

        log.info("  %s/%s: %d deployments", self.customer_id, env_name, len(summary_lines))
        return {
            "title":   f"[DeploymentState] {self.customer_name} — {env_name}",
            "content": content,
            "metadata": {
                "source":        "deployment_state",
                "customer":      self.customer_id,
                "customer_name": self.customer_name,
                "env":           env_name,
                "products":      self.products,
                "namespace":     namespace,
            },
        }

    def sync_kg(self, kg: "KgPoster") -> None:
        """
        Upsert KG entities and relationships derived from deployment state.

        Relationships created per environment:
          customer --[has_env]-->    deployment
          deployment --[runs]-->     service  (one per deployed image)
        """
        customer_id   = self.customer_id
        customer_name = self.customer_name

        kg.upsert_entity("customer", customer_id, customer_name, {
            "products": self.products,
        })

        envs = self.customer.get("environments", {})
        if not envs:
            return

        for env_name, env_cfg in envs.items():
            bastion   = env_cfg.get("k8s_bastion", "")
            namespace = env_cfg.get("k8s_namespace", "default")
            context   = env_cfg.get("k8s_context", "")
            if not bastion:
                continue

            deployment_id = f"{customer_id}/{env_name}"
            kg.upsert_entity("deployment", deployment_id,
                             f"{customer_name} — {env_name}", {
                                 "customer":   customer_id,
                                 "env":        env_name,
                                 "namespace":  namespace,
                             })
            kg.upsert_relationship("customer", customer_id, "has_env",
                                   "deployment", deployment_id)

            # Fetch live version → service relationships
            ctx_flag = f"--context={context}" if context else ""
            cmd = (
                f"kubectl {ctx_flag} -n {namespace} get deployments "
                f"-o jsonpath='{{range .items[*]}}{{.metadata.name}}\t"
                f"{{.spec.template.spec.containers[0].image}}\n{{end}}'"
            )
            out = self._ssh(bastion, cmd) or ""
            for line in out.strip().splitlines():
                parts = line.split("\t")
                if len(parts) < 2:
                    continue
                svc_name = parts[0]
                image    = parts[1]
                version  = image.split(":")[-1] if ":" in image else "unknown"
                svc_id   = svc_name
                kg.upsert_entity("service", svc_id, svc_name, {})
                kg.upsert_relationship(
                    "deployment", deployment_id, "runs",
                    "service", svc_id, {"version": version, "image": image},
                )

    def _ssh(self, bastion: str, remote_cmd: str) -> Optional[str]:
        try:
            result = subprocess.run(
                ["ssh",
                 "-o", "StrictHostKeyChecking=no",
                 "-o", "ConnectTimeout=10",
                 "-o", "BatchMode=yes",
                 bastion, remote_cmd],
                capture_output=True, text=True, timeout=45,
            )
            if result.returncode != 0:
                log.warning("SSH failed %s->%s: %s",
                            self.customer_id, bastion, result.stderr[:200])
                return None
            return result.stdout
        except subprocess.TimeoutExpired:
            log.warning("SSH timeout %s->%s", self.customer_id, bastion)
            return None
        except Exception as exc:
            log.warning("SSH error %s->%s: %s", self.customer_id, bastion, exc)
            return None


# ---------------------------------------------------------------------------
# Jira

# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

class JiraFetcher:
    def __init__(self, cfg: Config):
        self.base = cfg.jira_url
        self.auth = (cfg.jira_email, cfg.jira_token)
        self.max = cfg.jira_max_results
        self._rl = _ATLASSIAN_RL

    def _get(self, url, **kwargs):
        return _api_get(url, auth=self.auth, rate_limiter=self._rl, **kwargs)

    def list_projects(self):
        r = self._get(f"{self.base}/rest/api/3/project", timeout=30)
        r.raise_for_status()
        return [p["key"] for p in r.json()]

    def sync_kg_for_project(self, project: str, kg: "KgPoster", since: int | None = None) -> int:
        """
        Fetch all issues for *project* with KG-relevant fields and upsert entities +
        relationships into the knowledge graph.  Runs after the document sync so the
        graph mirrors what is already indexed.

        Returns the number of issues processed.
        """
        if since:
            dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(since - 300))
            jql = f'project = "{project}" AND updated >= "{dt}" ORDER BY updated DESC'
        else:
            jql = f'project = "{project}" ORDER BY updated DESC'
        params: dict = {
            "jql":        jql,
            "maxResults": 100,
            # issuelinks lets us detect "fixed by" links between Jira issues
            "fields":     "summary,status,issuetype,priority,fixVersions,issuelinks",
        }
        processed = 0
        while True:
            try:
                r = self._get(f"{self.base}/rest/api/3/search/jql",
                             params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log.warning("KG Jira fetch %s: %s", project, e)
                break
            batch = data.get("issues", [])
            if not batch:
                break
            for issue in batch:
                try:
                    self.extract_kg(issue, kg)
                    processed += 1
                except Exception as e:
                    log.debug("KG extract %s: %s", issue.get("key"), e)
            next_token = data.get("nextPageToken")
            if not next_token or processed >= self.max:
                break
            params = {**params, "nextPageToken": next_token}
        log.info("KG Jira %s: %d issues processed", project, processed)
        return processed

    def fetch_issues(self, project: str, since: int | None = None) -> list:
        # /rest/api/3/search was deprecated (returns 410 Gone).
        # /rest/api/3/search/jql uses cursor-based pagination via nextPageToken.
        if since:
            # 5-min buffer so items updated mid-crawl on the previous run aren't missed
            dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(since - 300))
            jql = f'project = "{project}" AND updated >= "{dt}" ORDER BY updated DESC'
        else:
            jql = f'project = "{project}" ORDER BY updated DESC'
        issues = []
        params: dict = {
            "jql": jql,
            "maxResults": 100,
            "fields": "summary,description,status,assignee,priority,labels,"
                      "comment,issuetype,created,updated,fixVersions,components",
        }
        while True:
            r = self._get(
                f"{self.base}/rest/api/3/search/jql",
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            batch = data.get("issues", [])
            if not batch:
                break
            for issue in batch:
                doc = self._to_doc(issue)
                if doc:
                    issues.append(doc)
            if len(issues) >= self.max:
                break
            next_token = data.get("nextPageToken")
            if not next_token:
                break
            params = {**params, "nextPageToken": next_token}
        mode = f"delta since {time.strftime('%Y-%m-%d %H:%M', time.gmtime(since))}" if since else "full"
        log.info("Jira %s: %d issues (%s)", project, len(issues), mode)
        return issues

    def fetch_remote_links(self, issue_key: str) -> list[dict]:
        """
        Fetch Jira remote links for a single issue.
        Returns list of {url, title, relationship} dicts.
        Skips on HTTP error (some projects disable remote links).
        """
        try:
            r = self._get(
                f"{self.base}/rest/api/3/issue/{issue_key}/remotelink",
                timeout=15,
            )
            if r.status_code == 403 or r.status_code == 404:
                return []
            r.raise_for_status()
            return [
                {
                    "url":          (link.get("object") or {}).get("url", ""),
                    "title":        (link.get("object") or {}).get("title", ""),
                    "relationship": link.get("relationship", "links to"),
                }
                for link in r.json()
                if (link.get("object") or {}).get("url")
            ]
        except Exception as e:
            log.debug("Remote links for %s: %s", issue_key, e)
            return []

    def extract_kg(self, issue: dict, kg: "KgPoster") -> None:
        """
        Upsert KG entities and relationships for one Jira issue.
        Called after the issue document is indexed.

        Relationships created:
          jira_issue --[fixed_by]-->   pull_request  (via Jira remote links to GitHub PRs)
          jira_issue --[contains]-->   pull_request  (via Jira issue links of type "is fixed by")
        """
        key = issue.get("key", "")
        if not key:
            return
        f = issue.get("fields", {})
        summary = f.get("summary", "")

        # Upsert the Jira issue entity
        kg.upsert_entity("jira_issue", key, f"[{key}] {summary}", {
            "status":     (f.get("status") or {}).get("name", ""),
            "issue_type": (f.get("issuetype") or {}).get("name", ""),
            "priority":   (f.get("priority") or {}).get("name", ""),
            "fix_versions": [v["name"] for v in f.get("fixVersions", [])],
        })

        # Remote links → GitHub PRs
        for link in self.fetch_remote_links(key):
            url = link["url"]
            m = _GITHUB_PR_RE.search(url)
            if not m:
                continue
            repo_name  = m.group(1)
            pr_number  = m.group(2)
            pr_id      = f"{repo_name}#{pr_number}"
            pr_title   = link["title"] or f"PR #{pr_number} in {repo_name}"
            relationship = link.get("relationship", "links to").lower()
            # Map Jira relationship labels to canonical KG relation names
            relation = "fixed_by" if any(w in relationship for w in ("fix", "resolv", "clos")) else "linked_to"
            kg.upsert_entity("pull_request", pr_id, pr_title, {
                "repo":      repo_name,
                "pr_number": pr_number,
                "url":       url,
            })
            kg.upsert_relationship("jira_issue", key, relation, "pull_request", pr_id)

        # Jira issue links (e.g. "is fixed by" pointing to another issue or PR)
        for il in (f.get("issuelinks") or []):
            il_type = (il.get("type") or {}).get("inward", "").lower()
            if "fix" not in il_type and "resolv" not in il_type:
                continue
            linked = il.get("inwardIssue") or il.get("outwardIssue") or {}
            linked_key = linked.get("key", "")
            if linked_key:
                kg.upsert_entity("jira_issue", linked_key,
                                 linked.get("fields", {}).get("summary", linked_key), {})
                kg.upsert_relationship("jira_issue", key, "fixed_by", "jira_issue", linked_key)

    def _to_doc(self, issue: dict) -> Optional[dict]:
        f = issue.get("fields", {})
        title = f"[{issue['key']}] {f.get('summary', '')}"
        desc = adf_to_text(f.get("description") or {})
        comments = [
            f"{(c.get('author') or {}).get('displayName','')}: {adf_to_text(c.get('body') or {})}"
            for c in (f.get("comment") or {}).get("comments", [])
            if adf_to_text(c.get("body") or {})
        ]
        content = "\n\n".join(p for p in [desc] + comments if p).strip() or title
        return {
            "title": title,
            "content": content,
            "metadata": {
                "source":      "jira",
                "source_id":   issue["key"],   # used for tombstone tracking (P0.3)
                "issue_key":   issue["key"],
                "project":     issue["key"].split("-")[0],
                "status":      (f.get("status") or {}).get("name", ""),
                "issue_type":  (f.get("issuetype") or {}).get("name", ""),
                "priority":    (f.get("priority") or {}).get("name", ""),
                "assignee":    (f.get("assignee") or {}).get("displayName", ""),
                "labels":      f.get("labels", []),
                "components":  [c["name"] for c in f.get("components", [])],
                "fix_versions": [v["name"] for v in f.get("fixVersions", [])],
                "url":         f"{self.base}/browse/{issue['key']}",
                "created":     f.get("created", ""),
                "updated":     f.get("updated", ""),
            },
        }


# ---------------------------------------------------------------------------
# Confluence
# ---------------------------------------------------------------------------

class ConfluenceFetcher:
    def __init__(self, cfg: Config):
        self.base = cfg.confluence_url
        self.auth = (cfg.confluence_email, cfg.confluence_token)
        self.max = cfg.confluence_max_results
        self._rl = _ATLASSIAN_RL

    def _get(self, url, **kwargs):
        return _api_get(url, auth=self.auth, rate_limiter=self._rl, **kwargs)

    def list_spaces(self):
        r = self._get(f"{self.base}/wiki/rest/api/space",
                      params={"limit": 200, "type": "global"}, timeout=30)
        r.raise_for_status()
        return [s["key"] for s in r.json().get("results", [])]

    def _fetch_children(self, parent_id: str, space: str,
                        acc: list, depth: int = 0) -> None:
        """Recursively fetch child pages of *parent_id*, appending docs to *acc*.

        Guards against runaway recursion with a hard depth limit of 8.
        """
        if depth >= 8:
            log.debug("Confluence child fetch: depth limit reached for page %s", parent_id)
            return
        start = 0
        while True:
            r = self._get(
                f"{self.base}/wiki/rest/api/content/{parent_id}/child/page",
                params={"expand": "body.storage,metadata.labels,history.lastUpdated",
                        "start": start, "limit": 50},
                timeout=30,
            )
            if r.status_code == 404:
                break  # page has no children endpoint
            r.raise_for_status()
            data = r.json()
            batch = data.get("results", [])
            if not batch:
                break
            for page in batch:
                doc = self._to_doc(page, space)
                if doc:
                    acc.append(doc)
                # Recurse regardless of whether _to_doc produced output so we
                # don't miss grandchildren of empty pages.
                self._fetch_children(page["id"], space, acc, depth=depth + 1)
            start += len(batch)
            if len(batch) < 50:
                break

    def fetch_pages(self, space: str, since: int | None = None) -> list:
        if since:
            return self._fetch_pages_delta(space, since)
        pages, start = [], 0
        top_level_count = 0
        while True:
            r = self._get(
                f"{self.base}/wiki/rest/api/content",
                params={"spaceKey": space, "type": "page", "status": "current",
                        "expand": "body.storage,metadata.labels,history.lastUpdated",
                        "start": start, "limit": 50},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            batch = data.get("results", [])
            if not batch:
                break
            for page in batch:
                doc = self._to_doc(page, space)
                if doc:
                    pages.append(doc)
                # Fetch child pages recursively for every top-level page.
                self._fetch_children(page["id"], space, pages)
            top_level_count += len(batch)
            start += len(batch)
            if len(batch) < 50 or start >= self.max:
                break
        child_count = len(pages) - top_level_count
        log.info("Confluence %s: %d pages (including %d child pages)",
                 space, len(pages), child_count)
        return pages

    def _fetch_pages_delta(self, space: str, since: int) -> list:
        """Fetch only pages modified since *since* (unix timestamp) using CQL search."""
        # 5-min buffer so pages touched mid-crawl on the previous run aren't missed
        dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(since - 300))
        cql = f'space = "{space}" AND type = "page" AND lastModified >= "{dt}"'
        pages, start = [], 0
        while True:
            r = self._get(
                f"{self.base}/wiki/rest/api/search",
                params={"cql": cql,
                        "expand": "body.storage,metadata.labels,history.lastUpdated",
                        "start": start, "limit": 50},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            batch = data.get("results", [])
            if not batch:
                break
            for result in batch:
                page = result.get("content") or result  # search wraps page in "content"
                doc = self._to_doc(page, space)
                if doc:
                    pages.append(doc)
            start += len(batch)
            if len(batch) < 50 or start >= self.max:
                break
        log.info("Confluence %s: %d pages (delta since %s)", space, len(pages),
                 time.strftime("%Y-%m-%d %H:%M", time.gmtime(since)))
        return pages

    def _to_doc(self, page: dict, space: str) -> Optional[dict]:
        title = page.get("title", "Untitled")
        html = (page.get("body") or {}).get("storage", {}).get("value", "")
        content = html_to_text(html).strip()
        if not content:
            return None
        labels = [l["name"] for l in
                  ((page.get("metadata") or {}).get("labels") or {}).get("results", [])]
        return {
            "title": f"[Confluence/{space}] {title}",
            "content": content,
            "metadata": {
                "source":    "confluence",
                "source_id": page.get("id", ""),   # used for tombstone tracking (P0.3)
                "space":     space,
                "page_id":   page.get("id", ""),
                "labels":    labels,
                "url":       f"{self.base}/wiki{page.get('_links', {}).get('webui', '')}",
                "updated":   (((page.get("history") or {}).get("lastUpdated") or {})
                              .get("when", "")),
            },
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def adf_to_text(node: dict, depth=0) -> str:
    if not node or not isinstance(node, dict):
        return ""
    t = node.get("type", "")
    if t == "text":
        return node.get("text", "")
    if t in ("hardBreak", "rule"):
        return "\n"
    if t in ("paragraph", "heading", "blockquote", "listItem"):
        return "".join(adf_to_text(c, depth+1) for c in node.get("content", [])) + "\n"
    if t in ("bulletList", "orderedList"):
        return "".join(f"• {adf_to_text(i, depth+1)}" for i in node.get("content", []))
    if t == "codeBlock":
        code = "".join(c.get("text", "") for c in node.get("content", []))
        return f"\n```\n{code}\n```\n"
    if t == "table":
        rows = [" | ".join(adf_to_text(cell, depth+1).strip()
                           for cell in row.get("content", []))
                for row in node.get("content", [])]
        return "\n".join(rows) + "\n"
    return "".join(adf_to_text(c, depth+1) for c in node.get("content", []))


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "ac:structured-macro"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _lang(ext: str) -> str:
    return {".py": "python", ".java": "java", ".ts": "typescript",
            ".js": "javascript", ".md": "markdown", ".yml": "yaml",
            ".yaml": "yaml", ".sh": "shell", ".json": "json"}.get(ext, ext.lstrip("."))


def _merge_small(chunks: list, min_chars: int = 200) -> list:
    merged, buf = [], ""
    for c in chunks:
        buf = (buf + "\n\n" + c).strip() if buf else c
        if len(buf) >= min_chars:
            merged.append(buf)
            buf = ""
    if buf:
        merged.append(buf)
    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="GreyOrange Searchly Connector")
    parser.add_argument("--only", choices=["shared", "jira", "confluence", "repos",
                                           "customer", "all-customers",
                                           "all-customers-deploy"],
                        help=(
                            "Sync only this source. "
                            "all-customers-deploy = deployment state only (no logs, no Jira). "
                            "all-customers = same (alias kept for backward compat)."
                        ))
    parser.add_argument("--customer", help="Customer ID for --only customer")
    parser.add_argument("--list-repos", action="store_true",
                        help="Print all repos from GitHub org (for products.yml)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="Ignore incremental state — re-index all repos from scratch")
    args = parser.parse_args()

    cfg = load_config(args)
    products_cfg = load_products()
    customers = load_customers()
    poster = SearchlyPoster(cfg)
    kg     = KgPoster(cfg)
    only = args.only
    sync_started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # ── List repos mode (helper for filling products.yml) ─────────────────
    if args.list_repos:
        org = products_cfg.get("github_org", "")
        if not org:
            print("Set github_org in products.yml first.")
            sys.exit(1)
        GitHubDiscovery(org, cfg.github_token).print_repos()
        return

    # ── Jira + Confluence + Repos — all three run in parallel ─────────────
    # Each section has its own adaptive pool (starts at 1 worker, scales with
    # CPU load).  Atlassian sections share _ATLASSIAN_RL (7 req/s cap across
    # both); GitHub sections share _GITHUB_RL (5 req/s cap).
    # Running in parallel means a slow Confluence full-fetch no longer blocks
    # GitHub repo indexing from starting.
    shared_threads: list[threading.Thread] = []
    shared_errors:  list[str] = []

    def _run_jira() -> None:
        if not (only is None or only in ("shared", "jira")):
            return
        if not (cfg.jira_url and cfg.jira_token):
            log.info("Jira not configured, skipping.")
            return
        try:
            jira = JiraFetcher(cfg)
            projects = cfg.jira_projects or jira.list_projects()
            log.info("Syncing Jira: %s (adaptive, max=%d)", projects, cfg.atlassian_workers)

            def _sync_jira_project(proj: str) -> None:
                state = _load_sync_state()
                since = None if cfg.force else state.get(f"jira_project_{proj}_completed_at")
                since = int(since) if since else None
                kg_since_ts = None if cfg.force else state.get(f"kg_jira_project_{proj}_completed_at")
                kg_since = int(kg_since_ts) if kg_since_ts else None
                try:
                    docs = [part for d in jira.fetch_issues(proj, since=since)
                            for part in split_doc(d)]
                    poster.post_batch(docs, workers=cfg.batch_size)
                    jira.sync_kg_for_project(proj, kg, since=kg_since)
                    now = int(time.time())
                    _update_state(f"jira_project_{proj}_completed_at", now)
                    _update_state(f"kg_jira_project_{proj}_completed_at", now)
                except Exception as e:
                    log.error("Jira %s: %s", proj, e)

            _AdaptivePool("jira", max_workers=cfg.atlassian_workers).map(
                _sync_jira_project, projects)
            poster.purge_stale("jira", sync_started_at)
        except Exception as exc:
            msg = f"Jira section failed: {exc}"
            log.error(msg)
            shared_errors.append(msg)

    def _run_confluence() -> None:
        if not (only is None or only in ("shared", "confluence")):
            return
        if not (cfg.confluence_url and cfg.confluence_token):
            log.info("Confluence not configured, skipping.")
            return
        try:
            conf = ConfluenceFetcher(cfg)
            spaces = cfg.confluence_spaces or conf.list_spaces()
            log.info("Syncing Confluence: %s (adaptive, max=%d)", spaces, cfg.atlassian_workers)

            def _sync_confluence_space(space: str) -> None:
                state = _load_sync_state()
                since = None if cfg.force else state.get(f"confluence_space_{space}_completed_at")
                since = int(since) if since else None
                try:
                    docs = [part for d in conf.fetch_pages(space, since=since)
                            for part in split_doc(d)]
                    poster.post_batch(docs, workers=cfg.batch_size)
                    _update_state(f"confluence_space_{space}_completed_at", int(time.time()))
                except Exception as e:
                    log.error("Confluence %s: %s", space, e)

            _AdaptivePool("confluence", max_workers=cfg.atlassian_workers).map(
                _sync_confluence_space, spaces)
            poster.purge_stale("confluence", sync_started_at)
        except Exception as exc:
            msg = f"Confluence section failed: {exc}"
            log.error(msg)
            shared_errors.append(msg)

    def _rss_mb() -> int:
        try:
            return int(Path("/proc/self/status").read_text().split("VmRSS:")[1].split()[0]) // 1024
        except Exception:
            return 0

    def _run_repos() -> None:
        if not (only is None or only in ("shared", "repos")):
            return
        if not products_cfg:
            log.info("products.yml not loaded, skipping repos.")
            return
        try:
            log.info("Indexing repos from products.yml (adaptive, max=%d) ... RSS=%dMB",
                     cfg.github_workers, _rss_mb())
            repo_indexer = RepoIndexer(cfg, products_cfg)
            repo_indexer.index_all_products(poster, workers=cfg.github_workers)
            log.info("Repos indexed. RSS=%dMB", _rss_mb())
            poster.purge_stale("git", sync_started_at)

            org = products_cfg.get("github_org", "")
            if org and cfg.github_token:
                repos_for_kg = [
                    r["name"] for r in repo_indexer._discover_repos()
                    if r["name"] not in repo_indexer._skip_repos
                ]
                log.info("Syncing KG for %d GitHub repos (%s, adaptive, max=%d)",
                         len(repos_for_kg), org, cfg.github_workers)

                def _sync_kg_repo(repo_name: str) -> None:
                    try:
                        kg_state = _load_sync_state()
                        kg_repo_ts = None if cfg.force else kg_state.get(
                            f"kg_github_repo_{repo_name}_completed_at")
                        kg_repo_since = (
                            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(kg_repo_ts)))
                            if kg_repo_ts else None
                        )
                        repo_indexer.sync_kg_for_repo(repo_name, org, kg, since=kg_repo_since)
                        _update_state(f"kg_github_repo_{repo_name}_completed_at", int(time.time()))
                    except Exception as e:
                        log.debug("KG GitHub %s/%s: %s", org, repo_name, e)

                _AdaptivePool("kg-gh", max_workers=cfg.github_workers).map(
                    _sync_kg_repo, repos_for_kg)
        except Exception as exc:
            msg = f"Repos section failed: {exc}"
            log.error(msg)
            shared_errors.append(msg)

    for fn, name in [(_run_jira, "jira"), (_run_confluence, "confluence"), (_run_repos, "repos")]:
        t = threading.Thread(target=fn, daemon=True, name=f"sync-{name}")
        t.start()
        shared_threads.append(t)

    for t in shared_threads:
        t.join()

    if shared_errors:
        log.warning("Shared sync completed with %d section error(s)", len(shared_errors))

    # Stamp completion time so the scheduler can skip a redundant re-run on
    # container restart if the last full sync finished within the interval.
    if only in ("shared", None):
        _update_state("last_shared_completed_at", int(time.time()))

    # ── Deployment state: single customer, all envs ───────────────────────
    if only == "customer":
        if not args.customer:
            print("Specify --customer <id>  (see customers.yml for IDs)")
            sys.exit(1)
        cust = next((c for c in customers if c["id"] == args.customer), None)
        if not cust:
            print(f"Customer '{args.customer}' not found in customers.yml")
            print("Available:", [c["id"] for c in customers])
            sys.exit(1)
        docs = CustomerDeployFetcher(cust, cfg).fetch_all_envs()
        log.info("Customer %s: %d deployment docs across all envs", cust["id"], len(docs))
        poster.post_batch(docs, workers=1)

    # ── Deployment state: all customers, all envs ─────────────────────────
    # Called by the scheduler every SYNC_DEPLOY_INTERVAL_MIN (default 60 min).
    # Indexes which image version is running per customer per env.
    # Pod logs are NOT indexed here — they are fetched live at query time.
    if only == "all-customers-deploy" or only == "all-customers" or (not only and customers):
        if not customers:
            log.info("No customers configured, skipping deployment state refresh.")
            return
        log.info(
            "Refreshing deployment state for %d customers (all configured envs)...",
            len(customers),
        )
        total_docs = 0
        for cust in customers:
            try:
                fetcher = CustomerDeployFetcher(cust, cfg)
                docs = fetcher.fetch_all_envs()
                if docs:
                    poster.post_batch(docs, workers=1)
                    total_docs += len(docs)
                # KG: customer → deployment → service relationships
                fetcher.sync_kg(kg)
            except Exception as exc:
                log.error("Customer %s deployment fetch failed: %s", cust["id"], exc)
        log.info("Deployment state: %d docs indexed across all customers", total_docs)

    poster.summary()
    kg.summary()


if __name__ == "__main__":
    main()
