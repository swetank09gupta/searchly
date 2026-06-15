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
             page_delay: float = 0.15) -> requests.Response:
    """
    GET with automatic retry and rate-limit handling.

    Handles:
      - 429 Too Many Requests → respects Retry-After header (or waits 60s)
      - 5xx Server Error      → exponential back-off (2, 4, 8, 16, 32s + jitter)
      - Network errors        → same back-off as 5xx

    page_delay: minimum seconds to wait between every call (polite pacing).
                Atlassian Cloud allows ~10 req/s per token; 0.15s keeps us at ~6/s.
    """
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


def _load_sync_state() -> dict:
    if SYNC_STATE_FILE.exists():
        try:
            return json.loads(SYNC_STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_sync_state(state: dict):
    try:
        SYNC_STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log.warning("Could not save sync state: %s", e)


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
    # Branch patterns to index in addition to the default branch.
    # Supports exact names ("develop") and globs ("release/*", "hotfix/*").
    # Empty list = default branch only.
    git_branches: list = field(default_factory=list)

    searchly_url: str = "http://localhost:8081"
    searchly_tenant: str = "default"
    searchly_user: str = "sync-bot"

    batch_size: int = 5
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
        git_branches=csv("GIT_BRANCHES"),

        searchly_url=opt("SEARCHLY_URL", "http://localhost:8081").rstrip("/"),
        searchly_tenant=opt("SEARCHLY_TENANT", "default"),
        searchly_user=opt("SEARCHLY_USER", "sync-bot"),

        batch_size=int(opt("SYNC_BATCH_SIZE", "5")),
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
        for product_name, product_cfg in self.products.get("products", {}).items():
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

    def index_all_products(self, poster: SearchlyPoster):
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
        updated_state = dict(self._sync_state)

        # ── Step 1: build repo → product mapping from products.yml ───────────
        repo_product_map = self._build_repo_product_map()

        # ── Step 2: collect the full repo work list ───────────────────────────
        # Start with any repos explicitly listed in products.yml (these always
        # run even if org discovery is unavailable).
        work: dict[str, tuple[str, str]] = {}   # repo_name → (product, priority)
        for repo_name, (product, priority) in repo_product_map.items():
            if repo_name not in self._skip_repos:
                work[repo_name] = (product, priority)

        # Layer in org-discovered repos.  Explicit products.yml entries take
        # precedence for product name / priority; everything else is "unclassified".
        for repo in self._discover_repos():
            name = repo["name"]
            if name in self._skip_repos:
                continue
            if name not in work:
                # Not explicitly mapped — infer product from GitHub topics if possible
                topics = repo.get("topics", [])
                product = topics[0] if topics else "unclassified"
                work[name] = (product, "high")

        if not work:
            log.warning(
                "No repos to index. Set github_org in products.yml or add repos: entries."
            )
            _save_sync_state(updated_state)
            return

        log.info("Indexing %d repos total (%d from products.yml, %d auto-discovered)",
                 len(work),
                 len(repo_product_map),
                 len(work) - len(repo_product_map))

        branch_patterns = self.cfg.git_branches  # e.g. ["develop", "release/*"]
        if branch_patterns:
            log.info("Branch patterns: default + %s", branch_patterns)

        # ── Step 3: index each repo (default branch + any configured branches) ─
        for repo_name, (product_name, priority) in sorted(work.items()):
            exts = self.include_exts if priority != "low" else self.DOCS_ONLY_EXTS
            clone_url = self._resolve_url(repo_name)
            auth_url  = self._auth_url(clone_url)

            # Always index the default branch
            docs, new_sha, state_key = self._index_repo(
                clone_url, repo_name, product_name, exts=exts, branch=None
            )
            if new_sha:
                updated_state[state_key] = new_sha
            if docs:
                poster.post_batch(docs, workers=self.cfg.batch_size)

            # Index any additional branch patterns.
            # _resolve_branches does a single ls-remote call per pattern, expanding
            # globs — so "release/*" automatically picks up every release branch,
            # including newly created ones, on every sync cycle.
            if branch_patterns:
                for branch_name, remote_sha in self._resolve_branches(auth_url, branch_patterns):
                    docs, new_sha, state_key = self._index_repo(
                        clone_url, repo_name, product_name,
                        exts=exts, branch=branch_name
                    )
                    if new_sha:
                        updated_state[state_key] = new_sha
                    if docs:
                        poster.post_batch(docs, workers=self.cfg.batch_size)

        _save_sync_state(updated_state)

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
        with tempfile.TemporaryDirectory() as tmp:
            clone_cmd = ["git", "clone", "--depth=1"]
            if branch:
                clone_cmd += ["--branch", branch]
            clone_cmd += [auth_url, tmp]

            log.info("Cloning %s ...", label)
            result = subprocess.run(clone_cmd, capture_output=True, text=True, timeout=180)
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

        log.info("  %s: %d chunks (%s)", label, len(docs), (actual_sha or "")[:8])
        return docs, actual_sha, state_key

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

    def list_projects(self):
        r = _api_get(f"{self.base}/rest/api/3/project", auth=self.auth, timeout=30)
        r.raise_for_status()
        return [p["key"] for p in r.json()]

    def fetch_issues(self, project: str) -> list:
        # /rest/api/3/search was deprecated (returns 410 Gone).
        # /rest/api/3/search/jql uses cursor-based pagination via nextPageToken.
        issues = []
        params: dict = {
            "jql": f'project = "{project}" ORDER BY updated DESC',
            "maxResults": 100,
            "fields": "summary,description,status,assignee,priority,labels,"
                      "comment,issuetype,created,updated,fixVersions,components",
        }
        while True:
            r = _api_get(
                f"{self.base}/rest/api/3/search/jql", auth=self.auth,
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
        log.info("Jira %s: %d issues", project, len(issues))
        return issues

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

    def list_spaces(self):
        r = _api_get(f"{self.base}/wiki/rest/api/space", auth=self.auth,
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
            r = _api_get(
                f"{self.base}/wiki/rest/api/content/{parent_id}/child/page",
                auth=self.auth,
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

    def fetch_pages(self, space: str) -> list:
        pages, start = [], 0
        top_level_count = 0
        while True:
            r = _api_get(
                f"{self.base}/wiki/rest/api/content", auth=self.auth,
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

    # ── Jira ──────────────────────────────────────────────────────────────
    if not only or only in ("shared", "jira"):
        if cfg.jira_url and cfg.jira_token:
            jira = JiraFetcher(cfg)
            projects = cfg.jira_projects or jira.list_projects()
            log.info("Syncing Jira: %s", projects)
            for proj in projects:
                try:
                    docs = [part for d in jira.fetch_issues(proj) for part in split_doc(d)]
                    poster.post_batch(docs, workers=cfg.batch_size)
                except Exception as e:
                    log.error("Jira %s: %s", proj, e)
            poster.purge_stale("jira", sync_started_at)
        else:
            log.info("Jira not configured, skipping.")

    # ── Confluence ────────────────────────────────────────────────────────
    if not only or only in ("shared", "confluence"):
        if cfg.confluence_url and cfg.confluence_token:
            conf = ConfluenceFetcher(cfg)
            spaces = cfg.confluence_spaces or conf.list_spaces()
            log.info("Syncing Confluence: %s", spaces)
            for space in spaces:
                try:
                    docs = [part for d in conf.fetch_pages(space) for part in split_doc(d)]
                    poster.post_batch(docs, workers=cfg.batch_size)
                except Exception as e:
                    log.error("Confluence %s: %s", space, e)
            poster.purge_stale("confluence", sync_started_at)
        else:
            log.info("Confluence not configured, skipping.")

    # ── Repos ─────────────────────────────────────────────────────────────
    if not only or only in ("shared", "repos"):
        if products_cfg:
            log.info("Indexing repos from products.yml ...")
            RepoIndexer(cfg, products_cfg).index_all_products(poster)
            poster.purge_stale("git", sync_started_at)
        else:
            log.info("products.yml not loaded, skipping repos.")

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
        log.info(
            "Refreshing deployment state for %d customers (all configured envs)...",
            len(customers),
        )
        total_docs = 0
        for cust in customers:
            try:
                docs = CustomerDeployFetcher(cust, cfg).fetch_all_envs()
                if docs:
                    poster.post_batch(docs, workers=1)
                    total_docs += len(docs)
            except Exception as exc:
                log.error("Customer %s deployment fetch failed: %s", cust["id"], exc)
        log.info("Deployment state: %d docs indexed across all customers", total_docs)

    poster.summary()


if __name__ == "__main__":
    main()
