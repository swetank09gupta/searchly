# ADR 0019: Incremental Repository Indexing via SHA Check + Multi-Branch Support

**Status:** Accepted
**Date:** 2026-06-14 (updated 2026-06-15)
**Layer:** Intelligence Agent (connectors/sync.py — RepoIndexer)

## Context

The knowledge base indexes GitHub repositories. Depending on organisation size this may be
tens to hundreds of repos, each potentially with multiple relevant branches (default, develop,
release/*, hotfix/*). The sync cron runs every 4 hours.

A naive approach — re-clone and re-index all repos and branches on each cycle — would:
- Saturate disk I/O and network bandwidth
- Generate duplicate chunks in OpenSearch
- Take too long to fit inside a 4-hour cycle for large repo sets

## Decision

### Default branch (always)

Use a **HEAD SHA check** before cloning to skip unchanged repositories.

```
1. git ls-remote <repo_url> HEAD
   → returns current HEAD SHA (one HTTPS call, ~100ms, no clone)

2. Compare with .sync_state.json["repo_name"]:
   - SHA matches → unchanged → skip
   - SHA differs (or absent) → clone + index

3. After indexing:  .sync_state.json["repo_name"] = <new_SHA>
```

### Additional branches (`GIT_BRANCHES` in `.env`)

Set comma-separated branch names and/or glob patterns:

```
GIT_BRANCHES=develop,release/*,hotfix/*
```

For each configured pattern, `git ls-remote refs/heads/<pattern>` is called per repo.
This single lightweight call expands globs server-side and returns every matching branch
with its current SHA — **new branches are automatically discovered** without any config
change on the next sync cycle.

State is tracked per branch independently:
- Default branch → state key `repo_name`
- Named branch   → state key `repo_name:branch_name`

Each branch is indexed with `branch` metadata on every chunk, enabling filtered queries.

**First run:** `.sync_state.json` is empty → all repos and all configured branches are
cloned and indexed. Subsequent runs only process repos/branches with new commits.

**Force re-index:** `python sync.py --only repos --force` ignores state and re-indexes
all repos and all branches.

## Consequences

**Positive**
- **Fast incremental cycles:** only repos/branches with new commits incur a clone.
- **Minimal bandwidth:** `git ls-remote` is one HTTPS request per repo per pattern (~100 bytes each).
- **New branches auto-discovered:** `release/*` picks up `release/v2.5` the cycle after it's created — no config change needed.
- **Correct:** a SHA change means at least one new commit; nothing is skipped incorrectly.
- **Resilient:** interrupted syncs are safe; state only records completed repos/branches.

**Negative**
- **Stale index after config changes:** if chunking strategy, detection patterns, or
  `GIT_INCLUDE_EXTENSIONS` change, existing indexed content is not automatically refreshed.
  Run with `--force`.
- **No deletion tracking:** deleted files leave stale chunks in OpenSearch until the next
  forced re-index. For knowledge Q&A, stale architecture docs are a minor issue.
- **Branch-per-clone:** each branch is a separate `git clone --depth 1 --branch <name>`.
  For `N` configured patterns matching `M` branches across `R` repos with commits, total
  clones = `R + (branches with commits)`. Set `GIT_BRANCHES` to only patterns relevant to
  your team's branching model.

**Neutral**
- Private repos require a GitHub PAT (`GIT_TOKEN`) with `repo` scope. Without it,
  `git ls-remote` returns 404 and repos are silently skipped.
- State file: `/app/.sync_state.json` inside the container, on the `sync-state` volume.

## Alternatives Considered

| Alternative | Rejection reason |
|---|---|
| Re-clone all repos/branches every cycle | Too slow and too much bandwidth |
| GitHub webhooks | Requires public-facing webhook endpoint; adds infrastructure |
| GitHub Events API | More complex than `git ls-remote`; same correctness |
| Single clone, fetch all branches | `--depth 1` doesn't fetch non-default branches cleanly; separate clones are simpler |
| Track branches in products.yml | Manual; glob patterns + auto-discovery is zero-maintenance |
