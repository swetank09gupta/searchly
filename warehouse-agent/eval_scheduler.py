"""
Continuous evaluation scheduler (P3.4).

Runs the eval suite nightly at 02:00 UTC via APScheduler.
Each run writes results to eval_history/<ISO-date>_<HH-MM>.json and compares
against the previous run to detect regressions (>10% drop in any key metric).

Wire up with: scheduler = EvalScheduler(...); scheduler.start()
Shut down with: scheduler.stop()
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from eval import run_eval

log = logging.getLogger(__name__)

HISTORY_DIR = Path(os.getenv("EVAL_HISTORY_DIR", "/app/eval_history"))
REGRESSION_THRESHOLD = float(os.getenv("EVAL_REGRESSION_THRESHOLD", "0.10"))

_TRACKED_METRICS = (
    "avg_answer_score",
    "avg_source_recall",
    "avg_retrieval_recall_at_20",
    "avg_keyword_hit_rate",
    "pass_rate",
)


class EvalScheduler:
    def __init__(
        self,
        agent_url: str,
        tenant: str,
        dataset_path: str,
        ollama_url: str,
        ollama_model: str,
        cron_hour: int = 2,
        cron_minute: int = 0,
    ):
        self.agent_url    = agent_url
        self.tenant       = tenant
        self.dataset_path = dataset_path
        self.ollama_url   = ollama_url
        self.ollama_model = ollama_model
        self._scheduler   = AsyncIOScheduler(timezone="UTC")
        self._scheduler.add_job(
            self._run_eval_job,
            trigger=CronTrigger(hour=cron_hour, minute=cron_minute, timezone="UTC"),
            id="nightly_eval",
            replace_existing=True,
            misfire_grace_time=3600,
        )

    def start(self):
        if not os.path.exists(self.dataset_path):
            log.warning(
                "Eval dataset not found at %s — nightly eval disabled. "
                "Create eval_dataset.json to enable.",
                self.dataset_path,
            )
            return
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        self._scheduler.start()
        log.info(
            "Nightly eval scheduler started (02:00 UTC). Dataset: %s",
            self.dataset_path,
        )

    def stop(self):
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    async def _run_eval_job(self):
        log.info("Starting nightly eval run")
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
        out_path = HISTORY_DIR / f"{ts}.json"

        try:
            summary = await _run_eval_capture(
                self.agent_url, self.tenant, self.dataset_path,
                self.ollama_url, self.ollama_model,
            )
            out_path.write_text(json.dumps(summary, indent=2))
            log.info("Eval run complete → %s", out_path)
            _check_regressions(summary, ts)
        except Exception:
            log.exception("Nightly eval run failed")


async def _run_eval_capture(
    agent_url: str, tenant: str, dataset_path: str,
    ollama_url: str, ollama_model: str,
) -> dict[str, Any]:
    """
    Run the eval suite and return the summary dict directly.
    Patches `run_eval` to capture the result instead of printing to stdout.
    """
    import importlib
    import io
    import sys
    import eval as eval_mod

    results: list[dict] = []
    _orig_open = __builtins__["open"] if isinstance(__builtins__, dict) else open

    with open(dataset_path) as f:
        dataset = json.load(f)

    import time
    import re
    import httpx
    from eval import call_agent, judge_answer, source_recall, retrieval_recall_at_k, keyword_hit_rate, reciprocal_rank

    for i, item in enumerate(dataset):
        qid      = item.get("id", f"q{i+1:03d}")
        question = item["question"]
        exp_src  = item.get("expected_sources", [])
        exp_kw   = item.get("expected_answer_keywords", [])
        customer = item.get("customer")
        env      = item.get("env")

        t0 = time.time()
        try:
            response     = await call_agent(agent_url, tenant, question, customer, env)
            elapsed      = time.time() - t0
            answer       = response.get("answer", "")
            sources      = response.get("sources", [])
            raw_chunks   = response.get("raw_sources", sources)

            src_recall   = source_recall(exp_src, sources)
            ret_recall   = retrieval_recall_at_k(exp_src, raw_chunks, k=20)
            kw_hit       = keyword_hit_rate(exp_kw, answer)
            mrr          = reciprocal_rank(exp_src, sources)
            answer_score = await judge_answer(ollama_url, ollama_model, question, exp_kw, answer)

            results.append({
                "id": qid, "question": question,
                "source_recall": round(src_recall, 3),
                "retrieval_recall_at_20": round(ret_recall, 3),
                "mrr": round(mrr, 3),
                "keyword_hit_rate": round(kw_hit, 3),
                "answer_score": answer_score,
                "elapsed_s": round(elapsed, 2),
            })
        except Exception as e:
            results.append({"id": qid, "question": question, "error": str(e)})

    ok = [r for r in results if "answer_score" in r]

    def avg(key: str) -> float:
        vals = [r[key] for r in ok if r.get(key) is not None and r.get(key) >= 0]
        return sum(vals) / len(vals) if vals else 0.0

    pass_rate = sum(1 for r in ok if r.get("answer_score", -1) >= 1) / max(len(ok), 1)
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n_questions": len(dataset),
        "n_scored": len(ok),
        "pass_rate":                     round(pass_rate, 4),
        "avg_answer_score":              round(avg("answer_score"), 4),
        "avg_source_recall":             round(avg("source_recall"), 4),
        "avg_retrieval_recall_at_20":    round(avg("retrieval_recall_at_20"), 4),
        "avg_mrr":                       round(avg("mrr"), 4),
        "avg_keyword_hit_rate":          round(avg("keyword_hit_rate"), 4),
        "results": results,
    }
    return summary


def _check_regressions(current: dict[str, Any], label: str):
    """Compare current run against the previous run; log warnings on regressions."""
    history_files = sorted(HISTORY_DIR.glob("*.json"))
    # The current file was just written; look for one before it
    prev_files = [f for f in history_files if f.stem < label]
    if not prev_files:
        log.info("No prior eval run to compare against — skipping regression check")
        return

    prev_path = prev_files[-1]
    try:
        prev = json.loads(prev_path.read_text())
    except Exception as e:
        log.warning("Could not read previous eval run %s: %s", prev_path, e)
        return

    regressions = []
    for metric in _TRACKED_METRICS:
        prev_val = prev.get(metric)
        curr_val = current.get(metric)
        if prev_val is None or curr_val is None or prev_val == 0:
            continue
        drop = (prev_val - curr_val) / prev_val
        if drop > REGRESSION_THRESHOLD:
            regressions.append(
                f"  {metric}: {prev_val:.3f} → {curr_val:.3f} ({drop:.1%} drop)"
            )

    if regressions:
        log.warning(
            "EVAL REGRESSION vs %s:\n%s", prev_path.name, "\n".join(regressions)
        )
    else:
        log.info("No regressions detected vs %s", prev_path.name)
