"""
Evaluation framework for the intelligence agent (P2.5).

Runs a test dataset of question/expected_sources/expected_answer triples against
the live agent and reports:
  - Source recall@k   : fraction of expected sources found in top-k results
  - Answer faithfulness: LLM-as-judge scoring (0-2) for each answer
  - Pass rate         : fraction of questions with score >= 1

Usage:
    python eval.py [--url http://localhost:8084] [--tenant demo-tenant] [--dataset eval_dataset.json]

Dataset format (eval_dataset.json):
[
  {
    "id": "q001",
    "question": "Why is the allocator crashing on customer acme-corp in prod?",
    "expected_sources": ["AES-891", "operator-backend"],
    "expected_answer_keywords": ["redis", "connection", "timeout"],
    "customer": "acme-corp",
    "env": "prod"
  },
  ...
]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

DEFAULT_DATASET = os.path.join(os.path.dirname(__file__), "eval_dataset.json")
JUDGE_PROMPT_TEMPLATE = """\
You are an evaluation judge for a warehouse intelligence assistant.

Question: {question}

Expected answer must mention: {expected_keywords}

Actual answer:
{actual_answer}

Score the actual answer on a scale of 0-2:
  0 = completely wrong or missing key information
  1 = partially correct, covers some expected points
  2 = correct and comprehensive

Reply with ONLY the score (0, 1, or 2), no explanation.
"""


async def call_agent(base_url: str, tenant: str, question: str,
                     customer: str | None, env: str | None) -> dict[str, Any]:
    payload = {
        "message": question,
        "customer": customer,
        "env": env,
    }
    headers = {"X-Tenant-Id": tenant, "X-User-Roles": "VIEWER"}
    async with httpx.AsyncClient(timeout=180.0) as client:
        r = await client.post(f"{base_url}/chat", json=payload, headers=headers)
        r.raise_for_status()
        return r.json()


async def judge_answer(ollama_url: str, ollama_model: str,
                       question: str, expected_keywords: list[str],
                       actual_answer: str) -> int:
    prompt = JUDGE_PROMPT_TEMPLATE.format(
        question=question,
        expected_keywords=", ".join(expected_keywords),
        actual_answer=actual_answer[:2000],
    )
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            f"{ollama_url}/api/generate",
            json={"model": ollama_model, "prompt": prompt, "stream": False},
        )
        if r.status_code != 200:
            return -1
        text = r.json().get("response", "").strip()
        m = re.search(r"[012]", text)
        return int(m.group(0)) if m else -1


# ── Individual metric functions ──────────────────────────────────────────────

def source_recall(expected_sources: list[str], actual_sources: list[str]) -> float:
    """Did the agent return the right source types? (e.g. JIRA, DEPLOYMENT)"""
    if not expected_sources:
        return 1.0
    hits = sum(
        1 for es in expected_sources
        if any(es.lower() in s.lower() for s in actual_sources)
    )
    return hits / len(expected_sources)


def retrieval_recall_at_k(expected_sources: list[str],
                           retrieved_chunks: list[str], k: int = 20) -> float:
    """
    Did the retrieval layer surface at least one expected source in top-k chunks?
    Measures the retrieval stack independently of the final answer.
    Requires the agent response to include 'raw_sources' (pre-rerank list).
    """
    if not expected_sources or not retrieved_chunks:
        return 0.0
    top_k = retrieved_chunks[:k]
    for es in expected_sources:
        if any(es.lower() in s.lower() for s in top_k):
            return 1.0
    return 0.0


def keyword_hit_rate(expected_keywords: list[str], answer: str) -> float:
    """Fraction of expected keywords present in the final answer (fast, no LLM)."""
    if not expected_keywords:
        return 1.0
    answer_l = answer.lower()
    hits = sum(1 for kw in expected_keywords if kw.lower() in answer_l)
    return hits / len(expected_keywords)


def reciprocal_rank(expected_sources: list[str], actual_sources: list[str]) -> float:
    """MRR contribution: 1/rank of the first expected source found in actual list."""
    for rank, src in enumerate(actual_sources, start=1):
        for es in expected_sources:
            if es.lower() in src.lower():
                return 1.0 / rank
    return 0.0


async def run_eval(base_url: str, tenant: str, dataset_path: str,
                   ollama_url: str, ollama_model: str) -> None:
    with open(dataset_path) as f:
        dataset = json.load(f)

    results = []
    for i, item in enumerate(dataset):
        qid       = item.get("id", f"q{i+1:03d}")
        question  = item["question"]
        exp_src   = item.get("expected_sources", [])
        exp_kw    = item.get("expected_answer_keywords", [])
        customer  = item.get("customer")
        env       = item.get("env")

        log.info("[%s] %s", qid, question[:80])
        t0 = time.time()
        try:
            response   = await call_agent(base_url, tenant, question, customer, env)
            elapsed    = time.time() - t0
            answer     = response.get("answer", "")
            sources    = response.get("sources", [])         # final sources used
            raw_chunks = response.get("raw_sources", sources)  # pre-rerank if available
            live_data  = response.get("has_live_data", False)

            # ── Three independent metrics ──────────────────────────────────────
            src_recall  = source_recall(exp_src, sources)
            ret_recall  = retrieval_recall_at_k(exp_src, raw_chunks, k=20)
            kw_hit      = keyword_hit_rate(exp_kw, answer)
            mrr         = reciprocal_rank(exp_src, sources)
            # LLM judge for final answer quality (heavier, but most informative)
            answer_score = await judge_answer(ollama_url, ollama_model, question, exp_kw, answer)

            results.append({
                "id":              qid,
                "question":        question,
                # Retrieval metrics (independent of LLM answer)
                "source_recall":   round(src_recall, 3),
                "retrieval_recall_at_20": round(ret_recall, 3),
                "mrr":             round(mrr, 3),
                # Answer metrics
                "keyword_hit_rate": round(kw_hit, 3),
                "answer_score":    answer_score,   # 0/1/2 from LLM judge
                # Meta
                "has_live_data":   live_data,
                "elapsed_s":       round(elapsed, 2),
                "answer_snippet":  answer[:200],
            })
            print(f"  [{qid}] ans={answer_score} src_recall={src_recall:.2f} "
                  f"ret_recall={ret_recall:.2f} kw={kw_hit:.2f} t={elapsed:.1f}s")
        except Exception as e:
            log.error("[%s] FAILED: %s", qid, e)
            results.append({"id": qid, "question": question, "error": str(e)})

    # Aggregate metrics
    ok = [r for r in results if "answer_score" in r]
    if not ok:
        print("No scored results.")
        return

    def avg(key: str) -> float:
        vals = [r[key] for r in ok if r.get(key) is not None and r.get(key) >= 0]
        return sum(vals) / len(vals) if vals else 0.0

    pass_rate     = sum(1 for r in ok if r.get("answer_score", -1) >= 1) / len(ok)
    avg_src_rec   = avg("source_recall")
    avg_ret_rec   = avg("retrieval_recall_at_20")
    avg_mrr       = avg("mrr")
    avg_kw        = avg("keyword_hit_rate")
    avg_ans       = avg("answer_score")

    print("\n── Evaluation Summary ───────────────────────────────────────────")
    print(f"  Questions          : {len(dataset)}")
    print(f"  Scored             : {len(ok)}")
    print(f"  Pass rate          : {pass_rate:.1%}  (answer_score >= 1)")
    print(f"")
    print(f"  ── Retrieval ─────────────────────────────────────────────────")
    print(f"  Source recall      : {avg_src_rec:.3f}  (did final answer use right sources?)")
    print(f"  Retrieval recall@20: {avg_ret_rec:.3f}  (was answer in top-20 before rerank?)")
    print(f"  MRR                : {avg_mrr:.3f}  (rank of first correct source)")
    print(f"")
    print(f"  ── Answer Quality ────────────────────────────────────────────")
    print(f"  Keyword hit rate   : {avg_kw:.3f}  (expected keywords present)")
    print(f"  LLM judge score    : {avg_ans:.2f} / 2.0")

    out_path = "eval_results.json"
    with open(out_path, "w") as f:
        json.dump({
            "summary": {
                "pass_rate":              pass_rate,
                "avg_answer_score":       avg_ans,
                "avg_source_recall":      avg_src_rec,
                "avg_retrieval_recall_at_20": avg_ret_rec,
                "avg_mrr":                avg_mrr,
                "avg_keyword_hit_rate":   avg_kw,
                "n_questions": len(dataset),
                "n_scored":    len(ok),
            },
            "results": results,
        }, f, indent=2)
    print(f"\nDetailed results written to {out_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Intelligence agent evaluator")
    parser.add_argument("--url",     default="http://localhost:8084", help="Agent base URL")
    parser.add_argument("--tenant",  default="demo-tenant")
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--ollama-url",   default="http://localhost:11434")
    parser.add_argument("--ollama-model", default="llama3.2:3b")
    args = parser.parse_args()

    if not os.path.exists(args.dataset):
        print(f"Dataset not found: {args.dataset}")
        print("Create eval_dataset.json with your test questions. See module docstring for format.")
        sys.exit(1)

    asyncio.run(run_eval(args.url, args.tenant, args.dataset,
                         args.ollama_url, args.ollama_model))
