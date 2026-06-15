"""
Warehouse Intelligence Agent — lifecycle-aware agentic loop.

Lifecycle stages affect the agent's behaviour:

  solution → Knowledge-only.  No cluster configured.  LLM is told the customer
             is in design/scoping phase and answers from docs + Jira + code.
             Great for "how would this work when we have X?" questions.

  dev      → Cluster available.  Logs + deployment state from dev cluster.
             Typical questions: "why is task X not allocating in dev?"

  testing  → Same as dev but pointed at test cluster.
             Typical questions: "we're failing E2E test Y — what does the OGA log show?"

  staging  → Pre-prod.  Full live data.
             Typical questions: "staging shows allocator crashing on scenario Z"

  prod     → Full live data, phrasing emphasises "this is production, be careful".
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from tools import TOOL_DEFINITIONS, TOOL_REGISTRY

log = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 5
OLLAMA_TIMEOUT  = 120.0


# ─── Intent classification ────────────────────────────────────────────────────

_OPERATIONAL_RE = re.compile(
    r"(where.is|why.is.not|why.isn.?t|not.coming|stuck|error|crash|down|"
    r"what.happened|what.went.wrong|order.status|robot.status|"
    r"why.was.picked|why.did.*pick|allocation|assigned|operator|"
    r"version.running|which.version|deployed.version|pod.restart|"
    r"log|exception|traceback|500|timeout|connection.refused)",
    re.IGNORECASE,
)


def _is_operational(question: str) -> bool:
    return bool(_OPERATIONAL_RE.search(question))


# ─── System prompt  ───────────────────────────────────────────────────────────

def _system_prompt(customer_record: dict | None, env_config: dict | None,
                   env_name: str | None, product: str | None) -> str:
    lines = [
        "You are a GreyOrange warehouse intelligence assistant.",
        "Your role is to answer questions about warehouse operations, robot behaviour,",
        "order allocation, software versions, and system errors.",
        "",
    ]

    if not customer_record:
        lines += [
            "You are answering a general knowledge question (no specific customer context).",
            "Use the search_knowledge tool to find relevant docs, code, and Jira issues.",
        ]
        return "\n".join(lines)

    stage = customer_record.get("lifecycle_stage", "solution")
    name  = customer_record.get("name", customer_record.get("id", ""))
    products = customer_record.get("products", [])

    lines += [
        f"CUSTOMER: {name}",
        f"PRODUCTS: {', '.join(products) or 'not specified'}",
        f"LIFECYCLE STAGE: {stage.upper()}",
    ]
    if env_name:
        lines.append(f"ENVIRONMENT: {env_name}")
    if product:
        lines.append(f"PRODUCT FILTER: {product}")
    lines.append("")

    if stage == "solution" or not env_config:
        lines += [
            "⚠️  This customer is in the SOLUTION / PRE-DEPLOYMENT phase.",
            "   No live cluster is configured yet, so you cannot fetch pod logs or deployments.",
            "   Answer using knowledge only: docs, code, Jira issues, Confluence pages.",
            "   Frame your answers as 'this is how it works / would work for this use case'.",
            "   Use search_knowledge to find relevant documentation.",
            "",
            "GUIDELINES:",
            "- Explain how the system works based on docs and code.",
            "- If the question is 'will this work?', reason from architecture docs.",
            "- Cite Jira tickets and Confluence pages when relevant.",
            "- If a scenario is unsupported or unclear, say so and link to related tickets.",
        ]
    else:
        env_label = {"dev": "DEVELOPMENT", "testing": "TESTING / QA",
                     "staging": "STAGING / UAT", "prod": "PRODUCTION"}.get(stage, stage.upper())
        prod_warning = (
            "\n⚠️  THIS IS PRODUCTION — be conservative. Prefer diagnostic actions over fixes."
            if stage == "prod" else ""
        )
        lines += [
            f"📡 Live cluster data is available for the {env_label} environment.{prod_warning}",
            "",
            "You have tools to fetch real-time data from this customer's cluster.",
            "Combine live evidence with documentation to give a grounded, actionable answer.",
            "",
            "GUIDELINES:",
            "- Start with get_pod_status to see if any service is unhealthy or crashing.",
            "- Use get_logs for error/exception investigation.",
            "- Use get_logs with a relevant service/keyword filter when the question is about task/order assignment.",
            "- Use get_deployment_state when asked about versions or recent deployments.",
            "- Use search_knowledge to find matching Jira bugs or doc explanations.",
            "- Always say WHICH pod/log line you found the evidence in.",
            "- Cite Jira keys (AES-xxx, GM-xxx) when you know a known bug matches.",
            "- If you can't find evidence, say 'not found in logs' — don't guess.",
        ]

    return "\n".join(lines)


# ─── Agentic loop ─────────────────────────────────────────────────────────────

async def run_agent(
    question:        str,
    customer_id:     str | None,
    customer_record: dict | None,
    env_config:      dict | None,     # None = solution phase or no cluster
    product:         str | None,
    ollama_url:      str,
    ollama_model:    str,
    searchly_url:    str,
    searchly_tenant: str,
) -> dict[str, Any]:
    """
    Run the warehouse agent.

    Returns:
      { answer, tools_called, tool_results, is_operational, env_used }
    """
    stage      = (customer_record or {}).get("lifecycle_stage", "solution")
    has_cluster = bool(env_config and
                       (env_config.get("k8s_bastion") or env_config.get("k8s_context")))
    operational = _is_operational(question) and has_cluster

    # For the tools, build a fake "customer" dict that tools.py expects
    # (it only needs bastion/context/namespace/pod_map and an id)
    tools_customer: dict | None = None
    if has_cluster and customer_record:
        tools_customer = {
            "id":            customer_id,
            "k8s_bastion":   env_config.get("k8s_bastion", ""),
            "k8s_context":   env_config.get("k8s_context", ""),
            "k8s_namespace": env_config.get("k8s_namespace", "default"),
            "pod_map":       env_config.get("pod_map", {}),
        }

    env_name = _find_env_name(customer_record, env_config)

    sys_prompt = _system_prompt(customer_record, env_config, env_name, product)
    messages: list[dict] = [
        {"role": "system", "content": sys_prompt},
        {"role": "user",   "content": question},
    ]

    tools_called: list[str] = []
    tool_results: dict[str, Any] = {}
    answer = ""

    if not operational:
        # Solution phase or knowledge-only query — single-shot, no tool loop
        # Still allow search_knowledge if the model wants to call it
        answer = await _generate(ollama_url, ollama_model, messages, tools=None)
        return {
            "answer":        answer or _no_answer(stage, question),
            "tools_called":  [],
            "tool_results":  {},
            "is_operational": False,
            "env_used":      env_name,
        }

    # ── Operational agentic loop ──────────────────────────────────────────────
    # Inject cluster-aware customer for tools
    _RUNTIME = {
        "customer_obj":   tools_customer,
        "searchly_url":   searchly_url,
        "searchly_tenant": searchly_tenant,
    }

    for round_num in range(MAX_TOOL_ROUNDS):
        response = await _chat(ollama_url, ollama_model, messages, tools=TOOL_DEFINITIONS)
        assistant_msg = response.get("message", {})
        tool_calls    = assistant_msg.get("tool_calls", [])

        if not tool_calls:
            answer = assistant_msg.get("content", "")
            break

        messages.append({"role": "assistant", **assistant_msg})

        for tc in tool_calls:
            fn_name = tc.get("function", {}).get("name", "")
            fn_args = tc.get("function", {}).get("arguments", {})
            if isinstance(fn_args, str):
                try:
                    fn_args = json.loads(fn_args)
                except json.JSONDecodeError:
                    fn_args = {}

            log.info("[round %d] tool=%s args=%s", round_num + 1, fn_name,
                     {k: v for k, v in fn_args.items() if k not in ("grep",)})

            fn = TOOL_REGISTRY.get(fn_name)
            result: dict
            if fn is None:
                result = {"error": f"Unknown tool: {fn_name}"}
            else:
                # Inject runtime constants the model shouldn't have to supply
                if fn_name in ("get_logs", "get_deployment_state",
                               "get_pod_status", "list_log_indices"):
                    # Replace customer_id with the resolved customer obj
                    fn_args["customer_obj"] = _RUNTIME["customer_obj"]
                    fn_args.pop("customer_id", None)
                elif fn_name == "search_knowledge":
                    fn_args.setdefault("searchly_url",   _RUNTIME["searchly_url"])
                    fn_args.setdefault("tenant",         _RUNTIME["searchly_tenant"])
                    fn_args.setdefault("customer_id",    customer_id)
                try:
                    result = await fn(**fn_args)
                except Exception as e:
                    log.warning("Tool %s failed: %s", fn_name, e)
                    result = {"error": str(e)}

            tools_called.append(fn_name)
            tool_results[fn_name] = result
            messages.append({
                "role":    "tool",
                "content": json.dumps(result, default=str)[:8000],
            })
    else:
        # Hit max rounds — ask for final synthesis
        messages.append({
            "role":    "user",
            "content": "Based on all the data gathered, give your final answer."
        })
        answer = await _generate(ollama_url, ollama_model, messages, tools=None)

    return {
        "answer":          (answer or "").strip() or _no_answer(stage, question),
        "tools_called":    list(dict.fromkeys(tools_called)),
        "tool_results":    tool_results,
        "is_operational":  True,
        "env_used":        env_name,
    }


def _find_env_name(customer_record: dict | None, env_config: dict | None) -> str | None:
    """Given an env_config, find its name by matching in the customer's environments dict."""
    if not customer_record or not env_config:
        return None
    for name, cfg in customer_record.get("environments", {}).items():
        if cfg == env_config:
            return name
    return None


def _no_answer(stage: str, question: str) -> str:
    if stage == "solution":
        return (
            "This customer is in the solution design phase and has no live cluster yet. "
            "I can answer questions about how the system works, architecture, and "
            "known Jira issues. Please rephrase if you'd like a knowledge-based answer."
        )
    return (
        "I gathered available data but could not synthesise a clear answer. "
        "Check tool_results for raw evidence."
    )


# ─── Ollama HTTP ──────────────────────────────────────────────────────────────

async def _chat(url: str, model: str, messages: list[dict],
                tools: list | None) -> dict:
    body: dict[str, Any] = {"model": model, "messages": messages, "stream": False}
    if tools:
        body["tools"] = tools
    try:
        async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
            r = await client.post(f"{url}/api/chat", json=body)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Ollama /api/chat error: %s", e)
        return {"message": {"content": None, "tool_calls": []}}


async def _generate(url: str, model: str, messages: list[dict],
                    tools: list | None) -> str | None:
    resp = await _chat(url, model, messages, tools)
    return resp.get("message", {}).get("content")
