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

    # ── Agentic loop: Planner → Execution → Synthesis ───────────────────────
    # search_knowledge is always available; live cluster tools only when cluster exists.
    _RUNTIME = {
        "customer_obj":   tools_customer,
        "searchly_url":   searchly_url,
        "searchly_tenant": searchly_tenant,
    }

    # Phase 1: Planning — ask LLM what tools to call and in what order.
    # For knowledge_only queries (no live cluster), search_knowledge is the only tool available,
    # so skip the LLM planner and call it directly. llama3.2:3b is too small to reliably
    # output structured JSON when it has a choice, but here there is no choice.
    if not operational:
        tool_calls_to_run = [{"function": {"name": "search_knowledge",
                                           "arguments": {"query": question}}}]
        log.info("knowledge_only: auto-calling search_knowledge")
    else:
        tool_calls_to_run = await _plan_tools(
            ollama_url, ollama_model, messages, tools_customer, customer_id,
            searchly_url, searchly_tenant,
            knowledge_only=False,
        )

    # Phase 2: Execution — run all planned tools.
    # Tools are independent (planner outputs a flat list, not a chain), so run in parallel.
    import asyncio

    async def _run_one(tc: dict) -> tuple[str, dict]:
        fn_name = tc.get("function", {}).get("name", "")
        fn_args = tc.get("function", {}).get("arguments", {})
        if isinstance(fn_args, str):
            try:
                fn_args = json.loads(fn_args)
            except json.JSONDecodeError:
                fn_args = {}
        log.info("[plan] tool=%s args=%s", fn_name,
                 {k: v for k, v in fn_args.items() if k not in ("grep",)})
        fn = TOOL_REGISTRY.get(fn_name)
        if fn is None:
            return fn_name, {"error": f"Unknown tool: {fn_name}"}
        if fn_name in ("get_logs", "get_deployment_state", "get_pod_status", "list_log_indices"):
            fn_args["customer_obj"] = _RUNTIME["customer_obj"]
            fn_args.pop("customer_id", None)
        elif fn_name == "search_knowledge":
            fn_args.setdefault("searchly_url",   _RUNTIME["searchly_url"])
            fn_args.setdefault("tenant",         _RUNTIME["searchly_tenant"])
            fn_args.setdefault("customer_id",    customer_id)
        try:
            return fn_name, await fn(**fn_args)
        except Exception as e:
            log.warning("Tool %s failed: %s", fn_name, e)
            return fn_name, {"error": str(e)}

    results_list = await asyncio.gather(*[_run_one(tc) for tc in tool_calls_to_run])
    for fn_name, result in results_list:
        tools_called.append(fn_name)
        tool_results[fn_name] = result
        messages.append({
            "role":    "tool",
            "content": json.dumps(result, default=str)[:8000],
        })

    # Phase 3: Synthesis — generate the final answer from all gathered evidence
    messages.append({
        "role":    "user",
        "content": "Based on all the data gathered above, give your final answer."
    })
    answer = await _generate(ollama_url, ollama_model, messages, tools=None)

    return {
        "answer":          (answer or "").strip() or _no_answer(stage, question),
        "tools_called":    list(dict.fromkeys(tools_called)),
        "tool_results":    tool_results,
        "is_operational":  operational,
        "env_used":        env_name,
    }


_LIVE_CLUSTER_TOOLS = frozenset({"get_logs", "get_deployment_state", "get_pod_status", "list_log_indices"})


async def _plan_tools(
    ollama_url: str, ollama_model: str, messages: list[dict],
    tools_customer: dict | None, customer_id: str | None,
    searchly_url: str, searchly_tenant: str,
    knowledge_only: bool = False,
) -> list[dict]:
    """
    Planner phase: ask the LLM to generate a list of tool calls without executing them.
    Returns a list of {function: {name, arguments}} dicts, capped at MAX_TOOL_ROUNDS.
    Falls back to an empty list on failure (caller then skips to synthesis with no tool data).

    knowledge_only=True: only search_knowledge is offered (no live cluster tools).
    """
    available_tools = [
        td["function"]["name"]
        for td in TOOL_DEFINITIONS
        if not (knowledge_only and td["function"]["name"] in _LIVE_CLUSTER_TOOLS)
    ]
    plan_messages = messages + [{
        "role":    "user",
        "content": (
            "Before answering, decide which tools you need to call and in what order. "
            "Reply with ONLY a JSON array of tool calls, no explanation. Format:\n"
            '[{"function":{"name":"<tool>","arguments":{<args>}}}, ...]\n'
            "Limit to at most 5 tool calls. Available tools: "
            + ", ".join(available_tools)
        ),
    }]
    try:
        raw = await _generate(ollama_url, ollama_model, plan_messages, tools=None)
        if not raw:
            return []
        # Extract the JSON array from the response
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if not m:
            return []
        tool_calls = json.loads(m.group(0))
        if not isinstance(tool_calls, list):
            return []
        # Validate, cap, and filter out live-cluster tools when not available
        valid = []
        for tc in tool_calls[:MAX_TOOL_ROUNDS]:
            if isinstance(tc, dict) and "function" in tc:
                fn_name = tc["function"].get("name", "")
                if fn_name not in TOOL_REGISTRY:
                    continue
                if knowledge_only and fn_name in _LIVE_CLUSTER_TOOLS:
                    continue
                valid.append(tc)
        log.info("Planner produced %d tool calls: %s",
                 len(valid), [tc["function"]["name"] for tc in valid])
        return valid
    except Exception as e:
        log.debug("Planner failed (falling back to synthesis only): %s", e)
        return []


def _find_env_name(customer_record: dict | None, env_config: dict | None) -> str | None:
    """Given an env_config, find its name by matching in the customer's environments dict."""
    if not customer_record or not env_config:
        return None
    for name, cfg in customer_record.get("environments", {}).items():
        if cfg == env_config:
            return name
    return None


def _tool_status_message(fn_name: str, fn_args: dict) -> str:
    if fn_name == "search_knowledge":
        q = fn_args.get("query", "")
        return f"Searching knowledge base{': ' + q[:50] + '…' if q else ''}…"
    if fn_name == "get_logs":
        svc = fn_args.get("service", "")
        return f"Fetching logs{' for ' + svc if svc else ''}…"
    if fn_name == "get_pod_status":
        return "Checking pod status…"
    if fn_name == "get_deployment_state":
        return "Reading deployment state…"
    if fn_name == "list_log_indices":
        return "Listing log indices…"
    return f"Running {fn_name}…"


async def _generate_stream(url: str, model: str, messages: list[dict]):
    """Yield content tokens from Ollama streaming /api/chat."""
    body: dict[str, Any] = {"model": model, "messages": messages, "stream": True}
    try:
        async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
            async with client.stream("POST", f"{url}/api/chat", json=body) as resp:
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        chunk = json.loads(line)
                        content = chunk.get("message", {}).get("content", "")
                        if content:
                            yield content
                    except (json.JSONDecodeError, KeyError):
                        continue
    except Exception as e:
        log.warning("Ollama stream error: %s", e)


async def run_agent_stream(
    question:        str,
    customer_id:     str | None,
    customer_record: dict | None,
    env_config:      dict | None,
    product:         str | None,
    ollama_url:      str,
    ollama_model:    str,
    searchly_url:    str,
    searchly_tenant: str,
):
    """
    Streaming version of run_agent.  Yields dicts:
      {"type": "status",  "message": "..."}         — pipeline progress for the UI
      {"type": "token",   "content": "..."}          — synthesis tokens
      {"type": "done", "answer": "...", ...}          — final metadata
    """
    stage       = (customer_record or {}).get("lifecycle_stage", "solution")
    has_cluster = bool(env_config and
                       (env_config.get("k8s_bastion") or env_config.get("k8s_context")))
    operational = _is_operational(question) and has_cluster

    tools_customer: dict | None = None
    if has_cluster and customer_record:
        tools_customer = {
            "id":            customer_id,
            "k8s_bastion":   env_config.get("k8s_bastion", ""),
            "k8s_context":   env_config.get("k8s_context", ""),
            "k8s_namespace": env_config.get("k8s_namespace", "default"),
            "pod_map":       env_config.get("pod_map", {}),
        }

    env_name   = _find_env_name(customer_record, env_config)
    sys_prompt = _system_prompt(customer_record, env_config, env_name, product)
    messages: list[dict] = [
        {"role": "system", "content": sys_prompt},
        {"role": "user",   "content": question},
    ]
    tools_called: list[str] = []
    tool_results: dict[str, Any] = {}

    _RUNTIME = {
        "customer_obj":    tools_customer,
        "searchly_url":    searchly_url,
        "searchly_tenant": searchly_tenant,
    }

    # Phase 1: Planning
    if not operational:
        tool_calls_to_run = [{"function": {"name": "search_knowledge",
                                           "arguments": {"query": question}}}]
    else:
        yield {"type": "status", "message": "Planning investigation…"}
        tool_calls_to_run = await _plan_tools(
            ollama_url, ollama_model, messages, tools_customer, customer_id,
            searchly_url, searchly_tenant, knowledge_only=False,
        )

    # Phase 2: Execution — run all tools in parallel, emit a single combined status line
    import asyncio

    if tool_calls_to_run:
        tool_names = [tc.get("function", {}).get("name", "") for tc in tool_calls_to_run]
        yield {"type": "status", "message": "Running: " + ", ".join(tool_names) + "…"}

    async def _run_one_stream(tc: dict) -> tuple[str, dict]:
        fn_name = tc.get("function", {}).get("name", "")
        fn_args = tc.get("function", {}).get("arguments", {})
        if isinstance(fn_args, str):
            try:
                fn_args = json.loads(fn_args)
            except json.JSONDecodeError:
                fn_args = {}
        fn = TOOL_REGISTRY.get(fn_name)
        if fn is None:
            return fn_name, {"error": f"Unknown tool: {fn_name}"}
        if fn_name in ("get_logs", "get_deployment_state", "get_pod_status", "list_log_indices"):
            fn_args["customer_obj"] = _RUNTIME["customer_obj"]
            fn_args.pop("customer_id", None)
        elif fn_name == "search_knowledge":
            fn_args.setdefault("searchly_url",    _RUNTIME["searchly_url"])
            fn_args.setdefault("tenant",          _RUNTIME["searchly_tenant"])
            fn_args.setdefault("customer_id",     customer_id)
        try:
            return fn_name, await fn(**fn_args)
        except Exception as e:
            log.warning("Tool %s failed: %s", fn_name, e)
            return fn_name, {"error": str(e)}

    results_list = await asyncio.gather(*[_run_one_stream(tc) for tc in tool_calls_to_run])
    for fn_name, result in results_list:
        tools_called.append(fn_name)
        tool_results[fn_name] = result
        messages.append({
            "role":    "tool",
            "content": json.dumps(result, default=str)[:8000],
        })

    # Phase 3: Streaming synthesis
    yield {"type": "status", "message": "Generating answer…"}
    messages.append({
        "role":    "user",
        "content": "Based on all the data gathered above, give your final answer.",
    })

    full_answer = ""
    async for token in _generate_stream(ollama_url, ollama_model, messages):
        full_answer += token
        yield {"type": "token", "content": token}

    if not full_answer:
        full_answer = _no_answer(stage, question)
        yield {"type": "token", "content": full_answer}

    yield {
        "type":           "done",
        "answer":         full_answer,
        "tools_called":   list(dict.fromkeys(tools_called)),
        "tool_results":   tool_results,
        "is_operational": operational,
        "env_used":       env_name,
    }


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
