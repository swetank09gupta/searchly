"""
Entity Extractor — uses the LLM to pull structured info from a question.

Given: "why is service X not responding in customer-a prod?"
Returns:
  {
    "customer_hint": "customer-a",
    "env_hint":      "prod",
    "product_hint":  null,
    "entity_ids":    ["X"],
    "intent":        "system_error"
  }

This runs a quick single-shot Ollama call (~0.5s) before the main agent loop,
so the resolver has structured input instead of raw freetext.

The LLM is told to output JSON — if it fails or times out, we fall back to
regex-based extraction which covers the common cases.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

log = logging.getLogger(__name__)

_EXTRACT_PROMPT = """Extract structured information from this support question.
Output ONLY valid JSON with these fields (use null if not found):
{
  "customer_hint": "the customer name or abbreviation mentioned",
  "env_hint": "dev | testing | staging | prod (if mentioned)",
  "product_hint": "pick-assist | core-platform | intralogistics | etc. (if mentioned)",
  "entity_ids": ["list of specific IDs: order IDs, asset IDs, operator IDs"],
  "intent": "order_status | asset_status | task_allocation | system_error | deployment | general"
}

Question: {question}

JSON:"""

# Regex fallback — catches the most common patterns without LLM
_CUSTOMER_HINTS = re.compile(
    r"\b(?:for|at|in|our|the|customer|client|site|warehouse)\s+"
    r"([A-Za-z0-9][A-Za-z0-9\s\-'\.]{2,40}?)(?:\s+(?:prod|staging|dev|test|env|cluster|site|dc)|\s*[?,\.]|$)",
    re.IGNORECASE,
)
_ENV_RE = re.compile(
    r"\b(prod(?:uction)?|staging|uat|pre.prod|test(?:ing)?|dev(?:elopment)?|qa)\b",
    re.IGNORECASE,
)
_ORDER_RE  = re.compile(r"\border[_\-\s]?(?:id[:\s#]?)?\s*([A-Z0-9\-]{4,20})\b", re.IGNORECASE)
_ROBOT_RE  = re.compile(r"\b(?:robot|bot|vehicle)\s*(?:id[:\s#]?)?\s*(\w+)\b", re.IGNORECASE)

_ENV_MAP = {
    "production": "prod", "prod": "prod",
    "staging": "staging", "uat": "staging", "pre-prod": "staging",
    "testing": "testing", "test": "testing", "qa": "testing",
    "development": "dev", "dev": "dev",
}

_FALLBACK_PRODUCTS = ["pick-assist", "core-platform", "intralogistics", "solution-builder", "rdc", "wms", "sre", "ai-ml"]


def _known_products() -> list[str]:
    try:
        from products_config import ids, load
        result = ids()
        if not result:   # load() not yet called (e.g. in unit tests)
            load()
            result = ids()
        return result or _FALLBACK_PRODUCTS
    except Exception:
        return _FALLBACK_PRODUCTS


_INTENT_PATTERNS = [
    (re.compile(r"order|tote|container|where.is|status.of",       re.I), "order_status"),
    (re.compile(r"robot|bot|vehicle|not.coming|not.moving|stuck", re.I), "robot_status"),
    (re.compile(r"allocat|assign|hungarian|task.order|picked.first", re.I), "task_allocation"),
    (re.compile(r"error|crash|exception|down|timeout|restart|500",  re.I), "system_error"),
    (re.compile(r"version|deployed|release|upgrade|image.tag",     re.I), "deployment"),
]


_STOPWORDS = frozenset({
    "is", "are", "was", "were", "has", "have", "had", "do", "does", "did",
    "in", "of", "at", "by", "for", "with", "to", "from", "on", "about",
    "a", "an", "the", "and", "or", "but", "not", "it", "its",
    "that", "this", "which", "who", "what", "where", "when", "how",
    "terms", "their", "our", "your",
})


def _trim_at_stopword(hint: str) -> str | None:
    """Keep words up to (not including) the first English function word."""
    words = hint.split()
    result = []
    for w in words:
        if w.lower() in _STOPWORDS:
            break
        result.append(w)
    return " ".join(result) if result else None


def _regex_extract(question: str) -> dict[str, Any]:
    """Fast regex fallback when LLM extraction fails."""
    env_m = _ENV_RE.search(question)
    env   = _ENV_MAP.get(_norm_env(env_m.group(1))) if env_m else None

    entities: list[str] = []
    for m in _ORDER_RE.finditer(question):
        entities.append(m.group(1))
    for m in _ROBOT_RE.finditer(question):
        entities.append(f"robot:{m.group(1)}")

    intent = "general"
    for pattern, name in _INTENT_PATTERNS:
        if pattern.search(question):
            intent = name
            break

    # Product hint: only accept known product IDs
    product_hint = None
    ql = question.lower()
    for pid in _known_products():
        if pid in ql or pid.replace("-", " ") in ql:
            product_hint = pid
            break

    # Customer hint: remove known env words and strip common prefixes
    customer_m = _CUSTOMER_HINTS.search(question)
    customer_hint = customer_m.group(1).strip() if customer_m else None
    if customer_hint:
        # Truncate at the first English function word — prevents "Acme is in terms of"
        # from a query like "project for Acme is in terms of development?"
        customer_hint = _trim_at_stopword(customer_hint)
        if customer_hint:
            customer_hint = re.sub(
                r"\b(prod(?:uction)?|staging|uat|test(?:ing)?|dev(?:elopment)?|qa|env|cluster"
                r"|product[s]?|line[s]?|customer[s]?|client[s]?|warehouse[s]?|name"
                r"|deploy(?:ment)?|service[s]?|project[s]?|issue[s]?|error[s]?|status)\b",
                "", customer_hint, flags=re.I
            ).strip(" ,.")
            # Remove product name from customer hint if it slipped in
            for pid in _known_products():
                customer_hint = customer_hint.replace(pid, "").replace(pid.replace("-", " "), "")
            customer_hint = customer_hint.strip(" ,.") or None

    return {
        "customer_hint": customer_hint,
        "env_hint":      env,
        "product_hint":  product_hint,
        "entity_ids":    entities,
        "intent":        intent,
    }


def _norm_env(s: str) -> str:
    return s.lower().replace(" ", "-").replace("_", "-")


async def extract_entities(question: str, ollama_url: str,
                           ollama_model: str) -> dict[str, Any]:
    """
    Extract structured entities from the question.
    Tries LLM first; falls back to regex on failure/timeout.
    """
    prompt = _EXTRACT_PROMPT.replace("{question}", question)
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{ollama_url}/api/generate",
                json={"model": ollama_model, "prompt": prompt, "stream": False},
            )
        if r.status_code == 200:
            raw = r.json().get("response", "")
            # Find the JSON blob even if wrapped in markdown
            m = re.search(r"\{[\s\S]+\}", raw)
            if m:
                parsed = json.loads(m.group())
                # Normalise env value
                if parsed.get("env_hint"):
                    parsed["env_hint"] = _ENV_MAP.get(
                        _norm_env(parsed["env_hint"]), parsed["env_hint"]
                    )
                log.debug("Entity extraction (LLM): %s", parsed)
                return parsed
    except Exception as e:
        log.debug("LLM entity extraction failed (%s), using regex fallback", e)

    result = _regex_extract(question)
    log.debug("Entity extraction (regex): %s", result)
    return result
