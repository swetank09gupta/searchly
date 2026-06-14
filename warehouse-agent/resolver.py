"""
Fuzzy Customer + Environment Resolver

Translates messy natural-language references into registry IDs.

Customer resolution priority:
  1. Exact ID match              ("sams-club-atlanta")
  2. Alias match                 (stored aliases: ["samsatl", "sam's club atl"])
  3. Token Jaccard similarity    ("samsclub atl" ~ "sams-club-atlanta")
  4. Normalised edit distance    ("samsclub" ~ "sams-club")
  5. LLM disambiguation          (when score is borderline, ask Ollama)

Environment resolution:
  - Regex on the question text:  "in prod", "dev cluster", "staging broke", etc.
  - Fallback: highest configured env for that customer

Alias learning:
  - Every time a fuzzy match succeeds, the hint string is stored as an alias
    so it resolves instantly next time (zero recomputation).
"""
from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any

# Confidence thresholds
EXACT_THRESHOLD    = 1.00
HIGH_THRESHOLD     = 0.80   # resolve silently
MEDIUM_THRESHOLD   = 0.55   # resolve with note ("I'm assuming you mean X")
LOW_THRESHOLD      = 0.30   # ask for confirmation
UNKNOWN_THRESHOLD  = 0.00   # ask for full clarification + offer to register


# ─── Text normalisation ───────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase, strip accents, collapse punctuation+whitespace to single space."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(s: str) -> set[str]:
    return set(_norm(s).split())


def _jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta and not tb:
        return 1.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def _edit_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()


def _score(hint: str, candidate_id: str, candidate_name: str,
           aliases: list[str]) -> float:
    hint_n = _norm(hint)
    # Alias exact match
    if hint_n in {_norm(a) for a in aliases}:
        return EXACT_THRESHOLD
    # ID / name edit similarity
    id_sim   = _edit_sim(hint, candidate_id)
    name_sim = _edit_sim(hint, candidate_name)
    jac_id   = _jaccard(hint, candidate_id)
    jac_name = _jaccard(hint, candidate_name)
    return max(id_sim, name_sim, jac_id, jac_name)


# ─── Environment extraction from text ────────────────────────────────────────

_ENV_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bproduction\b|\bprod\b",        re.I), "prod"),
    (re.compile(r"\bstaging\b|\buat\b|\bpre.prod\b", re.I), "staging"),
    (re.compile(r"\btesting\b|\btest\b|\bqa\b",    re.I), "testing"),
    (re.compile(r"\bdev(elopment)?\b|\bdevelop\b", re.I), "dev"),
]


def extract_env_hint(text: str) -> str | None:
    """Return the first env name found in the text, or None."""
    for pattern, env in _ENV_PATTERNS:
        if pattern.search(text):
            return env
    return None


# ─── Resolver ─────────────────────────────────────────────────────────────────

class ResolutionResult:
    """
    The result of trying to resolve a customer hint.

    Attributes:
      customer_id     resolved ID, or None
      env             resolved env, or None
      confidence      float 0–1
      needs_confirm   True → agent should tell the user what it assumed
      needs_input     True → agent must ask a clarification question
      candidates      list of (id, name, score) for multi-candidate cases
      message         human-readable explanation of what happened
    """
    __slots__ = ("customer_id", "env", "confidence", "needs_confirm",
                 "needs_input", "candidates", "message")

    def __init__(self, *, customer_id=None, env=None, confidence=0.0,
                 needs_confirm=False, needs_input=False,
                 candidates=None, message=""):
        self.customer_id  = customer_id
        self.env          = env
        self.confidence   = confidence
        self.needs_confirm = needs_confirm
        self.needs_input  = needs_input
        self.candidates   = candidates or []
        self.message      = message


class CustomerResolver:
    """
    Resolves free-text customer references to registry IDs.
    Mutates the registry to add aliases when a fuzzy match succeeds.
    """

    def __init__(self, registry):
        self._registry = registry

    def resolve(self, hint: str | None, env_hint: str | None,
                question: str = "") -> ResolutionResult:
        """
        Main entry point.

        hint       — raw customer string from the question (may be None)
        env_hint   — raw env string from question params or extracted text
        question   — full question text, used for env extraction if env_hint is None
        """
        # Extract env from the question text if not supplied
        if not env_hint:
            env_hint = extract_env_hint(question)

        if not hint:
            return ResolutionResult(
                needs_input=True,
                message="Which customer or warehouse are you asking about?",
            )

        hint_n = _norm(hint)
        customers = self._registry.list_customers()

        if not customers:
            from products_config import product_menu
            return ResolutionResult(
                needs_input=True,
                message=(
                    f"No customers are registered yet. "
                    f"Should I register **'{hint}'** now?\n\n"
                    f"Which products do they use? Reply with the number(s):\n\n"
                    + product_menu()
                ),
                candidates=[],
            )

        # ── Score every customer ──────────────────────────────────────────────
        scored: list[tuple[float, dict]] = []
        for c in customers:
            s = _score(hint, c["id"], c["name"], c.get("aliases", []))
            scored.append((s, c))
        scored.sort(key=lambda x: x[0], reverse=True)

        best_score, best = scored[0]

        # ── Resolve env within the matched customer ───────────────────────────
        def _pick_env(customer: dict) -> str | None:
            envs = customer.get("environments", {})
            if env_hint and env_hint in envs:
                return env_hint
            # Fallback: highest configured env
            from customer_registry import LIFECYCLE_ORDER
            for stage in reversed(LIFECYCLE_ORDER[1:]):
                if stage in envs:
                    return stage
            return env_hint  # store intent even if not configured yet

        # ── Exact or high-confidence match → resolve silently ─────────────────
        if best_score >= HIGH_THRESHOLD:
            self._learn_alias(best["id"], hint)
            env = _pick_env(best)
            return ResolutionResult(
                customer_id  = best["id"],
                env          = env,
                confidence   = best_score,
                needs_confirm = best_score < EXACT_THRESHOLD,
                message      = (
                    f"Resolved to '{best['name']}'"
                    + (f" ({env} env)" if env else "")
                    + ("" if best_score >= EXACT_THRESHOLD else f" (confidence {best_score:.0%})")
                ),
            )

        # ── Medium confidence → resolve but mention assumption ─────────────────
        if best_score >= MEDIUM_THRESHOLD:
            self._learn_alias(best["id"], hint)
            env = _pick_env(best)
            return ResolutionResult(
                customer_id  = best["id"],
                env          = env,
                confidence   = best_score,
                needs_confirm = True,
                message      = (
                    f"I'm assuming you mean **{best['name']}** — "
                    f"let me know if that's wrong."
                ),
            )

        # ── Low confidence → ask with candidates ──────────────────────────────
        top3 = [(s, c) for s, c in scored[:3] if s >= LOW_THRESHOLD]
        if top3:
            options = "\n".join(
                f"  • {c['name']} (id: `{c['id']}`)" for _, c in top3
            )
            return ResolutionResult(
                confidence   = best_score,
                needs_input  = True,
                candidates   = [(c["id"], c["name"], s) for s, c in top3],
                message      = (
                    f"I couldn't confidently match **'{hint}'** to a customer. "
                    f"Did you mean one of these?\n{options}\n\n"
                    f"Or say 'none of those' to register a new customer."
                ),
            )

        # ── No match → offer to register ──────────────────────────────────────
        from products_config import product_menu
        return ResolutionResult(
            confidence  = 0.0,
            needs_input = True,
            candidates  = [],
            message     = (
                f"I don't have a customer matching **'{hint}'** in the registry.\n\n"
                f"I can answer from general knowledge right now, "
                f"or register this as a new customer.\n\n"
                f"Which products does this customer use? Reply with the number(s):\n\n"
                + product_menu()
            ),
        )

    def _learn_alias(self, customer_id: str, hint: str):
        """Store the fuzzy hint as an alias so it resolves instantly next time."""
        hint_n = _norm(hint)
        try:
            c = self._registry.get(customer_id)
            if c:
                aliases = c.get("aliases", [])
                if hint_n not in {_norm(a) for a in aliases}:
                    aliases.append(hint)
                    self._registry.update(customer_id, aliases=aliases)
        except Exception:
            pass  # alias learning is best-effort
