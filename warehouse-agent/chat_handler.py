"""
Chat Handler — the orchestration layer for multi-turn warehouse queries.

Every incoming message goes through this flow:

  1. ENTITY EXTRACTION
     LLM (fast ~0.5s) extracts: customer_hint, env_hint, product_hint, entity_ids, intent

  2. CLARIFICATION RESPONSE (if previous turn left a pending question)
     Interpret user's reply to the agent's last question:
       "yes"        → confirm the suggested customer
       "no" / other → retry with different candidate or ask for full input
       "walmart nj" → treat as a new hint and re-resolve
       "prod"       → set env
       "pick-assist, greymatter" → treat as product list for new registration

  3. CUSTOMER RESOLUTION
     Fuzzy-match the hint against the registry.
     Result is one of:
       RESOLVED         → continue to agent
       NEEDS_CONFIRM    → tell user what we assumed, answer anyway
       NEEDS_INPUT      → ask a clarification question, return that as the "answer"

  4. ENV RESOLUTION
     Match env_hint to a configured environment for the customer.
     If the env is not yet configured → offer to record it (ask for cluster details)
     If no env hint and customer has multiple → pick highest configured

  5. AGENT CALL
     Run the warehouse agent with the resolved customer + env_config.

  6. SESSION UPDATE
     Persist resolved IDs, pending clarifications, conversation history.

All of this is transparent to the user — they just get an answer (or a question).
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from agent import run_agent, _is_operational
from customer_registry import CustomerRegistry, LIFECYCLE_ORDER, lifecycle_label
from entity_extractor import extract_entities
from products_config import product_menu, parse_selection, validate_products, ids as product_ids
from resolver import CustomerResolver, extract_env_hint, ResolutionResult
from session import Session, SessionStore

log = logging.getLogger(__name__)

# Products we know about (used when guessing from question)
KNOWN_PRODUCTS = ["pick-assist", "greymatter", "intralogistics", "gsb", "rdc", "wms", "sre"]


class ChatHandler:
    def __init__(
        self,
        registry:        CustomerRegistry,
        session_store:   SessionStore,
        ollama_url:      str,
        ollama_model:    str,
        searchly_url:    str,
        searchly_tenant: str,
    ):
        self.registry       = registry
        self.sessions       = session_store
        self.ollama_url     = ollama_url
        self.ollama_model   = ollama_model
        self.searchly_url   = searchly_url
        self.searchly_tenant = searchly_tenant
        self.resolver       = CustomerResolver(registry)

    async def handle(
        self,
        message:         str,
        session_id:      str | None,
        customer_hint:   str | None = None,   # from URL ?customer=
        env_hint:        str | None = None,   # from URL ?env=
        product_hint:    str | None = None,   # from URL ?product=
    ) -> dict[str, Any]:
        """
        Main entry point.  Returns a response dict:
          {
            session_id, answer, resolved_customer, resolved_env,
            lifecycle_stage, has_live_data, needs_clarification,
            tools_called, is_operational
          }
        """
        session = self.sessions.get_or_create(session_id)
        session.add_turn("user", message)

        # ── Step 1: Handle pending clarification from previous turn ───────────
        if session.pending:
            result = await self._handle_clarification(message, session)
            if result:
                return result

        # ── Step 2: Extract entities from this message ─────────────────────
        entities = await extract_entities(message, self.ollama_url, self.ollama_model)

        # URL params take precedence over extracted hints
        c_hint = customer_hint or entities.get("customer_hint")
        e_hint = env_hint      or entities.get("env_hint")
        p_hint = product_hint  or entities.get("product_hint")

        # If session already has a resolved customer and no new one mentioned → reuse
        if not c_hint and session.resolved_customer_id:
            c_hint = session.resolved_customer_id
        if not e_hint and session.resolved_env:
            e_hint = session.resolved_env

        # ── Step 3: Resolve customer ──────────────────────────────────────────
        resolution = self.resolver.resolve(c_hint, e_hint, question=message)

        if resolution.needs_input:
            # If no customer hint was given and the question doesn't need live cluster data,
            # treat it as a general knowledge query (new dev onboarding, SA demo, etc.)
            # and answer straight from the knowledge base — no clarification needed.
            if not c_hint and not _is_operational(message):
                agent_result = await run_agent(
                    question        = message,
                    customer_id     = None,
                    customer_record = None,
                    env_config      = None,
                    product         = p_hint,
                    ollama_url      = self.ollama_url,
                    ollama_model    = self.ollama_model,
                    searchly_url    = self.searchly_url,
                    searchly_tenant = self.searchly_tenant,
                )
                answer = agent_result["answer"]
                session.add_turn("agent", answer)
                return {
                    "session_id":          session.id,
                    "answer":              answer,
                    "resolved_customer":   None,
                    "resolved_env":        None,
                    "lifecycle_stage":     None,
                    "lifecycle_label":     None,
                    "has_live_data":       False,
                    "needs_clarification": False,
                    "tools_called":        agent_result["tools_called"],
                    "tool_results":        agent_result["tool_results"],
                    "is_operational":      False,
                }

            # No candidates means resolver showed the product menu → expect product reply
            pending_kind = "new_customer_products" if not resolution.candidates else "customer_match"
            # Can't proceed — ask the user
            session.set_pending(
                kind     = pending_kind,
                question = resolution.message,
                options  = [c[0] for c in resolution.candidates],
                context  = {
                    "original_question": message,
                    "customer_hint":     c_hint,
                    "env_hint":          e_hint,
                    "product_hint":      p_hint,
                    "entities":          entities,
                },
            )
            session.add_turn("agent", resolution.message)
            return self._clarification_response(session, resolution.message)

        # ── Step 4: Resolve env config ────────────────────────────────────────
        customer_record, env_config, env_name = self._resolve_env_config(
            resolution.customer_id, e_hint or resolution.env
        )

        # Env mentioned but not yet configured → offer to set it up
        if e_hint and not env_config and e_hint in LIFECYCLE_ORDER[1:]:
            ask = (
                f"**{customer_record.get('name', 'This customer')}** doesn't have a **{e_hint}** "
                f"environment configured yet. "
                f"I can answer from knowledge for now, or you can share the "
                f"cluster details and I'll set it up:\n\n"
                f"  • **k8s_bastion** (SSH jump host, e.g. `user@192.168.x.x`)\n"
                f"  • **k8s_context** (kubectl context name)\n"
                f"  • **k8s_namespace** (default: `default`)\n\n"
                f"Share the details, or say 'skip' to get a knowledge-only answer."
            )
            session.set_pending(
                kind    = "new_env_details",
                question = ask,
                context = {
                    "original_question": message,
                    "customer_id":       resolution.customer_id,
                    "env":               e_hint,
                    "customer_record":   customer_record,
                },
            )
            session.add_turn("agent", ask)
            return self._clarification_response(session, ask)

        # ── Step 5: Persist resolution to session ────────────────────────────
        session.resolved_customer_id = resolution.customer_id
        session.resolved_env         = env_name
        session.clear_pending()

        # ── Step 6: Run the agent ─────────────────────────────────────────────
        confirm_note = f"\n\n*({resolution.message})*" if resolution.needs_confirm else ""

        agent_result = await run_agent(
            question        = message,
            customer_id     = resolution.customer_id,
            customer_record = customer_record,
            env_config      = env_config,
            product         = p_hint,
            ollama_url      = self.ollama_url,
            ollama_model    = self.ollama_model,
            searchly_url    = self.searchly_url,
            searchly_tenant = self.searchly_tenant,
        )

        answer = agent_result["answer"] + confirm_note
        session.add_turn("agent", answer)

        # Compress history if it has grown long
        if session.needs_compression():
            await self._compress_session_history(session)

        return {
            "session_id":         session.id,
            "answer":             answer,
            "resolved_customer":  resolution.customer_id,
            "resolved_env":       env_name,
            "lifecycle_stage":    customer_record.get("lifecycle_stage"),
            "lifecycle_label":    lifecycle_label(customer_record.get("lifecycle_stage", "solution")),
            "has_live_data":      bool(env_config) and bool(agent_result["tools_called"]),
            "needs_clarification": False,
            "tools_called":       agent_result["tools_called"],
            "tool_results":       agent_result["tool_results"],
            "is_operational":     agent_result["is_operational"],
        }

    # ─── Clarification response handler ──────────────────────────────────────

    async def _handle_clarification(self, message: str, session: Session
                                    ) -> dict[str, Any] | None:
        """
        Interpret the user's reply to the agent's last clarification question.
        Returns a response dict, or None to fall through to normal processing.
        """
        pending = session.pending
        ctx     = pending.context
        msg_l   = message.lower().strip()

        # ── customer_match: we suggested candidates, user replies ─────────────
        if pending.kind == "customer_match":
            candidates = pending.options   # list of customer IDs

            # "yes" / "correct" / "that's right" → confirm first candidate
            if _is_affirmative(msg_l) and candidates:
                chosen_id = candidates[0]
                self._registry_learn_alias(chosen_id, ctx.get("customer_hint", ""))
                session.resolved_customer_id = chosen_id
                session.clear_pending()
                log.info("User confirmed customer: %s", chosen_id)
                # Re-run with the confirmed customer
                return await self.handle(
                    message      = ctx["original_question"],
                    session_id   = session.id,
                    customer_hint = chosen_id,
                    env_hint     = ctx.get("env_hint"),
                    product_hint = ctx.get("product_hint"),
                )

            # "no", "none of those" → ask for full clarification
            if _is_negative(msg_l) or "none" in msg_l:
                ask = (
                    "Got it. What's the full name of the customer or warehouse?\n\n"
                    "And which products do they use? Reply with the number(s):\n\n"
                    + product_menu()
                )
                session.set_pending(
                    kind    = "new_customer_products",
                    question = ask,
                    options  = product_ids(),
                    context  = ctx,
                )
                session.add_turn("agent", ask)
                return self._clarification_response(session, ask)

            # User picked one by number (1, 2, 3) or by name
            chosen = _pick_candidate(msg_l, candidates, self.registry)
            if chosen:
                self._registry_learn_alias(chosen, ctx.get("customer_hint", ""))
                session.resolved_customer_id = chosen
                session.clear_pending()
                return await self.handle(
                    message      = ctx["original_question"],
                    session_id   = session.id,
                    customer_hint = chosen,
                    env_hint     = ctx.get("env_hint"),
                    product_hint = ctx.get("product_hint"),
                )

            # Treat message as a new hint and re-resolve
            session.clear_pending()
            return None   # fall through to normal processing with message as hint

        # ── new_customer_products: user gave us the name + products ───────────
        if pending.kind == "new_customer_products":
            # Parse product selection (numbered or named, validated against known list)
            products = parse_selection(msg_l)
            valid_products, unknown = validate_products(products)

            # If nothing valid recognised, prompt again with the menu
            if not valid_products:
                ask = (
                    "I didn't catch a valid product selection. "
                    "Please reply with the number(s) from this list:\n\n"
                    + product_menu()
                )
                session.set_pending(
                    kind="new_customer_products", question=ask,
                    options=product_ids(), context=ctx,
                )
                session.add_turn("agent", ask)
                return self._clarification_response(session, ask)

            # Strip product names from the input to isolate the customer name
            name_part = message
            for p in valid_products:
                name_part = name_part.replace(p, "").replace(p.replace("-", " "), "")
            name_part = name_part.strip(" ,.")
            # Fall back to context hint if nothing meaningful remains
            # (covers: reply was just product numbers like "1,2", "1 and 2", empty, or too short)
            _selection_only = re.sub(r'\b(and|or)\b', '', name_part, flags=re.I).strip(' ,.')
            if not name_part or len(name_part) < 3 or re.match(r'^[\d\s,./]+$', _selection_only):
                name_part = ctx.get("customer_hint", "new-customer")

            cid = _slugify(name_part)
            try:
                self.registry.create(
                    customer_id     = cid,
                    name            = name_part,
                    products        = valid_products,
                    lifecycle_stage = "solution",
                )
                log.info("Auto-registered customer: %s products=%s", cid, valid_products)
            except ValueError:
                pass   # Already exists — fine

            session.resolved_customer_id = cid
            session.clear_pending()

            # Answer the original question with knowledge only
            return await self.handle(
                message      = ctx.get("original_question", message),
                session_id   = session.id,
                customer_hint = cid,
                env_hint     = ctx.get("env_hint"),
            )

        # ── new_env_details: user gave cluster info OR said "skip" ────────────
        if pending.kind == "new_env_details":
            customer_id = ctx["customer_id"]
            env         = ctx["env"]

            if "skip" in msg_l or _is_negative(msg_l):
                # Answer from knowledge only
                session.clear_pending()
                return await self.handle(
                    message       = ctx["original_question"],
                    session_id    = session.id,
                    customer_hint = customer_id,
                    env_hint      = None,   # don't request the unconfigured env
                )

            # Try to extract k8s details from the user's message
            bastion   = _extract_pattern(message, r"(\w[\w.@\-]+@[\d.]+)")
            context   = _extract_pattern(message, r"context[:\s]+([a-z0-9\-]+)")
            namespace = _extract_pattern(message, r"namespace[:\s]+([a-z0-9\-]+)") or "default"

            if bastion or context:
                self.registry.upsert_environment(
                    customer_id   = customer_id,
                    env           = env,
                    k8s_bastion   = bastion or "",
                    k8s_context   = context or "",
                    k8s_namespace = namespace,
                )
                log.info("Auto-registered env %s for customer %s", env, customer_id)
                session.clear_pending()
                # Re-run with the newly configured env
                return await self.handle(
                    message       = ctx["original_question"],
                    session_id    = session.id,
                    customer_hint = customer_id,
                    env_hint      = env,
                )

            # Couldn't parse the details — ask more specifically
            ask = (
                "I couldn't quite parse those details. Please share them like this:\n\n"
                "```\nbastion: user@192.168.x.x\ncontext: my-cluster-context\nnamespace: default\n```"
            )
            session.set_pending(
                kind="new_env_details", question=ask, context=ctx
            )
            session.add_turn("agent", ask)
            return self._clarification_response(session, ask)

        # Unknown pending kind → clear and reprocess
        session.clear_pending()
        return None

    # ─── Helpers ──────────────────────────────────────────────────────────────

    async def _compress_session_history(self, session) -> None:
        """
        Summarize older turns into rolling_summary + structured_memory via Ollama.
        Extracts machine-readable facts (customer, env, issue, findings) separately
        from the prose summary so downstream code can use them without parsing text.
        """
        older_turns = session.history[:-5]
        if not older_turns:
            return
        turns_text = "\n".join(
            f"{t['role'].upper()}: {t['content'][:500]}" for t in older_turns
        )
        existing = f"\nExisting summary:\n{session.rolling_summary}\n" if session.rolling_summary else ""

        prompt = (
            "Analyze this warehouse support conversation and output TWO sections.\n\n"
            "SECTION 1 — PROSE SUMMARY:\n"
            "One paragraph summarizing what happened, what was found, and current status.\n\n"
            "SECTION 2 — STRUCTURED JSON (valid JSON only, no markdown):\n"
            '{"customer":"<id or null>","environment":"<prod/staging/dev or null>",'
            '"active_issue":"<one sentence or null>","investigation_state":"<what was tried or null>",'
            '"known_findings":["<finding1>","<finding2>"],"resolved":<true or false>}\n\n'
            f"{existing}"
            f"Conversation:\n{turns_text}\n\n"
            "SECTION 1 — PROSE SUMMARY:"
        )
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{self.ollama_url}/api/generate",
                    json={"model": self.ollama_model, "prompt": prompt, "stream": False},
                )
                if resp.status_code != 200:
                    return
                raw = resp.json().get("response", "").strip()
                if not raw:
                    return

            # Split on "SECTION 2" marker
            parts = re.split(r"SECTION\s+2[^:]*:", raw, maxsplit=1, flags=re.IGNORECASE)
            summary = parts[0].strip()
            structured: dict | None = None
            if len(parts) > 1:
                json_text = parts[1].strip()
                m = re.search(r"\{.*\}", json_text, re.DOTALL)
                if m:
                    try:
                        import json as _json
                        structured = _json.loads(m.group(0))
                    except Exception:
                        pass

            session.apply_compression(summary, structured)
            log.debug("Compressed session %s: %d turns → summary + structured memory",
                      session.id, len(older_turns))
        except Exception as e:
            log.debug("History compression failed (non-fatal): %s", e)

    def _resolve_env_config(self, customer_id: str | None, env_hint: str | None
                             ) -> tuple[dict, dict | None, str | None]:
        """Returns (customer_record, env_config_or_None, env_name_or_None)."""
        if not customer_id:
            return {}, None, None
        try:
            record, env_cfg = self.registry.resolve_env(customer_id, env_hint)
            env_name = None
            if env_cfg:
                for name, cfg in record.get("environments", {}).items():
                    if cfg == env_cfg:
                        env_name = name
                        break
            return record, env_cfg, env_name
        except KeyError:
            return {}, None, None

    def _registry_learn_alias(self, customer_id: str, hint: str):
        if hint:
            try:
                c = self.registry.get(customer_id)
                if c:
                    aliases = c.get("aliases", [])
                    from resolver import _norm
                    if hint and _norm(hint) not in {_norm(a) for a in aliases}:
                        aliases.append(hint)
                        self.registry.update(customer_id, aliases=aliases)
            except Exception:
                pass

    @staticmethod
    def _clarification_response(session: Session, question: str) -> dict[str, Any]:
        return {
            "session_id":          session.id,
            "answer":              question,
            "resolved_customer":   session.resolved_customer_id,
            "resolved_env":        session.resolved_env,
            "lifecycle_stage":     None,
            "lifecycle_label":     None,
            "has_live_data":       False,
            "needs_clarification": True,
            "tools_called":        [],
            "tool_results":        {},
            "is_operational":      False,
        }


# ─── String helpers ───────────────────────────────────────────────────────────

def _is_affirmative(s: str) -> bool:
    return bool(re.search(r"\b(yes|yeah|yep|correct|right|that.?s right|exactly|sure|ok|okay)\b", s, re.I))


def _is_negative(s: str) -> bool:
    return bool(re.search(r"\b(no|nope|nah|not|wrong|incorrect|different|other)\b", s, re.I))


def _pick_candidate(msg: str, candidates: list[str], registry) -> str | None:
    """Try to map a user's reply to one of the candidate IDs."""
    # Numbered selection: "1", "2", "3" or "the first one"
    n_map = {"1": 0, "first": 0, "2": 1, "second": 1, "3": 2, "third": 2}
    for k, idx in n_map.items():
        if k in msg and idx < len(candidates):
            return candidates[idx]
    # Name fragment match
    for cid in candidates:
        c = registry.get(cid)
        if not c:
            continue
        name_tokens = set(c["name"].lower().split())
        msg_tokens  = set(msg.lower().split())
        if name_tokens & msg_tokens:
            return cid
    return None


def _extract_products(text: str) -> list[str]:
    found = []
    for p in ["pick-assist", "greymatter", "intralogistics", "gsb", "rdc", "wms"]:
        if p in text or p.replace("-", " ") in text:
            found.append(p)
    return found


def _slugify(s: str) -> str:
    import unicodedata, re
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:60]


def _extract_pattern(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1) if m else None
