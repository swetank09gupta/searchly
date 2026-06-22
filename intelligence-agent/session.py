"""
Session State — in-memory, TTL-based.

Tracks:
  - Pending clarifications (what the agent asked, what options it gave)
  - Resolved customer_id and env for the session
  - Conversation history (for multi-turn context)
  - Pending registration data (products the user mentioned for an unknown customer)

TTL: 30 minutes of inactivity → session expires.
A session_id is a UUID generated server-side on first message.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

SESSION_TTL_SECONDS   = 30 * 60   # 30 minutes
HISTORY_FULL_TURNS    = 5         # keep this many recent turns verbatim
HISTORY_COMPRESS_AT   = 10        # compress when history exceeds this many turns


@dataclass
class PendingClarification:
    """What the agent is waiting for the user to confirm or supply."""
    kind: str               # "customer_match" | "new_customer_products" | "new_env_details" | "env_choice"
    question: str           # the exact question the agent asked
    options: list[str] = field(default_factory=list)   # candidate IDs if kind == "customer_match"
    context: dict = field(default_factory=dict)         # arbitrary extra data


@dataclass
class Session:
    id:                  str
    created_at:          float = field(default_factory=time.time)
    last_active:         float = field(default_factory=time.time)

    # Resolved context (set once confirmed)
    resolved_customer_id: str | None = None
    resolved_env:         str | None = None

    # Pending clarification waiting for the user's next message
    pending:              PendingClarification | None = None

    # Pending registration (customer name + hints before products are confirmed)
    pending_registration: dict = field(default_factory=dict)

    # Conversation turns: list of {role: "user"|"agent", content: str}
    # Only HISTORY_FULL_TURNS most recent turns are kept verbatim.
    # Older turns are compressed into rolling_summary + structured_memory.
    history: list[dict] = field(default_factory=list)
    rolling_summary: str = ""   # prose summary of compressed turns

    # Structured memory — machine-readable context extracted from conversation
    structured_memory: dict = field(default_factory=lambda: {
        "customer":            None,   # resolved customer ID
        "environment":         None,   # prod / staging / dev
        "active_issue":        None,   # one-line problem statement
        "investigation_state": None,   # what has been tried / found so far
        "known_findings":      [],     # list of short finding strings
        "resolved":            False,  # True once the issue is solved
    })

    def touch(self):
        self.last_active = time.time()

    def is_expired(self) -> bool:
        return time.time() - self.last_active > SESSION_TTL_SECONDS

    def add_turn(self, role: str, content: str):
        self.history.append({"role": role, "content": content})

    def needs_compression(self) -> bool:
        return len(self.history) >= HISTORY_COMPRESS_AT

    def apply_compression(self, summary: str, structured: dict | None = None):
        """Store the LLM-generated summary + extracted structured facts; trim history."""
        self.rolling_summary = summary
        if structured:
            # Merge — don't overwrite non-None values with None
            for k, v in structured.items():
                if v is not None and v != [] and v != "":
                    self.structured_memory[k] = v
                elif k == "known_findings" and isinstance(v, list):
                    existing = self.structured_memory.get("known_findings", [])
                    self.structured_memory["known_findings"] = list(dict.fromkeys(existing + v))
        self.history = self.history[-HISTORY_FULL_TURNS:]

    def context_for_prompt(self) -> str:
        """Return conversation context suitable for injection into an LLM prompt."""
        parts = []
        mem = self.structured_memory
        if any(v for v in [mem.get("customer"), mem.get("environment"),
                            mem.get("active_issue"), mem.get("investigation_state")]):
            lines = ["[STRUCTURED MEMORY]"]
            if mem.get("customer"):         lines.append(f"  Customer: {mem['customer']}")
            if mem.get("environment"):      lines.append(f"  Environment: {mem['environment']}")
            if mem.get("active_issue"):     lines.append(f"  Active issue: {mem['active_issue']}")
            if mem.get("investigation_state"):
                lines.append(f"  Investigation: {mem['investigation_state']}")
            if mem.get("known_findings"):
                lines.append("  Findings:")
                for f in mem["known_findings"][:5]:
                    lines.append(f"    - {f}")
            parts.append("\n".join(lines))
        if self.rolling_summary:
            parts.append(f"[CONVERSATION SUMMARY]\n{self.rolling_summary}")
        if self.history:
            recent = "\n".join(
                f"{t['role'].upper()}: {t['content'][:300]}" for t in self.history
            )
            parts.append(f"[RECENT TURNS]\n{recent}")
        return "\n".join(parts)

    def set_pending(self, kind: str, question: str,
                    options: list[str] | None = None, context: dict | None = None):
        self.pending = PendingClarification(
            kind=kind,
            question=question,
            options=options or [],
            context=context or {},
        )

    def clear_pending(self):
        self.pending = None


class SessionStore:
    """Thread-safe in-memory session store."""

    def __init__(self):
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    def get_or_create(self, session_id: str | None) -> Session:
        with self._lock:
            self._maybe_cleanup()
            if session_id and session_id in self._sessions:
                s = self._sessions[session_id]
                if not s.is_expired():
                    s.touch()
                    return s
            # Create new
            new_id = session_id or str(uuid.uuid4())
            s = Session(id=new_id)
            self._sessions[new_id] = s
            return s

    def get(self, session_id: str) -> Session | None:
        with self._lock:
            s = self._sessions.get(session_id)
            if s and not s.is_expired():
                s.touch()
                return s
            return None

    def _maybe_cleanup(self):
        now = time.time()
        if now - self._last_cleanup > 300:    # cleanup every 5 min
            expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
            for sid in expired:
                del self._sessions[sid]
            self._last_cleanup = now


# Module-level singleton — shared across all requests in the process
store = SessionStore()
