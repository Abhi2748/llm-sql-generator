"""Conversation state store for multi-turn chat corrections.

Backing store is a module-level dict: a single-process, in-memory store.
This is intentional for the current portfolio/demo scale — same scope decision
as workflow/schema_cache.py / ADR 0002 (in-memory is fine for a single Cloud Run
instance; a multi-instance deployment would need a shared store such as Redis
that survives cold starts). Documented limitation, not an oversight.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# Single-process in-memory store (see module docstring / ADR 0002).
_CONVERSATIONS: Dict[str, Dict[str, Any]] = {}


def get_conversation(conversation_id: str) -> Optional[Dict[str, Any]]:
    """Return a shallow copy of stored conversation state, or None if unknown."""
    entry = _CONVERSATIONS.get(conversation_id)
    if entry is None:
        return None
    return dict(entry)


def set_conversation(conversation_id: str, state: Dict[str, Any]) -> None:
    """Persist conversation state under ``conversation_id``."""
    _CONVERSATIONS[conversation_id] = dict(state)


def clear_conversation_store() -> None:
    """Test helper: wipe the in-process store."""
    _CONVERSATIONS.clear()
