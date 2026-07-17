"""
HTTP API for the JSON→SQL console.

Callable helpers (`handle_chat`, `handle_generate`) stay framework-agnostic for
tests. The FastAPI `app` exposes them at:

  GET  /api/health    — liveness; always 200 (reports key configured, does not require it)
  POST /api/generate  — fresh run_workflow (stores conversation for follow-ups)
  POST /api/chat      — fresh conversation or correction turn

Run locally:  uvicorn api.index:app --reload --port 8000
"""

from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Uvicorn does not load .env by itself (unlike Streamlit / live scripts).
load_dotenv()

from workflow.conversation_store import get_conversation, set_conversation
from workflow.graph import run_workflow
from workflow.llm import LLM, build_chat_llm, default_llm_config
from workflow.nodes.chat_correction import apply_chat_correction


def _cors_allow_origins() -> List[str]:
    """
    CORS allowlist from ALLOWED_ORIGIN (comma-separated).

    Default "*" is fine for local dev/testing. Once the Vercel frontend is
    deployed, set ALLOWED_ORIGIN to that real domain (we don't have it yet).
    """
    raw = (os.getenv("ALLOWED_ORIGIN") or "").strip()
    if not raw or raw == "*":
        return ["*"]
    return [part.strip() for part in raw.split(",") if part.strip()] or ["*"]


def _require_llm(llm: Optional[LLM]) -> Tuple[Optional[LLM], Optional[Tuple[Dict[str, Any], int]]]:
    """Build the chat LLM, or return a JSON error if OPENAI_API_KEY is missing."""
    if llm is not None:
        return llm, None
    cfg = default_llm_config()
    if not cfg.api_key:
        return None, (
            {
                "error": (
                    "OPENAI_API_KEY is not set. Put it in the repo-root .env and "
                    "restart uvicorn (api.index loads .env via python-dotenv)."
                )
            },
            503,
        )
    try:
        return build_chat_llm(cfg), None
    except Exception as e:
        return None, ({"error": f"Failed to initialize LLM client: {e}"}, 503)


def _new_conversation_id() -> str:
    return str(uuid.uuid4())


def _snapshot_from_workflow(
    *,
    conversation_id: str,
    question: str,
    json_sample: Any,
    table_name: str,
    json_column: str,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    final_state = result.get("state") or {}
    return {
        "conversation_id": conversation_id,
        "question": question,
        "json_sample": json_sample,
        "table_name": table_name,
        "json_column": json_column,
        "query_spec": result.get("query_spec") or final_state.get("query_spec"),
        "plan": result.get("plan") or final_state.get("plan"),
        "schema_index": final_state.get("schema_index"),
        "schema_summary": result.get("schema_summary") or final_state.get("schema_summary"),
        "field_catalog": final_state.get("field_catalog"),
        "ranked_candidates": result.get("ranked_candidates")
        or final_state.get("ranked_candidates"),
        "validation": final_state.get("validation"),
        "critic_notes": result.get("critic_notes") or final_state.get("critic_notes"),
        "retry_exhausted_unresolved": result.get("retry_exhausted_unresolved"),
        "loop_exit_reason": result.get("loop_exit_reason"),
    }


def _public_response(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    ranked = snapshot.get("ranked_candidates") or []
    return {
        "conversation_id": snapshot.get("conversation_id"),
        "question": snapshot.get("question"),
        "ranked_candidates": ranked,
        "query_spec": snapshot.get("query_spec"),
        "plan": snapshot.get("plan"),
        "schema_summary": snapshot.get("schema_summary"),
        "critic_notes": snapshot.get("critic_notes"),
        "validation": snapshot.get("validation"),
        "repair_notes": snapshot.get("repair_notes"),
        "chat_correction": snapshot.get("chat_correction"),
        "retry_exhausted_unresolved": snapshot.get("retry_exhausted_unresolved"),
        "loop_exit_reason": snapshot.get("loop_exit_reason"),
        "top_sql": (ranked[0].get("sql") if ranked else None),
        "top_score": (ranked[0].get("score") if ranked else None),
    }


def handle_generate(
    payload: Dict[str, Any],
    *,
    llm: Optional[LLM] = None,
) -> Tuple[Dict[str, Any], int]:
    """
    Fresh generation.

    Payload: question, json_sample, table_name, json_column, max_retries (optional).
    Returns ranked_candidates (+ conversation_id so /api/chat corrections work).
    """
    payload = payload or {}
    question = payload.get("question")
    sample = payload.get("json_sample")
    table = payload.get("table_name") or "your_table"
    column = payload.get("json_column") or "raw_data"
    try:
        max_retries = int(payload.get("max_retries") if payload.get("max_retries") is not None else 2)
    except (TypeError, ValueError):
        max_retries = 2

    if not question or not str(question).strip():
        return {"error": "question is required"}, 400
    if sample is None:
        return {"error": "json_sample is required"}, 400

    llm_instance, err = _require_llm(llm)
    if err is not None:
        return err

    try:
        result = run_workflow(
            question=str(question).strip(),
            json_sample=sample,
            table_name=str(table),
            json_column=str(column),
            max_retries=max_retries,
            llm=llm_instance,
        )
    except Exception as e:
        return {"error": f"Generation failed: {e}"}, 500

    cid = _new_conversation_id()
    snapshot = _snapshot_from_workflow(
        conversation_id=cid,
        question=str(question).strip(),
        json_sample=sample,
        table_name=str(table),
        json_column=str(column),
        result=result,
    )
    set_conversation(cid, snapshot)
    # Same public shape as /api/chat so the console can render either path identically.
    return _public_response(snapshot), 200


def handle_chat(
    payload: Dict[str, Any],
    *,
    llm: Optional[LLM] = None,
    json_sample: Any = None,
    table_name: str = "your_table",
    json_column: str = "raw_data",
) -> Tuple[Dict[str, Any], int]:
    """
    Handle one chat turn.

    Payload:
      conversation_id: str | null
      question: str | null          (required for a fresh conversation)
      user_message: str | null      (required for a follow-up correction)
      json_sample / table_name / json_column optional on fresh turns
        (also accepted as top-level kwargs for tests / server wiring)

    Returns (response_body, http_status).
    """
    payload = payload or {}
    conversation_id = payload.get("conversation_id")
    question = payload.get("question")
    user_message = payload.get("user_message")

    sample = payload.get("json_sample", json_sample)
    table = payload.get("table_name") or table_name
    column = payload.get("json_column") or json_column

    cid_raw = str(conversation_id).strip() if conversation_id is not None else ""
    known = get_conversation(cid_raw) if cid_raw else None

    # Correction aimed at a conversation the store no longer has (e.g. Cloud Run recycle).
    # Do this before LLM init — no model call can recover a wiped in-memory store.
    has_correction = bool(user_message and str(user_message).strip())
    has_question = bool(question and str(question).strip())
    if cid_raw and known is None and has_correction and not has_question:
        return (
            {
                "error": (
                    "This conversation has expired or the server restarted. "
                    "Start a new question instead."
                ),
                "conversation_expired": True,
            },
            410,
        )

    llm_instance, err = _require_llm(llm)
    if err is not None:
        return err

    # Follow-up correction turn
    if known is not None and has_correction:
        try:
            updated = apply_chat_correction(known, str(user_message), llm=llm_instance)
        except Exception as e:
            return {"error": f"Correction failed: {e}"}, 500
        updated["conversation_id"] = known.get("conversation_id") or cid_raw
        set_conversation(str(updated["conversation_id"]), updated)
        return _public_response(updated), 200

    # Fresh conversation
    if not has_question:
        return {"error": "question is required for a new conversation"}, 400
    if sample is None:
        return {"error": "json_sample is required for a new conversation"}, 400

    cid = cid_raw or _new_conversation_id()
    try:
        result = run_workflow(
            question=str(question).strip(),
            json_sample=sample,
            table_name=str(table),
            json_column=str(column),
            max_retries=2,
            llm=llm_instance,
        )
    except Exception as e:
        return {"error": f"Generation failed: {e}"}, 500
    snapshot = _snapshot_from_workflow(
        conversation_id=cid,
        question=str(question).strip(),
        json_sample=sample,
        table_name=str(table),
        json_column=str(column),
        result=result,
    )
    set_conversation(cid, snapshot)
    return _public_response(snapshot), 200


app = FastAPI(title="JSON → SQL API")

# "*" + credentials is rejected by browsers; only enable credentials for a real allowlist.
_cors_origins = _cors_allow_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def api_health() -> JSONResponse:
    """
    Liveness for Cloud Run. Always 200 — reports whether OPENAI_API_KEY is
    configured without requiring it (so probes succeed before secrets are wired).
    """
    cfg = default_llm_config()
    return JSONResponse(
        content={
            "status": "ok",
            "openai_api_key_configured": bool(cfg.api_key),
        },
        status_code=200,
    )


@app.post("/api/generate")
async def api_generate(payload: Dict[str, Any]) -> JSONResponse:
    body, status = handle_generate(payload)
    return JSONResponse(content=body, status_code=status)


@app.post("/api/chat")
async def api_chat(payload: Dict[str, Any]) -> JSONResponse:
    body, status = handle_chat(payload)
    return JSONResponse(content=body, status_code=status)
