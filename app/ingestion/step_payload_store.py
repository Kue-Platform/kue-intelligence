"""StepPayloadStore — offload large Inngest step return values to persistent storage.

Inngest serializes and stores every ctx.step.run() return value for replay/retry,
applying the same 256 KB limit as inbound event payloads.  Large intermediate
results (enrichment slices, embedding vectors) must therefore never appear as raw
step return values.

Pattern
-------
  Step A (producer):
    data = _compute_large_thing()
    ref  = sps.write(run_id, "ingest_and_process", data)
    return {**counts, "step_ref": ref}           # tiny — safe to serialize

  Step B (consumer):
    large = sps.read(step_ref)                   # loads from Supabase/SQLite
    _use(large["semantic_result"])

Backends
--------
  • SupabaseStepPayloadStore  — used in production (supabase_url configured)
  • SqliteStepPayloadStore    — used locally / in tests
"""
from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from app.ingestion.db import get_connection
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import httpx

from app.core.config import Settings


# ---------------------------------------------------------------------------
# Reference format helpers
# ---------------------------------------------------------------------------

def _make_ref(run_id: str, step_name: str) -> str:
    """Build a compact string reference: ``run_id:step_name``."""
    return f"{run_id}:{step_name}"


def _parse_ref(ref: str) -> tuple[str, str]:
    """Split ``run_id:step_name`` into (run_id, step_name).

    Raises ValueError when the ref is malformed.
    """
    if ":" not in ref:
        raise ValueError(f"Invalid step_ref — expected 'run_id:step_name', got: {ref!r}")
    run_id, _, step_name = ref.partition(":")
    return run_id, step_name


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class StepPayloadStore(ABC):
    """Persistent store for intermediate Inngest step payloads."""

    @property
    @abstractmethod
    def store_name(self) -> str: ...

    @abstractmethod
    def write(self, run_id: str, step_name: str, payload: dict[str, Any]) -> str:
        """Persist *payload* and return an opaque step_ref string."""
        ...

    @abstractmethod
    def read(self, step_ref: str) -> dict[str, Any]:
        """Retrieve the payload previously written for *step_ref*.

        Raises KeyError when the ref is not found.
        """
        ...

    @abstractmethod
    def delete(self, step_ref: str) -> None:
        """Remove the payload for *step_ref* (best-effort, for housekeeping)."""
        ...


# ---------------------------------------------------------------------------
# SQLite backend  (local dev / tests)
# ---------------------------------------------------------------------------

class SqliteStepPayloadStore(StepPayloadStore):
    """SQLite-backed step payload store for local development and tests."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = Lock()
        self._ensure_db()

    @property
    def store_name(self) -> str:
        return f"sqlite:{self._db_path}"

    def _ensure_db(self) -> None:
        target = Path(self._db_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with get_connection(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS step_payloads (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id      TEXT NOT NULL,
                    step_name   TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    UNIQUE (run_id, step_name)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sps_run_step "
                "ON step_payloads(run_id, step_name)"
            )
            conn.commit()

    def write(self, run_id: str, step_name: str, payload: dict[str, Any]) -> str:
        created_at = datetime.now(UTC).isoformat()
        payload_json = json.dumps(payload, ensure_ascii=True)
        with self._lock:
            with get_connection(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO step_payloads (run_id, step_name, payload_json, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(run_id, step_name) DO UPDATE
                        SET payload_json = excluded.payload_json,
                            created_at   = excluded.created_at
                    """,
                    (run_id, step_name, payload_json, created_at),
                )
                conn.commit()
        return _make_ref(run_id, step_name)

    def read(self, step_ref: str) -> dict[str, Any]:
        run_id, step_name = _parse_ref(step_ref)
        with self._lock:
            with get_connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT payload_json FROM step_payloads "
                    "WHERE run_id = ? AND step_name = ?",
                    (run_id, step_name),
                ).fetchone()
        if row is None:
            raise KeyError(f"No step payload found for ref={step_ref!r}")
        return json.loads(str(row["payload_json"]))

    def delete(self, step_ref: str) -> None:
        run_id, step_name = _parse_ref(step_ref)
        with self._lock:
            with get_connection(self._db_path) as conn:
                conn.execute(
                    "DELETE FROM step_payloads WHERE run_id = ? AND step_name = ?",
                    (run_id, step_name),
                )
                conn.commit()


# ---------------------------------------------------------------------------
# Supabase backend  (production)
# ---------------------------------------------------------------------------

class SupabaseStepPayloadStore(StepPayloadStore):
    """Supabase-backed step payload store for production."""

    def __init__(self, *, supabase_url: str, api_key: str, table: str) -> None:
        self._supabase_url = supabase_url.rstrip("/")
        self._api_key = api_key
        self._table = table
        self._headers = {
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    @property
    def store_name(self) -> str:
        return f"supabase:{self._table}"

    def _rest_url(self) -> str:
        return f"{self._supabase_url}/rest/v1/{self._table}"

    def write(self, run_id: str, step_name: str, payload: dict[str, Any]) -> str:
        row = {
            "run_id": run_id,
            "step_name": step_name,
            "payload_json": payload,   # stored as JSONB
            "created_at": datetime.now(UTC).isoformat(),
        }

        response = httpx.post(
            self._rest_url(),
            headers={**self._headers, "Prefer": "resolution=merge-duplicates,return=minimal"},
            params={"on_conflict": "run_id,step_name"},
            json=[row],
            timeout=30.0,
        )
        if response.status_code >= 400:
            body = (response.text or "").lower()
            is_conflict = response.status_code == 409 or "23505" in body
            if is_conflict:
                patch_response = httpx.patch(
                    self._rest_url(),
                    headers=self._headers,
                    params={"run_id": f"eq.{run_id}", "step_name": f"eq.{step_name}"},
                    json={
                        "payload_json": payload,
                        "created_at": row["created_at"],
                    },
                    timeout=30.0,
                )
                if patch_response.status_code < 400:
                    return _make_ref(run_id, step_name)
            raise RuntimeError(
                f"StepPayloadStore write failed ({response.status_code}): {response.text}"
            )
        return _make_ref(run_id, step_name)

    def read(self, step_ref: str) -> dict[str, Any]:
        run_id, step_name = _parse_ref(step_ref)
        response = httpx.get(
            self._rest_url(),
            headers=self._headers,
            params={
                "run_id": f"eq.{run_id}",
                "step_name": f"eq.{step_name}",
                "select": "payload_json",
                "limit": "1",
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"StepPayloadStore read failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            raise KeyError(f"No step payload found for ref={step_ref!r}")
        payload = rows[0]["payload_json"]
        # PostgREST returns JSONB columns as already-parsed Python objects
        if isinstance(payload, str):
            return json.loads(payload)
        return dict(payload)

    def delete(self, step_ref: str) -> None:
        run_id, step_name = _parse_ref(step_ref)
        response = httpx.delete(
            self._rest_url(),
            headers=self._headers,
            params={"run_id": f"eq.{run_id}", "step_name": f"eq.{step_name}"},
            timeout=15.0,
        )
        # Tolerate 404 — already gone is fine for cleanup
        if response.status_code >= 400 and response.status_code != 404:
            raise RuntimeError(
                f"StepPayloadStore delete failed ({response.status_code}): {response.text}"
            )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_step_payload_store(settings: Settings) -> StepPayloadStore:
    return SqliteStepPayloadStore(db_path=settings.step_payloads_db_path)
