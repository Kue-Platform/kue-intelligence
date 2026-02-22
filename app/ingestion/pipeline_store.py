from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import httpx

from app.core.config import Settings
from app.schemas import IngestionSource


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class PipelineRunRecord:
    run_id: str
    trace_id: str
    tenant_id: str
    user_id: str
    source: str | None
    trigger_type: str
    status: str
    requested_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class PipelineStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def ensure_tenant_user(self, tenant_id: str, user_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def create_pipeline_run(
        self,
        *,
        trace_id: str,
        tenant_id: str,
        user_id: str,
        source: IngestionSource | None,
        trigger_type: str,
        source_event_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def mark_pipeline_status(
        self,
        *,
        run_id: str,
        status: str,
        error_json: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def start_stage_run(
        self,
        *,
        run_id: str,
        stage_key: str,
        layer_key: str,
        attempt: int = 1,
        records_in: int | None = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def finish_stage_run(
        self,
        *,
        stage_run_id: int,
        status: str,
        records_out: int | None = None,
        error_json: dict[str, Any] | None = None,
        llm: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_run_by_trace_id(self, trace_id: str) -> PipelineRunRecord | None:
        raise NotImplementedError


class SqlitePipelineStore(PipelineStore):
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
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenants (
                    tenant_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    plan TEXT NOT NULL DEFAULT 'free',
                    settings_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenant_users (
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'member',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, user_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    source TEXT,
                    trigger_type TEXT NOT NULL,
                    source_event_id TEXT,
                    status TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    error_json TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_trace "
                "ON pipeline_runs(tenant_id, trace_id)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    stage_key TEXT NOT NULL,
                    layer_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    started_at TEXT,
                    completed_at TEXT,
                    duration_ms INTEGER,
                    records_in INTEGER,
                    records_out INTEGER,
                    error_json TEXT
                )
                """
            )
            conn.commit()

    def ensure_tenant_user(self, tenant_id: str, user_id: str) -> None:
        now = _utc_now().isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO tenants (tenant_id, name, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(tenant_id) DO UPDATE SET updated_at=excluded.updated_at
                    """,
                    (tenant_id, tenant_id, now, now),
                )
                conn.execute(
                    """
                    INSERT INTO tenant_users (tenant_id, user_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(tenant_id, user_id) DO UPDATE SET updated_at=excluded.updated_at
                    """,
                    (tenant_id, user_id, now, now),
                )
                conn.commit()

    def create_pipeline_run(
        self,
        *,
        trace_id: str,
        tenant_id: str,
        user_id: str,
        source: IngestionSource | None,
        trigger_type: str,
        source_event_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        now = _utc_now().isoformat()
        run_id = str(uuid4())
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO pipeline_runs (
                        run_id, trace_id, tenant_id, user_id, source, trigger_type, source_event_id,
                        status, requested_at, started_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        trace_id,
                        tenant_id,
                        user_id,
                        str(source) if source else None,
                        trigger_type,
                        source_event_id,
                        "running",
                        now,
                        now,
                        json.dumps(metadata or {}, ensure_ascii=True),
                    ),
                )
                conn.commit()
        return run_id

    def mark_pipeline_status(
        self,
        *,
        run_id: str,
        status: str,
        error_json: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = _utc_now().isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = ?, completed_at = ?, error_json = ?, metadata_json = coalesce(?, metadata_json)
                    WHERE run_id = ?
                    """,
                    (
                        status,
                        now if status in {"succeeded", "failed", "partial"} else None,
                        json.dumps(error_json, ensure_ascii=True) if error_json else None,
                        json.dumps(metadata, ensure_ascii=True) if metadata is not None else None,
                        run_id,
                    ),
                )
                conn.commit()

    def start_stage_run(
        self,
        *,
        run_id: str,
        stage_key: str,
        layer_key: str,
        attempt: int = 1,
        records_in: int | None = None,
    ) -> int:
        now = _utc_now().isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO pipeline_stage_runs (
                        run_id, stage_key, layer_key, status, attempt, started_at, records_in
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, stage_key, layer_key, "running", attempt, now, records_in),
                )
                conn.commit()
                return int(cursor.lastrowid)

    def finish_stage_run(
        self,
        *,
        stage_run_id: int,
        status: str,
        records_out: int | None = None,
        error_json: dict[str, Any] | None = None,
        llm: dict[str, Any] | None = None,
    ) -> None:
        del llm
        now = _utc_now().isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                row = conn.execute(
                    "SELECT started_at FROM pipeline_stage_runs WHERE id = ?",
                    (stage_run_id,),
                ).fetchone()
                duration_ms = None
                if row and row[0]:
                    start = datetime.fromisoformat(row[0])
                    duration_ms = int((_utc_now() - start).total_seconds() * 1000)
                conn.execute(
                    """
                    UPDATE pipeline_stage_runs
                    SET status = ?, completed_at = ?, duration_ms = ?, records_out = ?, error_json = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        now,
                        duration_ms,
                        records_out,
                        json.dumps(error_json, ensure_ascii=True) if error_json else None,
                        stage_run_id,
                    ),
                )
                conn.commit()

    def get_run_by_trace_id(self, trace_id: str) -> PipelineRunRecord | None:
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT run_id, trace_id, tenant_id, user_id, source, trigger_type,
                           status, requested_at, started_at, completed_at
                    FROM pipeline_runs
                    WHERE trace_id = ?
                    ORDER BY requested_at DESC
                    LIMIT 1
                    """,
                    (trace_id,),
                ).fetchone()
        if row is None:
            return None
        return PipelineRunRecord(
            run_id=str(row["run_id"]),
            trace_id=str(row["trace_id"]),
            tenant_id=str(row["tenant_id"]),
            user_id=str(row["user_id"]),
            source=str(row["source"]) if row["source"] is not None else None,
            trigger_type=str(row["trigger_type"]),
            status=str(row["status"]),
            requested_at=datetime.fromisoformat(str(row["requested_at"])),
            started_at=datetime.fromisoformat(str(row["started_at"])) if row["started_at"] else None,
            completed_at=(
                datetime.fromisoformat(str(row["completed_at"])) if row["completed_at"] else None
            ),
        )


class SupabasePipelineStore(PipelineStore):
    def __init__(self, *, supabase_url: str, api_key: str) -> None:
        self._supabase_url = supabase_url.rstrip("/")
        self._api_key = api_key
        self._base_headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    @property
    def store_name(self) -> str:
        return "supabase:pipeline"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    def ensure_tenant_user(self, tenant_id: str, user_id: str) -> None:
        now = _utc_now().isoformat()
        tenant_payload = {
            "tenant_id": tenant_id,
            "name": tenant_id,
            "updated_at": now,
        }
        tenant_headers = dict(self._base_headers)
        tenant_headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        tenant_response = httpx.post(
            self._url("tenants"),
            headers=tenant_headers,
            params={"on_conflict": "tenant_id"},
            json=[tenant_payload],
            timeout=20.0,
        )
        if tenant_response.status_code >= 400:
            raise RuntimeError(
                f"Supabase tenants upsert failed ({tenant_response.status_code}): {tenant_response.text}"
            )

        user_payload = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "updated_at": now,
        }
        user_headers = dict(self._base_headers)
        user_headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        user_response = httpx.post(
            self._url("tenant_users"),
            headers=user_headers,
            params={"on_conflict": "tenant_id,user_id"},
            json=[user_payload],
            timeout=20.0,
        )
        if user_response.status_code >= 400:
            raise RuntimeError(
                f"Supabase tenant_users upsert failed ({user_response.status_code}): {user_response.text}"
            )

    def create_pipeline_run(
        self,
        *,
        trace_id: str,
        tenant_id: str,
        user_id: str,
        source: IngestionSource | None,
        trigger_type: str,
        source_event_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        run_payload = {
            "trace_id": trace_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "source": str(source) if source else None,
            "trigger_type": trigger_type,
            "source_event_id": source_event_id,
            "status": "running",
            "started_at": _utc_now().isoformat(),
            "metadata_json": metadata or {},
        }
        headers = dict(self._base_headers)
        headers["Prefer"] = "return=representation"
        response = httpx.post(
            self._url("pipeline_runs"),
            headers=headers,
            json=[run_payload],
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_runs insert failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            raise RuntimeError("Supabase pipeline_runs insert returned empty result")
        return str(rows[0]["run_id"])

    def mark_pipeline_status(
        self,
        *,
        run_id: str,
        status: str,
        error_json: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        body: dict[str, Any] = {"status": status}
        if status in {"succeeded", "failed", "partial"}:
            body["completed_at"] = _utc_now().isoformat()
        if error_json is not None:
            body["error_json"] = error_json
        if metadata is not None:
            body["metadata_json"] = metadata
        response = httpx.patch(
            self._url("pipeline_runs"),
            headers=self._base_headers,
            params={"run_id": f"eq.{run_id}"},
            json=body,
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_runs update failed ({response.status_code}): {response.text}"
            )

    def start_stage_run(
        self,
        *,
        run_id: str,
        stage_key: str,
        layer_key: str,
        attempt: int = 1,
        records_in: int | None = None,
    ) -> int:
        payload: dict[str, Any] = {
            "run_id": run_id,
            "stage_key": stage_key,
            "layer_key": layer_key,
            "status": "running",
            "attempt": attempt,
            "started_at": _utc_now().isoformat(),
        }
        if records_in is not None:
            payload["records_in"] = records_in
        headers = dict(self._base_headers)
        headers["Prefer"] = "return=representation"
        response = httpx.post(
            self._url("pipeline_stage_runs"),
            headers=headers,
            json=[payload],
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_stage_runs insert failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            raise RuntimeError("Supabase pipeline_stage_runs insert returned empty result")
        return int(rows[0]["id"])

    def finish_stage_run(
        self,
        *,
        stage_run_id: int,
        status: str,
        records_out: int | None = None,
        error_json: dict[str, Any] | None = None,
        llm: dict[str, Any] | None = None,
    ) -> None:
        # Read started_at to compute duration_ms.
        read_response = httpx.get(
            self._url("pipeline_stage_runs"),
            headers=self._base_headers,
            params={"id": f"eq.{stage_run_id}", "select": "started_at", "limit": 1},
            timeout=20.0,
        )
        if read_response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_stage_runs read failed ({read_response.status_code}): {read_response.text}"
            )
        rows = read_response.json()
        duration_ms = None
        if rows and rows[0].get("started_at"):
            started = datetime.fromisoformat(str(rows[0]["started_at"]).replace("Z", "+00:00"))
            duration_ms = int((_utc_now() - started).total_seconds() * 1000)

        body: dict[str, Any] = {
            "status": status,
            "completed_at": _utc_now().isoformat(),
        }
        if duration_ms is not None:
            body["duration_ms"] = duration_ms
        if records_out is not None:
            body["records_out"] = records_out
        if error_json is not None:
            body["error_json"] = error_json
        if llm:
            body["llm_prompt_name"] = llm.get("prompt_name")
            body["llm_model"] = llm.get("model")
            body["llm_prompt_tokens"] = llm.get("prompt_tokens")
            body["llm_completion_tokens"] = llm.get("completion_tokens")
            body["llm_total_tokens"] = llm.get("total_tokens")
            body["llm_cost_usd"] = llm.get("cost_usd")

        response = httpx.patch(
            self._url("pipeline_stage_runs"),
            headers=self._base_headers,
            params={"id": f"eq.{stage_run_id}"},
            json=body,
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_stage_runs update failed ({response.status_code}): {response.text}"
            )

    def get_run_by_trace_id(self, trace_id: str) -> PipelineRunRecord | None:
        response = httpx.get(
            self._url("pipeline_runs"),
            headers=self._base_headers,
            params={"trace_id": f"eq.{trace_id}", "order": "requested_at.desc", "limit": 1},
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase pipeline_runs query failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            return None
        row = rows[0]
        return PipelineRunRecord(
            run_id=str(row["run_id"]),
            trace_id=str(row["trace_id"]),
            tenant_id=str(row["tenant_id"]),
            user_id=str(row["user_id"]),
            source=str(row["source"]) if row.get("source") else None,
            trigger_type=str(row["trigger_type"]),
            status=str(row["status"]),
            requested_at=datetime.fromisoformat(str(row["requested_at"]).replace("Z", "+00:00")),
            started_at=(
                datetime.fromisoformat(str(row["started_at"]).replace("Z", "+00:00"))
                if row.get("started_at")
                else None
            ),
            completed_at=(
                datetime.fromisoformat(str(row["completed_at"]).replace("Z", "+00:00"))
                if row.get("completed_at")
                else None
            ),
        )


def create_pipeline_store(settings: Settings) -> PipelineStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabasePipelineStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqlitePipelineStore(db_path=settings.pipeline_db_path)
