from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import httpx

from app.core.config import Settings
from app.schemas import IngestionSource, RawCapturedEvent, SourceEvent


@dataclass
class RawCaptureResult:
    stored_count: int
    captured_at: datetime


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class RawEventStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist_source_events(
        self,
        events: list[SourceEvent],
        *,
        run_id: str | None = None,
        headers_json: dict[str, Any] | None = None,
        ingest_version: str = "v1",
    ) -> RawCaptureResult:
        raise NotImplementedError

    @abstractmethod
    def list_by_trace_id(self, trace_id: str) -> list[RawCapturedEvent]:
        raise NotImplementedError


class SqliteRawEventStore(RawEventStore):
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
                CREATE TABLE IF NOT EXISTS raw_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_event_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    headers_json TEXT NOT NULL DEFAULT '{}',
                    ingest_version TEXT NOT NULL DEFAULT 'v1',
                    payload_json TEXT NOT NULL,
                    captured_at TEXT NOT NULL
                )
                """
            )
            conn.execute("PRAGMA table_info(raw_events)")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(raw_events)").fetchall()}
            if "run_id" not in columns:
                conn.execute("ALTER TABLE raw_events ADD COLUMN run_id TEXT")
            if "headers_json" not in columns:
                conn.execute("ALTER TABLE raw_events ADD COLUMN headers_json TEXT NOT NULL DEFAULT '{}'")
            if "ingest_version" not in columns:
                conn.execute("ALTER TABLE raw_events ADD COLUMN ingest_version TEXT NOT NULL DEFAULT 'v1'")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_trace_id ON raw_events(trace_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_tenant_source "
                "ON raw_events(tenant_id, source, occurred_at)"
            )
            conn.commit()

    def persist_source_events(
        self,
        events: list[SourceEvent],
        *,
        run_id: str | None = None,
        headers_json: dict[str, Any] | None = None,
        ingest_version: str = "v1",
    ) -> RawCaptureResult:
        if not events:
            captured_at = datetime.now(UTC)
            return RawCaptureResult(stored_count=0, captured_at=captured_at)

        captured_at = datetime.now(UTC)
        rows = [
            (
                event.tenant_id,
                event.user_id,
                str(event.source),
                event.source_event_id,
                event.occurred_at.isoformat(),
                event.trace_id,
                run_id,
                json.dumps(headers_json or {}, ensure_ascii=True),
                ingest_version,
                json.dumps(event.payload, ensure_ascii=True),
                captured_at.isoformat(),
            )
            for event in events
        ]
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO raw_events (
                        tenant_id, user_id, source, source_event_id, occurred_at,
                        trace_id, run_id, headers_json, ingest_version, payload_json, captured_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.commit()
        return RawCaptureResult(stored_count=len(rows), captured_at=captured_at)

    def list_by_trace_id(self, trace_id: str) -> list[RawCapturedEvent]:
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT id, run_id, tenant_id, user_id, source, source_event_id,
                           occurred_at, trace_id, payload_json, captured_at
                    FROM raw_events
                    WHERE trace_id = ?
                    ORDER BY id ASC
                    """,
                    (trace_id,),
                )
                rows = cursor.fetchall()

        events: list[RawCapturedEvent] = []
        for row in rows:
            events.append(
                RawCapturedEvent(
                    raw_event_id=int(row["id"]),
                    run_id=str(row["run_id"]) if row["run_id"] is not None else None,
                    tenant_id=str(row["tenant_id"]),
                    user_id=str(row["user_id"]),
                    source=IngestionSource(str(row["source"])),
                    source_event_id=str(row["source_event_id"]),
                    occurred_at=_parse_iso_datetime(str(row["occurred_at"])),
                    trace_id=str(row["trace_id"]),
                    payload=json.loads(str(row["payload_json"])),
                    captured_at=_parse_iso_datetime(str(row["captured_at"])),
                )
            )
        return events


class SupabaseRawEventStore(RawEventStore):
    def __init__(self, *, supabase_url: str, api_key: str, table: str) -> None:
        self._supabase_url = supabase_url.rstrip("/")
        self._api_key = api_key
        self._table = table
        self._base_headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    @property
    def store_name(self) -> str:
        return f"supabase:{self._table}"

    def _rest_url(self) -> str:
        return f"{self._supabase_url}/rest/v1/{self._table}"

    def _is_duplicate_conflict(self, response: httpx.Response) -> bool:
        if response.status_code != 409:
            return False
        body = response.text.lower()
        return "duplicate key value violates unique constraint" in body

    def _upsert_single_row(self, row_payload: dict[str, Any]) -> None:
        headers = dict(self._base_headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        upsert_response = httpx.post(
            self._rest_url(),
            headers=headers,
            params={"on_conflict": "tenant_id,user_id,source,source_event_id"},
            json=[row_payload],
            timeout=20.0,
        )
        if upsert_response.status_code < 400:
            return

        if not self._is_duplicate_conflict(upsert_response):
            raise RuntimeError(
                f"Supabase raw event row upsert failed ({upsert_response.status_code}): {upsert_response.text}"
            )

        # Final fallback for strict conflict behavior: update existing row by dedup key.
        update_response = httpx.patch(
            self._rest_url(),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{row_payload['tenant_id']}",
                "user_id": f"eq.{row_payload['user_id']}",
                "source": f"eq.{row_payload['source']}",
                "source_event_id": f"eq.{row_payload['source_event_id']}",
            },
            json={
                "occurred_at": row_payload["occurred_at"],
                "trace_id": row_payload["trace_id"],
                "run_id": row_payload.get("run_id"),
                "headers_json": row_payload.get("headers_json", {}),
                "ingest_version": row_payload.get("ingest_version", "v1"),
                "payload_json": row_payload["payload_json"],
                "captured_at": row_payload["captured_at"],
            },
            timeout=20.0,
        )
        if update_response.status_code >= 400:
            raise RuntimeError(
                f"Supabase raw event row update failed ({update_response.status_code}): {update_response.text}"
            )

    def persist_source_events(
        self,
        events: list[SourceEvent],
        *,
        run_id: str | None = None,
        headers_json: dict[str, Any] | None = None,
        ingest_version: str = "v1",
    ) -> RawCaptureResult:
        if not events:
            captured_at = datetime.now(UTC)
            return RawCaptureResult(stored_count=0, captured_at=captured_at)

        captured_at = datetime.now(UTC)
        payload: list[dict[str, Any]] = []
        for event in events:
            payload.append(
                {
                    "tenant_id": event.tenant_id,
                    "user_id": event.user_id,
                    "source": str(event.source),
                    "source_event_id": event.source_event_id,
                    "occurred_at": event.occurred_at.isoformat(),
                    "trace_id": event.trace_id,
                    "run_id": run_id,
                    "headers_json": headers_json or {},
                    "ingest_version": ingest_version,
                    "payload_json": event.payload,
                    "captured_at": captured_at.isoformat(),
                }
            )

        headers = dict(self._base_headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        response = httpx.post(
            self._rest_url(),
            headers=headers,
            params={"on_conflict": "tenant_id,user_id,source,source_event_id"},
            json=payload,
            timeout=20.0,
        )
        if response.status_code >= 400:
            if not self._is_duplicate_conflict(response):
                raise RuntimeError(
                    f"Supabase raw event insert failed ({response.status_code}): {response.text}"
                )
            # Graceful fallback: process one row at a time and update existing duplicates.
            for row_payload in payload:
                self._upsert_single_row(row_payload)
        return RawCaptureResult(stored_count=len(payload), captured_at=captured_at)

    def list_by_trace_id(self, trace_id: str) -> list[RawCapturedEvent]:
        params = {
            "trace_id": f"eq.{trace_id}",
            "order": "id.asc",
        }
        response = httpx.get(
            self._rest_url(),
            headers=self._base_headers,
            params=params,
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase raw event query failed ({response.status_code}): {response.text}"
            )

        rows = response.json()
        events: list[RawCapturedEvent] = []
        for row in rows:
            payload = row.get("payload_json")
            if isinstance(payload, str):
                decoded_payload = json.loads(payload)
            else:
                decoded_payload = payload or {}
            events.append(
                RawCapturedEvent(
                    raw_event_id=int(row.get("id", 0)),
                    run_id=str(row["run_id"]) if row.get("run_id") else None,
                    tenant_id=str(row["tenant_id"]),
                    user_id=str(row["user_id"]),
                    source=IngestionSource(str(row["source"])),
                    source_event_id=str(row["source_event_id"]),
                    occurred_at=_parse_iso_datetime(str(row["occurred_at"])),
                    trace_id=str(row["trace_id"]),
                    payload=decoded_payload,
                    captured_at=_parse_iso_datetime(str(row["captured_at"])),
                )
            )
        return events


def create_raw_event_store(settings: Settings) -> RawEventStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseRawEventStore(
            supabase_url=settings.supabase_url,
            api_key=api_key,
            table=settings.supabase_raw_events_table,
        )
    return SqliteRawEventStore(db_path=settings.raw_events_db_path)
