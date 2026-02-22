from __future__ import annotations

import json
import hashlib
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import httpx

from app.core.config import Settings
from app.ingestion.parsers import ParsedCanonicalEvent
from app.schemas import CanonicalEvent, CanonicalEventType, IngestionSource


@dataclass
class CanonicalPersistResult:
    stored_count: int
    parsed_at: datetime


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class CanonicalEventStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist_parsed_events(
        self,
        parsed_events: list[ParsedCanonicalEvent],
        parser_version: str,
        *,
        run_id: str | None = None,
        schema_version: str = "v1",
        parse_status: str = "parsed",
    ) -> CanonicalPersistResult:
        raise NotImplementedError

    @abstractmethod
    def list_by_trace_id(self, trace_id: str) -> list[CanonicalEvent]:
        raise NotImplementedError


class SqliteCanonicalEventStore(CanonicalEventStore):
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
                CREATE TABLE IF NOT EXISTS canonical_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    raw_event_id INTEGER NOT NULL,
                    run_id TEXT,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_event_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    normalized_json TEXT NOT NULL,
                    parse_warnings_json TEXT NOT NULL,
                    parse_status TEXT NOT NULL DEFAULT 'parsed',
                    schema_version TEXT NOT NULL DEFAULT 'v1',
                    canonical_hash TEXT,
                    parser_version TEXT NOT NULL,
                    parsed_at TEXT NOT NULL
                )
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(canonical_events)").fetchall()}
            if "run_id" not in columns:
                conn.execute("ALTER TABLE canonical_events ADD COLUMN run_id TEXT")
            if "parse_status" not in columns:
                conn.execute(
                    "ALTER TABLE canonical_events ADD COLUMN parse_status TEXT NOT NULL DEFAULT 'parsed'"
                )
            if "schema_version" not in columns:
                conn.execute(
                    "ALTER TABLE canonical_events ADD COLUMN schema_version TEXT NOT NULL DEFAULT 'v1'"
                )
            if "canonical_hash" not in columns:
                conn.execute("ALTER TABLE canonical_events ADD COLUMN canonical_hash TEXT")
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_events_dedup
                ON canonical_events(raw_event_id, event_type, parser_version)
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_events_trace_id ON canonical_events(trace_id)"
            )
            conn.commit()

    def persist_parsed_events(
        self,
        parsed_events: list[ParsedCanonicalEvent],
        parser_version: str,
        *,
        run_id: str | None = None,
        schema_version: str = "v1",
        parse_status: str = "parsed",
    ) -> CanonicalPersistResult:
        if not parsed_events:
            parsed_at = datetime.now(UTC)
            return CanonicalPersistResult(stored_count=0, parsed_at=parsed_at)

        parsed_at = datetime.now(UTC)
        rows = [
            (
                event.raw_event_id,
                run_id,
                event.tenant_id,
                event.user_id,
                event.trace_id,
                str(event.source),
                event.source_event_id,
                event.occurred_at.isoformat(),
                str(event.event_type),
                json.dumps(event.normalized, ensure_ascii=True),
                json.dumps(event.parse_warnings, ensure_ascii=True),
                parse_status,
                schema_version,
                hashlib.sha256(
                    json.dumps(event.normalized, sort_keys=True, ensure_ascii=True).encode("utf-8")
                ).hexdigest(),
                parser_version,
                parsed_at.isoformat(),
            )
            for event in parsed_events
        ]
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO canonical_events (
                        raw_event_id, run_id, tenant_id, user_id, trace_id, source, source_event_id,
                        occurred_at, event_type, normalized_json, parse_warnings_json,
                        parse_status, schema_version, canonical_hash, parser_version, parsed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(raw_event_id, event_type, parser_version)
                    DO UPDATE SET
                        run_id=excluded.run_id,
                        normalized_json=excluded.normalized_json,
                        parse_warnings_json=excluded.parse_warnings_json,
                        parse_status=excluded.parse_status,
                        schema_version=excluded.schema_version,
                        canonical_hash=excluded.canonical_hash,
                        parsed_at=excluded.parsed_at
                    """,
                    rows,
                )
                conn.commit()
        return CanonicalPersistResult(stored_count=len(rows), parsed_at=parsed_at)

    def list_by_trace_id(self, trace_id: str) -> list[CanonicalEvent]:
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT raw_event_id, run_id, tenant_id, user_id, trace_id, source, source_event_id,
                           occurred_at, event_type, normalized_json, parse_warnings_json
                    FROM canonical_events
                    WHERE trace_id = ?
                    ORDER BY id ASC
                    """,
                    (trace_id,),
                )
                rows = cursor.fetchall()

        output: list[CanonicalEvent] = []
        for row in rows:
            output.append(
                CanonicalEvent(
                    raw_event_id=int(row["raw_event_id"]),
                    run_id=str(row["run_id"]) if row["run_id"] is not None else None,
                    tenant_id=str(row["tenant_id"]),
                    user_id=str(row["user_id"]),
                    trace_id=str(row["trace_id"]),
                    source=IngestionSource(str(row["source"])),
                    source_event_id=str(row["source_event_id"]),
                    occurred_at=_parse_iso_datetime(str(row["occurred_at"])),
                    event_type=CanonicalEventType(str(row["event_type"])),
                    normalized=json.loads(str(row["normalized_json"])),
                    parse_warnings=json.loads(str(row["parse_warnings_json"])),
                )
            )
        return output


class SupabaseCanonicalEventStore(CanonicalEventStore):
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

    def persist_parsed_events(
        self,
        parsed_events: list[ParsedCanonicalEvent],
        parser_version: str,
        *,
        run_id: str | None = None,
        schema_version: str = "v1",
        parse_status: str = "parsed",
    ) -> CanonicalPersistResult:
        if not parsed_events:
            parsed_at = datetime.now(UTC)
            return CanonicalPersistResult(stored_count=0, parsed_at=parsed_at)

        parsed_at = datetime.now(UTC)
        payload: list[dict[str, Any]] = []
        for event in parsed_events:
            canonical_hash = hashlib.sha256(
                json.dumps(event.normalized, sort_keys=True, ensure_ascii=True).encode("utf-8")
            ).hexdigest()
            payload.append(
                {
                    "raw_event_id": event.raw_event_id,
                    "run_id": run_id,
                    "tenant_id": event.tenant_id,
                    "user_id": event.user_id,
                    "trace_id": event.trace_id,
                    "source": str(event.source),
                    "source_event_id": event.source_event_id,
                    "occurred_at": event.occurred_at.isoformat(),
                    "event_type": str(event.event_type),
                    "normalized_json": event.normalized,
                    "parse_warnings_json": event.parse_warnings,
                    "parse_status": parse_status,
                    "schema_version": schema_version,
                    "canonical_hash": canonical_hash,
                    "parser_version": parser_version,
                    "parsed_at": parsed_at.isoformat(),
                }
            )

        headers = dict(self._base_headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        response = httpx.post(
            self._rest_url(),
            headers=headers,
            params={"on_conflict": "raw_event_id,event_type,parser_version"},
            json=payload,
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase canonical event insert failed ({response.status_code}): {response.text}"
            )
        return CanonicalPersistResult(stored_count=len(payload), parsed_at=parsed_at)

    def list_by_trace_id(self, trace_id: str) -> list[CanonicalEvent]:
        response = httpx.get(
            self._rest_url(),
            headers=self._base_headers,
            params={"trace_id": f"eq.{trace_id}", "order": "id.asc"},
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase canonical event query failed ({response.status_code}): {response.text}"
            )

        rows = response.json()
        output: list[CanonicalEvent] = []
        for row in rows:
            output.append(
                CanonicalEvent(
                    raw_event_id=int(row["raw_event_id"]),
                    run_id=str(row["run_id"]) if row.get("run_id") else None,
                    tenant_id=str(row["tenant_id"]),
                    user_id=str(row["user_id"]),
                    trace_id=str(row["trace_id"]),
                    source=IngestionSource(str(row["source"])),
                    source_event_id=str(row["source_event_id"]),
                    occurred_at=_parse_iso_datetime(str(row["occurred_at"])),
                    event_type=CanonicalEventType(str(row["event_type"])),
                    normalized=dict(row.get("normalized_json", {})),
                    parse_warnings=list(row.get("parse_warnings_json", [])),
                )
            )
        return output


def create_canonical_event_store(settings: Settings) -> CanonicalEventStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseCanonicalEventStore(
            supabase_url=settings.supabase_url,
            api_key=api_key,
            table=settings.supabase_canonical_events_table,
        )
    return SqliteCanonicalEventStore(db_path=settings.canonical_events_db_path)
