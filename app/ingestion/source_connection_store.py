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
from app.ingestion.db import get_connection
from app.schemas import IngestionSource


@dataclass
class SourceConnectionRecord:
    tenant_id: str
    user_id: str
    source: IngestionSource
    external_account_id: str
    scopes: list[str]
    token_json: dict[str, Any]
    token_expires_at: datetime | None = None
    status: str = "active"
    last_sync_at: datetime | None = None
    last_error: str | None = None


class SourceConnectionStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def upsert_connections(self, records: list[SourceConnectionRecord]) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_connections_for_user(
        self, tenant_id: str, user_id: str
    ) -> list[SourceConnectionRecord]:
        raise NotImplementedError


class SqliteSourceConnectionStore(SourceConnectionStore):
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
                CREATE TABLE IF NOT EXISTS source_connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    external_account_id TEXT NOT NULL,
                    scopes_json TEXT NOT NULL DEFAULT '[]',
                    token_json TEXT NOT NULL DEFAULT '{}',
                    token_expires_at TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    last_sync_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (tenant_id, user_id, source, external_account_id)
                )
                """
            )
            conn.commit()

    def upsert_connections(self, records: list[SourceConnectionRecord]) -> int:
        if not records:
            return 0
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with get_connection(self._db_path) as conn:
                for rec in records:
                    conn.execute(
                        """
                        INSERT INTO source_connections (
                            tenant_id, user_id, source, external_account_id,
                            scopes_json, token_json, token_expires_at, status,
                            last_sync_at, last_error, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(tenant_id, user_id, source, external_account_id)
                        DO UPDATE SET
                            scopes_json = excluded.scopes_json,
                            token_json = excluded.token_json,
                            token_expires_at = excluded.token_expires_at,
                            status = excluded.status,
                            last_sync_at = excluded.last_sync_at,
                            last_error = excluded.last_error,
                            updated_at = excluded.updated_at
                        """,
                        (
                            rec.tenant_id,
                            rec.user_id,
                            str(rec.source),
                            rec.external_account_id,
                            json.dumps(rec.scopes, ensure_ascii=True),
                            json.dumps(rec.token_json, ensure_ascii=True),
                            rec.token_expires_at.isoformat()
                            if rec.token_expires_at
                            else None,
                            rec.status,
                            rec.last_sync_at.isoformat() if rec.last_sync_at else None,
                            rec.last_error,
                            now,
                        ),
                    )
                conn.commit()
        return len(records)

    def get_connections_for_user(
        self, tenant_id: str, user_id: str
    ) -> list[SourceConnectionRecord]:
        with self._lock:
            with get_connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT tenant_id, user_id, source, external_account_id,
                           scopes_json, token_json, token_expires_at, status,
                           last_sync_at, last_error
                    FROM source_connections
                    WHERE tenant_id = ? AND user_id = ? AND status = 'active'
                    """,
                    (tenant_id, user_id),
                )
                rows = cursor.fetchall()
        result: list[SourceConnectionRecord] = []
        for row in rows:
            expires_raw = row["token_expires_at"]
            token_expires_at: datetime | None = None
            if expires_raw:
                try:
                    from app.ingestion.canonical_store import _parse_iso_datetime

                    token_expires_at = _parse_iso_datetime(str(expires_raw))
                except Exception:
                    pass
            result.append(
                SourceConnectionRecord(
                    tenant_id=str(row["tenant_id"]),
                    user_id=str(row["user_id"]),
                    source=IngestionSource(str(row["source"])),
                    external_account_id=str(row["external_account_id"]),
                    scopes=json.loads(str(row["scopes_json"])),
                    token_json=json.loads(str(row["token_json"])),
                    token_expires_at=token_expires_at,
                    status=str(row["status"]),
                )
            )
        return result


class SupabaseSourceConnectionStore(SourceConnectionStore):
    def __init__(self, *, supabase_url: str, api_key: str) -> None:
        self._supabase_url = supabase_url.rstrip("/")
        self._api_key = api_key
        self._headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }

    @property
    def store_name(self) -> str:
        return "supabase:source_connections"

    def _url(self) -> str:
        return f"{self._supabase_url}/rest/v1/source_connections"

    def upsert_connections(self, records: list[SourceConnectionRecord]) -> int:
        if not records:
            return 0
        payload = [
            {
                "tenant_id": rec.tenant_id,
                "user_id": rec.user_id,
                "source": str(rec.source),
                "external_account_id": rec.external_account_id,
                "scopes": rec.scopes,
                "token_json": rec.token_json,
                "token_expires_at": rec.token_expires_at.isoformat()
                if rec.token_expires_at
                else None,
                "status": rec.status,
                "last_sync_at": rec.last_sync_at.isoformat()
                if rec.last_sync_at
                else None,
                "last_error": rec.last_error,
                "updated_at": datetime.now(UTC).isoformat(),
            }
            for rec in records
        ]
        response = httpx.post(
            self._url(),
            headers=self._headers,
            params={"on_conflict": "tenant_id,user_id,source,external_account_id"},
            json=payload,
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase source_connections upsert failed ({response.status_code}): {response.text}"
            )
        return len(records)

    def get_connections_for_user(
        self, tenant_id: str, user_id: str
    ) -> list[SourceConnectionRecord]:
        raise NotImplementedError("Supabase source connection store is not used")


def create_source_connection_store(settings: Settings) -> SourceConnectionStore:
    return SqliteSourceConnectionStore(db_path=settings.pipeline_db_path)
