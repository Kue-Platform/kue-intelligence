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


@dataclass
class WarmPathEvent:
    event_id: str
    tenant_id: str
    event_type: str
    origin_entity_id: str
    target_entity_id: str
    selected_path_score: float | None = None
    selected_path_hop_count: int | None = None
    connector_entity_ids: list[str] | None = None
    metadata_json: dict[str, Any] | None = None
    recorded_at: str | None = None


class WarmPathEventStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist(self, event: WarmPathEvent) -> None:
        raise NotImplementedError


class SqliteWarmPathEventStore(WarmPathEventStore):
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
                CREATE TABLE IF NOT EXISTS warm_path_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    tenant_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    origin_entity_id TEXT NOT NULL,
                    target_entity_id TEXT NOT NULL,
                    selected_path_score REAL,
                    selected_path_hop_count INTEGER,
                    connector_entity_ids TEXT NOT NULL DEFAULT '[]',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    recorded_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def persist(self, event: WarmPathEvent) -> None:
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO warm_path_events (
                        event_id, tenant_id, event_type,
                        origin_entity_id, target_entity_id,
                        selected_path_score, selected_path_hop_count,
                        connector_entity_ids, metadata_json, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.tenant_id,
                        event.event_type,
                        event.origin_entity_id,
                        event.target_entity_id,
                        event.selected_path_score,
                        event.selected_path_hop_count,
                        json.dumps(event.connector_entity_ids or [], ensure_ascii=True),
                        json.dumps(event.metadata_json or {}, ensure_ascii=True),
                        event.recorded_at or datetime.now(UTC).isoformat(),
                    ),
                )
                conn.commit()


class SupabaseWarmPathEventStore(WarmPathEventStore):
    def __init__(self, *, supabase_url: str, api_key: str) -> None:
        self._supabase_url = supabase_url.rstrip("/")
        self._api_key = api_key
        self._headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    @property
    def store_name(self) -> str:
        return "supabase:warm_path_events"

    def _url(self) -> str:
        return f"{self._supabase_url}/rest/v1/warm_path_events"

    def persist(self, event: WarmPathEvent) -> None:
        payload = {
            "event_id": event.event_id,
            "tenant_id": event.tenant_id,
            "event_type": event.event_type,
            "origin_entity_id": event.origin_entity_id,
            "target_entity_id": event.target_entity_id,
            "selected_path_score": event.selected_path_score,
            "selected_path_hop_count": event.selected_path_hop_count,
            "connector_entity_ids": event.connector_entity_ids or [],
            "metadata_json": event.metadata_json or {},
            "recorded_at": event.recorded_at or datetime.now(UTC).isoformat(),
        }
        response = httpx.post(
            self._url(),
            headers=self._headers,
            json=payload,
            timeout=10.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase warm_path_events insert failed ({response.status_code}): {response.text}"
            )


def create_warm_path_event_store(settings: Settings) -> WarmPathEventStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseWarmPathEventStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteWarmPathEventStore(db_path=settings.pipeline_db_path)
