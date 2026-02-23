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
from app.ingestion.entity_resolution import EntityCandidate


@dataclass
class EntityPersistResult:
    resolved_count: int
    created_entities: int
    updated_entities: int
    identities_upserted: int


class EntityStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def upsert_entities(self, resolved_entities: list[EntityCandidate]) -> EntityPersistResult:
        raise NotImplementedError


class SqliteEntityStore(EntityStore):
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
                CREATE TABLE IF NOT EXISTS entities (
                    entity_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    primary_email TEXT,
                    company_norm TEXT,
                    title_norm TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_tenant_email
                ON entities(tenant_id, primary_email)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_identities (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_identity TEXT NOT NULL,
                    email TEXT,
                    confidence REAL NOT NULL DEFAULT 1.0,
                    is_primary INTEGER NOT NULL DEFAULT 1,
                    raw_event_id INTEGER,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_identities_unique
                ON entity_identities(tenant_id, source, source_identity)
                """
            )
            conn.commit()

    def _find_entity_id(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        source: str,
        source_event_id: str,
        primary_email: str | None,
    ) -> str | None:
        row = conn.execute(
            """
            SELECT entity_id
            FROM entity_identities
            WHERE tenant_id = ? AND source = ? AND source_identity = ?
            LIMIT 1
            """,
            (tenant_id, source, source_event_id),
        ).fetchone()
        if row:
            return str(row[0])

        if primary_email:
            row = conn.execute(
                """
                SELECT entity_id
                FROM entities
                WHERE tenant_id = ? AND primary_email = ?
                LIMIT 1
                """,
                (tenant_id, primary_email),
            ).fetchone()
            if row:
                return str(row[0])
        return None

    def upsert_entities(self, resolved_entities: list[EntityCandidate]) -> EntityPersistResult:
        if not resolved_entities:
            return EntityPersistResult(0, 0, 0, 0)

        created = 0
        updated = 0
        identities = 0
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                for entity in resolved_entities:
                    entity_id = self._find_entity_id(
                        conn,
                        tenant_id=entity.tenant_id,
                        source=entity.source,
                        source_event_id=entity.source_event_id,
                        primary_email=entity.primary_email,
                    )
                    if entity_id is None:
                        entity_id = str(uuid4())
                        conn.execute(
                            """
                            INSERT INTO entities (
                                entity_id, tenant_id, display_name, primary_email, company_norm, title_norm,
                                metadata_json, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                entity_id,
                                entity.tenant_id,
                                entity.display_name,
                                entity.primary_email,
                                entity.company_norm,
                                entity.title_norm,
                                json.dumps(entity.metadata_json, ensure_ascii=True),
                                now,
                                now,
                            ),
                        )
                        created += 1
                    else:
                        conn.execute(
                            """
                            UPDATE entities
                            SET display_name = ?, primary_email = coalesce(?, primary_email),
                                company_norm = coalesce(?, company_norm),
                                title_norm = coalesce(?, title_norm),
                                metadata_json = ?, updated_at = ?
                            WHERE entity_id = ?
                            """,
                            (
                                entity.display_name,
                                entity.primary_email,
                                entity.company_norm,
                                entity.title_norm,
                                json.dumps(entity.metadata_json, ensure_ascii=True),
                                now,
                                entity_id,
                            ),
                        )
                        updated += 1

                    conn.execute(
                        """
                        INSERT INTO entity_identities (
                            id, tenant_id, entity_id, source, source_identity, email, confidence, is_primary, raw_event_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(tenant_id, source, source_identity)
                        DO UPDATE SET
                            entity_id=excluded.entity_id,
                            email=excluded.email,
                            raw_event_id=excluded.raw_event_id
                        """,
                        (
                            str(uuid4()),
                            entity.tenant_id,
                            entity_id,
                            entity.source,
                            entity.source_event_id,
                            entity.primary_email,
                            1.0,
                            1,
                            entity.raw_event_id,
                            now,
                        ),
                    )
                    identities += 1
                conn.commit()

        return EntityPersistResult(len(resolved_entities), created, updated, identities)


class SupabaseEntityStore(EntityStore):
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
        return "supabase:entities"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    def _find_entity_id(self, candidate: EntityCandidate) -> str | None:
        response = httpx.get(
            self._url("entity_identities"),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{candidate.tenant_id}",
                "source": f"eq.{candidate.source}",
                "source_identity": f"eq.{candidate.source_event_id}",
                "select": "entity_id",
                "limit": 1,
            },
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase entity_identities query failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if rows:
            return str(rows[0]["entity_id"])

        if candidate.primary_email:
            response = httpx.get(
                self._url("entities"),
                headers=self._base_headers,
                params={
                    "tenant_id": f"eq.{candidate.tenant_id}",
                    "primary_email": f"eq.{candidate.primary_email}",
                    "select": "entity_id",
                    "limit": 1,
                },
                timeout=20.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase entities query failed ({response.status_code}): {response.text}"
                )
            rows = response.json()
            if rows:
                return str(rows[0]["entity_id"])
        return None

    def upsert_entities(self, resolved_entities: list[EntityCandidate]) -> EntityPersistResult:
        if not resolved_entities:
            return EntityPersistResult(0, 0, 0, 0)

        created = 0
        updated = 0
        identities = 0
        for candidate in resolved_entities:
            entity_id = self._find_entity_id(candidate)
            if entity_id is None:
                headers = dict(self._base_headers)
                headers["Prefer"] = "return=representation"
                response = httpx.post(
                    self._url("entities"),
                    headers=headers,
                    json=[
                        {
                            "tenant_id": candidate.tenant_id,
                            "display_name": candidate.display_name,
                            "primary_email": candidate.primary_email,
                            "company_norm": candidate.company_norm,
                            "title_norm": candidate.title_norm,
                            "metadata_json": candidate.metadata_json,
                        }
                    ],
                    timeout=20.0,
                )
                if response.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase entities insert failed ({response.status_code}): {response.text}"
                    )
                rows = response.json()
                entity_id = str(rows[0]["entity_id"])
                created += 1
            else:
                response = httpx.patch(
                    self._url("entities"),
                    headers=self._base_headers,
                    params={"entity_id": f"eq.{entity_id}"},
                    json={
                        "display_name": candidate.display_name,
                        "primary_email": candidate.primary_email,
                        "company_norm": candidate.company_norm,
                        "title_norm": candidate.title_norm,
                        "metadata_json": candidate.metadata_json,
                    },
                    timeout=20.0,
                )
                if response.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase entities update failed ({response.status_code}): {response.text}"
                    )
                updated += 1

            identity_payload = {
                "tenant_id": candidate.tenant_id,
                "entity_id": entity_id,
                "source": candidate.source,
                "source_identity": candidate.source_event_id,
                "email": candidate.primary_email,
                "confidence": 1.0,
                "is_primary": True,
                "raw_event_id": candidate.raw_event_id,
            }
            dedup_params = {
                "tenant_id": f"eq.{candidate.tenant_id}",
                "source": f"eq.{candidate.source}",
                "source_identity": f"eq.{candidate.source_event_id}",
            }
            lookup = httpx.get(
                self._url("entity_identities"),
                headers=self._base_headers,
                params={**dedup_params, "select": "id", "limit": 1},
                timeout=20.0,
            )
            if lookup.status_code < 400 and lookup.json():
                httpx.patch(
                    self._url("entity_identities"),
                    headers=self._base_headers,
                    params=dedup_params,
                    json={
                        "entity_id": entity_id,
                        "email": candidate.primary_email,
                        "raw_event_id": candidate.raw_event_id,
                    },
                    timeout=20.0,
                )
            else:
                response = httpx.post(
                    self._url("entity_identities"),
                    headers=self._base_headers,
                    json=[identity_payload],
                    timeout=20.0,
                )
                if response.status_code >= 400:
                    body = (response.text or "").lower()
                    is_conflict = response.status_code == 409 or "23505" in body
                    if not is_conflict:
                        raise RuntimeError(
                            f"Supabase entity_identities upsert failed ({response.status_code}): {response.text}"
                        )
                    # Race-condition fallback
                    httpx.patch(
                        self._url("entity_identities"),
                        headers=self._base_headers,
                        params=dedup_params,
                        json={
                            "entity_id": entity_id,
                            "email": candidate.primary_email,
                            "raw_event_id": candidate.raw_event_id,
                        },
                        timeout=20.0,
                    )
            identities += 1

        return EntityPersistResult(len(resolved_entities), created, updated, identities)


def create_entity_store(settings: Settings) -> EntityStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseEntityStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteEntityStore(db_path=settings.pipeline_db_path)
