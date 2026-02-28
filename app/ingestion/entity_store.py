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

    @staticmethod
    def _is_conflict(response: httpx.Response) -> bool:
        body = (response.text or "").lower()
        return response.status_code == 409 or "23505" in body

    def _bulk_fetch_identities(self, tenant_id: str, source_identities: list[str]) -> dict[str, str]:
        """Return {source_identity -> entity_id} for all known identities in one GET."""
        if not source_identities:
            return {}
        response = httpx.get(
            self._url("entity_identities"),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{tenant_id}",
                "source_identity": f"in.({','.join(source_identities)})",
                "select": "source_identity,entity_id",
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase entity_identities bulk fetch failed ({response.status_code}): {response.text}"
            )
        return {row["source_identity"]: str(row["entity_id"]) for row in response.json()}

    def _bulk_fetch_entities_by_email(self, tenant_id: str, emails: list[str]) -> dict[str, str]:
        """Return {primary_email -> entity_id} for all known emails in one GET."""
        if not emails:
            return {}
        response = httpx.get(
            self._url("entities"),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{tenant_id}",
                "primary_email": f"in.({','.join(emails)})",
                "select": "primary_email,entity_id",
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase entities bulk fetch failed ({response.status_code}): {response.text}"
            )
        return {row["primary_email"]: str(row["entity_id"]) for row in response.json()}

    def upsert_entities(self, resolved_entities: list[EntityCandidate]) -> EntityPersistResult:
        if not resolved_entities:
            return EntityPersistResult(0, 0, 0, 0)

        from collections import defaultdict
        by_tenant: dict[str, list[EntityCandidate]] = defaultdict(list)
        for c in resolved_entities:
            by_tenant[c.tenant_id].append(c)

        total_created = total_updated = total_identities = 0

        for tenant_id, candidates in by_tenant.items():
            # ── 1. Bulk-resolve already-known source identities ────────────────
            known_identities = self._bulk_fetch_identities(
                tenant_id, [c.source_event_id for c in candidates]
            )

            # ── 2. For unresolved candidates, match by email ───────────────────
            unresolved = [c for c in candidates if c.source_event_id not in known_identities]
            known_by_email = self._bulk_fetch_entities_by_email(
                tenant_id, [c.primary_email for c in unresolved if c.primary_email]
            )

            # Build full entity_id map: source_identity -> entity_id
            entity_id_map: dict[str, str] = dict(known_identities)
            existing_source_ids = set(known_identities.keys())
            for c in unresolved:
                if c.primary_email and c.primary_email in known_by_email:
                    entity_id_map[c.source_event_id] = known_by_email[c.primary_email]
                    existing_source_ids.add(c.source_event_id)

            # ── 3. Bulk-insert brand-new entities ─────────────────────────────
            new_candidates = [c for c in candidates if c.source_event_id not in entity_id_map]
            if new_candidates:
                ins_resp = httpx.post(
                    self._url("entities"),
                    headers={**self._base_headers, "Prefer": "return=representation"},
                    json=[
                        {
                            "tenant_id": c.tenant_id,
                            "display_name": c.display_name,
                            "primary_email": c.primary_email,
                            "company_norm": c.company_norm,
                            "title_norm": c.title_norm,
                            "metadata_json": c.metadata_json,
                        }
                        for c in new_candidates
                    ],
                    timeout=30.0,
                )
                if ins_resp.status_code >= 400:
                    if not self._is_conflict(ins_resp):
                        raise RuntimeError(
                            f"Supabase entities bulk insert failed ({ins_resp.status_code}): {ins_resp.text}"
                        )
                    # Conflict: insert one-by-one
                    for c in new_candidates:
                        sr = httpx.post(
                            self._url("entities"),
                            headers={**self._base_headers, "Prefer": "return=representation"},
                            json=[{
                                "tenant_id": c.tenant_id,
                                "display_name": c.display_name,
                                "primary_email": c.primary_email,
                                "company_norm": c.company_norm,
                                "title_norm": c.title_norm,
                                "metadata_json": c.metadata_json,
                            }],
                            timeout=20.0,
                        )
                        if sr.status_code >= 400:
                            if not self._is_conflict(sr):
                                raise RuntimeError(
                                    f"Supabase entities insert failed ({sr.status_code}): {sr.text}"
                                )
                            if c.primary_email:
                                fetched = self._bulk_fetch_entities_by_email(tenant_id, [c.primary_email])
                                if c.primary_email in fetched:
                                    entity_id_map[c.source_event_id] = fetched[c.primary_email]
                                    total_updated += 1
                        else:
                            rows = sr.json()
                            if rows:
                                entity_id_map[c.source_event_id] = str(rows[0]["entity_id"])
                                total_created += 1
                else:
                    returned = {row.get("primary_email"): str(row["entity_id"]) for row in ins_resp.json()}
                    for c in new_candidates:
                        eid = returned.get(c.primary_email)
                        if eid:
                            entity_id_map[c.source_event_id] = eid
                            total_created += 1

            # ── 4. Update already-existing entities (per-row PATCH) ────────────
            for c in candidates:
                if c.source_event_id not in existing_source_ids:
                    continue
                entity_id = entity_id_map.get(c.source_event_id)
                if not entity_id:
                    continue
                httpx.patch(
                    self._url("entities"),
                    headers=self._base_headers,
                    params={"entity_id": f"eq.{entity_id}"},
                    json={
                        "display_name": c.display_name,
                        "primary_email": c.primary_email,
                        "company_norm": c.company_norm,
                        "title_norm": c.title_norm,
                        "metadata_json": c.metadata_json,
                    },
                    timeout=20.0,
                )
                total_updated += 1

            # ── 5. Bulk-insert all entity_identities ───────────────────────────
            identity_rows = [
                {
                    "tenant_id": c.tenant_id,
                    "entity_id": entity_id_map[c.source_event_id],
                    "source": c.source,
                    "source_identity": c.source_event_id,
                    "email": c.primary_email,
                    "confidence": 1.0,
                    "is_primary": True,
                    "raw_event_id": c.raw_event_id,
                }
                for c in candidates
                if c.source_event_id in entity_id_map
            ]
            if identity_rows:
                id_resp = httpx.post(
                    self._url("entity_identities"),
                    headers=self._base_headers,
                    json=identity_rows,
                    timeout=30.0,
                )
                if id_resp.status_code >= 400:
                    if not self._is_conflict(id_resp):
                        raise RuntimeError(
                            f"Supabase entity_identities bulk insert failed ({id_resp.status_code}): {id_resp.text}"
                        )
                    # Fallback: PATCH each existing identity
                    for row in identity_rows:
                        httpx.patch(
                            self._url("entity_identities"),
                            headers=self._base_headers,
                            params={
                                "tenant_id": f"eq.{row['tenant_id']}",
                                "source": f"eq.{row['source']}",
                                "source_identity": f"eq.{row['source_identity']}",
                            },
                            json={
                                "entity_id": row["entity_id"],
                                "email": row["email"],
                                "raw_event_id": row["raw_event_id"],
                            },
                            timeout=20.0,
                        )
                total_identities += len(identity_rows)

        return EntityPersistResult(len(resolved_entities), total_created, total_updated, total_identities)


def create_entity_store(settings: Settings) -> EntityStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseEntityStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteEntityStore(db_path=settings.pipeline_db_path)

