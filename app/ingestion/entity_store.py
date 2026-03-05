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
from app.ingestion.metadata_extraction import MetadataCandidate


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

    @abstractmethod
    def upsert_metadata(self, metadata_candidates: list[MetadataCandidate]) -> int:
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

    def upsert_metadata(self, metadata_candidates: list[MetadataCandidate]) -> int:
        if not metadata_candidates:
            return 0
        updated = 0
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                for item in metadata_candidates:
                    row = conn.execute(
                        """
                        SELECT entity_id, metadata_json
                        FROM entities
                        WHERE tenant_id = ? AND primary_email = ?
                        LIMIT 1
                        """,
                        (item.tenant_id, item.primary_email),
                    ).fetchone()
                    if row is None:
                        continue
                    try:
                        existing_metadata = json.loads(str(row["metadata_json"] or "{}"))
                    except json.JSONDecodeError:
                        existing_metadata = {}
                    merged_metadata = dict(existing_metadata)
                    merged_metadata.update(item.metadata_json)
                    conn.execute(
                        """
                        UPDATE entities
                        SET display_name = coalesce(?, display_name),
                            company_norm = coalesce(?, company_norm),
                            title_norm = coalesce(?, title_norm),
                            metadata_json = ?,
                            updated_at = ?
                        WHERE entity_id = ?
                        """,
                        (
                            item.display_name,
                            item.metadata_json.get("company_norm"),
                            item.metadata_json.get("title_norm"),
                            json.dumps(merged_metadata, ensure_ascii=True),
                            now,
                            str(row["entity_id"]),
                        ),
                    )
                    updated += 1
                conn.commit()
        return updated


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

    @property
    def _upsert_headers(self) -> dict[str, str]:
        """Headers for a native PostgREST upsert (on_conflict + merge-duplicates)."""
        return {**self._base_headers, "Prefer": "resolution=merge-duplicates,return=minimal"}

    def _bulk_fetch_entities_by_email(self, tenant_id: str, emails: list[str]) -> dict[str, str]:
        """Return {primary_email -> entity_id} for all known emails in one GET."""
        if not emails:
            return {}
        unique_emails = list(set(emails))
        result: dict[str, str] = {}
        for i in range(0, len(unique_emails), 50):
            chunk = unique_emails[i:i + 50]
            response = httpx.get(
                self._url("entities"),
                headers=self._base_headers,
                params={
                    "tenant_id": f"eq.{tenant_id}",
                    "primary_email": 'in.("' + '","'.join(chunk) + '")',
                    "select": "primary_email,entity_id",
                },
                timeout=30.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase entities bulk fetch failed ({response.status_code}): {response.text}"
                )
            result.update({row["primary_email"]: str(row["entity_id"]) for row in response.json()})
        return result

    def upsert_entities(self, resolved_entities: list[EntityCandidate]) -> EntityPersistResult:
        if not resolved_entities:
            return EntityPersistResult(0, 0, 0, 0)

        from collections import defaultdict
        by_tenant: dict[str, list[EntityCandidate]] = defaultdict(list)
        for c in resolved_entities:
            by_tenant[c.tenant_id].append(c)

        total_created = total_updated = total_identities = 0

        for tenant_id, candidates in by_tenant.items():
            # ── 1. Bulk-fetch existing entities by email (1 GET) ───────────────
            # We still need entity_id values to write entity_identities rows.
            emails = [c.primary_email for c in candidates if c.primary_email]
            known_by_email = self._bulk_fetch_entities_by_email(tenant_id, emails)

            # Split into new vs existing
            new_candidates = [c for c in candidates if not known_by_email.get(c.primary_email)]
            existing_candidates = [c for c in candidates if known_by_email.get(c.primary_email)]

            entity_id_map: dict[str, str] = {}
            for c in existing_candidates:
                entity_id_map[c.source_event_id] = known_by_email[c.primary_email]

            # ── 2. Bulk-insert new entities (1 POST, return=representation) ────
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
                    raise RuntimeError(
                        f"Supabase entities bulk insert failed ({ins_resp.status_code}): {ins_resp.text}"
                    )
                returned = {row.get("primary_email"): str(row["entity_id"]) for row in ins_resp.json()}
                for c in new_candidates:
                    eid = returned.get(c.primary_email)
                    if eid:
                        entity_id_map[c.source_event_id] = eid
                        total_created += 1

            # ── 3. Bulk-update existing entities (1 POST upsert on entity_id) ───
            # Deduplicate by entity_id: multiple candidates can map to the same entity
            # (e.g. same contact in Gmail + calendar). Postgres raises PG21000 if
            # entity_id appears twice in one ON CONFLICT DO UPDATE batch.
            if existing_candidates:
                deduped_entities: dict[str, dict] = {}
                for c in existing_candidates:
                    eid = entity_id_map.get(c.source_event_id)
                    if not eid:
                        continue
                    deduped_entities[eid] = {
                        "entity_id": eid,
                        "tenant_id": c.tenant_id,
                        "display_name": c.display_name,
                        "primary_email": c.primary_email,
                        "company_norm": c.company_norm,
                        "title_norm": c.title_norm,
                        "metadata_json": c.metadata_json,
                    }
                update_rows = list(deduped_entities.values())

                if update_rows:
                    upd_resp = httpx.post(
                        self._url("entities"),
                        headers=self._upsert_headers,
                        params={"on_conflict": "entity_id"},
                        json=update_rows,
                        timeout=30.0,
                    )
                    if upd_resp.status_code >= 400:
                        raise RuntimeError(
                            f"Supabase entities bulk update failed ({upd_resp.status_code}): {upd_resp.text}"
                        )
                    total_updated += len(update_rows)

            # ── 4. Native upsert all entity_identities (1 POST) ───────────────
            # on_conflict=(tenant_id,source,source_identity) → merge-duplicates
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
                    headers=self._upsert_headers,
                    params={"on_conflict": "tenant_id,source,source_identity"},
                    json=identity_rows,
                    timeout=30.0,
                )
                if id_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase entity_identities upsert failed ({id_resp.status_code}): {id_resp.text}"
                    )
                total_identities += len(identity_rows)

        return EntityPersistResult(len(resolved_entities), total_created, total_updated, total_identities)

    def upsert_metadata(self, metadata_candidates: list[MetadataCandidate]) -> int:
        if not metadata_candidates:
            return 0

        from collections import defaultdict
        # Group by tenant (almost always one, but be safe)
        by_tenant: dict[str, list[MetadataCandidate]] = defaultdict(list)
        for item in metadata_candidates:
            if item.primary_email:
                by_tenant[item.tenant_id].append(item)

        updated = 0

        for tenant_id, items in by_tenant.items():
            emails = [item.primary_email for item in items]

            # ── 1. Bulk-fetch all matching entities in batched GETs ────────────
            unique_emails = list(set(emails))
            existing_by_email: dict[str, dict] = {}
            for i in range(0, len(unique_emails), 50):
                chunk = unique_emails[i:i + 50]
                existing_resp = httpx.get(
                    self._url("entities"),
                    headers=self._base_headers,
                    params={
                        "tenant_id": f"eq.{tenant_id}",
                        "primary_email": 'in.("' + '","'.join(chunk) + '")',
                        "select": "entity_id,primary_email,metadata_json",
                    },
                    timeout=30.0,
                )
                if existing_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase entities metadata bulk fetch failed ({existing_resp.status_code}): {existing_resp.text}"
                    )
                existing_by_email.update({row["primary_email"]: row for row in existing_resp.json()})

            # ── 2. Bulk-update matched entities (1 POST upsert on entity_id) ────
            # Deduplicate by entity_id: same entity may appear from multiple sources
            # (contacts + gmail + calendar). Postgres raises PG21000 if the same
            # entity_id appears twice in one ON CONFLICT DO UPDATE batch.
            deduped: dict[str, dict] = {}
            for item in items:
                existing = existing_by_email.get(item.primary_email)
                if not existing:
                    continue
                eid = str(existing["entity_id"])
                if eid not in deduped:
                    # Seed with the server-side metadata already on the row
                    deduped[eid] = {
                        "entity_id": eid,
                        "tenant_id": tenant_id,
                        "display_name": item.display_name,
                        "company_norm": item.metadata_json.get("company_norm"),
                        "title_norm": item.metadata_json.get("title_norm"),
                        "metadata_json": dict(existing.get("metadata_json") or {}),
                    }
                # Layer on the new metadata (last write wins per key)
                deduped[eid]["metadata_json"].update(item.metadata_json)
                deduped[eid]["company_norm"] = item.metadata_json.get("company_norm") or deduped[eid]["company_norm"]
                deduped[eid]["title_norm"] = item.metadata_json.get("title_norm") or deduped[eid]["title_norm"]
                deduped[eid]["display_name"] = item.display_name or deduped[eid]["display_name"]

            update_rows = list(deduped.values())

            if update_rows:
                bulk_resp = httpx.post(
                    self._url("entities"),
                    headers=self._upsert_headers,
                    params={"on_conflict": "entity_id"},
                    json=update_rows,
                    timeout=30.0,
                )
                if bulk_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase entities metadata bulk update failed ({bulk_resp.status_code}): {bulk_resp.text}"
                    )
                updated += len(update_rows)

        return updated


def create_entity_store(settings: Settings) -> EntityStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseEntityStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteEntityStore(db_path=settings.pipeline_db_path)
