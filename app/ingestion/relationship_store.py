from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import httpx

from app.core.config import Settings
from app.ingestion.relationship_extraction import InteractionCandidate, RelationshipAggregate


@dataclass
class RelationshipPersistResult:
    interaction_count: int
    relationship_count: int
    relationships_upserted: int


class RelationshipStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist(
        self,
        interactions: list[InteractionCandidate],
        relationships: list[RelationshipAggregate],
    ) -> RelationshipPersistResult:
        raise NotImplementedError


class SqliteRelationshipStore(RelationshipStore):
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
                CREATE TABLE IF NOT EXISTS interaction_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    actor_email TEXT,
                    target_email TEXT,
                    touchpoint_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    topic TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    from_email TEXT NOT NULL,
                    to_email TEXT NOT NULL,
                    relationship_type TEXT NOT NULL DEFAULT 'knows',
                    strength REAL NOT NULL DEFAULT 0.0,
                    first_interaction_at TEXT,
                    last_interaction_at TEXT,
                    interaction_count INTEGER NOT NULL DEFAULT 0,
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    UNIQUE (tenant_id, from_email, to_email, relationship_type)
                )
                """
            )
            conn.commit()

    def persist(
        self,
        interactions: list[InteractionCandidate],
        relationships: list[RelationshipAggregate],
    ) -> RelationshipPersistResult:
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                for item in interactions:
                    conn.execute(
                        """
                        INSERT INTO interaction_facts (
                            tenant_id, source, actor_email, target_email, touchpoint_type, occurred_at, topic, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.tenant_id,
                            item.source,
                            item.actor_email,
                            item.target_email,
                            item.touchpoint_type,
                            item.occurred_at,
                            item.topic,
                            json.dumps(item.payload_json, ensure_ascii=True),
                        ),
                    )

                for rel in relationships:
                    conn.execute(
                        """
                        INSERT INTO relationships (
                            tenant_id, from_email, to_email, relationship_type, strength, first_interaction_at,
                            last_interaction_at, interaction_count, evidence_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(tenant_id, from_email, to_email, relationship_type)
                        DO UPDATE SET
                            strength=excluded.strength,
                            first_interaction_at=excluded.first_interaction_at,
                            last_interaction_at=excluded.last_interaction_at,
                            interaction_count=excluded.interaction_count,
                            evidence_json=excluded.evidence_json
                        """,
                        (
                            rel.tenant_id,
                            rel.from_email,
                            rel.to_email,
                            rel.relationship_type,
                            rel.strength,
                            rel.first_interaction_at,
                            rel.last_interaction_at,
                            rel.interaction_count,
                            json.dumps(rel.evidence_json, ensure_ascii=True),
                        ),
                    )
                conn.commit()

        return RelationshipPersistResult(
            interaction_count=len(interactions),
            relationship_count=len(relationships),
            relationships_upserted=len(relationships),
        )


class SupabaseRelationshipStore(RelationshipStore):
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
        return "supabase:relationships"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    @staticmethod
    def _is_conflict(response: httpx.Response) -> bool:
        body = (response.text or "").lower()
        return response.status_code == 409 or "23505" in body

    def _bulk_resolve_entity_ids(self, tenant_id: str, emails: list[str]) -> dict[str, str]:
        """Return {email -> entity_id} for all given emails in one GET."""
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
                f"Supabase entities bulk lookup failed ({response.status_code}): {response.text}"
            )
        return {row["primary_email"]: str(row["entity_id"]) for row in response.json()}

    def persist(
        self,
        interactions: list[InteractionCandidate],
        relationships: list[RelationshipAggregate],
    ) -> RelationshipPersistResult:
        # ── Insert interaction_facts (already bulk - no change needed) ─────────
        if interactions:
            payload = [
                {
                    "tenant_id": item.tenant_id,
                    "source": item.source,
                    "touchpoint_type": item.touchpoint_type,
                    "occurred_at": item.occurred_at,
                    "topic": item.topic,
                    "payload_json": item.payload_json,
                }
                for item in interactions
            ]
            response = httpx.post(
                self._url("interaction_facts"),
                headers=self._base_headers,
                json=payload,
                timeout=30.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase interaction_facts insert failed ({response.status_code}): {response.text}"
                )

        if not relationships:
            return RelationshipPersistResult(
                interaction_count=len(interactions),
                relationship_count=0,
                relationships_upserted=0,
            )

        # ── 1. Bulk-resolve all entity IDs in one GET per tenant ──────────────
        from collections import defaultdict
        by_tenant: dict[str, list[RelationshipAggregate]] = defaultdict(list)
        for rel in relationships:
            by_tenant[rel.tenant_id].append(rel)

        total_upserted = 0

        for tenant_id, rels in by_tenant.items():
            # Collect unique emails
            all_emails = list({email for rel in rels for email in (rel.from_email, rel.to_email)})
            email_to_id = self._bulk_resolve_entity_ids(tenant_id, all_emails)

            # Build resolved payloads (skip pairs where either entity is unknown)
            resolved: list[tuple[RelationshipAggregate, str, str]] = []
            for rel in rels:
                from_id = email_to_id.get(rel.from_email)
                to_id = email_to_id.get(rel.to_email)
                if from_id and to_id:
                    resolved.append((rel, from_id, to_id))

            if not resolved:
                continue

            # ── 2. Bulk-fetch existing relationships in one GET ────────────────
            # Build composite key set for lookup
            existing_keys: set[tuple[str, str, str]] = set()
            # PostgREST doesn't support multi-column in() natively, so fetch all
            # for this tenant and filter locally.
            ex_resp = httpx.get(
                self._url("relationships"),
                headers=self._base_headers,
                params={
                    "tenant_id": f"eq.{tenant_id}",
                    "select": "from_entity_id,to_entity_id,relationship_type",
                },
                timeout=30.0,
            )
            if ex_resp.status_code >= 400:
                raise RuntimeError(
                    f"Supabase relationships fetch failed ({ex_resp.status_code}): {ex_resp.text}"
                )
            for row in ex_resp.json():
                existing_keys.add((
                    str(row["from_entity_id"]),
                    str(row["to_entity_id"]),
                    str(row["relationship_type"]),
                ))

            # ── 3. Split into new vs existing ─────────────────────────────────
            to_insert: list[dict] = []
            to_update: list[tuple[RelationshipAggregate, str, str]] = []

            for rel, from_id, to_id in resolved:
                key = (from_id, to_id, rel.relationship_type)
                if key in existing_keys:
                    to_update.append((rel, from_id, to_id))
                else:
                    to_insert.append({
                        "tenant_id": rel.tenant_id,
                        "from_entity_id": from_id,
                        "to_entity_id": to_id,
                        "relationship_type": rel.relationship_type,
                        "strength": rel.strength,
                        "first_interaction_at": rel.first_interaction_at,
                        "last_interaction_at": rel.last_interaction_at,
                        "interaction_count": rel.interaction_count,
                        "evidence_json": rel.evidence_json,
                    })

            # ── 4. Bulk-insert new relationships ──────────────────────────────
            if to_insert:
                ins_resp = httpx.post(
                    self._url("relationships"),
                    headers=self._base_headers,
                    json=to_insert,
                    timeout=30.0,
                )
                if ins_resp.status_code >= 400:
                    if not self._is_conflict(ins_resp):
                        raise RuntimeError(
                            f"Supabase relationships bulk insert failed ({ins_resp.status_code}): {ins_resp.text}"
                        )
                    # Conflict: fall back to one-by-one with PATCH on conflict
                    for row in to_insert:
                        sr = httpx.post(
                            self._url("relationships"),
                            headers=self._base_headers,
                            json=[row],
                            timeout=20.0,
                        )
                        if sr.status_code >= 400:
                            if not self._is_conflict(sr):
                                raise RuntimeError(
                                    f"Supabase relationships insert failed ({sr.status_code}): {sr.text}"
                                )
                            # Race-condition: patch
                            httpx.patch(
                                self._url("relationships"),
                                headers=self._base_headers,
                                params={
                                    "tenant_id": f"eq.{row['tenant_id']}",
                                    "from_entity_id": f"eq.{row['from_entity_id']}",
                                    "to_entity_id": f"eq.{row['to_entity_id']}",
                                    "relationship_type": f"eq.{row['relationship_type']}",
                                },
                                json={
                                    "strength": row["strength"],
                                    "first_interaction_at": row["first_interaction_at"],
                                    "last_interaction_at": row["last_interaction_at"],
                                    "interaction_count": row["interaction_count"],
                                    "evidence_json": row["evidence_json"],
                                },
                                timeout=20.0,
                            )
                total_upserted += len(to_insert)

            # ── 5. Update existing relationships (per-row PATCH) ──────────────
            for rel, from_id, to_id in to_update:
                httpx.patch(
                    self._url("relationships"),
                    headers=self._base_headers,
                    params={
                        "tenant_id": f"eq.{rel.tenant_id}",
                        "from_entity_id": f"eq.{from_id}",
                        "to_entity_id": f"eq.{to_id}",
                        "relationship_type": f"eq.{rel.relationship_type}",
                    },
                    json={
                        "strength": rel.strength,
                        "first_interaction_at": rel.first_interaction_at,
                        "last_interaction_at": rel.last_interaction_at,
                        "interaction_count": rel.interaction_count,
                        "evidence_json": rel.evidence_json,
                    },
                    timeout=20.0,
                )
                total_upserted += 1

        return RelationshipPersistResult(
            interaction_count=len(interactions),
            relationship_count=len(relationships),
            relationships_upserted=total_upserted,
        )


def create_relationship_store(settings: Settings) -> RelationshipStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseRelationshipStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteRelationshipStore(db_path=settings.pipeline_db_path)


