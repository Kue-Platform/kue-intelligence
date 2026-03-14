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
from app.ingestion.db import get_connection
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
        with get_connection(self._db_path) as conn:
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
            with get_connection(self._db_path) as conn:
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
                    f"Supabase entities bulk lookup failed ({response.status_code}): {response.text}"
                )
            result.update({row["primary_email"]: str(row["entity_id"]) for row in response.json()})
        return result

    def persist(
        self,
        interactions: list[InteractionCandidate],
        relationships: list[RelationshipAggregate],
    ) -> RelationshipPersistResult:
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

        from collections import defaultdict
        by_tenant: dict[str, list[RelationshipAggregate]] = defaultdict(list)
        for rel in relationships:
            by_tenant[rel.tenant_id].append(rel)

        total_upserted = 0

        for tenant_id, rels in by_tenant.items():
            all_emails = list({email for rel in rels for email in (rel.from_email, rel.to_email)})
            email_to_id = self._bulk_resolve_entity_ids(tenant_id, all_emails)

            # Build rows — deduplicate on (tenant_id, from, to, type) which is the
            # ON CONFLICT key. Same pair may appear from Gmail + Calendar interactions.
            deduped_rels: dict[tuple, dict] = {}
            for rel in rels:
                from_id = email_to_id.get(rel.from_email)
                to_id = email_to_id.get(rel.to_email)
                if not from_id or not to_id:
                    continue
                key = (rel.tenant_id, from_id, to_id, rel.relationship_type)
                deduped_rels[key] = {
                    "tenant_id": rel.tenant_id,
                    "from_entity_id": from_id,
                    "to_entity_id": to_id,
                    "relationship_type": rel.relationship_type,
                    "strength": rel.strength,
                    "first_interaction_at": rel.first_interaction_at,
                    "last_interaction_at": rel.last_interaction_at,
                    "interaction_count": rel.interaction_count,
                    "evidence_json": rel.evidence_json,
                }
            rows = list(deduped_rels.values())

            if not rows:
                continue

            # on_conflict=(tenant_id,from_entity_id,to_entity_id,relationship_type)
            upsert_resp = httpx.post(
                self._url("relationships"),
                headers={**self._base_headers, "Prefer": "resolution=merge-duplicates,return=minimal"},
                params={"on_conflict": "tenant_id,from_entity_id,to_entity_id,relationship_type"},
                json=rows,
                timeout=30.0,
            )
            if upsert_resp.status_code >= 400:
                raise RuntimeError(
                    f"Supabase relationships upsert failed ({upsert_resp.status_code}): {upsert_resp.text}"
                )
            total_upserted += len(rows)

        return RelationshipPersistResult(
            interaction_count=len(interactions),
            relationship_count=len(relationships),
            relationships_upserted=total_upserted,
        )


def create_relationship_store(settings: Settings) -> RelationshipStore:
    return SqliteRelationshipStore(db_path=settings.pipeline_db_path)
