from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import httpx

from app.core.config import Settings
from app.ingestion.semantic_prep import SemanticDocument


@dataclass
class SearchDocumentPersistResult:
    candidate_count: int
    stored_count: int
    skipped_no_entity: int


class SearchDocumentStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist_documents(self, documents: list[SemanticDocument]) -> SearchDocumentPersistResult:
        raise NotImplementedError


class SqliteSearchDocumentStore(SearchDocumentStore):
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
                    primary_email TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_documents (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    source_updated_at TEXT,
                    indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

    def persist_documents(self, documents: list[SemanticDocument]) -> SearchDocumentPersistResult:
        if not documents:
            return SearchDocumentPersistResult(0, 0, 0)
        stored = 0
        skipped = 0
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                for doc in documents:
                    if not doc.primary_email:
                        skipped += 1
                        continue
                    row = conn.execute(
                        """
                        SELECT entity_id
                        FROM entities
                        WHERE tenant_id = ? AND primary_email = ?
                        LIMIT 1
                        """,
                        (doc.tenant_id, doc.primary_email),
                    ).fetchone()
                    if row is None:
                        skipped += 1
                        continue
                    entity_id = str(row[0])
                    conn.execute(
                        """
                        INSERT INTO search_documents (
                            id, tenant_id, entity_id, doc_type, content, metadata_json, source_updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid4()),
                            doc.tenant_id,
                            entity_id,
                            doc.doc_type,
                            doc.content,
                            json.dumps(doc.metadata_json, ensure_ascii=True),
                            doc.source_updated_at,
                        ),
                    )
                    stored += 1
                conn.commit()
        return SearchDocumentPersistResult(len(documents), stored, skipped)


class SupabaseSearchDocumentStore(SearchDocumentStore):
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
        return "supabase:search_documents"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    def _bulk_resolve_entity_ids(self, tenant_id: str, emails: list[str]) -> dict[str, str]:
        """Return {primary_email -> entity_id} for all given emails in one GET."""
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

    def persist_documents(self, documents: list[SemanticDocument]) -> SearchDocumentPersistResult:
        if not documents:
            return SearchDocumentPersistResult(0, 0, 0)

        from collections import defaultdict
        by_tenant: dict[str, list[SemanticDocument]] = defaultdict(list)
        for doc in documents:
            by_tenant[doc.tenant_id].append(doc)

        payload: list[dict[str, Any]] = []
        skipped = 0

        for tenant_id, docs in by_tenant.items():
            # ── 1. Bulk-resolve all entity IDs in one GET per tenant ───────────
            emails = [doc.primary_email for doc in docs if doc.primary_email]
            skipped += sum(1 for doc in docs if not doc.primary_email)
            if not emails:
                continue
            email_to_id = self._bulk_resolve_entity_ids(tenant_id, emails)

            # ── 2. Build insert payload ────────────────────────────────────────
            for doc in docs:
                if not doc.primary_email:
                    continue
                entity_id = email_to_id.get(doc.primary_email)
                if not entity_id:
                    skipped += 1
                    continue
                payload.append({
                    "tenant_id": doc.tenant_id,
                    "entity_id": entity_id,
                    "doc_type": doc.doc_type,
                    "content": doc.content,
                    "metadata_json": doc.metadata_json,
                    "source_updated_at": doc.source_updated_at,
                })

        # ── 3. Bulk insert/upsert all documents in one POST ────────────────────
        if payload:
            response = httpx.post(
                self._url("search_documents"),
                headers={**self._base_headers, "Prefer": "resolution=merge-duplicates,return=minimal"},
                params={"on_conflict": "tenant_id,entity_id,doc_type,content"},
                json=payload,
                timeout=30.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase search_documents upsert failed ({response.status_code}): {response.text}"
                )
        return SearchDocumentPersistResult(len(documents), len(payload), skipped)


def create_search_document_store(settings: Settings) -> SearchDocumentStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseSearchDocumentStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteSearchDocumentStore(db_path=settings.pipeline_db_path)
