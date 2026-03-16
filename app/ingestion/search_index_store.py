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
from app.ingestion.search_indexing import HybridSignal


@dataclass
class SearchIndexPersistResult:
    applied_count: int
    skipped_no_entity: int


class SearchIndexStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def apply_hybrid_signals(
        self, signals: list[HybridSignal]
    ) -> SearchIndexPersistResult:
        raise NotImplementedError

    @abstractmethod
    def health_check(self, tenant_id: str) -> dict[str, Any]:
        raise NotImplementedError


class SqliteSearchIndexStore(SearchIndexStore):
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
                CREATE TABLE IF NOT EXISTS entities (
                    entity_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
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
                    embedding_json TEXT
                )
                """
            )
            conn.commit()

    def apply_hybrid_signals(
        self, signals: list[HybridSignal]
    ) -> SearchIndexPersistResult:
        if not signals:
            return SearchIndexPersistResult(0, 0)
        applied = 0
        skipped = 0
        with self._lock:
            with get_connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                for signal in signals:
                    if not signal.primary_email:
                        skipped += 1
                        continue
                    ent = conn.execute(
                        """
                        SELECT entity_id
                        FROM entities
                        WHERE tenant_id = ? AND primary_email = ?
                        LIMIT 1
                        """,
                        (signal.tenant_id, signal.primary_email),
                    ).fetchone()
                    if ent is None:
                        skipped += 1
                        continue
                    entity_id = str(ent["entity_id"])
                    row = conn.execute(
                        """
                        SELECT id, metadata_json
                        FROM search_documents
                        WHERE tenant_id = ? AND entity_id = ? AND doc_type = ? AND content = ?
                        LIMIT 1
                        """,
                        (signal.tenant_id, entity_id, signal.doc_type, signal.content),
                    ).fetchone()
                    if row is None:
                        skipped += 1
                        continue
                    try:
                        metadata = json.loads(str(row["metadata_json"] or "{}"))
                    except json.JSONDecodeError:
                        metadata = {}
                    metadata["hybrid_keywords"] = signal.keywords
                    metadata["embedding_ready"] = signal.embedding_ready
                    metadata["indexed"] = True
                    conn.execute(
                        """
                        UPDATE search_documents
                        SET metadata_json = ?
                        WHERE id = ?
                        """,
                        (json.dumps(metadata, ensure_ascii=True), str(row["id"])),
                    )
                    applied += 1
                conn.commit()
        return SearchIndexPersistResult(applied, skipped)

    def health_check(self, tenant_id: str) -> dict[str, Any]:
        with self._lock:
            with get_connection(self._db_path) as conn:
                total_docs = conn.execute(
                    "SELECT count(*) FROM search_documents WHERE tenant_id = ?",
                    (tenant_id,),
                ).fetchone()[0]
                indexed_docs = 0
                rows = conn.execute(
                    "SELECT metadata_json FROM search_documents WHERE tenant_id = ?",
                    (tenant_id,),
                ).fetchall()
                for row in rows:
                    try:
                        metadata = json.loads(str(row[0] or "{}"))
                    except json.JSONDecodeError:
                        metadata = {}
                    if metadata.get("indexed") is True:
                        indexed_docs += 1
        return {
            "tenant_id": tenant_id,
            "total_docs": int(total_docs),
            "indexed_docs": int(indexed_docs),
            "index_ready": int(total_docs) > 0 and int(indexed_docs) > 0,
        }


class SupabaseSearchIndexStore(SearchIndexStore):
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
        return "supabase:search_index"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    def _bulk_resolve_entity_ids(
        self, tenant_id: str, emails: list[str]
    ) -> dict[str, str]:
        """Return {primary_email -> entity_id} for all given emails in one GET."""
        if not emails:
            return {}
        unique_emails = list(set(emails))
        result: dict[str, str] = {}
        for i in range(0, len(unique_emails), 50):
            chunk = unique_emails[i : i + 50]
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
                    f"Supabase entity bulk lookup for indexing failed ({response.status_code}): {response.text}"
                )
            result.update(
                {row["primary_email"]: str(row["entity_id"]) for row in response.json()}
            )
        return result

    def apply_hybrid_signals(
        self, signals: list[HybridSignal]
    ) -> SearchIndexPersistResult:
        if not signals:
            return SearchIndexPersistResult(0, 0)

        from collections import defaultdict

        by_tenant: dict[str, list[HybridSignal]] = defaultdict(list)
        for s in signals:
            by_tenant[s.tenant_id].append(s)

        applied = 0
        skipped = 0

        for tenant_id, tenant_signals in by_tenant.items():
            valid = [s for s in tenant_signals if s.primary_email]
            skipped += len(tenant_signals) - len(valid)
            if not valid:
                continue

            emails = list({s.primary_email for s in valid})
            email_to_id = self._bulk_resolve_entity_ids(tenant_id, emails)

            entity_ids = list(email_to_id.values())
            if not entity_ids:
                skipped += len(valid)
                continue

            doc_by_key: dict[tuple[str, str, str], dict] = {}
            for i in range(0, len(entity_ids), 50):
                chunk = entity_ids[i : i + 50]
                doc_resp = httpx.get(
                    self._url("search_documents"),
                    headers=self._base_headers,
                    params={
                        "tenant_id": f"eq.{tenant_id}",
                        "entity_id": 'in.("' + '","'.join(chunk) + '")',
                        "select": "id,entity_id,doc_type,content,metadata_json",
                    },
                    timeout=30.0,
                )
                if doc_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase search_documents bulk fetch failed ({doc_resp.status_code}): {doc_resp.text}"
                    )
                for row in doc_resp.json():
                    key = (
                        str(row["entity_id"]),
                        str(row["doc_type"]),
                        str(row["content"]),
                    )
                    doc_by_key[key] = row

            # Deduplicate by `id` — same doc can match multiple signals (same entity,
            # different source types). Postgres raises PG21000 if id appears twice.
            deduped_updates: dict[str, dict] = {}
            for signal in valid:
                entity_id = email_to_id.get(signal.primary_email)
                if not entity_id:
                    skipped += 1
                    continue
                key = (entity_id, signal.doc_type, signal.content)
                row = doc_by_key.get(key)
                if not row:
                    skipped += 1
                    continue
                doc_id = str(row["id"])
                existing = deduped_updates.get(doc_id)
                metadata = dict((existing or row).get("metadata_json") or {})
                metadata["hybrid_keywords"] = signal.keywords
                metadata["embedding_ready"] = signal.embedding_ready
                metadata["indexed"] = True
                deduped_updates[doc_id] = {
                    "id": doc_id,
                    "tenant_id": tenant_id,
                    "entity_id": entity_id,
                    "doc_type": signal.doc_type,
                    "content": signal.content,
                    "metadata_json": metadata,
                }
            update_rows = list(deduped_updates.values())

            if update_rows:
                bulk_patch = httpx.post(
                    self._url("search_documents"),
                    headers={
                        **self._base_headers,
                        "Prefer": "resolution=merge-duplicates,return=minimal",
                    },
                    params={"on_conflict": "id"},
                    json=update_rows,
                    timeout=30.0,
                )
                if bulk_patch.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase search_documents bulk index update failed ({bulk_patch.status_code}): {bulk_patch.text}"
                    )
                applied += len(update_rows)

        return SearchIndexPersistResult(applied, skipped)

    def health_check(self, tenant_id: str) -> dict[str, Any]:
        # Use Prefer:count=exact with limit=0 to get counts without fetching rows.
        # PostgREST returns total in the Content-Range header as "*/N".
        count_headers = {**self._base_headers, "Prefer": "count=exact"}

        # Total documents count
        total_resp = httpx.get(
            self._url("search_documents"),
            headers=count_headers,
            params={"tenant_id": f"eq.{tenant_id}", "select": "id", "limit": "0"},
            timeout=20.0,
        )
        if total_resp.status_code not in (200, 206):
            raise RuntimeError(
                f"Supabase search_documents health count failed ({total_resp.status_code}): {total_resp.text}"
            )
        content_range = total_resp.headers.get("Content-Range", "*/0")
        total_docs = int(content_range.split("/")[-1] or "0")

        # Indexed documents count (metadata_json->>'indexed' = 'true')
        indexed_resp = httpx.get(
            self._url("search_documents"),
            headers=count_headers,
            params={
                "tenant_id": f"eq.{tenant_id}",
                "metadata_json->>indexed": "eq.true",
                "select": "id",
                "limit": "0",
            },
            timeout=20.0,
        )
        if indexed_resp.status_code not in (200, 206):
            raise RuntimeError(
                f"Supabase search_documents indexed count failed ({indexed_resp.status_code}): {indexed_resp.text}"
            )
        indexed_range = indexed_resp.headers.get("Content-Range", "*/0")
        indexed_docs = int(indexed_range.split("/")[-1] or "0")

        return {
            "tenant_id": tenant_id,
            "total_docs": total_docs,
            "indexed_docs": indexed_docs,
            "index_ready": total_docs > 0 and indexed_docs > 0,
        }


def create_search_index_store(settings: Settings) -> SearchIndexStore:
    return SqliteSearchIndexStore(db_path=settings.pipeline_db_path)
