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
    def apply_hybrid_signals(self, signals: list[HybridSignal]) -> SearchIndexPersistResult:
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
        with sqlite3.connect(self._db_path) as conn:
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

    def apply_hybrid_signals(self, signals: list[HybridSignal]) -> SearchIndexPersistResult:
        if not signals:
            return SearchIndexPersistResult(0, 0)
        applied = 0
        skipped = 0
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
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
            with sqlite3.connect(self._db_path) as conn:
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

    def _resolve_entity_id(self, tenant_id: str, email: str) -> str | None:
        response = httpx.get(
            self._url("entities"),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{tenant_id}",
                "primary_email": f"eq.{email}",
                "select": "entity_id",
                "limit": 1,
            },
            timeout=20.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase entity lookup for indexing failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            return None
        return str(rows[0]["entity_id"])

    def apply_hybrid_signals(self, signals: list[HybridSignal]) -> SearchIndexPersistResult:
        if not signals:
            return SearchIndexPersistResult(0, 0)
        applied = 0
        skipped = 0
        for signal in signals:
            if not signal.primary_email:
                skipped += 1
                continue
            entity_id = self._resolve_entity_id(signal.tenant_id, signal.primary_email)
            if not entity_id:
                skipped += 1
                continue
            read = httpx.get(
                self._url("search_documents"),
                headers=self._base_headers,
                params={
                    "tenant_id": f"eq.{signal.tenant_id}",
                    "entity_id": f"eq.{entity_id}",
                    "doc_type": f"eq.{signal.doc_type}",
                    "content": f"eq.{signal.content}",
                    "select": "id,metadata_json",
                    "limit": 1,
                },
                timeout=20.0,
            )
            if read.status_code >= 400:
                raise RuntimeError(
                    f"Supabase search_documents lookup failed ({read.status_code}): {read.text}"
                )
            rows = read.json()
            if not rows:
                skipped += 1
                continue
            row = rows[0]
            metadata = dict(row.get("metadata_json") or {})
            metadata["hybrid_keywords"] = signal.keywords
            metadata["embedding_ready"] = signal.embedding_ready
            metadata["indexed"] = True
            patch = httpx.patch(
                self._url("search_documents"),
                headers=self._base_headers,
                params={"id": f"eq.{row['id']}"},
                json={"metadata_json": metadata},
                timeout=20.0,
            )
            if patch.status_code >= 400:
                raise RuntimeError(
                    f"Supabase search_documents index patch failed ({patch.status_code}): {patch.text}"
                )
            applied += 1
        return SearchIndexPersistResult(applied, skipped)

    def health_check(self, tenant_id: str) -> dict[str, Any]:
        read = httpx.get(
            self._url("search_documents"),
            headers=self._base_headers,
            params={"tenant_id": f"eq.{tenant_id}", "select": "metadata_json"},
            timeout=20.0,
        )
        if read.status_code >= 400:
            raise RuntimeError(
                f"Supabase search_documents health read failed ({read.status_code}): {read.text}"
            )
        rows = read.json()
        total_docs = len(rows)
        indexed_docs = 0
        for row in rows:
            metadata = dict(row.get("metadata_json") or {})
            if metadata.get("indexed") is True:
                indexed_docs += 1
        return {
            "tenant_id": tenant_id,
            "total_docs": total_docs,
            "indexed_docs": indexed_docs,
            "index_ready": total_docs > 0 and indexed_docs > 0,
        }


def create_search_index_store(settings: Settings) -> SearchIndexStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseSearchIndexStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteSearchIndexStore(db_path=settings.pipeline_db_path)
