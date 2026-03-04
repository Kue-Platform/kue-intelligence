from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import httpx

from app.core.config import Settings
from app.ingestion.embeddings import EmbeddingVectorRecord


@dataclass
class EmbeddingPersistResult:
    persisted_count: int
    skipped_no_entity: int


class EmbeddingStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def persist_vectors(self, records: list[EmbeddingVectorRecord]) -> EmbeddingPersistResult:
        raise NotImplementedError


class SqliteEmbeddingStore(EmbeddingStore):
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
            # Non-breaking migration for existing sqlite table.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(search_documents)").fetchall()}
            if "embedding_json" not in columns:
                conn.execute("ALTER TABLE search_documents ADD COLUMN embedding_json TEXT")
            conn.commit()

    def persist_vectors(self, records: list[EmbeddingVectorRecord]) -> EmbeddingPersistResult:
        if not records:
            return EmbeddingPersistResult(0, 0)
        persisted = 0
        skipped = 0
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                for item in records:
                    if not item.primary_email:
                        skipped += 1
                        continue
                    entity_row = conn.execute(
                        """
                        SELECT entity_id
                        FROM entities
                        WHERE tenant_id = ? AND primary_email = ?
                        LIMIT 1
                        """,
                        (item.tenant_id, item.primary_email),
                    ).fetchone()
                    if entity_row is None:
                        skipped += 1
                        continue
                    entity_id = str(entity_row[0])
                    cursor = conn.execute(
                        """
                        UPDATE search_documents
                        SET embedding_json = ?
                        WHERE tenant_id = ? AND entity_id = ? AND doc_type = ? AND content = ?
                        """,
                        (
                            json.dumps(item.embedding, ensure_ascii=True),
                            item.tenant_id,
                            entity_id,
                            item.doc_type,
                            item.content,
                        ),
                    )
                    if cursor.rowcount > 0:
                        persisted += int(cursor.rowcount)
                conn.commit()
        return EmbeddingPersistResult(persisted, skipped)


class SupabaseEmbeddingStore(EmbeddingStore):
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
        return "supabase:embeddings"

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
                f"Supabase entity lookup for embeddings failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        if not rows:
            return None
        return str(rows[0]["entity_id"])

    def persist_vectors(self, records: list[EmbeddingVectorRecord]) -> EmbeddingPersistResult:
        if not records:
            return EmbeddingPersistResult(0, 0)
        persisted = 0
        skipped = 0
        for item in records:
            if not item.primary_email:
                skipped += 1
                continue
            entity_id = self._resolve_entity_id(item.tenant_id, item.primary_email)
            if not entity_id:
                skipped += 1
                continue
            vector_literal = "[" + ",".join(f"{v:.6f}" for v in item.embedding) + "]"
            response = httpx.patch(
                self._url("search_documents"),
                headers=self._base_headers,
                params={
                    "tenant_id": f"eq.{item.tenant_id}",
                    "entity_id": f"eq.{entity_id}",
                    "doc_type": f"eq.{item.doc_type}",
                    "content": f"eq.{item.content}",
                },
                json={"embedding": vector_literal},
                timeout=30.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase embedding persist failed ({response.status_code}): {response.text}"
                )
            persisted += 1
        return EmbeddingPersistResult(persisted, skipped)


def create_embedding_store(settings: Settings) -> EmbeddingStore:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        return SupabaseEmbeddingStore(supabase_url=settings.supabase_url, api_key=api_key)
    return SqliteEmbeddingStore(db_path=settings.pipeline_db_path)
