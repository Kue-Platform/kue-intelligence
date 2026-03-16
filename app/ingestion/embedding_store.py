from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import httpx

from app.core.config import Settings
from app.ingestion.db import get_connection
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
    def persist_vectors(
        self, records: list[EmbeddingVectorRecord]
    ) -> EmbeddingPersistResult:
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
            # Ensure the column exists for vector persistence.
            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(search_documents)"
                ).fetchall()
            }
            if "embedding_json" not in columns:
                conn.execute(
                    "ALTER TABLE search_documents ADD COLUMN embedding_json TEXT"
                )
            conn.commit()

    def persist_vectors(
        self, records: list[EmbeddingVectorRecord]
    ) -> EmbeddingPersistResult:
        if not records:
            return EmbeddingPersistResult(0, 0)
        persisted = 0
        skipped = 0
        with self._lock:
            with get_connection(self._db_path) as conn:
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
                    f"Supabase entity bulk lookup for embeddings failed ({response.status_code}): {response.text}"
                )
            result.update(
                {row["primary_email"]: str(row["entity_id"]) for row in response.json()}
            )
        return result

    def persist_vectors(
        self, records: list[EmbeddingVectorRecord]
    ) -> EmbeddingPersistResult:
        if not records:
            return EmbeddingPersistResult(0, 0)

        from collections import defaultdict

        by_tenant: dict[str, list[EmbeddingVectorRecord]] = defaultdict(list)
        for item in records:
            by_tenant[item.tenant_id].append(item)

        persisted = 0
        skipped = 0

        for tenant_id, items in by_tenant.items():
            valid = [i for i in items if i.primary_email]
            skipped += len(items) - len(valid)
            if not valid:
                continue

            emails = list({i.primary_email for i in valid})
            email_to_id = self._bulk_resolve_entity_ids(tenant_id, emails)

            # Deduplicate on (tenant_id, entity_id, doc_type, content) — the same
            # composite key used by ON CONFLICT. Postgres raises PG21000 if the
            # same row appears twice in one batch (e.g. Gmail + Calendar both emit
            # the same semantic document for a shared participant).
            deduped: dict[tuple, dict] = {}
            for item in valid:
                entity_id = email_to_id.get(item.primary_email)
                if not entity_id:
                    skipped += 1
                    continue
                key = (item.tenant_id, entity_id, item.doc_type, item.content)
                # pgvector expects a literal string like '[0.1,0.2,...]'
                vector_literal = (
                    "[" + ",".join(f"{v:.6f}" for v in item.embedding) + "]"
                )
                deduped[key] = {
                    "tenant_id": item.tenant_id,
                    "entity_id": entity_id,
                    "doc_type": item.doc_type,
                    "content": item.content,
                    "embedding": vector_literal,
                }

            update_rows = list(deduped.values())

            if update_rows:
                bulk_resp = httpx.post(
                    self._url("search_documents"),
                    headers={
                        **self._base_headers,
                        "Prefer": "resolution=merge-duplicates,return=minimal",
                    },
                    params={"on_conflict": "tenant_id,entity_id,doc_type,content"},
                    json=update_rows,
                    timeout=30.0,
                )
                if bulk_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Supabase embedding bulk persist failed ({bulk_resp.status_code}): {bulk_resp.text}"
                    )
                persisted += len(update_rows)

        return EmbeddingPersistResult(persisted, skipped)


def create_embedding_store(settings: Settings) -> EmbeddingStore:
    return SqliteEmbeddingStore(db_path=settings.pipeline_db_path)
