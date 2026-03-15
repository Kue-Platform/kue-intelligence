from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import httpx

from app.core.config import Settings
import sqlite3


class MergeCandidateStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def insert_candidates(self, candidates: list[dict[str, Any]]) -> int:
        raise NotImplementedError

    @abstractmethod
    def list_pending(self, tenant_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def resolve_candidate(self, candidate_id: str, status: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def log_merge(
        self,
        *,
        tenant_id: str,
        surviving_entity_id: str,
        merged_entity_id: str,
        match_signal: str,
        confidence: float,
        rollback_payload: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError


class SqliteMergeCandidateStore(MergeCandidateStore):
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
                CREATE TABLE IF NOT EXISTS merge_candidates (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    source_entity_id TEXT NOT NULL,
                    target_entity_id TEXT NOT NULL,
                    match_signals_json TEXT NOT NULL DEFAULT '{}',
                    confidence REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved_at TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(tenant_id, source_entity_id, target_entity_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_merge_log (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    surviving_entity_id TEXT NOT NULL,
                    merged_entity_id TEXT NOT NULL,
                    match_signal TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    merged_at TEXT NOT NULL,
                    rollback_payload_json TEXT
                )
                """
            )
            conn.commit()

    def insert_candidates(self, candidates: list[dict[str, Any]]) -> int:
        if not candidates:
            return 0
        inserted = 0
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                for c in candidates:
                    row_id = c.get("id") or str(uuid4())
                    created_at = c.get("created_at") or datetime.now(UTC).isoformat()
                    try:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO merge_candidates (
                                id, tenant_id, source_entity_id, target_entity_id,
                                match_signals_json, confidence, status, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                row_id,
                                c["tenant_id"],
                                c["source_entity_id"],
                                c["target_entity_id"],
                                json.dumps(c.get("match_signals", {}), ensure_ascii=True),
                                c["confidence"],
                                c.get("status", "pending"),
                                created_at,
                            ),
                        )
                        inserted += conn.execute("SELECT changes()").fetchone()[0]
                    except Exception:
                        # Skip rows that violate constraints
                        pass
                conn.commit()
        return inserted

    def list_pending(self, tenant_id: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, tenant_id, source_entity_id, target_entity_id,
                       match_signals_json, confidence, status, resolved_at, created_at
                FROM merge_candidates
                WHERE tenant_id = ? AND status = 'pending'
                ORDER BY confidence DESC
                """,
                (tenant_id,),
            ).fetchall()
        return [
            {
                "id": row[0],
                "tenant_id": row[1],
                "source_entity_id": row[2],
                "target_entity_id": row[3],
                "match_signals": json.loads(row[4] or "{}"),
                "confidence": row[5],
                "status": row[6],
                "resolved_at": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]

    def resolve_candidate(self, candidate_id: str, status: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    UPDATE merge_candidates
                    SET status = ?, resolved_at = ?
                    WHERE id = ?
                    """,
                    (status, now, candidate_id),
                )
                conn.commit()

    def log_merge(
        self,
        *,
        tenant_id: str,
        surviving_entity_id: str,
        merged_entity_id: str,
        match_signal: str,
        confidence: float,
        rollback_payload: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO entity_merge_log (
                        id, tenant_id, surviving_entity_id, merged_entity_id,
                        match_signal, confidence, merged_at, rollback_payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid4()),
                        tenant_id,
                        surviving_entity_id,
                        merged_entity_id,
                        match_signal,
                        confidence,
                        now,
                        json.dumps(rollback_payload, ensure_ascii=True) if rollback_payload else None,
                    ),
                )
                conn.commit()


class SupabaseMergeCandidateStore(MergeCandidateStore):
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
        return "supabase:merge_candidates"

    def _url(self, table: str) -> str:
        return f"{self._supabase_url}/rest/v1/{table}"

    @property
    def _upsert_headers(self) -> dict[str, str]:
        return {**self._base_headers, "Prefer": "resolution=merge-duplicates,return=minimal"}

    def insert_candidates(self, candidates: list[dict[str, Any]]) -> int:
        if not candidates:
            return 0
        now = datetime.now(UTC).isoformat()
        rows = [
            {
                "id": c.get("id") or str(uuid4()),
                "tenant_id": c["tenant_id"],
                "source_entity_id": c["source_entity_id"],
                "target_entity_id": c["target_entity_id"],
                "match_signals_json": c.get("match_signals", {}),
                "confidence": c["confidence"],
                "status": c.get("status", "pending"),
                "created_at": c.get("created_at") or now,
            }
            for c in candidates
        ]
        response = httpx.post(
            self._url("merge_candidates"),
            headers=self._upsert_headers,
            params={"on_conflict": "tenant_id,source_entity_id,target_entity_id"},
            json=rows,
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase merge_candidates upsert failed ({response.status_code}): {response.text}"
            )
        return len(rows)

    def list_pending(self, tenant_id: str) -> list[dict[str, Any]]:
        response = httpx.get(
            self._url("merge_candidates"),
            headers=self._base_headers,
            params={
                "tenant_id": f"eq.{tenant_id}",
                "status": "eq.pending",
                "order": "confidence.desc",
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase merge_candidates list failed ({response.status_code}): {response.text}"
            )
        rows = response.json()
        return [
            {
                "id": row["id"],
                "tenant_id": row["tenant_id"],
                "source_entity_id": row["source_entity_id"],
                "target_entity_id": row["target_entity_id"],
                "match_signals": row.get("match_signals_json", {}),
                "confidence": row["confidence"],
                "status": row["status"],
                "resolved_at": row.get("resolved_at"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def resolve_candidate(self, candidate_id: str, status: str) -> None:
        now = datetime.now(UTC).isoformat()
        response = httpx.patch(
            self._url("merge_candidates"),
            headers=self._base_headers,
            params={"id": f"eq.{candidate_id}"},
            json={"status": status, "resolved_at": now},
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase merge_candidates resolve failed ({response.status_code}): {response.text}"
            )

    def log_merge(
        self,
        *,
        tenant_id: str,
        surviving_entity_id: str,
        merged_entity_id: str,
        match_signal: str,
        confidence: float,
        rollback_payload: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        response = httpx.post(
            self._url("entity_merge_log"),
            headers=self._base_headers,
            json={
                "id": str(uuid4()),
                "tenant_id": tenant_id,
                "surviving_entity_id": surviving_entity_id,
                "merged_entity_id": merged_entity_id,
                "match_signal": match_signal,
                "confidence": confidence,
                "merged_at": now,
                "rollback_payload_json": rollback_payload,
            },
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Supabase entity_merge_log insert failed ({response.status_code}): {response.text}"
            )


def create_merge_candidate_store(settings: Settings) -> MergeCandidateStore:
    return SqliteMergeCandidateStore(db_path=settings.pipeline_db_path)
