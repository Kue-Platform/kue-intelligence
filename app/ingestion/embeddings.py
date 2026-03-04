from __future__ import annotations

import hashlib
from dataclasses import dataclass
from threading import Lock
from typing import Any, Literal

from pydantic import BaseModel


EMBEDDING_DIMS = 1536

_CACHE_LOCK = Lock()
_EMBEDDING_CACHE: dict[str, list[float]] = {}


class EmbeddingRequest(BaseModel):
    tenant_id: str
    primary_email: str | None = None
    doc_type: str
    content: str


class EmbeddingVectorRecord(BaseModel):
    tenant_id: str
    primary_email: str | None = None
    doc_type: str
    content: str
    embedding: list[float]


class EmbeddingLookupResult(BaseModel):
    stage: Literal["embedding"] = "embedding"
    candidate_count: int
    cache_hit_count: int
    cache_miss_count: int
    hits: list[EmbeddingVectorRecord]
    misses: list[EmbeddingRequest]


class EmbeddingGenerationResult(BaseModel):
    stage: Literal["embedding"] = "embedding"
    generated_count: int
    generated: list[EmbeddingVectorRecord]


def _text_key(doc_type: str, content: str) -> str:
    return f"{doc_type}::{content}".strip()


def _deterministic_embedding(text: str, dims: int = EMBEDDING_DIMS) -> list[float]:
    # Deterministic pseudo-embedding for MVP wiring; replace with model call later.
    vector: list[float] = []
    seed = text.encode("utf-8")
    counter = 0
    while len(vector) < dims:
        digest = hashlib.sha256(seed + str(counter).encode("ascii")).digest()
        counter += 1
        for i in range(0, len(digest), 2):
            if len(vector) >= dims:
                break
            chunk = int.from_bytes(digest[i : i + 2], "big", signed=False)
            value = (chunk / 65535.0) * 2.0 - 1.0
            vector.append(round(value, 6))
    return vector


def build_embedding_requests(semantic_payload: dict[str, Any]) -> list[EmbeddingRequest]:
    docs = list(semantic_payload.get("documents", []))
    output: list[EmbeddingRequest] = []
    for doc in docs:
        content = str(doc.get("content") or "").strip()
        if not content:
            continue
        output.append(
            EmbeddingRequest(
                tenant_id=str(doc.get("tenant_id")),
                primary_email=str(doc.get("primary_email")).lower()
                if doc.get("primary_email")
                else None,
                doc_type=str(doc.get("doc_type")),
                content=content,
            )
        )
    return output


def embedding_cache_lookup(requests: list[EmbeddingRequest]) -> EmbeddingLookupResult:
    hits: list[EmbeddingVectorRecord] = []
    misses: list[EmbeddingRequest] = []
    with _CACHE_LOCK:
        for req in requests:
            key = _text_key(req.doc_type, req.content)
            embedding = _EMBEDDING_CACHE.get(key)
            if embedding is None:
                misses.append(req)
            else:
                hits.append(
                    EmbeddingVectorRecord(
                        tenant_id=req.tenant_id,
                        primary_email=req.primary_email,
                        doc_type=req.doc_type,
                        content=req.content,
                        embedding=embedding,
                    )
                )
    return EmbeddingLookupResult(
        candidate_count=len(requests),
        cache_hit_count=len(hits),
        cache_miss_count=len(misses),
        hits=hits,
        misses=misses,
    )


def generate_embeddings(misses: list[EmbeddingRequest]) -> EmbeddingGenerationResult:
    generated: list[EmbeddingVectorRecord] = []
    for req in misses:
        key = _text_key(req.doc_type, req.content)
        embedding = _deterministic_embedding(key)
        generated.append(
            EmbeddingVectorRecord(
                tenant_id=req.tenant_id,
                primary_email=req.primary_email,
                doc_type=req.doc_type,
                content=req.content,
                embedding=embedding,
            )
        )
    return EmbeddingGenerationResult(generated_count=len(generated), generated=generated)


def embedding_cache_store(records: list[EmbeddingVectorRecord]) -> int:
    with _CACHE_LOCK:
        for record in records:
            _EMBEDDING_CACHE[_text_key(record.doc_type, record.content)] = record.embedding
    return len(records)


@dataclass
class EmbeddingPersistPayload:
    total_records: int
    records: list[EmbeddingVectorRecord]
