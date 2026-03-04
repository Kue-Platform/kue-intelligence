from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel


class HybridSignal(BaseModel):
    tenant_id: str
    primary_email: str | None = None
    doc_type: str
    content: str
    keywords: list[str]
    embedding_ready: bool


class HybridSignalResult(BaseModel):
    stage: Literal["search_indexing"] = "search_indexing"
    signal_count: int
    signals: list[HybridSignal]


def _keywords(text: str, limit: int = 12) -> list[str]:
    tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
    uniq: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if len(token) < 3:
            continue
        if token in seen:
            continue
        seen.add(token)
        uniq.append(token)
        if len(uniq) >= limit:
            break
    return uniq


def build_hybrid_signals(vectors_payload: dict[str, Any]) -> HybridSignalResult:
    records = list(vectors_payload.get("records", []))
    signals: list[HybridSignal] = []
    for item in records:
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        embedding = item.get("embedding")
        signals.append(
            HybridSignal(
                tenant_id=str(item.get("tenant_id")),
                primary_email=(
                    str(item.get("primary_email")).lower()
                    if item.get("primary_email")
                    else None
                ),
                doc_type=str(item.get("doc_type")),
                content=content,
                keywords=_keywords(content),
                embedding_ready=isinstance(embedding, list) and len(embedding) > 0,
            )
        )
    return HybridSignalResult(signal_count=len(signals), signals=signals)
