from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any


# Weights mirror the NestJS ranking engine
_W_RETRIEVAL = 0.35
_W_GRAPH_STRENGTH = 0.25
_W_TEMPORAL = 0.15
_W_DEGREE_PENALTY = 0.15
_W_SHARED_CONTEXT = 0.10


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None


def _retrieval_score(result: dict[str, Any], query_tokens: list[str]) -> float:
    if not query_tokens:
        return 1.0
    name = (result.get("name") or "").lower()
    email = (result.get("primary_email") or "").lower()
    company = (result.get("company_name") or "").lower()
    text = f"{name} {email} {company}"
    hits = sum(1 for t in query_tokens if t in text)
    return min(hits / len(query_tokens), 1.0)


def _temporal_score(result: dict[str, Any]) -> float:
    last_interaction = _parse_dt(result.get("last_interaction_at"))
    interaction_count = int(result.get("interaction_count") or 0)
    if last_interaction is None:
        return 0.0
    age_days = max((datetime.now(UTC) - last_interaction).days, 0)
    recency = 0.7 * math.exp(-age_days / 90.0)
    frequency = 0.3 * min(math.log(interaction_count + 1) / math.log(201), 1.0)
    return recency + frequency


def _degree_penalty(result: dict[str, Any]) -> float:
    degree = int(result.get("degree") or 1)
    return max(0.0, 1.0 - 0.003 * max(0, degree - 50))


def _shared_context_score(result: dict[str, Any]) -> float:
    shared = int(result.get("shared_connections") or 0)
    return min(shared / 10.0, 1.0)


def rank_results(
    results: list[dict[str, Any]],
    query_tokens: list[str],
) -> list[dict[str, Any]]:
    for r in results:
        strength = float(r.get("strength") or 0.0)

        score = (
            _W_RETRIEVAL * _retrieval_score(r, query_tokens)
            + _W_GRAPH_STRENGTH * min(strength, 1.0)
            + _W_TEMPORAL * _temporal_score(r)
            + _W_DEGREE_PENALTY * _degree_penalty(r)
            + _W_SHARED_CONTEXT * _shared_context_score(r)
        )
        r["_score"] = round(score, 4)
        r["_explain"] = {
            "retrieval": round(_W_RETRIEVAL * _retrieval_score(r, query_tokens), 4),
            "graph_strength": round(_W_GRAPH_STRENGTH * min(strength, 1.0), 4),
            "temporal": round(_W_TEMPORAL * _temporal_score(r), 4),
            "degree_penalty": round(_W_DEGREE_PENALTY * _degree_penalty(r), 4),
            "shared_context": round(_W_SHARED_CONTEXT * _shared_context_score(r), 4),
        }

    return sorted(results, key=lambda x: x["_score"], reverse=True)
