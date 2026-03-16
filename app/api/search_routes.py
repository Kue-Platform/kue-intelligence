from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.deps import get_current_user
from app.core.config import settings
from app.ingestion.graph_store import create_graph_store
from app.search.cypher_builder import build_cypher
from app.search.query_parser import parse_query
from app.search.ranking import rank_results

router = APIRouter(prefix="/v1/search", tags=["search"])


@lru_cache(maxsize=1)
def _get_graph_store():  # type: ignore[return]
    return create_graph_store(settings)


def _run_search(
    *,
    query: str,
    tenant_id: str,
    user_id: str,
    limit: int,
    explain: bool,
) -> dict[str, Any]:
    graph_store = _get_graph_store()
    if graph_store.store_name == "graph:disabled":
        raise HTTPException(
            status_code=503,
            detail="Neo4j is not configured. Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD.",
        )

    parsed = parse_query(query)
    cypher_query = build_cypher(
        parsed, tenant_id=tenant_id, user_id=user_id, limit=limit
    )

    try:
        raw_results = graph_store._run_query(cypher_query.cypher, **cypher_query.params)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Neo4j query failed: {exc}"
        ) from exc

    query_tokens = parsed.name_tokens or [t.lower() for t in query.split()]
    ranked = rank_results(list(raw_results), query_tokens)

    if not explain:
        for r in ranked:
            r.pop("_explain", None)

    return {
        "query": query,
        "intent": parsed.intent,
        "result_type": cypher_query.result_type,
        "total": len(ranked),
        "results": ranked,
    }


@router.get("")
def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(default=20, ge=1, le=100),
    explain: bool = Query(default=False),
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Search for people and companies in the graph."""
    return _run_search(
        query=q,
        tenant_id=current_user["tenant_id"],
        user_id=current_user["sub"],
        limit=limit,
        explain=explain,
    )


@router.get("/quick")
def search_quick(
    q: str = Query(..., description="Search query"),
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Quick search returning top 5 results without ranking explanation."""
    return _run_search(
        query=q,
        tenant_id=current_user["tenant_id"],
        user_id=current_user["sub"],
        limit=5,
        explain=False,
    )


@router.get("/suggestions")
def search_suggestions(
    q: str = Query(..., min_length=2, description="Prefix query for autocomplete"),
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Return name/email suggestions for autocomplete (top 8, no ranking details)."""
    graph_store = _get_graph_store()
    if graph_store.store_name == "graph:disabled":
        return {"query": q, "suggestions": []}

    tenant_id: str = current_user["tenant_id"]
    try:
        results = graph_store._run_query(
            """
            MATCH (p:Person {tenant_id: $tenant_id})
            WHERE toLower(p.name) STARTS WITH toLower($prefix)
               OR toLower(p.primary_email) STARTS WITH toLower($prefix)
            RETURN p.entity_id AS entity_id, p.name AS name, p.primary_email AS primary_email
            LIMIT 8
            """,
            tenant_id=tenant_id,
            prefix=q,
        )
    except Exception:
        return {"query": q, "suggestions": []}

    suggestions = [
        {
            "entity_id": r.get("entity_id"),
            "name": r.get("name"),
            "email": r.get("primary_email"),
        }
        for r in results
    ]
    return {"query": q, "suggestions": suggestions}
