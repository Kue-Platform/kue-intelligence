from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.deps import get_current_user
from app.core.config import settings
from app.ingestion.graph_store import create_graph_store
from app.network.traverse import intro_path, network_stats, second_degree_connections

router = APIRouter(prefix="/v1/network", tags=["network"])


@lru_cache(maxsize=1)
def _get_graph_store():  # type: ignore[return]
    return create_graph_store(settings)


def _require_neo4j():
    store = _get_graph_store()
    if store.store_name == "graph:disabled":
        raise HTTPException(
            status_code=503,
            detail="Neo4j is not configured. Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD.",
        )
    return store


@router.get("/second-degree/{entity_id}")
def get_second_degree(
    entity_id: str,
    limit: int = Query(default=25, ge=1, le=100),
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Return second-degree connections for a given Person entity_id."""
    graph_store = _require_neo4j()
    tenant_id: str = current_user["tenant_id"]

    try:
        results = second_degree_connections(
            graph_store,
            tenant_id=tenant_id,
            entity_id=entity_id,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Neo4j query failed: {exc}"
        ) from exc

    return {
        "entity_id": entity_id,
        "tenant_id": tenant_id,
        "total": len(results),
        "connections": results,
    }


@router.get("/intro-path")
def get_intro_path(
    from_entity_id: str = Query(..., description="Source person entity_id"),
    to_entity_id: str = Query(..., description="Target person entity_id"),
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Find the shortest introduction path between two Person nodes."""
    graph_store = _require_neo4j()
    tenant_id: str = current_user["tenant_id"]

    try:
        result = intro_path(
            graph_store,
            tenant_id=tenant_id,
            from_entity_id=from_entity_id,
            to_entity_id=to_entity_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Neo4j query failed: {exc}"
        ) from exc

    return {"tenant_id": tenant_id, **result}


@router.get("/stats")
def get_network_stats(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Return high-level graph statistics for the current tenant."""
    graph_store = _require_neo4j()
    tenant_id: str = current_user["tenant_id"]

    try:
        return network_stats(graph_store, tenant_id=tenant_id)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Neo4j query failed: {exc}"
        ) from exc
