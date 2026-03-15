from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import lru_cache
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import settings
from app.ingestion.graph_store import GraphStore, create_graph_store
from app.ingestion.warm_path import WarmPathCache, WarmPathService, create_warm_path_cache
from app.ingestion.warm_path_event_store import (
    WarmPathEvent,
    create_warm_path_event_store,
)
from app.schemas import (
    WarmPath,
    WarmPathEdge,
    WarmPathEventRequest,
    WarmPathEventResponse,
    WarmPathNode,
    WarmPathOriginResponse,
    WarmPathResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/warm-path", tags=["warm-path"])


@lru_cache(maxsize=1)
def _get_graph_store() -> GraphStore:
    return create_graph_store(settings)


@lru_cache(maxsize=1)
def _get_cache() -> WarmPathCache:
    return create_warm_path_cache(settings)


@lru_cache(maxsize=1)
def _get_event_store():
    return create_warm_path_event_store(settings)


def get_graph_store() -> GraphStore:
    return _get_graph_store()


def _build_service(store: GraphStore = Depends(get_graph_store)) -> WarmPathService:
    return WarmPathService(
        graph_store=store,
        _cache=_get_cache(),
        _large_network_threshold=settings.warm_path_large_network_threshold,
        _large_network_min_strength=settings.warm_path_large_network_min_strength,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_response(
    tenant_id: str,
    origin_entity_id: str,
    raw_paths: list[dict],
    query_mode: str,
    target_entity_id: str | None = None,
) -> WarmPathResponse:
    paths = [
        WarmPath(
            path_nodes=[WarmPathNode(**n) for n in p["path_nodes"]],
            path_edges=[WarmPathEdge(**e) for e in p["path_edges"]],
            path_score=p["path_score"],
            hop_count=p["hop_count"],
            is_direct=p.get("is_direct", p["hop_count"] == 1),
        )
        for p in raw_paths
    ]
    return WarmPathResponse(
        tenant_id=tenant_id,
        origin_entity_id=origin_entity_id,
        target_entity_id=target_entity_id,
        paths=paths,
        total_paths_found=len(paths),
        query_mode=query_mode,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{tenant_id}/to/{target_entity_id}", response_model=WarmPathResponse)
def find_warm_paths_to_target(
    tenant_id: str,
    target_entity_id: str,
    origin_entity_id: str = Query(..., description="The origin person's entity_id"),
    max_hops: int = Query(3, ge=1, le=3, description="Maximum path depth (1-3)"),
    max_paths: int = Query(5, ge=1, le=20, description="Maximum paths to return"),
    svc: WarmPathService = Depends(_build_service),
) -> WarmPathResponse:
    try:
        raw = svc.find_paths_to_target(
            tenant_id=tenant_id,
            origin_entity_id=origin_entity_id,
            target_entity_id=target_entity_id,
            max_hops=max_hops,
            max_paths=max_paths,
        )
    except Exception as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise HTTPException(
                status_code=504,
                detail="Warm path query timed out. Try reducing max_hops.",
            ) from exc
        raise
    return _to_response(tenant_id, origin_entity_id, raw, "targeted", target_entity_id)


@router.get("/{tenant_id}/discover", response_model=WarmPathResponse)
def discover_warmest_paths(
    tenant_id: str,
    origin_entity_id: str = Query(..., description="The origin person's entity_id"),
    max_hops: int = Query(3, ge=1, le=3, description="Maximum path depth (1-3)"),
    max_paths: int = Query(20, ge=1, le=50, description="Maximum paths to return"),
    min_score: float = Query(0.0, ge=0.0, le=100.0, description="Minimum path score (0-100)"),
    svc: WarmPathService = Depends(_build_service),
) -> WarmPathResponse:
    try:
        raw = svc.discover_warmest_paths(
            tenant_id=tenant_id,
            origin_entity_id=origin_entity_id,
            max_hops=max_hops,
            max_paths=max_paths,
            min_score=min_score,
        )
    except Exception as exc:
        if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
            raise HTTPException(
                status_code=504,
                detail="Warm path query timed out. Try reducing max_hops.",
            ) from exc
        raise
    return _to_response(tenant_id, origin_entity_id, raw, "discover")


@router.get("/{tenant_id}/resolve-origin", response_model=WarmPathOriginResponse)
def resolve_origin(
    tenant_id: str,
    email: str = Query(..., description="Email address to resolve to an entity_id"),
    svc: WarmPathService = Depends(_build_service),
) -> WarmPathOriginResponse:
    entity_id = svc.resolve_origin(tenant_id=tenant_id, email=email)
    return WarmPathOriginResponse(
        tenant_id=tenant_id,
        email=email,
        entity_id=entity_id,
        found=entity_id is not None,
    )


@router.post("/{tenant_id}/events", response_model=WarmPathEventResponse)
def log_warm_path_event(
    tenant_id: str,
    body: WarmPathEventRequest,
) -> WarmPathEventResponse:
    """Log a warm-path interaction event for the feedback loop.

    Used by the frontend to record path selections, intro requests, and
    outcomes so future ranking can be improved.
    """
    event_id = str(uuid4())
    recorded_at = datetime.now(UTC)

    logger.info(
        "warm_path_event tenant_id=%s event_type=%s event_id=%s "
        "origin=%s target=%s connectors=%s score=%s hops=%s meta=%s",
        tenant_id,
        body.event_type,
        event_id,
        body.origin_entity_id,
        body.target_entity_id,
        body.connector_entity_ids,
        body.selected_path_score,
        body.selected_path_hop_count,
        body.metadata,
    )

    event = WarmPathEvent(
        event_id=event_id,
        tenant_id=tenant_id,
        event_type=body.event_type,
        origin_entity_id=body.origin_entity_id,
        target_entity_id=body.target_entity_id,
        selected_path_score=body.selected_path_score,
        selected_path_hop_count=body.selected_path_hop_count,
        connector_entity_ids=body.connector_entity_ids,
        metadata_json=body.metadata,
        recorded_at=recorded_at.isoformat(),
    )

    store = _get_event_store()
    try:
        store.persist(event)
    except Exception:
        logger.exception("Failed to persist warm_path_event %s", event_id)

    return WarmPathEventResponse(
        tenant_id=tenant_id,
        event_id=event_id,
        event_type=body.event_type,
        recorded_at=recorded_at,
    )
