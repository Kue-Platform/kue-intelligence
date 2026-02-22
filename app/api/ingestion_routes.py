from functools import lru_cache
import inspect
from typing import Awaitable, Callable
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
import inngest

from app.core.config import settings
from app.ingestion.canonical_store import CanonicalEventStore, create_canonical_event_store
from app.ingestion.connectors import trigger_mock_connector
from app.ingestion.google_connector import (
    GoogleConnectorError,
    resolve_google_state,
)
from app.ingestion.raw_store import RawEventStore
from app.ingestion.raw_store import create_raw_event_store
from app.inngest.runtime import inngest_client
from app.schemas import (
    GoogleOAuthMockCallbackRequest,
    GoogleOAuthCallbackResponse,
    IngestionMockTriggerRequest,
    IngestionMockTriggerResponse,
    Layer2ManualCaptureRequest,
    Layer3EventsResponse,
    PipelineRunResponse,
    RawEventsByTraceResponse,
)

router = APIRouter(prefix="/v1/ingestion", tags=["ingestion"])


@lru_cache(maxsize=1)
def _get_raw_event_store() -> RawEventStore:
    return create_raw_event_store(settings)


def get_raw_event_store() -> RawEventStore:
    return _get_raw_event_store()


@lru_cache(maxsize=1)
def _get_canonical_event_store() -> CanonicalEventStore:
    return create_canonical_event_store(settings)


def get_canonical_event_store() -> CanonicalEventStore:
    return _get_canonical_event_store()


async def _send_inngest_event(*, name: str, data: dict) -> str | None:
    send_fn = getattr(inngest_client, "send")
    payload = inngest.Event(name=name, data=data)
    result = send_fn([payload])
    if inspect.isawaitable(result):
        result = await result

    if isinstance(result, dict):
        ids = result.get("ids")
        if isinstance(ids, list) and ids:
            return str(ids[0])
        if "id" in result:
            return str(result["id"])
        return None

    result_id = getattr(result, "id", None)
    if result_id:
        return str(result_id)
    ids = getattr(result, "ids", None)
    if isinstance(ids, list) and ids:
        return str(ids[0])
    return None


InngestDispatcher = Callable[[str, dict], Awaitable[str | None]]


def get_inngest_dispatcher() -> InngestDispatcher:
    async def _dispatch(name: str, data: dict) -> str | None:
        return await _send_inngest_event(name=name, data=data)

    return _dispatch


@router.post("/mock", response_model=IngestionMockTriggerResponse)
def trigger_ingestion_mock(payload: IngestionMockTriggerRequest) -> IngestionMockTriggerResponse:
    try:
        result = trigger_mock_connector(
            source=payload.source,
            trigger_type=payload.trigger_type,
            payload=payload.payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return IngestionMockTriggerResponse(
        connector_event_id=result.connector_event_id,
        trace_id=result.trace_id,
        source=result.source,
        trigger_type=result.trigger_type,
        accepted_at=result.accepted_at,
        records_detected=result.records_detected,
        payload_shape=result.payload_shape,
    )


@router.get("/google/oauth/callback", response_model=GoogleOAuthCallbackResponse)
async def google_oauth_callback(
    code: str | None = None,
    state: str | None = None,
    tenant_id: str | None = None,
    user_id: str | None = None,
    error: str | None = None,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
) -> GoogleOAuthCallbackResponse:
    if error:
        raise HTTPException(status_code=400, detail=f"Google OAuth returned an error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing OAuth authorization code.")

    try:
        resolved_tenant_id, resolved_user_id = resolve_google_state(
            state=state,
            tenant_id=tenant_id,
            user_id=user_id,
        )
        trace_id = f"trace_{uuid4().hex}"
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        event_id = await dispatch_event(
            "kue/user.connected",
            {
                "trace_id": trace_id,
                "tenant_id": resolved_tenant_id,
                "user_id": resolved_user_id,
                "code": code,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return GoogleOAuthCallbackResponse(
        tenant_id=resolved_tenant_id,
        user_id=resolved_user_id,
        trace_id=trace_id,
        source_events=[],
        counts={},
        pipeline_event_name="kue/user.connected",
        pipeline_event_id=event_id,
    )


@router.post("/google/oauth/callback/mock", response_model=GoogleOAuthCallbackResponse)
async def google_oauth_callback_mock(
    request: GoogleOAuthMockCallbackRequest,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
) -> GoogleOAuthCallbackResponse:
    try:
        resolved_tenant_id, resolved_user_id = resolve_google_state(
            state=request.state,
            tenant_id=request.tenant_id,
            user_id=request.user_id,
        )
        trace_id = request.trace_id or f"trace_{uuid4().hex}"
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        event_id = await dispatch_event(
            "kue/user.mock_connected",
            {
                "trace_id": trace_id,
                "tenant_id": resolved_tenant_id,
                "user_id": resolved_user_id,
                "source_type": str(request.source_type),
                "payload": request.payload,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return GoogleOAuthCallbackResponse(
        tenant_id=resolved_tenant_id,
        user_id=resolved_user_id,
        trace_id=trace_id,
        source_events=[],
        counts={},
        pipeline_event_name="kue/user.mock_connected",
        pipeline_event_id=event_id,
    )


@router.get("/raw-events/{trace_id}", response_model=RawEventsByTraceResponse)
def get_raw_events_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
) -> RawEventsByTraceResponse:
    events = raw_store.list_by_trace_id(trace_id)
    return RawEventsByTraceResponse(
        trace_id=trace_id,
        total=len(events),
        events=events,
    )


@router.post("/layer2/capture", response_model=PipelineRunResponse)
async def run_layer2_capture(
    request: Layer2ManualCaptureRequest,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
) -> PipelineRunResponse:
    trace_id = request.source_events[0].trace_id if request.source_events else None
    if trace_id is None:
        raise HTTPException(status_code=400, detail="source_events must include trace_id")
    try:
        event_id = await dispatch_event(
            "pipeline/run.requested",
            {
                "trace_id": trace_id,
                "tenant_id": request.source_events[0].tenant_id,
                "user_id": request.source_events[0].user_id,
                "parser_version": settings.parser_version,
                "source_events": [event.model_dump(mode="json") for event in request.source_events],
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc
    return PipelineRunResponse(
        run_id=None,
        event_name="pipeline/run.requested",
        event_id=event_id,
        trace_id=trace_id,
    )


@router.post("/stage/canonicalization/replay/{trace_id}", response_model=PipelineRunResponse)
async def replay_stage_canonicalization(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
) -> PipelineRunResponse:
    events = raw_store.list_by_trace_id(trace_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")
    try:
        event_id = await dispatch_event(
            "pipeline/stage.canonicalization.replay.requested",
            {
                "trace_id": trace_id,
                "tenant_id": events[0].tenant_id,
                "user_id": events[0].user_id,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc
    return PipelineRunResponse(
        run_id=None,
        event_name="pipeline/stage.canonicalization.replay.requested",
        event_id=event_id,
        trace_id=trace_id,
    )


@router.post("/layer3/parse/{trace_id}", response_model=PipelineRunResponse)
async def parse_layer3_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
) -> PipelineRunResponse:
    events = raw_store.list_by_trace_id(trace_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")
    try:
        event_id = await dispatch_event(
            "pipeline/stage.canonicalization.replay.requested",
            {
                "trace_id": trace_id,
                "tenant_id": events[0].tenant_id,
                "user_id": events[0].user_id,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc
    return PipelineRunResponse(
        run_id=None,
        event_name="pipeline/stage.canonicalization.replay.requested",
        event_id=event_id,
        trace_id=trace_id,
    )


@router.get("/layer3/events/{trace_id}", response_model=Layer3EventsResponse)
def get_layer3_events_by_trace(
    trace_id: str,
    canonical_store: CanonicalEventStore = Depends(get_canonical_event_store),
) -> Layer3EventsResponse:
    events = canonical_store.list_by_trace_id(trace_id)
    return Layer3EventsResponse(
        trace_id=trace_id,
        total=len(events),
        events=events,
    )
