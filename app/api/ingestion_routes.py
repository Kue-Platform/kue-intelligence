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
from app.ingestion.pipeline_store import PipelineStore, create_pipeline_store
from app.ingestion.raw_store import RawEventStore
from app.ingestion.raw_store import create_raw_event_store
from app.inngest.runtime import inngest_client
from app.ingestion.parsers import parse_raw_events
from app.ingestion.enrichment import clean_and_enrich_events
from app.ingestion.entity_resolution import extract_entity_candidates, merge_entity_candidates
from app.ingestion.entity_store import EntityStore, create_entity_store
from app.ingestion.validators import validate_parsed_events
from app.schemas import (
    CanonicalEventType,
    CanonicalEvent,
    GoogleOAuthMockCallbackRequest,
    GoogleOAuthCallbackResponse,
    IngestionMockTriggerRequest,
    IngestionMockTriggerResponse,
    IngestionSource,
    Layer2ManualCaptureRequest,
    Layer3EventsResponse,
    Layer3ParseResponse,
    Layer4ValidationFailure,
    Layer4ValidationResponse,
    Layer5EnrichmentResponse,
    Layer6EntityResolutionResponse,
    CanonicalParseFailure,
    Layer3PersistenceSummary,
    PipelineRunStatusResponse,
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


@lru_cache(maxsize=1)
def _get_pipeline_store() -> PipelineStore:
    return create_pipeline_store(settings)


def get_pipeline_store() -> PipelineStore:
    return _get_pipeline_store()


@lru_cache(maxsize=1)
def _get_entity_store() -> EntityStore:
    return create_entity_store(settings)


def get_entity_store() -> EntityStore:
    return _get_entity_store()


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
                "source": str(request.source_events[0].source),
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
                "source": str(events[0].source),
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


@router.post("/layer3/parse/{trace_id}", response_model=Layer3ParseResponse)
def parse_layer3_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    canonical_store: CanonicalEventStore = Depends(get_canonical_event_store),
) -> Layer3ParseResponse:
    raw_events = raw_store.list_by_trace_id(trace_id)
    if not raw_events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")

    parse_result = parse_raw_events(raw_events)
    persist_result = canonical_store.persist_parsed_events(
        parse_result.parsed_events,
        settings.parser_version,
        run_id=raw_events[0].run_id,
    )

    parsed_events = [
        CanonicalEvent(
            raw_event_id=item.raw_event_id,
            run_id=raw_events[0].run_id,
            tenant_id=item.tenant_id,
            user_id=item.user_id,
            trace_id=item.trace_id,
            source=IngestionSource(str(item.source)),
            source_event_id=item.source_event_id,
            occurred_at=item.occurred_at,
            event_type=CanonicalEventType(str(item.event_type)),
            normalized=item.normalized,
            parse_warnings=item.parse_warnings,
        )
        for item in parse_result.parsed_events
    ]

    return Layer3ParseResponse(
        trace_id=trace_id,
        total_raw_events=len(raw_events),
        parsed_count=len(parse_result.parsed_events),
        failed_count=len(parse_result.failures),
        parsed_events=parsed_events,
        failures=[
            CanonicalParseFailure(
                raw_event_id=f.raw_event_id,
                source_event_id=f.source_event_id,
                reason=f.reason,
            )
            for f in parse_result.failures
        ],
        persistence=Layer3PersistenceSummary(
            stored_count=persist_result.stored_count,
            store=canonical_store.store_name,
            parser_version=settings.parser_version,
            parsed_at=persist_result.parsed_at,
        ),
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


@router.post("/layer4/validate/{trace_id}", response_model=Layer4ValidationResponse)
def validate_layer4_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
) -> Layer4ValidationResponse:
    raw_events = raw_store.list_by_trace_id(trace_id)
    if not raw_events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")

    parse_result = parse_raw_events(raw_events)
    validation_result = validate_parsed_events(
        {
            "parsed_events": [
                {
                    "raw_event_id": item.raw_event_id,
                    "tenant_id": item.tenant_id,
                    "user_id": item.user_id,
                    "trace_id": item.trace_id,
                    "source": str(item.source),
                    "source_event_id": item.source_event_id,
                    "occurred_at": item.occurred_at.isoformat(),
                    "event_type": str(item.event_type),
                    "normalized": item.normalized,
                    "parse_warnings": item.parse_warnings,
                }
                for item in parse_result.parsed_events
            ]
        }
    )
    return Layer4ValidationResponse(
        trace_id=trace_id,
        total_parsed_events=len(parse_result.parsed_events),
        valid_count=validation_result.valid_count,
        invalid_count=validation_result.invalid_count,
        invalid_events=[
            Layer4ValidationFailure(
                raw_event_id=item.raw_event_id,
                source_event_id=item.source_event_id,
                event_type=item.event_type,
                reason=item.reason,
            )
            for item in validation_result.invalid_events
        ],
    )


@router.post("/layer5/enrich/{trace_id}", response_model=Layer5EnrichmentResponse)
def enrich_layer5_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
) -> Layer5EnrichmentResponse:
    raw_events = raw_store.list_by_trace_id(trace_id)
    if not raw_events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")

    parse_result = parse_raw_events(raw_events)
    validation_result = validate_parsed_events(
        {
            "parsed_events": [
                {
                    "raw_event_id": item.raw_event_id,
                    "tenant_id": item.tenant_id,
                    "user_id": item.user_id,
                    "trace_id": item.trace_id,
                    "source": str(item.source),
                    "source_event_id": item.source_event_id,
                    "occurred_at": item.occurred_at.isoformat(),
                    "event_type": str(item.event_type),
                    "normalized": item.normalized,
                    "parse_warnings": item.parse_warnings,
                }
                for item in parse_result.parsed_events
            ]
        }
    )
    enrichment_result = clean_and_enrich_events(validation_result.model_dump(mode="json"))

    sample: list[CanonicalEvent] = []
    for item in enrichment_result.parsed_events[:5]:
        sample.append(
            CanonicalEvent(
                raw_event_id=int(item["raw_event_id"]),
                run_id=raw_events[0].run_id,
                tenant_id=str(item["tenant_id"]),
                user_id=str(item["user_id"]),
                trace_id=str(item["trace_id"]),
                source=IngestionSource(str(item["source"])),
                source_event_id=str(item["source_event_id"]),
                occurred_at=item["occurred_at"],
                event_type=CanonicalEventType(str(item["event_type"])),
                normalized=dict(item["normalized"]),
                parse_warnings=list(item.get("parse_warnings", [])),
            )
        )

    return Layer5EnrichmentResponse(
        trace_id=trace_id,
        valid_count=validation_result.valid_count,
        enriched_count=enrichment_result.enriched_count,
        sample=sample,
    )


@router.post("/layer6/resolve/{trace_id}", response_model=Layer6EntityResolutionResponse)
def resolve_layer6_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    entity_store: EntityStore = Depends(get_entity_store),
) -> Layer6EntityResolutionResponse:
    raw_events = raw_store.list_by_trace_id(trace_id)
    if not raw_events:
        raise HTTPException(status_code=404, detail=f"No raw events found for trace_id={trace_id}")

    parse_result = parse_raw_events(raw_events)
    validation_result = validate_parsed_events(
        {
            "parsed_events": [
                {
                    "raw_event_id": item.raw_event_id,
                    "tenant_id": item.tenant_id,
                    "user_id": item.user_id,
                    "trace_id": item.trace_id,
                    "source": str(item.source),
                    "source_event_id": item.source_event_id,
                    "occurred_at": item.occurred_at.isoformat(),
                    "event_type": str(item.event_type),
                    "normalized": item.normalized,
                    "parse_warnings": item.parse_warnings,
                }
                for item in parse_result.parsed_events
            ]
        }
    )
    enrichment_result = clean_and_enrich_events(validation_result.model_dump(mode="json"))
    exact_match = extract_entity_candidates(enrichment_result.model_dump(mode="json"))
    merge_result = merge_entity_candidates(exact_match.model_dump(mode="json"))
    persist_result = entity_store.upsert_entities(merge_result.resolved_entities)

    return Layer6EntityResolutionResponse(
        trace_id=trace_id,
        candidate_count=exact_match.candidate_count,
        resolved_count=merge_result.resolved_count,
        created_entities=persist_result.created_entities,
        updated_entities=persist_result.updated_entities,
        identities_upserted=persist_result.identities_upserted,
        store=entity_store.store_name,
    )


@router.get("/pipeline/run/{trace_id}", response_model=PipelineRunStatusResponse)
def get_pipeline_run_by_trace(
    trace_id: str,
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
) -> PipelineRunStatusResponse:
    run = pipeline_store.get_run_by_trace_id(trace_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"No pipeline run found for trace_id={trace_id}")
    return PipelineRunStatusResponse(
        run_id=run.run_id,
        trace_id=run.trace_id,
        tenant_id=run.tenant_id,
        user_id=run.user_id,
        source=run.source,
        trigger_type=run.trigger_type,
        status=run.status,
        requested_at=run.requested_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )
