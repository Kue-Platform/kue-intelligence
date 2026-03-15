from functools import lru_cache
import io
import json
import inspect
from typing import Awaitable, Callable
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Header, Request
import inngest

from app.core.config import settings
from app.ingestion.canonical_store import CanonicalEventStore, create_canonical_event_store
from app.ingestion.connectors import trigger_mock_connector
from app.ingestion.csv_import import (
    normalize_csv_rows_to_source_events,
    normalize_csv_text_to_source_events,
)
from app.ingestion.linkedin_zip_import import extract_linkedin_zip_to_source_events
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
from app.ingestion.metadata_extraction import extract_metadata_candidates
from app.ingestion.admin_reset import reset_all_data
from app.ingestion.relationship_extraction import compute_relationship_strength, extract_interactions
from app.ingestion.relationship_store import RelationshipStore, create_relationship_store
from app.ingestion.semantic_prep import build_semantic_documents
from app.ingestion.search_document_store import SearchDocumentStore, create_search_document_store
from app.ingestion.embeddings import (
    build_embedding_requests,
    embedding_cache_lookup,
    embedding_cache_store,
    generate_embeddings,
)
from app.ingestion.embedding_store import EmbeddingStore, create_embedding_store
from app.ingestion.cache_registry import cache_registry
from app.ingestion.search_indexing import build_hybrid_signals
from app.ingestion.search_index_store import SearchIndexStore, create_search_index_store
from app.ingestion.source_connection_store import (
    SourceConnectionRecord,
    SourceConnectionStore,
    create_source_connection_store,
)
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
    Layer7RelationshipResponse,
    Layer8MetadataResponse,
    Layer9SemanticResponse,
    Layer10EmbeddingResponse,
    Layer11CacheResponse,
    Layer12IndexResponse,
    CanonicalParseFailure,
    Layer3PersistenceSummary,
    PipelineRunStatusResponse,
    PipelineRunResponse,
    RawEventsByTraceResponse,
    AdminResetResponse,
    CsvImportMockRequest,
    CsvImportResponse,
    CsvImportSourceHint,
    LinkedInZipImportResponse,
    LinkedInZipFileStats,
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


@lru_cache(maxsize=1)
def _get_relationship_store() -> RelationshipStore:
    return create_relationship_store(settings)


def get_relationship_store() -> RelationshipStore:
    return _get_relationship_store()


@lru_cache(maxsize=1)
def _get_search_document_store() -> SearchDocumentStore:
    return create_search_document_store(settings)


def get_search_document_store() -> SearchDocumentStore:
    return _get_search_document_store()


@lru_cache(maxsize=1)
def _get_embedding_store() -> EmbeddingStore:
    return create_embedding_store(settings)


def get_embedding_store() -> EmbeddingStore:
    return _get_embedding_store()


@lru_cache(maxsize=1)
def _get_search_index_store() -> SearchIndexStore:
    return create_search_index_store(settings)


def get_search_index_store() -> SearchIndexStore:
    return _get_search_index_store()


@lru_cache(maxsize=1)
def _get_source_connection_store() -> SourceConnectionStore:
    return create_source_connection_store(settings)


def get_source_connection_store() -> SourceConnectionStore:
    return _get_source_connection_store()


def _assert_admin_reset_token(token: str | None) -> None:
    if not settings.admin_reset_token:
        raise HTTPException(status_code=503, detail="ADMIN_RESET_TOKEN is not configured.")
    if token != settings.admin_reset_token:
        raise HTTPException(status_code=401, detail="Invalid admin reset token.")


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


@router.post("/import/csv/mock", response_model=CsvImportResponse)
async def import_csv_mock(
    request: CsvImportMockRequest,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
    raw_store: RawEventStore = Depends(get_raw_event_store),
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
    source_connection_store: SourceConnectionStore = Depends(get_source_connection_store),
) -> CsvImportResponse:
    trace_id = request.trace_id or f"trace_{uuid4().hex}"
    normalized = normalize_csv_rows_to_source_events(
        tenant_id=request.tenant_id,
        user_id=request.user_id,
        trace_id=trace_id,
        source_hint=request.source_hint,
        rows=request.rows,
        column_map=request.column_map,
    )
    if normalized.rows_accepted == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "CSV import failed: no valid rows found.",
                "rows_total": normalized.rows_total,
                "rows_skipped": normalized.rows_skipped,
                "warning_samples": normalized.warning_samples,
            },
        )

    pipeline_store.ensure_tenant_user(request.tenant_id, request.user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=trace_id,
        tenant_id=request.tenant_id,
        user_id=request.user_id,
        source=normalized.source,
        trigger_type="pipeline/run.requested",
        source_event_id=normalized.source_events[0].source_event_id,
        metadata={
            "event_name": "pipeline/run.requested",
            "ingest_path": "import/csv/mock",
            "import_file_name": request.file_name or "mock.csv",
            "rows_total": normalized.rows_total,
            "rows_accepted": normalized.rows_accepted,
            "rows_skipped": normalized.rows_skipped,
            "template_detected": normalized.template_detected,
            "mapping_mode": normalized.mapping_mode,
            "warning_samples": normalized.warning_samples[:5],
        },
    )
    raw_store.persist_source_events(normalized.source_events, run_id=run_id, ingest_version="v1")
    source_connection_store.upsert_connections(
        [
            SourceConnectionRecord(
                tenant_id=request.tenant_id,
                user_id=request.user_id,
                source=normalized.source,
                external_account_id=f"{normalized.source.value}_csv:{request.user_id}",
                scopes=[],
                token_json={
                    "import_type": "csv",
                    "file_name": request.file_name or "mock.csv",
                    "template_detected": normalized.template_detected,
                    "mapping_mode": normalized.mapping_mode,
                },
                status="active",
                last_sync_at=normalized.source_events[0].occurred_at,
                last_error=None,
            )
        ]
    )

    try:
        event_id = await dispatch_event(
            "pipeline/run.requested",
            {
                "trace_id": trace_id,
                "tenant_id": request.tenant_id,
                "user_id": request.user_id,
                "run_id": run_id,
                "source": str(normalized.source),
                "source_count": normalized.rows_accepted,
                "pre_stored": True,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return CsvImportResponse(
        run_id=run_id,
        event_name="pipeline/run.requested",
        event_id=event_id,
        trace_id=trace_id,
        rows_total=normalized.rows_total,
        rows_accepted=normalized.rows_accepted,
        rows_skipped=normalized.rows_skipped,
        template_detected=normalized.template_detected,
        mapping_mode=normalized.mapping_mode,
        warning_samples=normalized.warning_samples[:10],
    )


@router.post("/import/csv", response_model=CsvImportResponse)
async def import_csv(
    request: Request,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
    raw_store: RawEventStore = Depends(get_raw_event_store),
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
    source_connection_store: SourceConnectionStore = Depends(get_source_connection_store),
) -> CsvImportResponse:
    form = await request.form()
    file_obj = form.get("file")
    if file_obj is None:
        raise HTTPException(status_code=400, detail="Missing file in multipart form.")

    tenant_id = str(form.get("tenant_id") or "").strip()
    user_id = str(form.get("user_id") or "").strip()
    source_hint_raw = str(form.get("source_hint") or "").strip()
    trace_id_raw = str(form.get("trace_id") or "").strip() or None
    column_map = str(form.get("column_map") or "").strip() or None

    if not tenant_id or not user_id:
        raise HTTPException(status_code=400, detail="tenant_id and user_id are required.")
    try:
        source_hint = CsvImportSourceHint(source_hint_raw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid source_hint.") from exc

    file_bytes = await file_obj.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="CSV file is empty.")
    try:
        csv_text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV file must be UTF-8 encoded.") from exc

    parsed_column_map: dict[str, str] | None = None
    if column_map:
        try:
            loaded = json.loads(column_map)
            if isinstance(loaded, dict):
                parsed_column_map = {str(k): str(v) for k, v in loaded.items()}
            else:
                raise ValueError("column_map must be a JSON object")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid column_map JSON: {exc}") from exc

    resolved_trace_id = trace_id_raw or f"trace_{uuid4().hex}"
    normalized = normalize_csv_text_to_source_events(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=resolved_trace_id,
        source_hint=source_hint,
        csv_text=csv_text,
        column_map=parsed_column_map,
    )
    if normalized.rows_accepted == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "CSV import failed: no valid rows found.",
                "rows_total": normalized.rows_total,
                "rows_skipped": normalized.rows_skipped,
                "warning_samples": normalized.warning_samples,
            },
        )

    pipeline_store.ensure_tenant_user(tenant_id, user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=resolved_trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source=normalized.source,
        trigger_type="pipeline/run.requested",
        source_event_id=normalized.source_events[0].source_event_id,
        metadata={
            "event_name": "pipeline/run.requested",
            "ingest_path": "import/csv",
            "import_file_name": getattr(file_obj, "filename", None) or "upload.csv",
            "rows_total": normalized.rows_total,
            "rows_accepted": normalized.rows_accepted,
            "rows_skipped": normalized.rows_skipped,
            "template_detected": normalized.template_detected,
            "mapping_mode": normalized.mapping_mode,
            "warning_samples": normalized.warning_samples[:5],
        },
    )
    raw_store.persist_source_events(normalized.source_events, run_id=run_id, ingest_version="v1")
    source_connection_store.upsert_connections(
        [
            SourceConnectionRecord(
                tenant_id=tenant_id,
                user_id=user_id,
                source=normalized.source,
                external_account_id=f"{normalized.source.value}_csv:{user_id}",
                scopes=[],
                token_json={
                    "import_type": "csv",
                    "file_name": getattr(file_obj, "filename", None) or "upload.csv",
                    "template_detected": normalized.template_detected,
                    "mapping_mode": normalized.mapping_mode,
                },
                status="active",
                last_sync_at=normalized.source_events[0].occurred_at,
                last_error=None,
            )
        ]
    )
    try:
        event_id = await dispatch_event(
            "pipeline/run.requested",
            {
                "trace_id": resolved_trace_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "run_id": run_id,
                "source": str(normalized.source),
                "source_count": normalized.rows_accepted,
                "pre_stored": True,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return CsvImportResponse(
        run_id=run_id,
        event_name="pipeline/run.requested",
        event_id=event_id,
        trace_id=resolved_trace_id,
        rows_total=normalized.rows_total,
        rows_accepted=normalized.rows_accepted,
        rows_skipped=normalized.rows_skipped,
        template_detected=normalized.template_detected,
        mapping_mode=normalized.mapping_mode,
        warning_samples=normalized.warning_samples[:10],
    )


@router.post("/import/linkedin-zip", response_model=LinkedInZipImportResponse)
async def import_linkedin_zip(
    request: Request,
    dispatch_event: InngestDispatcher = Depends(get_inngest_dispatcher),
    raw_store: RawEventStore = Depends(get_raw_event_store),
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
    source_connection_store: SourceConnectionStore = Depends(get_source_connection_store),
) -> LinkedInZipImportResponse:
    import zipfile

    form = await request.form()
    file_obj = form.get("file")
    if file_obj is None:
        raise HTTPException(status_code=400, detail="Missing file in multipart form.")

    tenant_id = str(form.get("tenant_id") or "").strip()
    user_id = str(form.get("user_id") or "").strip()
    trace_id_raw = str(form.get("trace_id") or "").strip() or None

    if not tenant_id or not user_id:
        raise HTTPException(status_code=400, detail="tenant_id and user_id are required.")

    file_bytes = await file_obj.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    if not zipfile.is_zipfile(io.BytesIO(file_bytes)):
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid ZIP archive.")

    resolved_trace_id = trace_id_raw or f"trace_{uuid4().hex}"

    result = extract_linkedin_zip_to_source_events(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=resolved_trace_id,
        zip_bytes=file_bytes,
    )
    if result.total_events == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "LinkedIn ZIP import failed: no valid events found.",
                "warnings": result.warnings,
            },
        )

    pipeline_store.ensure_tenant_user(tenant_id, user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=resolved_trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source=IngestionSource.LINKEDIN,
        trigger_type="pipeline/run.requested",
        source_event_id=result.source_events[0].source_event_id,
        metadata={
            "event_name": "pipeline/run.requested",
            "ingest_path": "import/linkedin-zip",
            "import_file_name": getattr(file_obj, "filename", None) or "linkedin_export.zip",
            "total_events": result.total_events,
            "file_stats": [
                {
                    "file_name": fr.file_name,
                    "rows_total": fr.rows_total,
                    "rows_accepted": fr.rows_accepted,
                    "rows_skipped": fr.rows_skipped,
                }
                for fr in result.file_results
            ],
        },
    )
    raw_store.persist_source_events(result.source_events, run_id=run_id, ingest_version="v1")
    source_connection_store.upsert_connections(
        [
            SourceConnectionRecord(
                tenant_id=tenant_id,
                user_id=user_id,
                source=IngestionSource.LINKEDIN,
                external_account_id=f"linkedin_zip:{user_id}",
                scopes=[],
                token_json={
                    "import_type": "linkedin_zip",
                    "file_name": getattr(file_obj, "filename", None) or "linkedin_export.zip",
                    "total_events": result.total_events,
                },
                status="active",
                last_sync_at=result.source_events[0].occurred_at,
                last_error=None,
            )
        ]
    )

    try:
        event_id = await dispatch_event(
            "pipeline/run.requested",
            {
                "trace_id": resolved_trace_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "run_id": run_id,
                "source": str(IngestionSource.LINKEDIN),
                "source_count": result.total_events,
                "pre_stored": True,
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return LinkedInZipImportResponse(
        run_id=run_id,
        event_name="pipeline/run.requested",
        event_id=event_id,
        trace_id=resolved_trace_id,
        total_events=result.total_events,
        file_stats=[
            LinkedInZipFileStats(
                file_name=fr.file_name,
                rows_total=fr.rows_total,
                rows_accepted=fr.rows_accepted,
                rows_skipped=fr.rows_skipped,
            )
            for fr in result.file_results
        ],
        warnings=result.warnings[:20],
    )


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
    raw_store: RawEventStore = Depends(get_raw_event_store),
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
    source_connection_store: SourceConnectionStore = Depends(get_source_connection_store),
) -> GoogleOAuthCallbackResponse:
    """Store-first mock ingestion.

    Converts the callback payload to source events, persists them, then emits a
    compact Inngest event that references the persisted data.
    """
    from app.ingestion.google_connector import (
        GoogleOAuthConnector,
        GoogleOAuthContext,
        GoogleMockSourceType,
    )

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
        connector = GoogleOAuthConnector()
        source_events = connector.handle_mock_callback(
            context=GoogleOAuthContext(
                tenant_id=resolved_tenant_id,
                user_id=resolved_user_id,
                trace_id=trace_id,
            ),
            source_type=GoogleMockSourceType(str(request.source_type)),
            payload=dict(request.payload),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Mock connector failed: {exc}") from exc

    pipeline_store.ensure_tenant_user(resolved_tenant_id, resolved_user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=trace_id,
        tenant_id=resolved_tenant_id,
        user_id=resolved_user_id,
        source=source_events[0].source if source_events else request.source_type,
        trigger_type="kue/user.mock_connected",
        source_event_id=str(source_events[0].source_event_id) if source_events else None,
        metadata={"event_name": "kue/user.mock_connected", "ingest_path": "mock_oauth"},
    )
    source_map = {
        GoogleMockSourceType.CONTACTS: IngestionSource.GOOGLE_CONTACTS,
        GoogleMockSourceType.GMAIL: IngestionSource.GMAIL,
        GoogleMockSourceType.CALENDAR: IngestionSource.GOOGLE_CALENDAR,
    }
    source_connection_store.upsert_connections(
        [
            SourceConnectionRecord(
                tenant_id=resolved_tenant_id,
                user_id=resolved_user_id,
                source=source_map[GoogleMockSourceType(str(request.source_type))],
                external_account_id=f"mock:{resolved_user_id}",
                scopes=[],
                token_json={"provider": "google", "mock": True},
                status="active",
            )
        ]
    )

    try:
        raw_store.persist_source_events(source_events, run_id=run_id, ingest_version="v1")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Raw store persist failed: {exc}") from exc

    try:
        event_id = await dispatch_event(
            "kue/user.mock_connected",
            {
                "trace_id": trace_id,
                "tenant_id": resolved_tenant_id,
                "user_id": resolved_user_id,
                "run_id": run_id,
                "source_count": len(source_events),
                "pre_stored": True,
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
        pipeline_status="accepted",
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
    raw_store: RawEventStore = Depends(get_raw_event_store),
    pipeline_store: PipelineStore = Depends(get_pipeline_store),
) -> PipelineRunResponse:
    """Store-first manual capture endpoint.

    Persists source events to raw storage and emits a compact pipeline event.
    """
    trace_id = request.source_events[0].trace_id if request.source_events else None
    if trace_id is None:
        raise HTTPException(status_code=400, detail="source_events must include trace_id")

    tenant_id = request.source_events[0].tenant_id
    user_id = request.source_events[0].user_id
    source = str(request.source_events[0].source)

    pipeline_store.ensure_tenant_user(tenant_id, user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source=request.source_events[0].source,
        trigger_type="pipeline/run.requested",
        source_event_id=str(request.source_events[0].source_event_id),
        metadata={"event_name": "pipeline/run.requested", "ingest_path": "layer2/capture"},
    )

    try:
        raw_store.persist_source_events(
            request.source_events,
            run_id=run_id,
            ingest_version="v1",
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Raw store persist failed: {exc}") from exc

    try:
        event_id = await dispatch_event(
            "pipeline/run.requested",
            {
                "trace_id": trace_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "run_id": run_id,
                "source": source,
                "source_count": len(request.source_events),
                "parser_version": settings.parser_version,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Inngest dispatch failed: {exc}") from exc

    return PipelineRunResponse(
        run_id=run_id,
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


@router.post("/layer7/relationships/{trace_id}", response_model=Layer7RelationshipResponse)
def relationship_layer7_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    relationship_store: RelationshipStore = Depends(get_relationship_store),
) -> Layer7RelationshipResponse:
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
    interactions = extract_interactions(enrichment_result.model_dump(mode="json"))
    strengths = compute_relationship_strength(interactions.model_dump(mode="json"))
    persist_result = relationship_store.persist(interactions.interactions, strengths.relationships)
    return Layer7RelationshipResponse(
        trace_id=trace_id,
        interaction_count=persist_result.interaction_count,
        relationship_count=persist_result.relationship_count,
        relationships_upserted=persist_result.relationships_upserted,
        store=relationship_store.store_name,
    )


@router.post("/layer8/metadata/{trace_id}", response_model=Layer8MetadataResponse)
def metadata_layer8_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    entity_store: EntityStore = Depends(get_entity_store),
) -> Layer8MetadataResponse:
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
    metadata_result = extract_metadata_candidates(enrichment_result.model_dump(mode="json"))
    updated_count = entity_store.upsert_metadata(metadata_result.candidates)
    return Layer8MetadataResponse(
        trace_id=trace_id,
        candidate_count=metadata_result.candidate_count,
        updated_count=updated_count,
        store=entity_store.store_name,
    )


@router.post("/layer9/semantic/{trace_id}", response_model=Layer9SemanticResponse)
def semantic_layer9_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    entity_store: EntityStore = Depends(get_entity_store),
    search_store: SearchDocumentStore = Depends(get_search_document_store),
) -> Layer9SemanticResponse:
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

    # Ensure entities exist so semantic docs can bind entity_id.
    exact_match = extract_entity_candidates(enrichment_result.model_dump(mode="json"))
    merge_result = merge_entity_candidates(exact_match.model_dump(mode="json"))
    entity_store.upsert_entities(merge_result.resolved_entities)

    semantic_result = build_semantic_documents(enrichment_result.model_dump(mode="json"))
    persist_result = search_store.persist_documents(semantic_result.documents)
    return Layer9SemanticResponse(
        trace_id=trace_id,
        document_count=persist_result.candidate_count,
        stored_count=persist_result.stored_count,
        skipped_no_entity=persist_result.skipped_no_entity,
        store=search_store.store_name,
    )


@router.post("/layer10/embed/{trace_id}", response_model=Layer10EmbeddingResponse)
def embedding_layer10_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    entity_store: EntityStore = Depends(get_entity_store),
    search_store: SearchDocumentStore = Depends(get_search_document_store),
    embedding_store: EmbeddingStore = Depends(get_embedding_store),
) -> Layer10EmbeddingResponse:
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
    entity_store.upsert_entities(merge_result.resolved_entities)

    semantic_result = build_semantic_documents(enrichment_result.model_dump(mode="json"))
    search_store.persist_documents(semantic_result.documents)

    requests = build_embedding_requests(semantic_result.model_dump(mode="json"))
    lookup = embedding_cache_lookup(requests)
    generated = generate_embeddings(lookup.misses)
    combined_records = lookup.hits + generated.generated
    cache_store_count = embedding_cache_store(generated.generated)
    persist_result = embedding_store.persist_vectors(combined_records)

    return Layer10EmbeddingResponse(
        trace_id=trace_id,
        candidate_count=lookup.candidate_count,
        cache_hit_count=lookup.cache_hit_count,
        cache_miss_count=lookup.cache_miss_count,
        generated_count=generated.generated_count,
        cache_store_count=cache_store_count,
        persisted_count=persist_result.persisted_count,
        skipped_no_entity=persist_result.skipped_no_entity,
        store=embedding_store.store_name,
    )


@router.post("/layer11/cache/{trace_id}", response_model=Layer11CacheResponse)
def cache_layer11_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
) -> Layer11CacheResponse:
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

    enrichment_cached = 0
    for event in enrichment_result.parsed_events:
        key = f"{event.get('tenant_id')}:{event.get('source_event_id')}"
        cache_registry.put(namespace="enrichment", version="v1", key=key, value=dict(event))
        enrichment_cached += 1

    semantic_result = build_semantic_documents(enrichment_result.model_dump(mode="json"))
    requests = build_embedding_requests(semantic_result.model_dump(mode="json"))
    lookup = embedding_cache_lookup(requests)
    generated = generate_embeddings(lookup.misses)
    combined_records = lookup.hits + generated.generated
    embedding_cached = embedding_cache_store(combined_records)
    for record in combined_records:
        key = f"{record.tenant_id}:{record.doc_type}:{record.content}"
        cache_registry.put(
            namespace="embedding",
            version="v1",
            key=key,
            value={"embedding": record.embedding},
        )

    stats = cache_registry.stats()
    return Layer11CacheResponse(
        trace_id=trace_id,
        enrichment_cached_count=enrichment_cached,
        embedding_cached_count=embedding_cached,
        total_entries=int(stats.get("total_entries", 0)),
        namespaces={k: int(v) for k, v in dict(stats.get("namespaces", {})).items()},
    )


@router.post("/layer12/index/{trace_id}", response_model=Layer12IndexResponse)
def index_layer12_by_trace(
    trace_id: str,
    raw_store: RawEventStore = Depends(get_raw_event_store),
    entity_store: EntityStore = Depends(get_entity_store),
    search_store: SearchDocumentStore = Depends(get_search_document_store),
    embedding_store: EmbeddingStore = Depends(get_embedding_store),
    index_store: SearchIndexStore = Depends(get_search_index_store),
) -> Layer12IndexResponse:
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
    entity_store.upsert_entities(merge_result.resolved_entities)

    semantic_result = build_semantic_documents(enrichment_result.model_dump(mode="json"))
    search_store.persist_documents(semantic_result.documents)

    requests = build_embedding_requests(semantic_result.model_dump(mode="json"))
    lookup = embedding_cache_lookup(requests)
    generated = generate_embeddings(lookup.misses)
    records = lookup.hits + generated.generated
    embedding_cache_store(records)
    embedding_store.persist_vectors(records)

    signals = build_hybrid_signals(
        {"records": [r.model_dump(mode="json") for r in records]}
    )
    persisted = index_store.apply_hybrid_signals(signals.signals)
    tenant_id = raw_events[0].tenant_id
    health = index_store.health_check(tenant_id)

    return Layer12IndexResponse(
        trace_id=trace_id,
        signal_count=signals.signal_count,
        applied_count=persisted.applied_count,
        skipped_no_entity=persisted.skipped_no_entity,
        health=health,
        store=index_store.store_name,
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


@router.post("/admin/reset", response_model=AdminResetResponse)
def reset_ingestion_data(
    x_admin_reset_token: str | None = Header(default=None),
) -> AdminResetResponse:
    _assert_admin_reset_token(x_admin_reset_token)
    try:
        result = reset_all_data(settings)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return AdminResetResponse(ok=result.ok, mode=result.mode, details=result.details)
