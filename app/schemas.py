from typing import Any
from datetime import datetime

from enum import StrEnum

from pydantic import BaseModel, Field


class IngestionSource(StrEnum):
    GOOGLE_CONTACTS = "google_contacts"
    GMAIL = "gmail"
    GOOGLE_CALENDAR = "google_calendar"
    LINKEDIN = "linkedin"
    TWITTER = "twitter"
    CSV_IMPORT = "csv_import"


class ConnectorTriggerType(StrEnum):
    WEBHOOK = "webhook"
    POLLING = "polling"
    FILE_UPLOAD = "file_upload"
    MANUAL = "manual"


class IngestionMockTriggerRequest(BaseModel):
    source: IngestionSource
    trigger_type: ConnectorTriggerType = ConnectorTriggerType.MANUAL
    payload: dict[str, Any] = Field(
        ...,
        description="Source-specific mock payload. "
        "google_contacts expects contacts[], gmail expects messages[], linkedin expects profiles[].",
    )


class IngestionMockTriggerResponse(BaseModel):
    connector_event_id: str
    trace_id: str
    source: IngestionSource
    trigger_type: ConnectorTriggerType
    accepted_at: datetime
    records_detected: int
    payload_shape: str
    status: str = "accepted"


class SourceEvent(BaseModel):
    tenant_id: str
    user_id: str
    source: IngestionSource
    source_event_id: str
    occurred_at: datetime
    trace_id: str
    payload: dict[str, Any]


class RawCapturedEvent(BaseModel):
    raw_event_id: int
    run_id: str | None = None
    tenant_id: str
    user_id: str
    source: IngestionSource
    source_event_id: str
    occurred_at: datetime
    trace_id: str
    payload: dict[str, Any]
    captured_at: datetime


class GoogleOAuthCallbackResponse(BaseModel):
    provider: str = "google"
    tenant_id: str
    user_id: str
    trace_id: str
    source_events: list[SourceEvent]
    counts: dict[str, int]
    pipeline_event_name: str
    pipeline_event_id: str | None = None
    pipeline_status: str = "accepted"


class GoogleMockSourceType(StrEnum):
    CONTACTS = "contacts"
    GMAIL = "gmail"
    CALENDAR = "calendar"


class GoogleOAuthMockCallbackRequest(BaseModel):
    source_type: GoogleMockSourceType
    payload: dict[str, Any] = Field(
        ...,
        description="Raw mock payload as returned by Google API for the selected source_type.",
    )
    tenant_id: str | None = None
    user_id: str | None = None
    state: str | None = None
    trace_id: str | None = None


class Layer2ManualCaptureRequest(BaseModel):
    source_events: list[SourceEvent] = Field(..., min_length=1)


class RawEventsByTraceResponse(BaseModel):
    trace_id: str
    total: int
    events: list[RawCapturedEvent]


class CanonicalEventType(StrEnum):
    CONTACT = "contact"
    EMAIL_MESSAGE = "email_message"
    CALENDAR_EVENT = "calendar_event"


class CanonicalEvent(BaseModel):
    raw_event_id: int
    run_id: str | None = None
    tenant_id: str
    user_id: str
    trace_id: str
    source: IngestionSource
    source_event_id: str
    occurred_at: datetime
    event_type: CanonicalEventType
    normalized: dict[str, Any]
    parse_warnings: list[str] = []


class CanonicalParseFailure(BaseModel):
    raw_event_id: int
    source_event_id: str
    reason: str


class Layer3PersistenceSummary(BaseModel):
    stored_count: int
    store: str
    parser_version: str
    parsed_at: datetime


class Layer3ParseResponse(BaseModel):
    trace_id: str
    total_raw_events: int
    parsed_count: int
    failed_count: int
    parsed_events: list[CanonicalEvent]
    failures: list[CanonicalParseFailure]
    persistence: Layer3PersistenceSummary | None = None


class Layer3EventsResponse(BaseModel):
    trace_id: str
    total: int
    events: list[CanonicalEvent]


class Layer4ValidationFailure(BaseModel):
    raw_event_id: int
    source_event_id: str
    event_type: str
    reason: str


class Layer4ValidationResponse(BaseModel):
    trace_id: str
    total_parsed_events: int
    valid_count: int
    invalid_count: int
    invalid_events: list[Layer4ValidationFailure]


class Layer5EnrichmentResponse(BaseModel):
    trace_id: str
    valid_count: int
    enriched_count: int
    sample: list[CanonicalEvent] = []


class Layer6EntityResolutionResponse(BaseModel):
    trace_id: str
    candidate_count: int
    resolved_count: int
    created_entities: int
    updated_entities: int
    identities_upserted: int
    store: str


class Layer7RelationshipResponse(BaseModel):
    trace_id: str
    interaction_count: int
    relationship_count: int
    relationships_upserted: int
    store: str


class Layer8MetadataResponse(BaseModel):
    trace_id: str
    candidate_count: int
    updated_count: int
    store: str


class Layer9SemanticResponse(BaseModel):
    trace_id: str
    document_count: int
    stored_count: int
    skipped_no_entity: int
    store: str


class Layer10EmbeddingResponse(BaseModel):
    trace_id: str
    candidate_count: int
    cache_hit_count: int
    cache_miss_count: int
    generated_count: int
    cache_store_count: int
    persisted_count: int
    skipped_no_entity: int
    store: str


class Layer11CacheResponse(BaseModel):
    trace_id: str
    enrichment_cached_count: int
    embedding_cached_count: int
    total_entries: int
    namespaces: dict[str, int]


class Layer12IndexResponse(BaseModel):
    trace_id: str
    signal_count: int
    applied_count: int
    skipped_no_entity: int
    health: dict[str, Any]
    store: str


class PipelineRunResponse(BaseModel):
    run_id: str | None = None
    event_name: str
    event_id: str | None = None
    trace_id: str
    status: str = "accepted"


class PipelineRunStatusResponse(BaseModel):
    run_id: str
    trace_id: str
    tenant_id: str
    user_id: str
    source: str | None = None
    trigger_type: str
    status: str
    requested_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class AdminResetResponse(BaseModel):
    ok: bool
    mode: str
    details: dict[str, Any] = {}


class CsvImportSourceHint(StrEnum):
    LINKEDIN = "linkedin"
    CSV_IMPORT = "csv_import"
    GOOGLE_CONTACTS = "google_contacts"


class CsvImportMockRequest(BaseModel):
    tenant_id: str
    user_id: str
    source_hint: CsvImportSourceHint
    rows: list[dict[str, Any]] = Field(..., min_length=1)
    trace_id: str | None = None
    column_map: dict[str, str] | None = None
    file_name: str | None = None


class CsvImportResponse(BaseModel):
    run_id: str
    event_name: str
    event_id: str | None = None
    trace_id: str
    rows_total: int
    rows_accepted: int
    rows_skipped: int
    template_detected: str
    mapping_mode: str
    warning_samples: list[str] = []
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Warm Path
# ---------------------------------------------------------------------------


class WarmPathNode(BaseModel):
    entity_id: str
    name: str | None = None
    primary_email: str | None = None
    company: str | None = None


class WarmPathEdge(BaseModel):
    from_entity_id: str
    to_entity_id: str
    strength: float
    interaction_count: int
    last_interaction_at: str | None = None
    channels: list[str] = []
    edge_warmth: float


class WarmPath(BaseModel):
    path_nodes: list[WarmPathNode]
    path_edges: list[WarmPathEdge]
    path_score: float  # 0–100 scale
    hop_count: int
    is_direct: bool = False


class WarmPathResponse(BaseModel):
    tenant_id: str
    origin_entity_id: str
    target_entity_id: str | None = None
    paths: list[WarmPath]
    total_paths_found: int
    query_mode: str  # "targeted" | "discover"


class WarmPathOriginResponse(BaseModel):
    tenant_id: str
    email: str
    entity_id: str | None = None
    found: bool


class WarmPathEventRequest(BaseModel):
    event_type: str = Field(
        ...,
        description="Event type, e.g. 'path_selected', 'intro_requested'.",
    )
    origin_entity_id: str
    target_entity_id: str
    selected_path_score: float | None = None
    selected_path_hop_count: int | None = None
    connector_entity_ids: list[str] = []
    metadata: dict[str, Any] = {}


class WarmPathEventResponse(BaseModel):
    tenant_id: str
    event_id: str
    event_type: str
    recorded_at: datetime
