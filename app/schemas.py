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


class PipelineRunResponse(BaseModel):
    run_id: str | None = None
    event_name: str
    event_id: str | None = None
    trace_id: str
    status: str = "accepted"
