from typing import Any
from datetime import datetime

from enum import StrEnum

from pydantic import BaseModel, Field


class AnalyzeDataRequest(BaseModel):
    values: list[float] = Field(..., min_length=1, description="Numeric values to analyze")


class JobCreatedResponse(BaseModel):
    task_id: str
    status: str


class JobStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None


class IngestionSource(StrEnum):
    GOOGLE_CONTACTS = "google_contacts"
    GMAIL = "gmail"
    GOOGLE_CALENDAR = "google_calendar"
    LINKEDIN = "linkedin"


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


class GoogleOAuthCallbackResponse(BaseModel):
    provider: str = "google"
    tenant_id: str
    user_id: str
    trace_id: str
    source_events: list[SourceEvent]
    counts: dict[str, int]
