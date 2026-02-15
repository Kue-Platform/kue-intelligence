from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from app.schemas import ConnectorTriggerType, IngestionSource


@dataclass
class ConnectorTriggerResult:
    connector_event_id: str
    trace_id: str
    source: IngestionSource
    trigger_type: ConnectorTriggerType
    accepted_at: datetime
    records_detected: int
    payload_shape: str


def _detect_shape(payload: dict) -> str:
    if not payload:
        return "empty_object"
    if len(payload) == 1:
        only_value = next(iter(payload.values()))
        return type(only_value).__name__
    return "object"


def _validate_google_contacts(payload: dict) -> int:
    contacts = payload.get("contacts")
    if not isinstance(contacts, list) or not contacts:
        raise ValueError("google_contacts payload must include a non-empty 'contacts' list")
    return len(contacts)


def _validate_gmail(payload: dict) -> int:
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("gmail payload must include a non-empty 'messages' list")
    return len(messages)


def _validate_linkedin(payload: dict) -> int:
    profiles = payload.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        raise ValueError("linkedin payload must include a non-empty 'profiles' list")
    return len(profiles)


def trigger_mock_connector(
    *,
    source: IngestionSource,
    trigger_type: ConnectorTriggerType,
    payload: dict,
) -> ConnectorTriggerResult:
    if source == IngestionSource.GOOGLE_CONTACTS:
        records_detected = _validate_google_contacts(payload)
    elif source == IngestionSource.GMAIL:
        records_detected = _validate_gmail(payload)
    else:
        records_detected = _validate_linkedin(payload)

    return ConnectorTriggerResult(
        connector_event_id=f"conn_{uuid4().hex}",
        trace_id=f"trace_{uuid4().hex}",
        source=source,
        trigger_type=trigger_type,
        accepted_at=datetime.now(UTC),
        records_detected=records_detected,
        payload_shape=_detect_shape(payload),
    )

