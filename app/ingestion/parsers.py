from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from app.schemas import CanonicalEventType, IngestionSource, RawCapturedEvent


@dataclass
class ParsedCanonicalEvent:
    raw_event_id: int
    tenant_id: str
    user_id: str
    trace_id: str
    source: IngestionSource
    source_event_id: str
    occurred_at: datetime
    event_type: CanonicalEventType
    normalized: dict[str, Any]
    parse_warnings: list[str]


@dataclass
class ParseFailure:
    raw_event_id: int
    source_event_id: str
    reason: str


@dataclass
class ParseResult:
    parsed_events: list[ParsedCanonicalEvent]
    failures: list[ParseFailure]


def _split_name(full_name: str) -> tuple[str | None, str | None]:
    parts = [part for part in full_name.split() if part]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def _to_iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _safe_email(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return lowered or None


def _parse_header_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_contact(raw_event: RawCapturedEvent) -> ParsedCanonicalEvent:
    payload = raw_event.payload
    warnings: list[str] = []

    names = payload.get("names", [])
    display_name = None
    if isinstance(names, list) and names:
        display_name = names[0].get("displayName")
    if not display_name:
        display_name = payload.get("name")
    if not display_name:
        display_name = "Unknown"
        warnings.append("missing_contact_name")

    first_name, last_name = _split_name(str(display_name))

    email_values: list[str] = []
    email_addresses = payload.get("emailAddresses", [])
    if isinstance(email_addresses, list):
        for item in email_addresses:
            value = _safe_email(item.get("value"))
            if value:
                email_values.append(value)
    single_email = _safe_email(payload.get("email"))
    if single_email:
        email_values.append(single_email)

    phone_values: list[str] = []
    phones = payload.get("phoneNumbers", [])
    if isinstance(phones, list):
        for item in phones:
            value = item.get("value")
            if isinstance(value, str) and value.strip():
                phone_values.append(value.strip())

    company = None
    title = None
    organizations = payload.get("organizations", [])
    if isinstance(organizations, list) and organizations:
        company = organizations[0].get("name")
        title = organizations[0].get("title")
    company = company or payload.get("company")
    title = title or payload.get("title")

    normalized = {
        "full_name": str(display_name).strip(),
        "first_name": first_name,
        "last_name": last_name,
        "emails": sorted(set(email_values)),
        "phones": sorted(set(phone_values)),
        "company": company,
        "title": title,
    }

    return ParsedCanonicalEvent(
        raw_event_id=raw_event.raw_event_id,
        tenant_id=raw_event.tenant_id,
        user_id=raw_event.user_id,
        trace_id=raw_event.trace_id,
        source=raw_event.source,
        source_event_id=raw_event.source_event_id,
        occurred_at=raw_event.occurred_at,
        event_type=CanonicalEventType.CONTACT,
        normalized=normalized,
        parse_warnings=warnings,
    )


def _parse_gmail(raw_event: RawCapturedEvent) -> ParsedCanonicalEvent:
    payload = raw_event.payload
    warnings: list[str] = []

    internal_date = payload.get("internalDate")
    occurred_at = raw_event.occurred_at
    if isinstance(internal_date, str) and internal_date.isdigit():
        occurred_at = datetime.fromtimestamp(int(internal_date) / 1000, tz=UTC)

    subject = None
    from_email = None
    to_emails: list[str] = []
    header_date = None

    full_payload = payload.get("payload", {})
    headers = full_payload.get("headers", []) if isinstance(full_payload, dict) else []
    if isinstance(headers, list):
        for header in headers:
            name = str(header.get("name", "")).lower()
            value = header.get("value")
            if not isinstance(value, str):
                continue
            if name == "subject":
                subject = value
            elif name == "from":
                from_email = value
            elif name == "to":
                to_emails = [_safe_email(x) for x in value.split(",") if _safe_email(x)]
            elif name == "date":
                header_date = _parse_header_date(value)

    if not subject:
        subject = payload.get("subject")
    if not from_email:
        from_email = payload.get("from")
    if not to_emails:
        raw_to = payload.get("to")
        if isinstance(raw_to, list):
            to_emails = [_safe_email(x) for x in raw_to if _safe_email(x)]
        elif isinstance(raw_to, str):
            to_emails = [_safe_email(x) for x in raw_to.split(",") if _safe_email(x)]

    occurred_at = header_date or occurred_at
    if not subject:
        warnings.append("missing_subject")

    normalized = {
        "thread_id": payload.get("threadId"),
        "subject": subject,
        "from": _safe_email(from_email) if isinstance(from_email, str) else None,
        "to": sorted(set(to_emails)),
        "snippet": payload.get("snippet"),
        "occurred_at": _to_iso_utc(occurred_at),
    }

    return ParsedCanonicalEvent(
        raw_event_id=raw_event.raw_event_id,
        tenant_id=raw_event.tenant_id,
        user_id=raw_event.user_id,
        trace_id=raw_event.trace_id,
        source=raw_event.source,
        source_event_id=raw_event.source_event_id,
        occurred_at=occurred_at,
        event_type=CanonicalEventType.EMAIL_MESSAGE,
        normalized=normalized,
        parse_warnings=warnings,
    )


def _parse_calendar(raw_event: RawCapturedEvent) -> ParsedCanonicalEvent:
    payload = raw_event.payload
    warnings: list[str] = []

    start_obj = payload.get("start", {})
    end_obj = payload.get("end", {})
    start_at = start_obj.get("dateTime") or start_obj.get("date")
    end_at = end_obj.get("dateTime") or end_obj.get("date")

    attendees = payload.get("attendees", [])
    attendee_emails: list[str] = []
    if isinstance(attendees, list):
        for item in attendees:
            email = _safe_email(item.get("email"))
            if email:
                attendee_emails.append(email)

    summary = payload.get("summary")
    if not summary:
        warnings.append("missing_summary")

    normalized = {
        "summary": summary,
        "location": payload.get("location"),
        "start_at": start_at,
        "end_at": end_at,
        "attendee_emails": sorted(set(attendee_emails)),
        "status": payload.get("status"),
    }

    return ParsedCanonicalEvent(
        raw_event_id=raw_event.raw_event_id,
        tenant_id=raw_event.tenant_id,
        user_id=raw_event.user_id,
        trace_id=raw_event.trace_id,
        source=raw_event.source,
        source_event_id=raw_event.source_event_id,
        occurred_at=raw_event.occurred_at,
        event_type=CanonicalEventType.CALENDAR_EVENT,
        normalized=normalized,
        parse_warnings=warnings,
    )


def parse_raw_events(raw_events: list[RawCapturedEvent]) -> ParseResult:
    parsed: list[ParsedCanonicalEvent] = []
    failures: list[ParseFailure] = []

    for event in raw_events:
        try:
            if event.source == IngestionSource.GOOGLE_CONTACTS:
                parsed.append(_parse_contact(event))
            elif event.source == IngestionSource.GMAIL:
                parsed.append(_parse_gmail(event))
            elif event.source == IngestionSource.GOOGLE_CALENDAR:
                parsed.append(_parse_calendar(event))
            else:
                failures.append(
                    ParseFailure(
                        raw_event_id=event.raw_event_id,
                        source_event_id=event.source_event_id,
                        reason=f"unsupported_source:{event.source}",
                    )
                )
        except Exception as exc:
            failures.append(
                ParseFailure(
                    raw_event_id=event.raw_event_id,
                    source_event_id=event.source_event_id,
                    reason=f"parse_error:{exc}",
                )
            )

    return ParseResult(parsed_events=parsed, failures=failures)

