from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class CleaningEnrichmentResult(BaseModel):
    stage: Literal["cleaning_enrichment"] = "cleaning_enrichment"
    enriched_count: int
    parsed_events: list[dict[str, Any]]


def _normalize_company(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(value.strip().split())
    return cleaned.title() if cleaned else None


def _normalize_title(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(value.strip().split())
    return cleaned.title() if cleaned else None


def _normalize_contact(event: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(event.get("normalized", {}))
    emails = normalized.get("emails", [])
    if isinstance(emails, list):
        normalized["emails"] = sorted({str(email).strip().lower() for email in emails if str(email).strip()})
    normalized["company"] = _normalize_company(normalized.get("company"))
    normalized["title"] = _normalize_title(normalized.get("title"))

    tags: list[str] = []
    if normalized.get("company"):
        tags.append(f"company:{str(normalized['company']).lower().replace(' ', '_')}")
    if normalized.get("title"):
        tags.append(f"title:{str(normalized['title']).lower().replace(' ', '_')}")
    normalized["tags"] = sorted(set(tags))

    enriched = dict(event)
    enriched["normalized"] = normalized
    warnings = list(enriched.get("parse_warnings", []))
    warnings.append("enrichment_applied")
    enriched["parse_warnings"] = warnings
    return enriched


def _normalize_email_event(event: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(event.get("normalized", {}))
    from_email = normalized.get("from")
    if isinstance(from_email, str):
        normalized["from"] = from_email.strip().lower()
    to_emails = normalized.get("to", [])
    if isinstance(to_emails, list):
        normalized["to"] = sorted(
            {str(email).strip().lower() for email in to_emails if str(email).strip()}
        )

    enriched = dict(event)
    enriched["normalized"] = normalized
    warnings = list(enriched.get("parse_warnings", []))
    warnings.append("enrichment_applied")
    enriched["parse_warnings"] = warnings
    return enriched


def _normalize_calendar_event(event: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(event.get("normalized", {}))
    attendee_emails = normalized.get("attendee_emails", [])
    if isinstance(attendee_emails, list):
        normalized["attendee_emails"] = sorted(
            {str(email).strip().lower() for email in attendee_emails if str(email).strip()}
        )

    enriched = dict(event)
    enriched["normalized"] = normalized
    warnings = list(enriched.get("parse_warnings", []))
    warnings.append("enrichment_applied")
    enriched["parse_warnings"] = warnings
    return enriched


def clean_and_enrich_events(validation_result_payload: dict[str, Any]) -> CleaningEnrichmentResult:
    valid_events = list(validation_result_payload.get("parsed_events", []))
    output: list[dict[str, Any]] = []
    for event in valid_events:
        event_type = str(event.get("event_type"))
        if event_type == "contact":
            output.append(_normalize_contact(event))
        elif event_type == "email_message":
            output.append(_normalize_email_event(event))
        elif event_type == "calendar_event":
            output.append(_normalize_calendar_event(event))
        else:
            output.append(event)

    return CleaningEnrichmentResult(
        enriched_count=len(output),
        parsed_events=output,
    )
