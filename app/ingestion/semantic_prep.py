from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class SemanticDocument(BaseModel):
    tenant_id: str
    primary_email: str | None = None
    doc_type: str
    content: str
    metadata_json: dict[str, Any]
    source_updated_at: str | None = None


class SemanticPrepResult(BaseModel):
    stage: Literal["semantic_prep"] = "semantic_prep"
    document_count: int
    documents: list[SemanticDocument]


def _clean(value: str | None) -> str | None:
    if not value:
        return None
    trimmed = " ".join(value.strip().split())
    return trimmed if trimmed else None


def build_semantic_documents(enrichment_payload: dict[str, Any]) -> SemanticPrepResult:
    parsed_events = list(enrichment_payload.get("parsed_events", []))
    docs: list[SemanticDocument] = []
    for event in parsed_events:
        event_type = str(event.get("event_type"))
        normalized = dict(event.get("normalized", {}))
        tenant_id = str(event.get("tenant_id"))
        source_event_id = str(event.get("source_event_id"))
        occurred_at = str(event.get("occurred_at"))
        source = str(event.get("source"))

        if event_type == "contact":
            emails = normalized.get("emails", [])
            primary_email = (
                str(emails[0]).strip().lower()
                if isinstance(emails, list) and emails
                else None
            )
            content_parts = [
                _clean(normalized.get("full_name")),
                _clean(normalized.get("title")),
                _clean(normalized.get("company")),
                " ".join([str(e).lower() for e in emails])
                if isinstance(emails, list)
                else None,
            ]
            content = " | ".join([p for p in content_parts if p]) or "contact"
            docs.append(
                SemanticDocument(
                    tenant_id=tenant_id,
                    primary_email=primary_email,
                    doc_type="contact_profile",
                    content=content,
                    metadata_json={
                        "source": source,
                        "source_event_id": source_event_id,
                        "event_type": event_type,
                        "tags": normalized.get("tags", []),
                    },
                    source_updated_at=occurred_at,
                )
            )

        elif event_type == "email_message":
            from_email = normalized.get("from")
            subject = _clean(normalized.get("subject"))
            snippet = _clean(normalized.get("snippet"))
            content = " | ".join([p for p in [subject, snippet] if p]) or "email"
            docs.append(
                SemanticDocument(
                    tenant_id=tenant_id,
                    primary_email=str(from_email).strip().lower()
                    if from_email
                    else None,
                    doc_type="email_summary",
                    content=content,
                    metadata_json={
                        "source": source,
                        "source_event_id": source_event_id,
                        "event_type": event_type,
                        "thread_id": normalized.get("thread_id"),
                        "to": normalized.get("to", []),
                    },
                    source_updated_at=normalized.get("occurred_at") or occurred_at,
                )
            )

        elif event_type == "calendar_event":
            attendee_emails = normalized.get("attendee_emails", [])
            primary_email = (
                str(attendee_emails[0]).strip().lower()
                if isinstance(attendee_emails, list) and attendee_emails
                else None
            )
            summary = _clean(normalized.get("summary"))
            location = _clean(normalized.get("location"))
            content = (
                " | ".join([p for p in [summary, location] if p]) or "calendar_event"
            )
            docs.append(
                SemanticDocument(
                    tenant_id=tenant_id,
                    primary_email=primary_email,
                    doc_type="calendar_summary",
                    content=content,
                    metadata_json={
                        "source": source,
                        "source_event_id": source_event_id,
                        "event_type": event_type,
                        "attendees": attendee_emails,
                    },
                    source_updated_at=occurred_at,
                )
            )

    return SemanticPrepResult(document_count=len(docs), documents=docs)
