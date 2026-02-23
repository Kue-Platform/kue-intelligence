from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator


class ContactCanonicalModel(BaseModel):
    full_name: str = Field(min_length=1)
    first_name: str | None = None
    last_name: str | None = None
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    company: str | None = None
    title: str | None = None

    @field_validator("emails", mode="after")
    @classmethod
    def emails_must_be_lowercase(cls, value: list[str]) -> list[str]:
        return [item.strip().lower() for item in value if item.strip()]


class EmailCanonicalModel(BaseModel):
    thread_id: str | None = None
    subject: str | None = None
    from_: str | None = Field(default=None, alias="from")
    to: list[str] = Field(default_factory=list)
    snippet: str | None = None
    occurred_at: str

    @field_validator("to", mode="after")
    @classmethod
    def normalize_to_list(cls, value: list[str]) -> list[str]:
        return [item.strip().lower() for item in value if item.strip()]


class CalendarCanonicalModel(BaseModel):
    summary: str | None = None
    location: str | None = None
    start_at: str | None = None
    end_at: str | None = None
    attendee_emails: list[str] = Field(default_factory=list)
    status: str | None = None

    @field_validator("attendee_emails", mode="after")
    @classmethod
    def normalize_attendees(cls, value: list[str]) -> list[str]:
        return [item.strip().lower() for item in value if item.strip()]


class CanonicalValidationFailure(BaseModel):
    raw_event_id: int
    source_event_id: str
    event_type: str
    reason: str


class CanonicalValidationResult(BaseModel):
    stage: Literal["validation"] = "validation"
    valid_count: int
    invalid_count: int
    parsed_events: list[dict[str, Any]]
    invalid_events: list[CanonicalValidationFailure]


def _validate_single_event(event_type: str, normalized: dict[str, Any]) -> None:
    if event_type == "contact":
        ContactCanonicalModel.model_validate(normalized)
        return
    if event_type == "email_message":
        EmailCanonicalModel.model_validate(normalized)
        return
    if event_type == "calendar_event":
        CalendarCanonicalModel.model_validate(normalized)
        return
    raise ValueError(f"unsupported_event_type:{event_type}")


def validate_parsed_events(parse_result_payload: dict[str, Any]) -> CanonicalValidationResult:
    parsed_events = list(parse_result_payload.get("parsed_events", []))
    valid_events: list[dict[str, Any]] = []
    invalid_events: list[CanonicalValidationFailure] = []

    for item in parsed_events:
        event_type = str(item.get("event_type", ""))
        raw_event_id = int(item.get("raw_event_id", 0))
        source_event_id = str(item.get("source_event_id", "unknown"))
        normalized = item.get("normalized")
        try:
            if not isinstance(normalized, dict):
                raise ValueError("normalized_payload_not_object")
            _validate_single_event(event_type, normalized)
            valid_events.append(item)
        except (ValidationError, ValueError) as exc:
            invalid_events.append(
                CanonicalValidationFailure(
                    raw_event_id=raw_event_id,
                    source_event_id=source_event_id,
                    event_type=event_type,
                    reason=str(exc),
                )
            )

    return CanonicalValidationResult(
        valid_count=len(valid_events),
        invalid_count=len(invalid_events),
        parsed_events=valid_events,
        invalid_events=invalid_events,
    )
