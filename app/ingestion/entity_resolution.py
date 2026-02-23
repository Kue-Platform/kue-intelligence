from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class EntityCandidate(BaseModel):
    raw_event_id: int
    tenant_id: str
    user_id: str
    source: str
    source_event_id: str
    display_name: str
    primary_email: str | None = None
    company_norm: str | None = None
    title_norm: str | None = None
    metadata_json: dict[str, Any] = {}


class ExactMatchResult(BaseModel):
    stage: Literal["entity_resolution"] = "entity_resolution"
    candidate_count: int
    candidates: list[EntityCandidate]


class MergeResult(BaseModel):
    stage: Literal["entity_resolution"] = "entity_resolution"
    resolved_count: int
    resolved_entities: list[EntityCandidate]


def extract_entity_candidates(enrichment_payload: dict[str, Any]) -> ExactMatchResult:
    parsed_events = list(enrichment_payload.get("parsed_events", []))
    candidates: list[EntityCandidate] = []
    for event in parsed_events:
        event_type = str(event.get("event_type"))
        if event_type != "contact":
            continue
        normalized = dict(event.get("normalized", {}))
        emails = normalized.get("emails", [])
        primary_email = None
        if isinstance(emails, list) and emails:
            primary_email = str(emails[0]).strip().lower() or None
        display_name = str(normalized.get("full_name") or "Unknown").strip() or "Unknown"
        candidates.append(
            EntityCandidate(
                raw_event_id=int(event["raw_event_id"]),
                tenant_id=str(event["tenant_id"]),
                user_id=str(event["user_id"]),
                source=str(event["source"]),
                source_event_id=str(event["source_event_id"]),
                display_name=display_name,
                primary_email=primary_email,
                company_norm=(
                    str(normalized.get("company")).strip() if normalized.get("company") else None
                ),
                title_norm=(str(normalized.get("title")).strip() if normalized.get("title") else None),
                metadata_json={"tags": normalized.get("tags", [])},
            )
        )

    return ExactMatchResult(candidate_count=len(candidates), candidates=candidates)


def merge_entity_candidates(exact_match_payload: dict[str, Any]) -> MergeResult:
    candidates = [
        EntityCandidate.model_validate(item)
        for item in list(exact_match_payload.get("candidates", []))
    ]
    dedup: dict[str, EntityCandidate] = {}
    for candidate in candidates:
        key = (
            f"{candidate.tenant_id}:email:{candidate.primary_email}"
            if candidate.primary_email
            else f"{candidate.tenant_id}:source:{candidate.source}:{candidate.source_event_id}"
        )
        dedup[key] = candidate
    resolved_entities = list(dedup.values())
    return MergeResult(resolved_count=len(resolved_entities), resolved_entities=resolved_entities)
