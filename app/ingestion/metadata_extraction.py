from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class MetadataCandidate(BaseModel):
    tenant_id: str
    primary_email: str
    display_name: str
    metadata_json: dict[str, Any]


class MetadataExtractResult(BaseModel):
    stage: Literal["metadata_extraction"] = "metadata_extraction"
    candidate_count: int
    candidates: list[MetadataCandidate]


def _norm_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = " ".join(value.strip().split())
    return cleaned if cleaned else None


def _tagify(prefix: str, value: str | None) -> str | None:
    normalized = _norm_text(value)
    if not normalized:
        return None
    return f"{prefix}:{normalized.lower().replace(' ', '_')}"


def extract_metadata_candidates(
    enrichment_payload: dict[str, Any],
) -> MetadataExtractResult:
    parsed_events = list(enrichment_payload.get("parsed_events", []))
    candidates: list[MetadataCandidate] = []

    for event in parsed_events:
        if str(event.get("event_type")) != "contact":
            continue
        normalized = dict(event.get("normalized", {}))
        emails = normalized.get("emails", [])
        if not isinstance(emails, list) or not emails:
            continue
        primary_email = str(emails[0]).strip().lower()
        if not primary_email:
            continue

        company = _norm_text(normalized.get("company"))
        title = _norm_text(normalized.get("title"))
        full_name = _norm_text(normalized.get("full_name")) or "Unknown"

        tags: set[str] = set()
        for existing in normalized.get("tags", []):
            if isinstance(existing, str) and existing.strip():
                tags.add(existing.strip().lower())
        for computed in [_tagify("company", company), _tagify("title", title)]:
            if computed:
                tags.add(computed)

        metadata_json = {
            "company_norm": company,
            "title_norm": title,
            "full_name": full_name,
            "tags": sorted(tags),
            "source": str(event.get("source")),
            "source_event_id": str(event.get("source_event_id")),
        }
        candidates.append(
            MetadataCandidate(
                tenant_id=str(event["tenant_id"]),
                primary_email=primary_email,
                display_name=full_name,
                metadata_json=metadata_json,
            )
        )

    return MetadataExtractResult(candidate_count=len(candidates), candidates=candidates)
