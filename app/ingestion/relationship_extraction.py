from __future__ import annotations

from collections import defaultdict
from typing import Any, Literal

from pydantic import BaseModel


class InteractionCandidate(BaseModel):
    tenant_id: str
    source: str
    touchpoint_type: str
    occurred_at: str
    actor_email: str | None = None
    target_email: str | None = None
    topic: str | None = None
    payload_json: dict[str, Any] = {}


class RelationshipAggregate(BaseModel):
    tenant_id: str
    from_email: str
    to_email: str
    relationship_type: str = "knows"
    interaction_count: int
    strength: float
    first_interaction_at: str
    last_interaction_at: str
    evidence_json: list[dict[str, Any]]


class InteractionExtractResult(BaseModel):
    stage: Literal["relationship_extraction"] = "relationship_extraction"
    interaction_count: int
    interactions: list[InteractionCandidate]


class RelationshipStrengthResult(BaseModel):
    stage: Literal["relationship_extraction"] = "relationship_extraction"
    relationship_count: int
    relationships: list[RelationshipAggregate]


def _norm_email(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    if "<" in v and ">" in v:
        start = v.find("<")
        end = v.find(">", start + 1)
        if start >= 0 and end > start:
            v = v[start + 1 : end]
    return v if v else None


def extract_interactions(enrichment_payload: dict[str, Any]) -> InteractionExtractResult:
    parsed_events = list(enrichment_payload.get("parsed_events", []))
    interactions: list[InteractionCandidate] = []

    for event in parsed_events:
        normalized = dict(event.get("normalized", {}))
        source = str(event.get("source"))
        occurred_at = str(event.get("occurred_at"))
        tenant_id = str(event.get("tenant_id"))
        event_type = str(event.get("event_type"))

        if event_type == "email_message":
            actor = _norm_email(normalized.get("from"))
            recipients = normalized.get("to", [])
            if actor and isinstance(recipients, list):
                for target in recipients:
                    target_email = _norm_email(str(target))
                    if not target_email or target_email == actor:
                        continue
                    interactions.append(
                        InteractionCandidate(
                            tenant_id=tenant_id,
                            source=source,
                            touchpoint_type="email",
                            occurred_at=occurred_at,
                            actor_email=actor,
                            target_email=target_email,
                            topic=normalized.get("subject"),
                            payload_json={"thread_id": normalized.get("thread_id")},
                        )
                    )

        elif event_type == "calendar_event":
            attendees = normalized.get("attendee_emails", [])
            if isinstance(attendees, list) and len(attendees) >= 2:
                cleaned = [x for x in (_norm_email(str(a)) for a in attendees) if x]
                for i, actor in enumerate(cleaned):
                    for j, target in enumerate(cleaned):
                        if i == j:
                            continue
                        interactions.append(
                            InteractionCandidate(
                                tenant_id=tenant_id,
                                source=source,
                                touchpoint_type="meeting",
                                occurred_at=occurred_at,
                                actor_email=actor,
                                target_email=target,
                                topic=normalized.get("summary"),
                                payload_json={"status": normalized.get("status")},
                            )
                        )

    return InteractionExtractResult(interaction_count=len(interactions), interactions=interactions)


def compute_relationship_strength(extract_payload: dict[str, Any]) -> RelationshipStrengthResult:
    interactions = [
        InteractionCandidate.model_validate(item)
        for item in list(extract_payload.get("interactions", []))
    ]
    grouped: dict[tuple[str, str, str], list[InteractionCandidate]] = defaultdict(list)
    for item in interactions:
        if not item.actor_email or not item.target_email:
            continue
        key = (item.tenant_id, item.actor_email, item.target_email)
        grouped[key].append(item)

    relationships: list[RelationshipAggregate] = []
    for (tenant_id, from_email, to_email), items in grouped.items():
        ordered = sorted(items, key=lambda x: x.occurred_at)
        count = len(ordered)
        relationships.append(
            RelationshipAggregate(
                tenant_id=tenant_id,
                from_email=from_email,
                to_email=to_email,
                interaction_count=count,
                strength=round(min(1.0, count / 10.0), 4),
                first_interaction_at=ordered[0].occurred_at,
                last_interaction_at=ordered[-1].occurred_at,
                evidence_json=[
                    {
                        "touchpoint_type": x.touchpoint_type,
                        "source": x.source,
                        "occurred_at": x.occurred_at,
                        "topic": x.topic,
                    }
                    for x in ordered[-5:]
                ],
            )
        )

    return RelationshipStrengthResult(
        relationship_count=len(relationships),
        relationships=relationships,
    )
