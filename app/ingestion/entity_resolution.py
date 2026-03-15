from __future__ import annotations

import re
from collections import defaultdict
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
    phones: list[str] = []
    secondary_emails: list[str] = []
    linkedin_url: str | None = None
    name_norm: str | None = None
    email_domain: str | None = None


class ExactMatchResult(BaseModel):
    stage: Literal["entity_resolution"] = "entity_resolution"
    candidate_count: int
    candidates: list[EntityCandidate]


class MergeResult(BaseModel):
    stage: Literal["entity_resolution"] = "entity_resolution"
    resolved_count: int
    resolved_entities: list[EntityCandidate]


class MatchSignal(BaseModel):
    signal_type: str
    confidence: float
    evidence: dict[str, Any] = {}


class ResolvedEntityGroup(BaseModel):
    canonical: EntityCandidate
    members: list[EntityCandidate]
    signals: list[MatchSignal]
    min_confidence: float


class MultiSignalResolutionResult(BaseModel):
    stage: Literal["entity_resolution"] = "entity_resolution"
    groups: list[ResolvedEntityGroup]
    resolved_count: int
    merge_candidate_count: int
    merge_candidates: list[dict[str, Any]] = []


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


def _normalize_phone(value: str) -> str | None:
    digits = re.sub(r"\D", "", value)
    digits = digits[-10:]
    if len(digits) < 7:
        return None
    return digits


def _compute_name_norm(display_name: str) -> str | None:
    norm = " ".join(display_name.strip().lower().split())
    return norm if norm and norm != "unknown" else None


def _compute_email_domain(email: str | None) -> str | None:
    if email and "@" in email:
        return email.split("@")[1].lower()
    return None


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
        secondary_emails = (
            [str(e).strip().lower() for e in emails[1:]]
            if isinstance(emails, list) and len(emails) > 1
            else []
        )
        display_name = str(normalized.get("full_name") or "Unknown").strip() or "Unknown"
        phones_raw = normalized.get("phones", [])
        phones = [str(p) for p in phones_raw] if isinstance(phones_raw, list) else []
        linkedin_url = normalized.get("linkedin_url")
        if linkedin_url is not None:
            linkedin_url = str(linkedin_url).strip() or None
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
                phones=phones,
                secondary_emails=secondary_emails,
                linkedin_url=linkedin_url,
                name_norm=_compute_name_norm(display_name),
                email_domain=_compute_email_domain(primary_email),
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


def _count_non_null_fields(c: EntityCandidate) -> int:
    count = 0
    if c.primary_email:
        count += 1
    if c.company_norm:
        count += 1
    if c.title_norm:
        count += 1
    if c.phones:
        count += len(c.phones)
    if c.secondary_emails:
        count += len(c.secondary_emails)
    if c.linkedin_url:
        count += 1
    if c.name_norm:
        count += 1
    if c.email_domain:
        count += 1
    return count


def resolve_entities_multi_signal(
    candidates: list[EntityCandidate],
    *,
    auto_merge_threshold: float = 0.85,
) -> MultiSignalResolutionResult:
    if not candidates:
        return MultiSignalResolutionResult(
            groups=[], resolved_count=0, merge_candidate_count=0, merge_candidates=[]
        )

    n = len(candidates)
    uf = _UnionFind(n)
    merge_signals: dict[tuple[int, int], list[MatchSignal]] = defaultdict(list)

    # Step A — Build signal indices
    email_index: dict[str, list[int]] = defaultdict(list)
    phone_index: dict[str, list[int]] = defaultdict(list)
    linkedin_index: dict[str, list[int]] = defaultdict(list)
    name_company_index: dict[tuple[str, str], list[int]] = defaultdict(list)
    name_domain_index: dict[tuple[str, str], list[int]] = defaultdict(list)

    for i, c in enumerate(candidates):
        if c.primary_email:
            email_index[c.primary_email].append(i)
        for se in c.secondary_emails:
            if se:
                email_index[se].append(i)
        for phone in c.phones:
            np = _normalize_phone(phone)
            if np:
                phone_index[np].append(i)
        if c.linkedin_url:
            linkedin_index[c.linkedin_url].append(i)
        if c.name_norm and c.company_norm:
            name_company_index[(c.name_norm, c.company_norm)].append(i)
        if c.name_norm and c.email_domain:
            name_domain_index[(c.name_norm, c.email_domain)].append(i)

    # Step B — Union-Find merges
    def _union_index(
        index: dict[Any, list[int]], signal_type: str, confidence: float
    ) -> None:
        for key, indices in index.items():
            if len(indices) < 2:
                continue
            anchor = indices[0]
            for other in indices[1:]:
                uf.union(anchor, other)
                pair = (min(anchor, other), max(anchor, other))
                merge_signals[pair].append(
                    MatchSignal(
                        signal_type=signal_type,
                        confidence=confidence,
                        evidence={"key": key if isinstance(key, str) else list(key)},
                    )
                )

    _union_index(email_index, "email", 0.99)
    _union_index(linkedin_index, "linkedin_url", 0.99)
    _union_index(name_company_index, "name_company", 0.90)
    _union_index(name_domain_index, "name_domain", 0.90)
    _union_index(phone_index, "phone", 0.85)

    # Step C — Extract connected components
    components: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        components[uf.find(i)].append(i)

    groups: list[ResolvedEntityGroup] = []
    merge_candidates_list: list[dict[str, Any]] = []

    for _root, member_indices in components.items():
        members = [candidates[i] for i in member_indices]

        # Collect all signals for this group
        group_signals: list[MatchSignal] = []
        for i_idx, i_val in enumerate(member_indices):
            for j_val in member_indices[i_idx + 1 :]:
                pair = (min(i_val, j_val), max(i_val, j_val))
                if pair in merge_signals:
                    group_signals.extend(merge_signals[pair])

        # Choose canonical: most non-null fields, prefer google_contacts, prefer has email
        canonical = max(
            members,
            key=lambda c: (
                _count_non_null_fields(c),
                1 if c.source == "google_contacts" else 0,
                1 if c.primary_email else 0,
            ),
        )

        if len(member_indices) == 1:
            # Singleton — always a resolved group, no signals needed
            groups.append(
                ResolvedEntityGroup(
                    canonical=canonical,
                    members=members,
                    signals=[],
                    min_confidence=1.0,
                )
            )
            continue

        min_confidence = min(s.confidence for s in group_signals) if group_signals else 1.0

        if min_confidence < auto_merge_threshold:
            merge_candidates_list.append(
                {
                    "canonical": canonical.model_dump(),
                    "members": [m.model_dump() for m in members],
                    "signals": [s.model_dump() for s in group_signals],
                    "min_confidence": min_confidence,
                }
            )
        else:
            groups.append(
                ResolvedEntityGroup(
                    canonical=canonical,
                    members=members,
                    signals=group_signals,
                    min_confidence=min_confidence,
                )
            )

    return MultiSignalResolutionResult(
        groups=groups,
        resolved_count=sum(len(g.members) for g in groups),
        merge_candidate_count=len(merge_candidates_list),
        merge_candidates=merge_candidates_list,
    )
