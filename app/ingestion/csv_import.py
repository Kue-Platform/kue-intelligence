from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.schemas import CsvImportSourceHint, IngestionSource, SourceEvent


CANONICAL_FIELDS = (
    "display_name",
    "primary_email",
    "company",
    "title",
    "location",
    "linkedin_url",
)


ALIASES: dict[str, tuple[str, ...]] = {
    "display_name": (
        "name",
        "full_name",
        "display_name",
        "first_name_last_name",
        "first_name last_name",
    ),
    "primary_email": (
        "email",
        "email_address",
        "e_mail",
        "e_mail_1_value",
        "e_mail_1___value",
        "email_1_value",
        "email_1___value",
    ),
    "company": ("company", "company_name", "organization", "organization_1_name"),
    "title": ("title", "position", "job_title", "organization_1_title"),
    "location": ("location", "address", "address_1_formatted", "city"),
    "linkedin_url": ("linkedin_url", "profile_url", "public_profile_url", "url"),
}


LINKEDIN_TEMPLATE_KEYS = {
    "first_name",
    "last_name",
    "email_address",
    "company",
}

GOOGLE_CONTACTS_TEMPLATE_KEYS = {
    "name",
    "e_mail_1_value",
}


@dataclass
class CsvNormalizeOutput:
    source: IngestionSource
    template_detected: str
    mapping_mode: str
    rows_total: int
    rows_accepted: int
    rows_skipped: int
    warning_samples: list[str]
    source_events: list[SourceEvent]


def _canon_header(value: str) -> str:
    lowered = value.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "_", lowered)
    return lowered.strip("_")


def _detect_template(header_keys: set[str]) -> str:
    if LINKEDIN_TEMPLATE_KEYS.issubset(header_keys):
        return "linkedin_export"
    if GOOGLE_CONTACTS_TEMPLATE_KEYS.issubset(header_keys):
        return "google_contacts_export"
    return "unknown"


def _resolve_mapping(
    headers: list[str],
    template_detected: str,
    source_hint: CsvImportSourceHint,
    column_map: dict[str, str] | None,
) -> tuple[dict[str, str], str]:
    by_canon = {_canon_header(h): h for h in headers}

    if column_map:
        mapping: dict[str, str] = {}
        for field in CANONICAL_FIELDS:
            raw = column_map.get(field)
            if not raw:
                continue
            if raw in headers:
                mapping[field] = raw
                continue
            canon = _canon_header(raw)
            if canon in by_canon:
                mapping[field] = by_canon[canon]
        return mapping, "column_map"

    mapping: dict[str, str] = {}
    if template_detected == "linkedin_export":
        for canonical, candidate in (
            ("display_name", "name"),
            ("primary_email", "email_address"),
            ("company", "company"),
            ("title", "position"),
            ("location", "location"),
            ("linkedin_url", "public_profile_url"),
        ):
            if candidate in by_canon:
                mapping[canonical] = by_canon[candidate]
        return mapping, "template"

    if template_detected == "google_contacts_export" or source_hint == CsvImportSourceHint.GOOGLE_CONTACTS:
        for canonical, candidate in (
            ("display_name", "name"),
            ("primary_email", "e_mail_1_value"),
            ("company", "organization_1_name"),
            ("title", "organization_1_title"),
            ("location", "address_1_formatted"),
        ):
            if candidate in by_canon:
                mapping[canonical] = by_canon[candidate]
        return mapping, "template"

    for field, aliases in ALIASES.items():
        for alias in aliases:
            if alias in by_canon:
                mapping[field] = by_canon[alias]
                break
    return mapping, "alias_inference"


def _parse_csv_rows(content: str) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(content))
    headers = list(reader.fieldnames or [])
    rows: list[dict[str, str]] = []
    for row in reader:
        rows.append({str(k): (v if v is not None else "") for k, v in row.items() if k is not None})
    return headers, rows


def _source_from_hint(source_hint: CsvImportSourceHint) -> IngestionSource:
    if source_hint == CsvImportSourceHint.LINKEDIN:
        return IngestionSource.LINKEDIN
    if source_hint == CsvImportSourceHint.GOOGLE_CONTACTS:
        return IngestionSource.GOOGLE_CONTACTS
    return IngestionSource.CSV_IMPORT


def normalize_csv_rows_to_source_events(
    *,
    tenant_id: str,
    user_id: str,
    trace_id: str,
    source_hint: CsvImportSourceHint,
    rows: list[dict[str, Any]],
    headers: list[str] | None = None,
    column_map: dict[str, str] | None = None,
) -> CsvNormalizeOutput:
    all_headers = headers or sorted({str(k) for row in rows for k in row.keys()})
    header_keys = {_canon_header(h) for h in all_headers}
    template_detected = _detect_template(header_keys)
    mapping, mapping_mode = _resolve_mapping(all_headers, template_detected, source_hint, column_map)
    source = _source_from_hint(source_hint)

    warning_samples: list[str] = []
    source_events: list[SourceEvent] = []
    skipped = 0
    now = datetime.now(UTC)

    for idx, row in enumerate(rows, start=1):
        def pick(field: str) -> str | None:
            header = mapping.get(field)
            if not header:
                return None
            value = row.get(header)
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        display_name = pick("display_name")
        primary_email = pick("primary_email")
        company = pick("company")
        title = pick("title")
        location = pick("location")
        linkedin_url = pick("linkedin_url")

        row_warnings: list[str] = []
        if not display_name and not primary_email:
            skipped += 1
            row_warnings.append("missing_name_and_email")
            warning_samples.append(f"row {idx}: missing_name_and_email")
            continue
        if not display_name:
            display_name = primary_email or "Unknown"
            row_warnings.append("missing_name")
        if linkedin_url and not linkedin_url.lower().startswith("http"):
            row_warnings.append("malformed_linkedin_url")
            warning_samples.append(f"row {idx}: malformed_linkedin_url")

        payload = {
            "name": display_name,
            "email": primary_email,
            "company": company,
            "title": title,
            "location": location,
            "linkedin_url": linkedin_url,
            "names": [{"displayName": display_name}],
            "emailAddresses": [{"value": primary_email}] if primary_email else [],
            "organizations": [{"name": company, "title": title}] if company or title else [],
            "csv_row_index": idx,
            "csv_warnings": row_warnings,
        }

        source_events.append(
            SourceEvent(
                tenant_id=tenant_id,
                user_id=user_id,
                source=source,
                source_event_id=f"{source.value}_csv_{idx}",
                occurred_at=now,
                trace_id=trace_id,
                payload=payload,
            )
        )

    return CsvNormalizeOutput(
        source=source,
        template_detected=template_detected,
        mapping_mode=mapping_mode,
        rows_total=len(rows),
        rows_accepted=len(source_events),
        rows_skipped=skipped,
        warning_samples=warning_samples[:20],
        source_events=source_events,
    )


def normalize_csv_text_to_source_events(
    *,
    tenant_id: str,
    user_id: str,
    trace_id: str,
    source_hint: CsvImportSourceHint,
    csv_text: str,
    column_map: dict[str, str] | None = None,
) -> CsvNormalizeOutput:
    headers, rows = _parse_csv_rows(csv_text)
    return normalize_csv_rows_to_source_events(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=trace_id,
        source_hint=source_hint,
        rows=rows,
        headers=headers,
        column_map=column_map,
    )
