from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.schemas import IngestionSource, SourceEvent


@dataclass
class LinkedInFileResult:
    file_name: str
    rows_total: int = 0
    rows_accepted: int = 0
    rows_skipped: int = 0
    warning_samples: list[str] = field(default_factory=list)


@dataclass
class LinkedInZipOutput:
    file_results: list[LinkedInFileResult]
    source_events: list[SourceEvent]
    total_events: int
    warnings: list[str]


def _read_csv_from_zip(zf: zipfile.ZipFile, path: str) -> str | None:
    try:
        raw = zf.read(path)
        return raw.decode("utf-8-sig")
    except (KeyError, UnicodeDecodeError):
        try:
            return raw.decode("latin-1")  # type: ignore[possibly-undefined]
        except Exception:
            return None


def _parse_csv_text(text: str) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(text))
    headers = list(reader.fieldnames or [])
    rows: list[dict[str, str]] = []
    for row in reader:
        rows.append({str(k): (v if v is not None else "") for k, v in row.items() if k is not None})
    return headers, rows


def _parse_date(value: str | None) -> datetime:
    if not value or not value.strip():
        return datetime.now(UTC)
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Connections.csv
# ---------------------------------------------------------------------------


def _parse_connections_csv(
    csv_text: str,
    tenant_id: str,
    user_id: str,
    trace_id: str,
) -> tuple[LinkedInFileResult, list[SourceEvent]]:
    _, rows = _parse_csv_text(csv_text)
    result = LinkedInFileResult(file_name="Connections.csv", rows_total=len(rows))
    events: list[SourceEvent] = []

    for idx, row in enumerate(rows, start=1):
        first = (row.get("First Name") or "").strip()
        last = (row.get("Last Name") or "").strip()
        display_name = f"{first} {last}".strip() or None
        email = (row.get("Email Address") or "").strip() or None
        company = (row.get("Company") or "").strip() or None
        title = (row.get("Position") or "").strip() or None
        linkedin_url = (row.get("URL") or "").strip() or None
        connected_on = row.get("Connected On")

        if not display_name and not email:
            result.rows_skipped += 1
            result.warning_samples.append(f"row {idx}: missing_name_and_email")
            continue

        if not display_name:
            display_name = email or "Unknown"

        payload: dict[str, Any] = {
            "_event_kind": "contact",
            "name": display_name,
            "email": email,
            "company": company,
            "title": title,
            "linkedin_url": linkedin_url,
            "names": [{"displayName": display_name}],
            "emailAddresses": [{"value": email}] if email else [],
            "organizations": [{"name": company, "title": title}] if company or title else [],
            "csv_row_index": idx,
        }

        events.append(
            SourceEvent(
                tenant_id=tenant_id,
                user_id=user_id,
                source=IngestionSource.LINKEDIN,
                source_event_id=f"linkedin_zip_conn_{idx}",
                occurred_at=_parse_date(connected_on),
                trace_id=trace_id,
                payload=payload,
            )
        )
        result.rows_accepted += 1

    return result, events


# ---------------------------------------------------------------------------
# Owner profile: Profile.csv + Positions.csv + Education.csv + Skills.csv
#                 + Recommendations_Received.csv
# ---------------------------------------------------------------------------


def _parse_owner_profile(
    profile_text: str | None,
    positions_text: str | None,
    education_text: str | None,
    skills_text: str | None,
    recommendations_text: str | None,
    tenant_id: str,
    user_id: str,
    trace_id: str,
) -> tuple[LinkedInFileResult, list[SourceEvent]]:
    result = LinkedInFileResult(file_name="Profile (combined)")

    if not profile_text:
        result.warning_samples.append("Profile.csv not found in ZIP")
        return result, []

    _, profile_rows = _parse_csv_text(profile_text)
    if not profile_rows:
        result.rows_total = 0
        result.rows_skipped = 0
        result.warning_samples.append("Profile.csv is empty")
        return result, []

    result.rows_total = 1
    p = profile_rows[0]
    first = (p.get("First Name") or "").strip()
    last = (p.get("Last Name") or "").strip()
    display_name = f"{first} {last}".strip() or "Unknown"
    headline = (p.get("Headline") or "").strip() or None
    summary = (p.get("Summary") or "").strip() or None
    industry = (p.get("Industry") or "").strip() or None
    geo = (p.get("Geo Location") or "").strip() or None

    # Positions
    organizations: list[dict[str, Any]] = []
    if positions_text:
        _, pos_rows = _parse_csv_text(positions_text)
        for pr in pos_rows:
            organizations.append({
                "name": (pr.get("Company Name") or "").strip() or None,
                "title": (pr.get("Title") or "").strip() or None,
                "description": (pr.get("Description") or "").strip() or None,
                "location": (pr.get("Location") or "").strip() or None,
                "started_on": (pr.get("Started On") or "").strip() or None,
                "finished_on": (pr.get("Finished On") or "").strip() or None,
            })

    # Education
    education: list[dict[str, Any]] = []
    if education_text:
        _, edu_rows = _parse_csv_text(education_text)
        for er in edu_rows:
            education.append({
                "school_name": (er.get("School Name") or "").strip() or None,
                "degree_name": (er.get("Degree Name") or "").strip() or None,
                "start_date": (er.get("Start Date") or "").strip() or None,
                "end_date": (er.get("End Date") or "").strip() or None,
                "activities": (er.get("Activities") or "").strip() or None,
                "notes": (er.get("Notes") or "").strip() or None,
            })

    # Skills
    skills: list[str] = []
    if skills_text:
        _, skill_rows = _parse_csv_text(skills_text)
        for sr in skill_rows:
            name = (sr.get("Name") or "").strip()
            if name:
                skills.append(name)

    # Recommendations
    recommendations: list[dict[str, str | None]] = []
    if recommendations_text:
        _, rec_rows = _parse_csv_text(recommendations_text)
        for rr in rec_rows:
            recommendations.append({
                "recommender": (rr.get("Recommender") or "").strip() or None,
                "recommender_position": (rr.get("Recommender-Position") or "").strip() or None,
                "text": (rr.get("Text") or "").strip() or None,
                "date": (rr.get("Creation Date") or "").strip() or None,
            })

    # First org is current company/title — _parse_contact() picks this up
    company = organizations[0]["name"] if organizations else None
    title = organizations[0]["title"] if organizations else None

    payload: dict[str, Any] = {
        "_event_kind": "contact",
        "_is_owner": True,
        "name": display_name,
        "email": None,
        "company": company,
        "title": title,
        "location": geo,
        "names": [{"displayName": display_name}],
        "emailAddresses": [],
        "organizations": organizations,
        "linkedin_headline": headline,
        "linkedin_summary": summary,
        "linkedin_industry": industry,
        "education": education,
        "skills": skills,
        "recommendations": recommendations,
    }

    event = SourceEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        source=IngestionSource.LINKEDIN,
        source_event_id="linkedin_zip_owner_profile",
        occurred_at=datetime.now(UTC),
        trace_id=trace_id,
        payload=payload,
    )
    result.rows_accepted = 1
    return result, [event]


# ---------------------------------------------------------------------------
# Messages.csv
# ---------------------------------------------------------------------------


def _parse_messages_csv(
    csv_text: str,
    tenant_id: str,
    user_id: str,
    trace_id: str,
) -> tuple[LinkedInFileResult, list[SourceEvent]]:
    _, rows = _parse_csv_text(csv_text)
    result = LinkedInFileResult(file_name="Messages.csv", rows_total=len(rows))
    events: list[SourceEvent] = []

    for idx, row in enumerate(rows, start=1):
        conversation_id = (row.get("Conversation ID") or "").strip()
        conversation_title = (row.get("Conversation Title") or "").strip()
        from_name = (row.get("From") or "").strip()
        sender_url = (row.get("Sender Profile URL") or "").strip() or None
        to_name = (row.get("To") or "").strip()
        date_str = (row.get("Date") or "").strip()
        subject = (row.get("Subject") or "").strip() or None
        content = (row.get("Content") or "").strip()

        if not from_name and not to_name:
            result.rows_skipped += 1
            result.warning_samples.append(f"row {idx}: missing_from_and_to")
            continue

        payload: dict[str, Any] = {
            "_event_kind": "message",
            "threadId": conversation_id or None,
            "subject": subject or conversation_title or None,
            "from": from_name or None,
            "to": [to_name] if to_name else [],
            "snippet": content[:500] if content else None,
            "from_profile_url": sender_url,
        }

        events.append(
            SourceEvent(
                tenant_id=tenant_id,
                user_id=user_id,
                source=IngestionSource.LINKEDIN,
                source_event_id=f"linkedin_zip_msg_{idx}",
                occurred_at=_parse_date(date_str),
                trace_id=trace_id,
                payload=payload,
            )
        )
        result.rows_accepted += 1

    return result, events


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

# Map basenames (lowercased) to the files we care about
_RECOGNIZED_BASENAMES = {
    "connections.csv",
    "profile.csv",
    "positions.csv",
    "education.csv",
    "skills.csv",
    "recommendations_received.csv",
    "messages.csv",
}


def _find_file(zf: zipfile.ZipFile, basename: str) -> str | None:
    """Find a file in the ZIP by case-insensitive basename match."""
    for name in zf.namelist():
        if name.lower().endswith("/" + basename) or name.lower() == basename:
            return name
    return None


def extract_linkedin_zip_to_source_events(
    *,
    tenant_id: str,
    user_id: str,
    trace_id: str,
    zip_bytes: bytes,
) -> LinkedInZipOutput:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    all_events: list[SourceEvent] = []
    file_results: list[LinkedInFileResult] = []
    warnings: list[str] = []

    # --- Connections ---
    connections_path = _find_file(zf, "connections.csv")
    if connections_path:
        text = _read_csv_from_zip(zf, connections_path)
        if text:
            fr, evts = _parse_connections_csv(text, tenant_id, user_id, trace_id)
            file_results.append(fr)
            all_events.extend(evts)
        else:
            warnings.append("Connections.csv: could not decode")
    else:
        warnings.append("Connections.csv not found in ZIP")

    # --- Owner profile (combines multiple files) ---
    profile_text = None
    positions_text = None
    education_text = None
    skills_text = None
    recs_text = None

    for basename, setter in [
        ("profile.csv", "profile_text"),
        ("positions.csv", "positions_text"),
        ("education.csv", "education_text"),
        ("skills.csv", "skills_text"),
        ("recommendations_received.csv", "recs_text"),
    ]:
        path = _find_file(zf, basename)
        if path:
            content = _read_csv_from_zip(zf, path)
            if setter == "profile_text":
                profile_text = content
            elif setter == "positions_text":
                positions_text = content
            elif setter == "education_text":
                education_text = content
            elif setter == "skills_text":
                skills_text = content
            elif setter == "recs_text":
                recs_text = content

    fr, evts = _parse_owner_profile(
        profile_text, positions_text, education_text, skills_text, recs_text,
        tenant_id, user_id, trace_id,
    )
    file_results.append(fr)
    all_events.extend(evts)

    # --- Messages ---
    messages_path = _find_file(zf, "messages.csv")
    if messages_path:
        text = _read_csv_from_zip(zf, messages_path)
        if text:
            fr, evts = _parse_messages_csv(text, tenant_id, user_id, trace_id)
            file_results.append(fr)
            all_events.extend(evts)
        else:
            warnings.append("Messages.csv: could not decode")
    else:
        warnings.append("Messages.csv not found in ZIP")

    return LinkedInZipOutput(
        file_results=file_results,
        source_events=all_events,
        total_events=len(all_events),
        warnings=warnings,
    )
