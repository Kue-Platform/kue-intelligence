from __future__ import annotations

import io
import tempfile
import zipfile
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_inngest_dispatcher,
    get_pipeline_store,
    get_raw_event_store,
    get_source_connection_store,
)
from app.ingestion.linkedin_zip_import import extract_linkedin_zip_to_source_events
from app.ingestion.parsers import parse_raw_events
from app.ingestion.pipeline_store import SqlitePipelineStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.source_connection_store import SqliteSourceConnectionStore
from app.main import app
from app.schemas import CanonicalEventType, IngestionSource, RawCapturedEvent

client = TestClient(app)


def _fake_dispatcher(event_id: str):
    async def _dispatch(_: str, __: dict) -> str:
        return event_id

    return _dispatch


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


CONNECTIONS_CSV = (
    "First Name,Last Name,URL,Email Address,Company,Position,Connected On\n"
    "Jane,Doe,https://linkedin.com/in/jane-doe,jane@example.com,Acme,CTO,2023-01-15\n"
    "John,Smith,https://linkedin.com/in/john-smith,,Stripe,Engineer,2022-06-10\n"
)

PROFILE_CSV = (
    "First Name,Last Name,Maiden Name,Address,Birth Date,Headline,Summary,Industry,Zip Code,Geo Location,Twitter Handles,Websites,Instant Messengers\n"
    'Harun,Yilmaz,,"123 Tech St",1990-05-15,CTO & Co-Founder,Building things,Computer Software,94105,"San Francisco, CA",,https://example.com,\n'
)

POSITIONS_CSV = (
    "Company Name,Title,Description,Location,Started On,Finished On\n"
    'Plateron,Co-Founder & CTO,Building POS,"San Francisco, CA",2022-03,\n'
    "Outpace,Lead Engineer,Competitive intel,Remote,2020-01,2022-02\n"
)

EDUCATION_CSV = (
    "School Name,Start Date,End Date,Notes,Degree Name,Activities\n"
    'UIUC,2012-08,2016-05,,BS Computer Science,"ACM, Startup@Illinois"\n'
)

SKILLS_CSV = "Name\nTypeScript\nPython\nGo\n"

RECOMMENDATIONS_CSV = (
    "Recommender,Recommender-Position,Text,Creation Date\n"
    "Sarah Chen,Engineering Manager at Google,Harun is great.,2023-08-14\n"
)

MESSAGES_CSV = (
    "Conversation ID,Conversation Title,From,Sender Profile URL,To,Date,Subject,Content,Folder\n"
    "conv_001,Job opportunity,Jane Doe,https://linkedin.com/in/jane-doe,Harun Yilmaz,2024-01-10,Job opportunity,Hi there!,INBOX\n"
    "conv_002,Coffee chat,Harun Yilmaz,,John Smith,2024-02-15,Re: Coffee chat?,Sure thing,INBOX\n"
)


# ---------------------------------------------------------------------------
# Unit tests: extract_linkedin_zip_to_source_events
# ---------------------------------------------------------------------------


def test_extract_connections_from_zip() -> None:
    zip_bytes = _make_zip({"Connections.csv": CONNECTIONS_CSV})
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    conn_stats = [fr for fr in result.file_results if fr.file_name == "Connections.csv"]
    assert len(conn_stats) == 1
    assert conn_stats[0].rows_total == 2
    assert conn_stats[0].rows_accepted == 2

    conn_events = [e for e in result.source_events if e.source_event_id.startswith("linkedin_zip_conn_")]
    assert len(conn_events) == 2
    assert conn_events[0].payload["name"] == "Jane Doe"
    assert conn_events[0].payload["email"] == "jane@example.com"
    assert conn_events[0].payload["_event_kind"] == "contact"
    assert conn_events[0].source == IngestionSource.LINKEDIN
    # Second connection has no email — should still be accepted
    assert conn_events[1].payload["name"] == "John Smith"
    assert conn_events[1].payload["email"] is None


def test_extract_owner_profile_combines_files() -> None:
    zip_bytes = _make_zip({
        "Profile.csv": PROFILE_CSV,
        "Positions.csv": POSITIONS_CSV,
        "Education.csv": EDUCATION_CSV,
        "Skills.csv": SKILLS_CSV,
        "Recommendations_Received.csv": RECOMMENDATIONS_CSV,
    })
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    owner_events = [e for e in result.source_events if e.source_event_id == "linkedin_zip_owner_profile"]
    assert len(owner_events) == 1
    payload = owner_events[0].payload
    assert payload["name"] == "Harun Yilmaz"
    assert payload["_event_kind"] == "contact"
    assert payload["_is_owner"] is True
    assert payload["linkedin_headline"] == "CTO & Co-Founder"
    assert payload["linkedin_summary"] == "Building things"
    # Positions become organizations
    assert len(payload["organizations"]) == 2
    assert payload["organizations"][0]["name"] == "Plateron"
    assert payload["company"] == "Plateron"
    assert payload["title"] == "Co-Founder & CTO"
    # Education
    assert len(payload["education"]) == 1
    assert payload["education"][0]["school_name"] == "UIUC"
    # Skills
    assert payload["skills"] == ["TypeScript", "Python", "Go"]
    # Recommendations
    assert len(payload["recommendations"]) == 1
    assert payload["recommendations"][0]["recommender"] == "Sarah Chen"


def test_extract_messages_from_zip() -> None:
    zip_bytes = _make_zip({"Messages.csv": MESSAGES_CSV})
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    msg_stats = [fr for fr in result.file_results if fr.file_name == "Messages.csv"]
    assert len(msg_stats) == 1
    assert msg_stats[0].rows_total == 2
    assert msg_stats[0].rows_accepted == 2

    msg_events = [e for e in result.source_events if e.source_event_id.startswith("linkedin_zip_msg_")]
    assert len(msg_events) == 2
    assert msg_events[0].payload["_event_kind"] == "message"
    assert msg_events[0].payload["threadId"] == "conv_001"
    assert msg_events[0].payload["subject"] == "Job opportunity"
    assert msg_events[0].payload["from"] == "Jane Doe"
    assert msg_events[0].payload["to"] == ["Harun Yilmaz"]
    assert msg_events[0].payload["snippet"] == "Hi there!"


def test_full_zip_all_files() -> None:
    zip_bytes = _make_zip({
        "Connections.csv": CONNECTIONS_CSV,
        "Profile.csv": PROFILE_CSV,
        "Positions.csv": POSITIONS_CSV,
        "Education.csv": EDUCATION_CSV,
        "Skills.csv": SKILLS_CSV,
        "Recommendations_Received.csv": RECOMMENDATIONS_CSV,
        "Messages.csv": MESSAGES_CSV,
    })
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    # 2 connections + 1 owner profile + 2 messages = 5
    assert result.total_events == 5
    assert len(result.file_results) == 3  # Connections, Profile (combined), Messages


def test_zip_with_nested_directory() -> None:
    zip_bytes = _make_zip({
        "linkedin_export/Connections.csv": CONNECTIONS_CSV,
        "linkedin_export/Profile.csv": PROFILE_CSV,
    })
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    conn_events = [e for e in result.source_events if e.source_event_id.startswith("linkedin_zip_conn_")]
    assert len(conn_events) == 2


def test_zip_missing_files_still_works() -> None:
    zip_bytes = _make_zip({"Connections.csv": CONNECTIONS_CSV})
    result = extract_linkedin_zip_to_source_events(
        tenant_id="t1", user_id="u1", trace_id="tr1", zip_bytes=zip_bytes,
    )

    assert result.total_events == 2  # just connections
    assert any("Messages.csv not found" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Parser routing tests
# ---------------------------------------------------------------------------


def test_parser_routes_linkedin_message_by_event_kind() -> None:
    now = datetime(2024, 1, 10, tzinfo=UTC)
    raw = RawCapturedEvent(
        raw_event_id=1,
        tenant_id="t1",
        user_id="u1",
        trace_id="tr1",
        source=IngestionSource.LINKEDIN,
        source_event_id="linkedin_zip_msg_1",
        occurred_at=now,
        captured_at=now,
        payload={
            "_event_kind": "message",
            "threadId": "conv_001",
            "subject": "Job opportunity",
            "from": "Jane Doe",
            "to": ["Harun Yilmaz"],
            "snippet": "Hi there!",
        },
    )
    result = parse_raw_events([raw])
    assert len(result.parsed_events) == 1
    assert result.parsed_events[0].event_type == CanonicalEventType.EMAIL_MESSAGE
    assert result.parsed_events[0].normalized["subject"] == "Job opportunity"
    assert result.parsed_events[0].normalized["from"] == "Jane Doe"
    assert result.parsed_events[0].normalized["to"] == ["Harun Yilmaz"]


def test_parser_backward_compat_no_event_kind() -> None:
    now = datetime(2024, 1, 10, tzinfo=UTC)
    raw = RawCapturedEvent(
        raw_event_id=2,
        tenant_id="t1",
        user_id="u1",
        trace_id="tr1",
        source=IngestionSource.LINKEDIN,
        source_event_id="linkedin_csv_1",
        occurred_at=now,
        captured_at=now,
        payload={
            "name": "Jane Doe",
            "email": "jane@example.com",
            "names": [{"displayName": "Jane Doe"}],
            "emailAddresses": [{"value": "jane@example.com"}],
            "organizations": [],
        },
    )
    result = parse_raw_events([raw])
    assert len(result.parsed_events) == 1
    assert result.parsed_events[0].event_type == CanonicalEventType.CONTACT


# ---------------------------------------------------------------------------
# Integration test: endpoint
# ---------------------------------------------------------------------------


def test_endpoint_import_linkedin_zip() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_linkedin_zip_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw.db")
    pipeline_store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    source_conn_store = SqliteSourceConnectionStore(db_path=f"{tmpdir}/pipeline.db")

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: pipeline_store
    app.dependency_overrides[get_source_connection_store] = lambda: source_conn_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_zip_1")

    zip_bytes = _make_zip({
        "Connections.csv": CONNECTIONS_CSV,
        "Profile.csv": PROFILE_CSV,
        "Positions.csv": POSITIONS_CSV,
        "Messages.csv": MESSAGES_CSV,
    })

    try:
        response = client.post(
            "/v1/ingestion/import/linkedin-zip",
            files={"file": ("linkedin_export.zip", zip_bytes, "application/zip")},
            data={"tenant_id": "tenant_1", "user_id": "user_1"},
        )
        assert response.status_code == 200, response.json()
        body = response.json()
        assert body["status"] == "accepted"
        assert body["total_events"] == 5  # 2 conn + 1 owner + 2 msg
        assert len(body["file_stats"]) == 3
        assert body["trace_id"].startswith("trace_")
    finally:
        app.dependency_overrides.clear()


def test_endpoint_rejects_non_zip() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_linkedin_zip_bad_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw.db")
    pipeline_store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    source_conn_store = SqliteSourceConnectionStore(db_path=f"{tmpdir}/pipeline.db")

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: pipeline_store
    app.dependency_overrides[get_source_connection_store] = lambda: source_conn_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_zip_bad")

    try:
        response = client.post(
            "/v1/ingestion/import/linkedin-zip",
            files={"file": ("not_a_zip.txt", b"hello world", "text/plain")},
            data={"tenant_id": "tenant_1", "user_id": "user_1"},
        )
        assert response.status_code == 400
        assert "not a valid ZIP" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()
