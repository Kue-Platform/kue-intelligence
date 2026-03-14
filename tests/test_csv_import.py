import json
import tempfile
import importlib.util
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_inngest_dispatcher,
    get_pipeline_store,
    get_raw_event_store,
    get_source_connection_store,
)
from app.ingestion.csv_import import normalize_csv_rows_to_source_events, normalize_csv_text_to_source_events
from app.ingestion.parsers import parse_raw_events
from app.ingestion.pipeline_store import SqlitePipelineStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.source_connection_store import SqliteSourceConnectionStore
from app.main import app
from app.schemas import CsvImportSourceHint, IngestionSource, RawCapturedEvent

client = TestClient(app)


def _fake_dispatcher(event_id: str):
    async def _dispatch(_: str, __: dict) -> str:
        return event_id

    return _dispatch


def test_csv_normalizer_detects_linkedin_template() -> None:
    output = normalize_csv_text_to_source_events(
        tenant_id="tenant_1",
        user_id="user_1",
        trace_id="trace_csv_1",
        source_hint=CsvImportSourceHint.LINKEDIN,
        csv_text=(
            "First Name,Last Name,Email Address,Company,Position,Location,Public Profile URL\n"
            "Alan,Turing,alan@example.com,Venture Labs,Partner,San Francisco,https://linkedin.com/in/alan\n"
        ),
    )

    assert output.template_detected == "linkedin_export"
    assert output.mapping_mode == "template"
    assert output.rows_total == 1
    assert output.rows_accepted == 1
    assert output.source_events[0].source.value == "linkedin"


def test_csv_normalizer_unknown_template_with_column_map() -> None:
    output = normalize_csv_rows_to_source_events(
        tenant_id="tenant_1",
        user_id="user_1",
        trace_id="trace_csv_2",
        source_hint=CsvImportSourceHint.CSV_IMPORT,
        rows=[{"Full Name": "Harun Rashid", "Mail": "harun@example.com"}],
        headers=["Full Name", "Mail"],
        column_map={"display_name": "Full Name", "primary_email": "Mail"},
    )

    assert output.template_detected == "unknown"
    assert output.mapping_mode == "column_map"
    assert output.rows_accepted == 1
    assert output.rows_skipped == 0


def test_import_csv_mock_store_first_success() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_csv_mock_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw.db")
    pipeline_store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    source_conn_store = SqliteSourceConnectionStore(db_path=f"{tmpdir}/pipeline.db")

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: pipeline_store
    app.dependency_overrides[get_source_connection_store] = lambda: source_conn_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_csv_mock_1")

    response = client.post(
        "/v1/ingestion/import/csv/mock",
        json={
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "source_hint": "linkedin",
            "rows": [
                {
                    "First Name": "Rachel",
                    "Last Name": "Lee",
                    "Email Address": "rachel@stripe.com",
                    "Company": "Stripe",
                    "Position": "Engineering Manager",
                    "Public Profile URL": "https://linkedin.com/in/rachel",
                }
            ],
            "file_name": "linkedin_export.csv",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["event_id"] == "evt_csv_mock_1"
    assert body["rows_total"] == 1
    assert body["rows_accepted"] == 1
    assert body["rows_skipped"] == 0
    assert body["template_detected"] == "linkedin_export"


def test_import_csv_mock_hard_fail_when_all_rows_invalid() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_csv_mock_invalid_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw.db")
    pipeline_store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    source_conn_store = SqliteSourceConnectionStore(db_path=f"{tmpdir}/pipeline.db")

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: pipeline_store
    app.dependency_overrides[get_source_connection_store] = lambda: source_conn_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_csv_mock_2")

    response = client.post(
        "/v1/ingestion/import/csv/mock",
        json={
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "source_hint": "csv_import",
            "rows": [{"Foo": "", "Bar": ""}],
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["rows_total"] == 1
    assert detail["rows_skipped"] == 1


def test_import_csv_multipart_success() -> None:
    if importlib.util.find_spec("multipart") is None:
        return
    tmpdir = tempfile.mkdtemp(prefix="kue_csv_upload_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw.db")
    pipeline_store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    source_conn_store = SqliteSourceConnectionStore(db_path=f"{tmpdir}/pipeline.db")

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: pipeline_store
    app.dependency_overrides[get_source_connection_store] = lambda: source_conn_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_csv_upload_1")

    csv_content = (
        "Name,E-mail 1 - Value,Organization 1 - Name,Organization 1 - Title\n"
        "Harun Rashid,harun@example.com,Kue,Founder\n"
    )
    response = client.post(
        "/v1/ingestion/import/csv",
        files={"file": ("google_contacts.csv", csv_content, "text/csv")},
        data={
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "source_hint": "google_contacts",
            "column_map": json.dumps({"display_name": "Name", "primary_email": "E-mail 1 - Value"}),
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["event_id"] == "evt_csv_upload_1"
    assert body["rows_total"] == 1
    assert body["rows_accepted"] == 1


def test_parser_supports_linkedin_and_csv_import_as_contact_events() -> None:
    raw_events = [
        RawCapturedEvent(
            raw_event_id=1,
            tenant_id="tenant_1",
            user_id="user_1",
            source=IngestionSource.LINKEDIN,
            source_event_id="linkedin_csv_1",
            occurred_at=datetime.now(UTC),
            trace_id="trace_parser_csv_1",
            payload={"name": "Alan Turing", "email": "alan@example.com"},
            captured_at=datetime.now(UTC),
        ),
        RawCapturedEvent(
            raw_event_id=2,
            tenant_id="tenant_1",
            user_id="user_1",
            source=IngestionSource.CSV_IMPORT,
            source_event_id="csv_import_1",
            occurred_at=datetime.now(UTC),
            trace_id="trace_parser_csv_2",
            payload={"name": "Rachel Lee", "email": "rachel@example.com"},
            captured_at=datetime.now(UTC),
        ),
    ]
    result = parse_raw_events(raw_events)
    assert len(result.failures) == 0
    assert len(result.parsed_events) == 2
    assert all(str(item.event_type) == "contact" for item in result.parsed_events)
