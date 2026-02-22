import tempfile
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_canonical_event_store, get_raw_event_store
from app.ingestion.canonical_store import SqliteCanonicalEventStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer3_parse_contacts_gmail_calendar() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_")
    store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    canonical_store = SqliteCanonicalEventStore(db_path=f"{tmpdir}/canonical_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: store
    app.dependency_overrides[get_canonical_event_store] = lambda: canonical_store

    trace_id = "trace_layer3_ok"
    now = datetime.now(UTC)
    store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_1",
                    "names": [{"displayName": "Alan Turing"}],
                    "emailAddresses": [{"value": "Alan@Example.com"}],
                    "organizations": [{"name": "Stripe", "title": "VC"}],
                },
            ),
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GMAIL,
                source_event_id="msg_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "id": "msg_1",
                    "internalDate": "1735725600000",
                    "snippet": "Quick question",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Intro"},
                            {"name": "From", "value": "harun@example.com"},
                            {"name": "To", "value": "alan@example.com"},
                        ]
                    },
                },
            ),
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CALENDAR,
                source_event_id="evt_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "id": "evt_1",
                    "summary": "Coffee",
                    "start": {"dateTime": "2025-01-02T12:00:00Z"},
                    "end": {"dateTime": "2025-01-02T12:30:00Z"},
                    "attendees": [{"email": "alan@example.com"}],
                },
            ),
        ]
    )

    response = client.post(f"/v1/ingestion/layer3/parse/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["trace_id"] == trace_id
    assert body["total_raw_events"] == 3
    assert body["parsed_count"] == 3
    assert body["failed_count"] == 0
    assert body["persistence"]["stored_count"] == 3
    event_types = sorted(item["event_type"] for item in body["parsed_events"])
    assert event_types == ["calendar_event", "contact", "email_message"]


def test_layer3_parse_reports_unsupported_source() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_")
    store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    canonical_store = SqliteCanonicalEventStore(db_path=f"{tmpdir}/canonical_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: store
    app.dependency_overrides[get_canonical_event_store] = lambda: canonical_store

    trace_id = "trace_layer3_fail"
    now = datetime.now(UTC)
    store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.TWITTER,
                source_event_id="tweet_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={"id": "tweet_1"},
            )
        ]
    )

    response = client.post(f"/v1/ingestion/layer3/parse/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["parsed_count"] == 0
    assert body["failed_count"] == 1
    assert body["failures"][0]["reason"] == "unsupported_source:twitter"


def test_layer3_events_lookup_returns_persisted_records() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_")
    store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    canonical_store = SqliteCanonicalEventStore(db_path=f"{tmpdir}/canonical_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: store
    app.dependency_overrides[get_canonical_event_store] = lambda: canonical_store

    trace_id = "trace_layer3_lookup"
    now = datetime.now(UTC)
    store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_77",
                occurred_at=now,
                trace_id=trace_id,
                payload={"resourceName": "people/c_77", "names": [{"displayName": "Phoebe Buffay"}]},
            )
        ]
    )

    parse_response = client.post(f"/v1/ingestion/layer3/parse/{trace_id}")
    assert parse_response.status_code == 200

    lookup = client.get(f"/v1/ingestion/layer3/events/{trace_id}")
    app.dependency_overrides.clear()

    assert lookup.status_code == 200
    body = lookup.json()
    assert body["trace_id"] == trace_id
    assert body["total"] == 1
    assert body["events"][0]["event_type"] == "contact"
