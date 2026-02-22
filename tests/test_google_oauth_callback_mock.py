import tempfile
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_inngest_dispatcher, get_raw_event_store
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_google_oauth_callback_mock_contacts() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_mock_1")
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "contacts",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "connections": [
                    {
                        "resourceName": "people/c_1",
                        "names": [{"displayName": "Alan Turing"}],
                        "metadata": {"sources": [{"updateTime": "2025-01-01T10:00:00Z"}]},
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"] == {}
    assert body["pipeline_event_id"] == "evt_mock_1"
    assert body["source_events"] == []
    app.dependency_overrides.clear()


def test_google_oauth_callback_mock_gmail() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_mock_2")
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "gmail",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "messages": [
                    {
                        "id": "msg_1",
                        "internalDate": "1735725600000",
                        "snippet": "hello",
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"] == {}
    assert body["source_events"] == []
    assert body["pipeline_event_id"] == "evt_mock_2"
    app.dependency_overrides.clear()


def test_google_oauth_callback_mock_calendar() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_mock_3")
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "calendar",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "items": [
                    {
                        "id": "evt_1",
                        "summary": "Intro Meeting",
                        "updated": "2025-01-02T12:00:00Z",
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"] == {}
    assert body["source_events"] == []
    assert body["pipeline_event_id"] == "evt_mock_3"
    app.dependency_overrides.clear()


def test_google_oauth_callback_mock_requires_identity_context() -> None:
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "contacts",
            "payload": {"connections": [{"resourceName": "people/c_1"}]},
        },
    )

    assert response.status_code == 400
    assert "tenant_id/user_id" in response.json()["detail"]


def test_raw_events_lookup_by_trace_id() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_")
    store = SqliteRawEventStore(
        db_path=f"{tmpdir}/raw_events.db"
    )
    app.dependency_overrides[get_raw_event_store] = lambda: store
    store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_1",
                occurred_at=datetime.now(UTC),
                trace_id="trace_test_1",
                payload={"resourceName": "people/c_1"},
            )
        ]
    )

    lookup = client.get("/v1/ingestion/raw-events/trace_test_1")
    app.dependency_overrides.clear()

    assert lookup.status_code == 200
    body = lookup.json()
    assert body["trace_id"] == "trace_test_1"
    assert body["total"] == 1
    assert body["events"][0]["source"] == "google_contacts"


def _fake_dispatcher(event_id: str):
    async def _dispatch(_: str, __: dict) -> str:
        return event_id

    return _dispatch
