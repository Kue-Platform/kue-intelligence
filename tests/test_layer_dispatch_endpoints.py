from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_canonical_event_store,
    get_inngest_dispatcher,
    get_raw_event_store,
)
from app.ingestion.canonical_store import SqliteCanonicalEventStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_manual_layer2_capture_dispatches_pipeline_event() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_layer2_manual")
    response = client.post(
        "/v1/ingestion/layer2/capture",
        json={
            "source_events": [
                {
                    "tenant_id": "tenant_1",
                    "user_id": "user_1",
                    "source": "google_contacts",
                    "source_event_id": "people/c_100",
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "trace_id": "trace_layer2_manual",
                    "payload": {"resourceName": "people/c_100"},
                }
            ]
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["event_name"] == "pipeline/run.requested"
    assert body["event_id"] == "evt_layer2_manual"
    assert body["trace_id"] == "trace_layer2_manual"
    assert body["status"] == "accepted"


def test_canonicalization_replay_endpoint() -> None:
    raw_store = SqliteRawEventStore(db_path="/tmp/kue_test_raw_replay.db")
    canonical_store = SqliteCanonicalEventStore(db_path="/tmp/kue_test_canonical_replay.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_canonical_event_store] = lambda: canonical_store
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_replay_1")
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_replay",
                occurred_at=datetime.now(UTC),
                trace_id="trace_abc",
                payload={"resourceName": "people/c_replay", "names": [{"displayName": "Replay User"}]},
            )
        ]
    )
    response = client.post("/v1/ingestion/stage/canonicalization/replay/trace_abc")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["trace_id"] == "trace_abc"
    assert body["event_name"] == "pipeline/stage.canonicalization.replay.requested"
    assert body["event_id"] == "evt_replay_1"


def _fake_dispatcher(event_id: str):
    async def _dispatch(_: str, __: dict) -> str:
        return event_id

    return _dispatch
