from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_raw_event_store
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.validators import validate_parsed_events
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_validate_parsed_events_rejects_invalid_shapes() -> None:
    result = validate_parsed_events(
        {
            "parsed_events": [
                {
                    "raw_event_id": 1,
                    "source_event_id": "x1",
                    "event_type": "contact",
                    "normalized": "bad-shape",
                },
                {
                    "raw_event_id": 2,
                    "source_event_id": "x2",
                    "event_type": "unsupported",
                    "normalized": {},
                },
            ]
        }
    )
    assert result.valid_count == 0
    assert result.invalid_count == 2


def test_layer4_validate_endpoint_returns_counts() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_layer4_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l4",
                occurred_at=datetime.now(UTC),
                trace_id="trace_layer4_1",
                payload={"resourceName": "people/c_l4", "names": [{"displayName": "Layer Four"}]},
            )
        ]
    )
    response = client.post("/v1/ingestion/layer4/validate/trace_layer4_1")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["trace_id"] == "trace_layer4_1"
    assert body["total_parsed_events"] == 1
    assert body["valid_count"] == 1
    assert body["invalid_count"] == 0
