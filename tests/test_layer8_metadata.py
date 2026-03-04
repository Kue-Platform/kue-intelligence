from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_entity_store, get_raw_event_store
from app.ingestion.entity_store import SqliteEntityStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer8_metadata_endpoint_updates_entity_metadata() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer8_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=f"{tmpdir}/entities.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store

    trace_id = "trace_layer8_1"
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l8",
                occurred_at=datetime.now(UTC),
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_l8",
                    "names": [{"displayName": "Rachel Lee"}],
                    "emailAddresses": [{"value": "rachel@stripe.com"}],
                    "organizations": [{"name": "stripe", "title": "engineering manager"}],
                },
            )
        ]
    )

    # Ensure entity exists first.
    resolve_response = client.post(f"/v1/ingestion/layer6/resolve/{trace_id}")
    assert resolve_response.status_code == 200

    metadata_response = client.post(f"/v1/ingestion/layer8/metadata/{trace_id}")
    app.dependency_overrides.clear()
    assert metadata_response.status_code == 200
    body = metadata_response.json()
    assert body["candidate_count"] == 1
    assert body["updated_count"] == 1
