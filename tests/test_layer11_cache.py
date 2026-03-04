from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_raw_event_store
from app.ingestion.cache_registry import cache_registry
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer11_cache_endpoint_records_entries() -> None:
    cache_registry.clear()
    tmpdir = tempfile.mkdtemp(prefix="kue_layer11_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store

    trace_id = "trace_layer11_1"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l11",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_l11",
                    "names": [{"displayName": "Alan Turing"}],
                    "emailAddresses": [{"value": "alan@venturelabs.com"}],
                    "organizations": [{"name": "Venture Labs", "title": "Partner"}],
                },
            )
        ]
    )

    response = client.post(f"/v1/ingestion/layer11/cache/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["enrichment_cached_count"] >= 1
    assert body["embedding_cached_count"] >= 1
    assert body["total_entries"] >= 2
    assert "enrichment" in body["namespaces"]
