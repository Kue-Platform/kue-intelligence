from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_raw_event_store
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer5_enrichment_endpoint_returns_normalized_contact() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_raw_layer5_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l5",
                occurred_at=datetime.now(UTC),
                trace_id="trace_layer5_1",
                payload={
                    "resourceName": "people/c_l5",
                    "names": [{"displayName": "Layer Five"}],
                    "emailAddresses": [{"value": "LAYER.FIVE@EXAMPLE.COM"}],
                    "organizations": [{"name": "stripe   ", "title": "engineering lead"}],
                },
            )
        ]
    )

    response = client.post("/v1/ingestion/layer5/enrich/trace_layer5_1")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["valid_count"] == 1
    assert body["enriched_count"] == 1
    assert len(body["sample"]) == 1
    normalized = body["sample"][0]["normalized"]
    assert normalized["company"] == "Stripe"
    assert normalized["title"] == "Engineering Lead"
    assert normalized["emails"] == ["layer.five@example.com"]
