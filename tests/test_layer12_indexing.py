from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_embedding_store,
    get_entity_store,
    get_raw_event_store,
    get_search_document_store,
    get_search_index_store,
)
from app.ingestion.embedding_store import SqliteEmbeddingStore
from app.ingestion.entity_store import SqliteEntityStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.search_document_store import SqliteSearchDocumentStore
from app.ingestion.search_index_store import SqliteSearchIndexStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer12_index_endpoint_applies_hybrid_signals() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer12_")
    db_path = f"{tmpdir}/pipeline.db"
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=db_path)
    search_store = SqliteSearchDocumentStore(db_path=db_path)
    embedding_store = SqliteEmbeddingStore(db_path=db_path)
    index_store = SqliteSearchIndexStore(db_path=db_path)

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store
    app.dependency_overrides[get_search_document_store] = lambda: search_store
    app.dependency_overrides[get_embedding_store] = lambda: embedding_store
    app.dependency_overrides[get_search_index_store] = lambda: index_store

    trace_id = "trace_layer12_1"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l12",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_l12",
                    "names": [{"displayName": "Rachel Lee"}],
                    "emailAddresses": [{"value": "rachel@stripe.com"}],
                    "organizations": [{"name": "Stripe", "title": "Engineering Manager"}],
                },
            )
        ]
    )

    response = client.post(f"/v1/ingestion/layer12/index/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["signal_count"] >= 1
    assert body["applied_count"] >= 1
    assert body["health"]["index_ready"] is True
