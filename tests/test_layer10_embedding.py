from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_embedding_store,
    get_entity_store,
    get_raw_event_store,
    get_search_document_store,
)
from app.ingestion.embedding_store import SqliteEmbeddingStore
from app.ingestion.entity_store import SqliteEntityStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.search_document_store import SqliteSearchDocumentStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer10_embedding_endpoint_persists_vectors() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer10_")
    db_path = f"{tmpdir}/pipeline.db"
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=db_path)
    search_store = SqliteSearchDocumentStore(db_path=db_path)
    embedding_store = SqliteEmbeddingStore(db_path=db_path)

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store
    app.dependency_overrides[get_search_document_store] = lambda: search_store
    app.dependency_overrides[get_embedding_store] = lambda: embedding_store

    trace_id = "trace_layer10_1"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l10",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_l10",
                    "names": [{"displayName": "Alan Turing"}],
                    "emailAddresses": [{"value": "alan@venturelabs.com"}],
                    "organizations": [{"name": "Venture Labs", "title": "Partner"}],
                },
            )
        ]
    )

    response = client.post(f"/v1/ingestion/layer10/embed/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["candidate_count"] >= 1
    assert body["generated_count"] >= 1
    assert body["persisted_count"] >= 1
