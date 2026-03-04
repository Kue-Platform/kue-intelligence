from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_entity_store,
    get_raw_event_store,
    get_search_document_store,
)
from app.ingestion.entity_store import SqliteEntityStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.search_document_store import SqliteSearchDocumentStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer9_semantic_endpoint_persists_documents() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer9_")
    db_path = f"{tmpdir}/pipeline.db"
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=db_path)
    search_store = SqliteSearchDocumentStore(db_path=db_path)

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store
    app.dependency_overrides[get_search_document_store] = lambda: search_store

    trace_id = "trace_layer9_1"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_l9",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_l9",
                    "names": [{"displayName": "Alan Turing"}],
                    "emailAddresses": [{"value": "alan@venturelabs.com"}],
                    "organizations": [{"name": "Venture Labs", "title": "Partner"}],
                },
            ),
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GMAIL,
                source_event_id="msg_l9_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "id": "msg_l9_1",
                    "threadId": "thread_l9",
                    "internalDate": str(int(now.timestamp() * 1000)),
                    "snippet": "Quick intro",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Intro request"},
                            {"name": "From", "value": "Alan Turing <alan@venturelabs.com>"},
                            {"name": "To", "value": "Harun Rashid <harun@example.com>"},
                        ]
                    },
                },
            ),
        ]
    )

    response = client.post(f"/v1/ingestion/layer9/semantic/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["document_count"] >= 1
    assert body["stored_count"] >= 1
