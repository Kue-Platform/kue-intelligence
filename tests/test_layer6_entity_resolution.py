from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_entity_store, get_raw_event_store
from app.ingestion.entity_store import SqliteEntityStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer6_resolution_upserts_entities_and_identities() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer6_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=f"{tmpdir}/entities.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store

    trace_id = "trace_layer6_1"
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_601",
                occurred_at=datetime.now(UTC),
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_601",
                    "names": [{"displayName": "Alan Turing"}],
                    "emailAddresses": [{"value": "alan@example.com"}],
                    "organizations": [{"name": "stripe", "title": "partner"}],
                },
            )
        ]
    )

    response = client.post(f"/v1/ingestion/layer6/resolve/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["candidate_count"] == 1
    assert body["resolved_count"] == 1
    assert body["created_entities"] == 1
    assert body["identities_upserted"] == 1


def test_layer6_resolution_dedups_by_email() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer6_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    entity_store = SqliteEntityStore(db_path=f"{tmpdir}/entities.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_entity_store] = lambda: entity_store

    trace_id = "trace_layer6_2"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_611",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_611",
                    "names": [{"displayName": "Harun"}],
                    "emailAddresses": [{"value": "same@example.com"}],
                },
            ),
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c_612",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "resourceName": "people/c_612",
                    "names": [{"displayName": "Harun Rashid"}],
                    "emailAddresses": [{"value": "same@example.com"}],
                },
            ),
        ]
    )

    response = client.post(f"/v1/ingestion/layer6/resolve/{trace_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["candidate_count"] == 2
    assert body["resolved_count"] == 1
    assert body["created_entities"] == 1
