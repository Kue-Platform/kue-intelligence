from datetime import UTC, datetime
import tempfile

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_raw_event_store, get_relationship_store
from app.ingestion.raw_store import SqliteRawEventStore
from app.ingestion.relationship_store import SqliteRelationshipStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)


def test_layer7_relationship_endpoint_builds_relationships() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_layer7_")
    raw_store = SqliteRawEventStore(db_path=f"{tmpdir}/raw_events.db")
    relationship_store = SqliteRelationshipStore(db_path=f"{tmpdir}/relations.db")
    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_relationship_store] = lambda: relationship_store

    trace_id = "trace_layer7_1"
    now = datetime.now(UTC)
    raw_store.persist_source_events(
        [
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GMAIL,
                source_event_id="msg_l7_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "id": "msg_l7_1",
                    "threadId": "thread_l7",
                    "internalDate": str(int(now.timestamp() * 1000)),
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Intro to Rachel"},
                            {"name": "From", "value": "Alan Turing <alan@venturelabs.com>"},
                            {"name": "To", "value": "Harun Rashid <harun@example.com>"},
                        ]
                    },
                },
            ),
            SourceEvent(
                tenant_id="tenant_1",
                user_id="user_1",
                source=IngestionSource.GOOGLE_CALENDAR,
                source_event_id="cal_l7_1",
                occurred_at=now,
                trace_id=trace_id,
                payload={
                    "id": "cal_l7_1",
                    "summary": "Warm intro",
                    "updated": now.isoformat(),
                    "start": {"dateTime": now.isoformat()},
                    "end": {"dateTime": now.isoformat()},
                    "attendees": [
                        {"email": "harun@example.com"},
                        {"email": "alan@venturelabs.com"},
                        {"email": "rachel@stripe.com"},
                    ],
                },
            ),
        ]
    )

    response = client.post(f"/v1/ingestion/layer7/relationships/{trace_id}")
    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["interaction_count"] > 0
    assert body["relationship_count"] > 0
    assert body["relationships_upserted"] > 0
