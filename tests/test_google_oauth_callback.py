import base64
import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_google_connector
from app.main import app
from app.schemas import IngestionSource, SourceEvent


class _FakeGoogleConnector:
    async def handle_callback(self, *, code: str, context):  # type: ignore[no-untyped-def]
        assert code == "auth_code_123"
        return [
            SourceEvent(
                tenant_id=context.tenant_id,
                user_id=context.user_id,
                source=IngestionSource.GOOGLE_CONTACTS,
                source_event_id="people/c123",
                occurred_at=datetime.now(UTC),
                trace_id=context.trace_id,
                payload={"resourceName": "people/c123"},
            ),
            SourceEvent(
                tenant_id=context.tenant_id,
                user_id=context.user_id,
                source=IngestionSource.GMAIL,
                source_event_id="msg_123",
                occurred_at=datetime.now(UTC),
                trace_id=context.trace_id,
                payload={"id": "msg_123"},
            ),
            SourceEvent(
                tenant_id=context.tenant_id,
                user_id=context.user_id,
                source=IngestionSource.GOOGLE_CALENDAR,
                source_event_id="cal_123",
                occurred_at=datetime.now(UTC),
                trace_id=context.trace_id,
                payload={"id": "cal_123"},
            ),
        ]


def _state(tenant_id: str, user_id: str) -> str:
    raw = json.dumps({"tenant_id": tenant_id, "user_id": user_id}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def test_google_oauth_callback_success_query_tenant_user() -> None:
    app.dependency_overrides[get_google_connector] = lambda: _FakeGoogleConnector()
    client = TestClient(app)

    response = client.get(
        "/v1/ingestion/google/oauth/callback",
        params={
            "code": "auth_code_123",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
        },
    )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "google"
    assert body["tenant_id"] == "tenant_1"
    assert body["user_id"] == "user_1"
    assert body["counts"]["google_contacts"] == 1
    assert body["counts"]["gmail"] == 1
    assert body["counts"]["google_calendar"] == 1
    assert len(body["source_events"]) == 3


def test_google_oauth_callback_success_state_resolution() -> None:
    app.dependency_overrides[get_google_connector] = lambda: _FakeGoogleConnector()
    client = TestClient(app)

    response = client.get(
        "/v1/ingestion/google/oauth/callback",
        params={
            "code": "auth_code_123",
            "state": _state("tenant_a", "user_a"),
        },
    )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == "tenant_a"
    assert body["user_id"] == "user_a"


def test_google_oauth_callback_missing_identity_context() -> None:
    client = TestClient(app)
    response = client.get(
        "/v1/ingestion/google/oauth/callback",
        params={"code": "auth_code_123"},
    )
    assert response.status_code == 400
    assert "tenant_id/user_id" in response.json()["detail"]

