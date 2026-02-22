import base64
import json

from fastapi.testclient import TestClient

from app.api.ingestion_routes import get_inngest_dispatcher
from app.main import app


def _state(tenant_id: str, user_id: str) -> str:
    raw = json.dumps({"tenant_id": tenant_id, "user_id": user_id}).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def test_google_oauth_callback_success_query_tenant_user() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_stage_123")
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
    assert body["counts"] == {}
    assert body["source_events"] == []
    assert body["pipeline_event_name"] == "kue/user.connected"
    assert body["pipeline_event_id"] == "evt_stage_123"
    assert body["pipeline_status"] == "accepted"


def test_google_oauth_callback_success_state_resolution() -> None:
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher("evt_stage_abc")
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
    assert body["pipeline_event_id"] == "evt_stage_abc"


def test_google_oauth_callback_missing_identity_context() -> None:
    client = TestClient(app)
    response = client.get(
        "/v1/ingestion/google/oauth/callback",
        params={"code": "auth_code_123"},
    )
    assert response.status_code == 400
    assert "tenant_id/user_id" in response.json()["detail"]


def _fake_dispatcher(event_id: str):
    async def _dispatch(_: str, __: dict) -> str:
        return event_id

    return _dispatch
