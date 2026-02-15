from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_google_oauth_callback_mock_contacts() -> None:
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "contacts",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "connections": [
                    {
                        "resourceName": "people/c_1",
                        "names": [{"displayName": "Alan Turing"}],
                        "metadata": {"sources": [{"updateTime": "2025-01-01T10:00:00Z"}]},
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["google_contacts"] == 1
    assert body["source_events"][0]["source"] == "google_contacts"
    assert body["source_events"][0]["source_event_id"] == "people/c_1"


def test_google_oauth_callback_mock_gmail() -> None:
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "gmail",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "messages": [
                    {
                        "id": "msg_1",
                        "internalDate": "1735725600000",
                        "snippet": "hello",
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["gmail"] == 1
    assert body["source_events"][0]["source"] == "gmail"
    assert body["source_events"][0]["source_event_id"] == "msg_1"


def test_google_oauth_callback_mock_calendar() -> None:
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "calendar",
            "tenant_id": "tenant_1",
            "user_id": "user_1",
            "payload": {
                "items": [
                    {
                        "id": "evt_1",
                        "summary": "Intro Meeting",
                        "updated": "2025-01-02T12:00:00Z",
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["google_calendar"] == 1
    assert body["source_events"][0]["source"] == "google_calendar"
    assert body["source_events"][0]["source_event_id"] == "evt_1"


def test_google_oauth_callback_mock_requires_identity_context() -> None:
    response = client.post(
        "/v1/ingestion/google/oauth/callback/mock",
        json={
            "source_type": "contacts",
            "payload": {"connections": [{"resourceName": "people/c_1"}]},
        },
    )

    assert response.status_code == 400
    assert "tenant_id/user_id" in response.json()["detail"]

