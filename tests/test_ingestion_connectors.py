from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_ingestion_mock_google_contacts() -> None:
    response = client.post(
        "/v1/ingestion/mock",
        json={
            "source": "google_contacts",
            "trigger_type": "manual",
            "payload": {"contacts": [{"name": "Alan Turing"}, {"name": "Rachel Carson"}]},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["source"] == "google_contacts"
    assert body["records_detected"] == 2
    assert body["connector_event_id"].startswith("conn_")
    assert body["trace_id"].startswith("trace_")


def test_ingestion_mock_gmail() -> None:
    response = client.post(
        "/v1/ingestion/mock",
        json={
            "source": "gmail",
            "trigger_type": "webhook",
            "payload": {"messages": [{"subject": "Hello"}, {"subject": "Warm intro"}]},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "gmail"
    assert body["records_detected"] == 2


def test_ingestion_mock_linkedin() -> None:
    response = client.post(
        "/v1/ingestion/mock",
        json={
            "source": "linkedin",
            "trigger_type": "polling",
            "payload": {"profiles": [{"name": "Alex Chen"}]},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "linkedin"
    assert body["records_detected"] == 1


def test_ingestion_mock_invalid_payload_returns_400() -> None:
    response = client.post(
        "/v1/ingestion/mock",
        json={
            "source": "gmail",
            "trigger_type": "manual",
            "payload": {"contacts": [{"name": "wrong shape"}]},
        },
    )

    assert response.status_code == 400
    assert "messages" in response.json()["detail"]

