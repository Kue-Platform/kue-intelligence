from fastapi.testclient import TestClient

from app.api import ingestion_routes
from app.main import app

client = TestClient(app)


def test_admin_reset_requires_configured_token(monkeypatch) -> None:
    monkeypatch.setattr(ingestion_routes.settings, "admin_reset_token", "")
    response = client.post("/v1/ingestion/admin/reset")
    assert response.status_code == 503


def test_admin_reset_rejects_invalid_token(monkeypatch) -> None:
    monkeypatch.setattr(ingestion_routes.settings, "admin_reset_token", "secret")
    response = client.post(
        "/v1/ingestion/admin/reset",
        headers={"x-admin-reset-token": "wrong"},
    )
    assert response.status_code == 401


def test_admin_reset_success(monkeypatch) -> None:
    monkeypatch.setattr(ingestion_routes.settings, "admin_reset_token", "secret")

    def fake_reset_all_data(_settings):  # type: ignore[no-untyped-def]
        class _R:
            ok = True
            mode = "test"
            details = {"tables_cleared": 1}

        return _R()

    monkeypatch.setattr(ingestion_routes, "reset_all_data", fake_reset_all_data)
    response = client.post(
        "/v1/ingestion/admin/reset",
        headers={"x-admin-reset-token": "secret"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["mode"] == "test"
