from datetime import UTC, datetime

import httpx

from app.ingestion.raw_store import SupabaseRawEventStore
from app.schemas import IngestionSource, SourceEvent


def test_supabase_raw_store_falls_back_on_duplicate_conflict(monkeypatch) -> None:
    store = SupabaseRawEventStore(
        supabase_url="https://example.supabase.co",
        api_key="test-key",
        table="raw_events",
    )
    event = SourceEvent(
        tenant_id="tenant_demo",
        user_id="user_demo",
        source=IngestionSource.GOOGLE_CONTACTS,
        source_event_id="people/c_103",
        occurred_at=datetime.now(UTC),
        trace_id="trace_dup",
        payload={"resourceName": "people/c_103"},
    )

    calls: list[tuple[str, str]] = []

    def fake_post(url, *, headers=None, params=None, json=None, timeout=None):  # type: ignore[no-untyped-def]
        calls.append(("POST", str(params)))
        # First batch insert fails with duplicate key.
        if len(calls) == 1:
            return httpx.Response(
                409,
                text='{"message":"duplicate key value violates unique constraint \\"uq_raw_events_source_dedup\\""}',
            )
        # Row-wise upsert succeeds.
        return httpx.Response(201, text="")

    def fake_patch(url, *, headers=None, params=None, json=None, timeout=None):  # type: ignore[no-untyped-def]
        calls.append(("PATCH", str(params)))
        return httpx.Response(204, text="")

    def fake_get(url, *, headers=None, params=None, timeout=None):  # type: ignore[no-untyped-def]
        return httpx.Response(200, json=[{"id": 1}])

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(httpx, "patch", fake_patch)
    monkeypatch.setattr(httpx, "get", fake_get)

    result = store.persist_source_events([event], run_id="run_1")
    assert result.stored_count == 1
    assert any(method == "POST" for method, _ in calls)
