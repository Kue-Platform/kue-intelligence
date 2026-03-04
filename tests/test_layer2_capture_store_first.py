"""Tests for the store-first layer2/capture endpoint.

Verifies that:
  1. Events are persisted to raw_store BEFORE Inngest is dispatched.
  2. The Inngest event payload contains NO source_events blob — only references.
  3. The response carries the pre-created run_id and correct trace_id.
  4. Dispatching large batches (that would previously exceed 256 KB) succeeds.
"""
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.ingestion_routes import (
    get_inngest_dispatcher,
    get_pipeline_store,
    get_raw_event_store,
)
from app.ingestion.pipeline_store import PipelineStore
from app.ingestion.raw_store import SqliteRawEventStore
from app.main import app
from app.schemas import IngestionSource, SourceEvent

client = TestClient(app)

_NOW = datetime.now(UTC).isoformat()


def _make_event(i: int, trace_id: str, source: str = "google_contacts") -> dict:
    return {
        "tenant_id": "tenant_store_first",
        "user_id": "user_store_first",
        "source": source,
        "source_event_id": f"evt_{i}",
        "occurred_at": _NOW,
        "trace_id": trace_id,
        "payload": {"resourceName": f"people/c_{i}", "x": "y" * 100},
    }


def _fake_dispatcher(captured: list[dict]):
    """Dispatcher that records every (name, data) pair it receives."""
    dispatched_events: list[Any] = []

    async def _dispatch(name: str, data: dict) -> str:
        dispatched_events.append({"name": name, "data": data})
        captured.clear()
        captured.append({"name": name, "data": data})
        return "evt_store_first_test"

    return _dispatch


def test_layer2_capture_stores_events_before_dispatch():
    """Events must be in the raw store before the Inngest dispatch fires."""
    raw_store = SqliteRawEventStore(db_path="/tmp/kue_test_store_first.db")
    dispatched: list[dict] = []

    # Mock pipeline_store so we control run_id
    mock_pipeline = MagicMock(spec=PipelineStore)
    mock_pipeline.create_pipeline_run.return_value = "run_store_first_001"

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: mock_pipeline
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher(dispatched)

    trace_id = "trace_store_first_001"
    response = client.post(
        "/v1/ingestion/layer2/capture",
        json={"source_events": [_make_event(1, trace_id), _make_event(2, trace_id)]},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200, response.text
    body = response.json()

    # Response shape
    assert body["trace_id"] == trace_id
    assert body["event_name"] == "pipeline/run.requested"
    assert body["event_id"] == "evt_store_first_test"
    assert body["run_id"] == "run_store_first_001"

    # Events stored in raw_store before dispatch
    stored = raw_store.list_by_trace_id(trace_id)
    assert len(stored) == 2, "Both events must be in raw_store"

    # Inngest event must NOT contain source_events blob
    assert len(dispatched) == 1
    evt_data = dispatched[0]["data"]
    assert "source_events" not in evt_data, "Event payload must not embed source_events"
    assert evt_data["trace_id"] == trace_id
    assert evt_data["source_count"] == 2
    assert evt_data["run_id"] == "run_store_first_001"


def test_layer2_capture_lightweight_event_for_large_batch():
    """Even a large batch (that would exceed 256 KB inline) dispatches fine
    because source_events are never put in the Inngest event payload.
    """
    raw_store = SqliteRawEventStore(db_path="/tmp/kue_test_store_first_large.db")
    dispatched: list[dict] = []

    mock_pipeline = MagicMock(spec=PipelineStore)
    mock_pipeline.create_pipeline_run.return_value = "run_large_001"

    app.dependency_overrides[get_raw_event_store] = lambda: raw_store
    app.dependency_overrides[get_pipeline_store] = lambda: mock_pipeline
    app.dependency_overrides[get_inngest_dispatcher] = lambda: _fake_dispatcher(dispatched)

    trace_id = "trace_store_first_large"
    # 500 events with fat payloads — would be ~300+ KB if embedded
    events = [_make_event(i, trace_id, source="gmail") for i in range(500)]

    response = client.post(
        "/v1/ingestion/layer2/capture",
        json={"source_events": events},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200, response.text

    import json
    evt_data = dispatched[0]["data"]
    payload_bytes = len(json.dumps(evt_data).encode())
    assert payload_bytes < 262_144, (
        f"Inngest event payload ({payload_bytes} bytes) must be under 256 KB"
    )
    assert evt_data["source_count"] == 500


def test_layer2_capture_missing_trace_id():
    """source_events without a trace_id should return 422 (validation error)."""
    response = client.post(
        "/v1/ingestion/layer2/capture",
        json={"source_events": []},
    )
    assert response.status_code == 422
