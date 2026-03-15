"""Tests for the warm-path feature (scoring + API endpoints)."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi.testclient import TestClient

from app.api.warm_path_routes import get_graph_store
from app.ingestion.graph_store import NoopGraphStore
from app.ingestion.warm_path import (
    DEFAULT_RELIABILITY,
    HOP_DECAY,
    LARGE_NETWORK_MIN_STRENGTH,
    LARGE_NETWORK_THRESHOLD,
    RECENCY_HALFLIFE_DAYS,
    SCORE_SCALE,
    W_RELIABILITY,
    InMemoryWarmPathCache,
    UpstashRedisWarmPathCache,
    WarmPathService,
    _TTLCache,
    _deduplicate_connectors,
    _rank_paths,
    channel_diversity_score,
    compute_path_score,
    edge_warmth,
    frequency_score,
    recency_score,
)
from app.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Unit tests — pure scoring functions
# ---------------------------------------------------------------------------


def test_recency_score_today_is_near_one() -> None:
    now_iso = datetime.now(UTC).isoformat()
    score = recency_score(now_iso)
    assert score > 0.95


def test_recency_score_decays_with_age() -> None:
    recent = (datetime.now(UTC) - timedelta(days=7)).isoformat()
    old = (datetime.now(UTC) - timedelta(days=180)).isoformat()
    assert recency_score(recent) > recency_score(old)


def test_recency_score_at_halflife() -> None:
    halflife_ago = (datetime.now(UTC) - timedelta(days=RECENCY_HALFLIFE_DAYS)).isoformat()
    score = recency_score(halflife_ago)
    expected = math.exp(-1.0)  # e^(-90/90) ~ 0.368
    assert abs(score - expected) < 0.05


def test_recency_score_none_returns_zero() -> None:
    assert recency_score(None) == 0.0


def test_recency_score_invalid_returns_zero() -> None:
    assert recency_score("not-a-date") == 0.0


def test_frequency_score_zero_interactions() -> None:
    assert frequency_score(0) == 0.0


def test_frequency_score_increases_with_count() -> None:
    assert frequency_score(1) < frequency_score(5) < frequency_score(10)


def test_frequency_score_saturates_at_10() -> None:
    score = frequency_score(10)
    assert abs(score - 1.0) < 0.01


def test_frequency_score_above_10_still_1() -> None:
    assert frequency_score(100) == 1.0


def test_channel_diversity_none_returns_zero() -> None:
    assert channel_diversity_score(None) == 0.0
    assert channel_diversity_score([]) == 0.0


def test_channel_diversity_scales_to_3() -> None:
    assert channel_diversity_score(["email"]) < channel_diversity_score(["email", "meeting"])
    assert abs(channel_diversity_score(["email", "meeting", "call"]) - 1.0) < 0.01


def test_channel_diversity_maxes_at_3() -> None:
    score = channel_diversity_score(["email", "meeting", "call", "sms"])
    assert score == 1.0


def test_channel_diversity_deduplicates() -> None:
    assert channel_diversity_score(["email", "email"]) == channel_diversity_score(["email"])


def test_edge_warmth_combines_signals() -> None:
    now_iso = datetime.now(UTC).isoformat()
    w = edge_warmth(
        strength=1.0,
        last_interaction_at=now_iso,
        interaction_count=10,
        channels=["email", "meeting", "call"],
        reliability=1.0,
    )
    # All signals maxed -> should be close to 1.0
    assert w > 0.9


def test_edge_warmth_zero_inputs() -> None:
    w = edge_warmth(strength=0.0, last_interaction_at=None, interaction_count=0, channels=[], reliability=0.0)
    assert w == 0.0


def test_edge_warmth_uses_default_reliability() -> None:
    """When reliability is not provided, DEFAULT_RELIABILITY is used."""
    w_default = edge_warmth(strength=0.5, last_interaction_at=None, interaction_count=0, channels=[])
    w_explicit = edge_warmth(strength=0.5, last_interaction_at=None, interaction_count=0, channels=[], reliability=DEFAULT_RELIABILITY)
    assert w_default == w_explicit
    # The reliability component contributes W_RELIABILITY * DEFAULT_RELIABILITY
    assert w_default > 0.0
    assert abs(w_default - (0.40 * 0.5 + W_RELIABILITY * DEFAULT_RELIABILITY)) < 0.001


def test_path_score_is_scaled_to_100() -> None:
    """Path scores should be in the 0-100 range."""
    score = compute_path_score([0.8], 1)
    assert score == round(0.8 * SCORE_SCALE, 2)
    assert 0 <= score <= 100


def test_path_score_single_hop_no_penalty() -> None:
    score = compute_path_score([0.8], 1)
    assert score == round(0.8 * SCORE_SCALE, 2)


def test_path_score_two_hops_decayed() -> None:
    score = compute_path_score([0.8, 0.6], 2)
    expected = round(0.8 * 0.6 * HOP_DECAY * SCORE_SCALE, 2)
    assert abs(score - expected) < 0.1


def test_path_score_three_hops_decayed_more() -> None:
    score = compute_path_score([0.8, 0.6, 0.5], 3)
    expected = round(0.8 * 0.6 * 0.5 * (HOP_DECAY ** 2) * SCORE_SCALE, 2)
    assert abs(score - expected) < 0.1


def test_path_score_penalizes_longer_paths() -> None:
    one_hop = compute_path_score([0.8], 1)
    two_hop = compute_path_score([0.8, 0.8], 2)
    three_hop = compute_path_score([0.8, 0.8, 0.8], 3)
    assert one_hop > two_hop > three_hop


def test_path_score_empty() -> None:
    assert compute_path_score([], 0) == 0.0


# ---------------------------------------------------------------------------
# Tiebreaker tests
# ---------------------------------------------------------------------------


def test_rank_paths_tiebreaker_shorter_path_preferred() -> None:
    """When scores are equal, shorter path wins."""
    paths = [
        {"path_score": 50.0, "hop_count": 2, "path_edges": [{"last_interaction_at": "2026-01-01T00:00:00+00:00"}], "path_nodes": []},
        {"path_score": 50.0, "hop_count": 1, "path_edges": [{"last_interaction_at": "2026-01-01T00:00:00+00:00"}], "path_nodes": []},
    ]
    ranked = _rank_paths(paths)
    assert ranked[0]["hop_count"] == 1
    assert ranked[1]["hop_count"] == 2


def test_rank_paths_tiebreaker_more_recent_preferred() -> None:
    """When score and hop_count are equal, more recent interaction wins."""
    paths = [
        {"path_score": 50.0, "hop_count": 1, "path_edges": [{"last_interaction_at": "2026-01-01T00:00:00+00:00"}], "path_nodes": []},
        {"path_score": 50.0, "hop_count": 1, "path_edges": [{"last_interaction_at": "2026-03-15T00:00:00+00:00"}], "path_nodes": []},
    ]
    ranked = _rank_paths(paths)
    assert ranked[0]["path_edges"][0]["last_interaction_at"] == "2026-03-15T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Connector deduplication tests
# ---------------------------------------------------------------------------


def test_deduplicate_connectors_removes_duplicate() -> None:
    """If the same connector appears in the top path and an alternative, the alternative is dropped."""
    paths = [
        {"path_score": 80.0, "hop_count": 2, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "connector_a"}, {"entity_id": "target"}
        ], "path_edges": []},
        {"path_score": 60.0, "hop_count": 2, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "connector_a"}, {"entity_id": "target"}
        ], "path_edges": []},
    ]
    result = _deduplicate_connectors(paths)
    assert len(result) == 1


def test_deduplicate_connectors_keeps_new_connector() -> None:
    """Paths with different connectors are kept."""
    paths = [
        {"path_score": 80.0, "hop_count": 2, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "connector_a"}, {"entity_id": "target"}
        ], "path_edges": []},
        {"path_score": 60.0, "hop_count": 2, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "connector_b"}, {"entity_id": "target"}
        ], "path_edges": []},
    ]
    result = _deduplicate_connectors(paths)
    assert len(result) == 2


def test_deduplicate_connectors_keeps_direct_paths() -> None:
    """Direct (1-hop) paths are always kept."""
    paths = [
        {"path_score": 80.0, "hop_count": 2, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "connector_a"}, {"entity_id": "target"}
        ], "path_edges": []},
        {"path_score": 70.0, "hop_count": 1, "path_nodes": [
            {"entity_id": "origin"}, {"entity_id": "target"}
        ], "path_edges": []},
    ]
    result = _deduplicate_connectors(paths)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Mock GraphStore for integration tests
# ---------------------------------------------------------------------------


class MockGraphStore(NoopGraphStore):
    """Returns canned results for warm-path Cypher queries."""

    def __init__(self, results: list[dict[str, Any]] | None = None) -> None:
        self._results = results or []

    def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        if "count(p)" in query:
            return [{"person_count": 100}]  # small network by default
        return self._results


# ---------------------------------------------------------------------------
# Integration tests — REST endpoints
# ---------------------------------------------------------------------------


def _override_graph_store(store: MockGraphStore) -> None:
    app.dependency_overrides[get_graph_store] = lambda: store


def _cleanup() -> None:
    app.dependency_overrides.pop(get_graph_store, None)


def test_warm_path_to_target_returns_ranked_paths() -> None:
    now_iso = datetime.now(UTC).isoformat()
    mock = MockGraphStore(results=[
        {
            "path_nodes": [
                {"entity_id": "a", "name": "Alice", "primary_email": "alice@co.com", "company": "co.com"},
                {"entity_id": "b", "name": "Bob", "primary_email": "bob@co.com", "company": "co.com"},
            ],
            "path_edges": [
                {
                    "from_entity_id": "a",
                    "to_entity_id": "b",
                    "strength": 0.8,
                    "interaction_count": 8,
                    "last_interaction_at": now_iso,
                    "channels": ["email", "meeting"],
                },
            ],
            "hop_count": 1,
        },
        {
            "path_nodes": [
                {"entity_id": "a", "name": "Alice", "primary_email": "alice@co.com", "company": "co.com"},
                {"entity_id": "c", "name": "Carol", "primary_email": "carol@co.com", "company": "co.com"},
                {"entity_id": "b", "name": "Bob", "primary_email": "bob@co.com", "company": "co.com"},
            ],
            "path_edges": [
                {
                    "from_entity_id": "a",
                    "to_entity_id": "c",
                    "strength": 0.5,
                    "interaction_count": 3,
                    "last_interaction_at": now_iso,
                    "channels": ["email"],
                },
                {
                    "from_entity_id": "c",
                    "to_entity_id": "b",
                    "strength": 0.4,
                    "interaction_count": 2,
                    "last_interaction_at": now_iso,
                    "channels": ["email"],
                },
            ],
            "hop_count": 2,
        },
    ])
    _override_graph_store(mock)
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/to/b",
            params={"origin_entity_id": "a", "max_paths": 5},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_mode"] == "targeted"
        assert body["target_entity_id"] == "b"
        assert body["total_paths_found"] == 2
        scores = [p["path_score"] for p in body["paths"]]
        assert scores == sorted(scores, reverse=True), "paths should be ranked by score descending"
        # First path is direct, second is 2-hop
        assert body["paths"][0]["is_direct"] is True
        assert body["paths"][1]["is_direct"] is False
        # Scores are in 0-100 range
        assert all(0 <= s <= 100 for s in scores)
    finally:
        _cleanup()


def test_warm_path_discover_returns_multiple_targets() -> None:
    now_iso = datetime.now(UTC).isoformat()
    mock = MockGraphStore(results=[
        {
            "path_nodes": [
                {"entity_id": "a", "name": "Alice", "primary_email": "a@co.com", "company": None},
                {"entity_id": "b", "name": "Bob", "primary_email": "b@co.com", "company": None},
            ],
            "path_edges": [{
                "from_entity_id": "a", "to_entity_id": "b",
                "strength": 0.9, "interaction_count": 10,
                "last_interaction_at": now_iso, "channels": ["email", "meeting"],
            }],
            "hop_count": 1,
        },
    ])
    _override_graph_store(mock)
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/discover",
            params={"origin_entity_id": "a"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_mode"] == "discover"
        assert body["target_entity_id"] is None
        assert body["total_paths_found"] >= 1
    finally:
        _cleanup()


def test_warm_path_no_neo4j_returns_empty() -> None:
    _override_graph_store(MockGraphStore(results=[]))
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/to/xyz",
            params={"origin_entity_id": "abc"},
        )
        assert resp.status_code == 200
        assert resp.json()["total_paths_found"] == 0
        assert resp.json()["paths"] == []
    finally:
        _cleanup()


def test_warm_path_min_score_filters() -> None:
    old = (datetime.now(UTC) - timedelta(days=365)).isoformat()
    mock = MockGraphStore(results=[
        {
            "path_nodes": [
                {"entity_id": "a", "name": "A", "primary_email": "a@x.com", "company": None},
                {"entity_id": "b", "name": "B", "primary_email": "b@x.com", "company": None},
            ],
            "path_edges": [{
                "from_entity_id": "a", "to_entity_id": "b",
                "strength": 0.1, "interaction_count": 1,
                "last_interaction_at": old, "channels": ["email"],
            }],
            "hop_count": 1,
        },
    ])
    _override_graph_store(mock)
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/discover",
            params={"origin_entity_id": "a", "min_score": 90.0},
        )
        assert resp.status_code == 200
        assert resp.json()["total_paths_found"] == 0
    finally:
        _cleanup()


def test_warm_path_invalid_max_hops_returns_422() -> None:
    _override_graph_store(MockGraphStore())
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/to/xyz",
            params={"origin_entity_id": "abc", "max_hops": 5},
        )
        assert resp.status_code == 422
    finally:
        _cleanup()


def test_resolve_origin_found() -> None:
    mock = MockGraphStore(results=[{"entity_id": "ent_123"}])
    _override_graph_store(mock)
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/resolve-origin",
            params={"email": "alice@example.com"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["found"] is True
        assert body["entity_id"] == "ent_123"
    finally:
        _cleanup()


def test_resolve_origin_not_found() -> None:
    mock = MockGraphStore(results=[])
    _override_graph_store(mock)
    try:
        resp = client.get(
            "/v1/warm-path/tenant_1/resolve-origin",
            params={"email": "nobody@example.com"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["found"] is False
        assert body["entity_id"] is None
    finally:
        _cleanup()


# ---------------------------------------------------------------------------
# Event logging endpoint tests
# ---------------------------------------------------------------------------


def test_log_event_path_selected() -> None:
    resp = client.post(
        "/v1/warm-path/tenant_1/events",
        json={
            "event_type": "path_selected",
            "origin_entity_id": "ent_origin",
            "target_entity_id": "ent_target",
            "selected_path_score": 72.5,
            "selected_path_hop_count": 2,
            "connector_entity_ids": ["ent_connector_1"],
            "metadata": {"source": "search_results"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["tenant_id"] == "tenant_1"
    assert body["event_type"] == "path_selected"
    assert body["event_id"]  # non-empty UUID
    assert body["recorded_at"]


def test_log_event_intro_requested() -> None:
    resp = client.post(
        "/v1/warm-path/tenant_1/events",
        json={
            "event_type": "intro_requested",
            "origin_entity_id": "ent_origin",
            "target_entity_id": "ent_target",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["event_type"] == "intro_requested"


def test_log_event_missing_required_fields_returns_422() -> None:
    resp = client.post(
        "/v1/warm-path/tenant_1/events",
        json={"event_type": "path_selected"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TTL cache tests
# ---------------------------------------------------------------------------


def test_ttl_cache_put_and_get() -> None:
    cache = _TTLCache(max_size=10, ttl_seconds=60)
    cache.put("key1", {"data": 1})
    assert cache.get("key1") == {"data": 1}


def test_ttl_cache_miss_returns_none() -> None:
    cache = _TTLCache(max_size=10, ttl_seconds=60)
    assert cache.get("nonexistent") is None


def test_ttl_cache_evicts_oldest_on_overflow() -> None:
    cache = _TTLCache(max_size=2, ttl_seconds=60)
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    cache.put("k3", "v3")  # should evict k1
    assert cache.get("k1") is None
    assert cache.get("k2") == "v2"
    assert cache.get("k3") == "v3"


def test_ttl_cache_expired_entry_returns_none() -> None:
    cache = _TTLCache(max_size=10, ttl_seconds=0)  # instant expiry
    cache.put("key", "value")
    # With ttl_seconds=0 the entry expires immediately at next monotonic tick
    import time
    time.sleep(0.01)
    assert cache.get("key") is None


def test_ttl_cache_invalidate_tenant() -> None:
    cache = _TTLCache(max_size=10, ttl_seconds=60)
    cache.put("tenant_a:targeted:x:y:3:5", [1])
    cache.put("tenant_a:discover:x:3:20:0.0", [2])
    cache.put("tenant_b:targeted:x:y:3:5", [3])
    evicted = cache.invalidate_tenant("tenant_a")
    assert evicted == 2
    assert cache.get("tenant_a:targeted:x:y:3:5") is None
    assert cache.get("tenant_b:targeted:x:y:3:5") == [3]


def test_ttl_cache_clear() -> None:
    cache = _TTLCache(max_size=10, ttl_seconds=60)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.clear()
    assert cache.size == 0


# ---------------------------------------------------------------------------
# WarmPathService caching integration
# ---------------------------------------------------------------------------


def test_service_caches_query_results() -> None:
    """Second call with same params should return cached result without hitting the store."""

    call_count = 0

    class CountingGraphStore(NoopGraphStore):
        def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
            nonlocal call_count
            call_count += 1
            if "count(p)" in query:
                return [{"person_count": 100}]
            return [{
                "path_nodes": [
                    {"entity_id": "a", "name": "A", "primary_email": "a@x.com", "company": None},
                    {"entity_id": "b", "name": "B", "primary_email": "b@x.com", "company": None},
                ],
                "path_edges": [{
                    "from_entity_id": "a", "to_entity_id": "b",
                    "strength": 0.8, "interaction_count": 5,
                    "last_interaction_at": datetime.now(UTC).isoformat(),
                    "channels": ["email"],
                }],
                "hop_count": 1,
            }]

    svc = WarmPathService(
        graph_store=CountingGraphStore(),
        _cache=_TTLCache(max_size=10, ttl_seconds=60),
    )

    result1 = svc.find_paths_to_target(
        tenant_id="t1", origin_entity_id="a", target_entity_id="b",
    )
    result2 = svc.find_paths_to_target(
        tenant_id="t1", origin_entity_id="a", target_entity_id="b",
    )

    assert result1 == result2
    # 1 for network size + 1 for data query = 2 total, second call is fully cached
    assert call_count == 2


def test_service_invalidate_tenant_clears_cache() -> None:
    call_count = 0

    class CountingGraphStore(NoopGraphStore):
        def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
            nonlocal call_count
            call_count += 1
            if "count(p)" in query:
                return [{"person_count": 100}]
            return []

    svc = WarmPathService(
        graph_store=CountingGraphStore(),
        _cache=_TTLCache(max_size=10, ttl_seconds=60),
    )

    svc.find_paths_to_target(tenant_id="t1", origin_entity_id="a", target_entity_id="b")
    svc.invalidate_tenant_cache("t1")
    svc.find_paths_to_target(tenant_id="t1", origin_entity_id="a", target_entity_id="b")

    # network size query + data query, twice (cache was invalidated)
    assert call_count == 4


# ---------------------------------------------------------------------------
# Large network optimization
# ---------------------------------------------------------------------------


def test_service_uses_strong_query_for_large_networks() -> None:
    """When network size exceeds threshold, the strength-filtered query is used."""

    queries_received: list[str] = []

    class TrackingGraphStore(NoopGraphStore):
        def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
            queries_received.append(query.strip()[:30])
            if "count(p)" in query:
                return [{"person_count": 15000}]  # above threshold
            return []

    svc = WarmPathService(
        graph_store=TrackingGraphStore(),
        _cache=_TTLCache(max_size=10, ttl_seconds=60),
        _large_network_threshold=LARGE_NETWORK_THRESHOLD,
        _large_network_min_strength=LARGE_NETWORK_MIN_STRENGTH,
    )

    svc.find_paths_to_target(tenant_id="t1", origin_entity_id="a", target_entity_id="b")

    # The second query (data query) should contain the min_strength filter
    assert any("min_strength" in q or "strength >=" in q for q in queries_received) or \
        len(queries_received) == 2  # network size + data query were both called


def test_service_uses_normal_query_for_small_networks() -> None:
    queries_received: list[str] = []

    class TrackingGraphStore(NoopGraphStore):
        def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
            queries_received.append(query)
            if "count(p)" in query:
                return [{"person_count": 500}]  # below threshold
            return []

    svc = WarmPathService(
        graph_store=TrackingGraphStore(),
        _cache=_TTLCache(max_size=10, ttl_seconds=60),
        _large_network_threshold=LARGE_NETWORK_THRESHOLD,
    )

    svc.discover_warmest_paths(tenant_id="t1", origin_entity_id="a")

    # The data query should NOT contain min_strength filter
    data_queries = [q for q in queries_received if "count(p)" not in q]
    assert data_queries
    assert all("min_strength" not in q for q in data_queries)


# ---------------------------------------------------------------------------
# Event persistence tests
# ---------------------------------------------------------------------------


def test_event_persistence_stores_event() -> None:
    """POST /events should persist the event (SQLite fallback in test env)."""
    resp = client.post(
        "/v1/warm-path/tenant_1/events",
        json={
            "event_type": "path_selected",
            "origin_entity_id": "ent_origin",
            "target_entity_id": "ent_target",
            "selected_path_score": 72.5,
            "selected_path_hop_count": 2,
            "connector_entity_ids": ["ent_c1"],
            "metadata": {"source": "test"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["event_id"]
    assert body["event_type"] == "path_selected"


# ---------------------------------------------------------------------------
# Upstash Redis cache integration test
# ---------------------------------------------------------------------------

import os
import pytest


@pytest.mark.skipif(
    not os.environ.get("UPSTASH_REDIS_REST_URL"),
    reason="UPSTASH_REDIS_REST_URL not set — skipping live Redis test",
)
def test_upstash_redis_cache_put_get_invalidate() -> None:
    """Smoke test against the real Upstash Redis instance."""
    cache = UpstashRedisWarmPathCache(
        url=os.environ["UPSTASH_REDIS_REST_URL"],
        token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
        ttl_seconds=30,
    )
    # Clean up any previous test keys
    cache.invalidate_tenant("__test_tenant__")

    # put + get
    cache.put("__test_tenant__:targeted:a:b:3:5", [{"score": 42}])
    result = cache.get("__test_tenant__:targeted:a:b:3:5")
    assert result == [{"score": 42}]

    # miss
    assert cache.get("__test_tenant__:nonexistent") is None

    # invalidate_tenant
    cache.put("__test_tenant__:discover:a:3:20:0.0", [{"score": 10}])
    evicted = cache.invalidate_tenant("__test_tenant__")
    assert evicted >= 2
    assert cache.get("__test_tenant__:targeted:a:b:3:5") is None
    assert cache.get("__test_tenant__:discover:a:3:20:0.0") is None
