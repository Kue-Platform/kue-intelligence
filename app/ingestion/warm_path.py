from __future__ import annotations

import json
import logging
import math
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Lock
from typing import Any

from app.core.config import Settings
from app.ingestion.graph_store import GraphStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring constants — aligned with PRD Section 4.2
# ---------------------------------------------------------------------------
# PRD weights: connector_strength=40%, connector_to_target=30%,
# reliability=15%, path_length=10%, timing=5%.
#
# Since we score per-edge and multiply, the per-edge weights map to:
#   - Relationship strength (40% PRD) → W_STRENGTH
#   - Connector-to-target (30% PRD) → captured via per-edge multiplication
#   - Reliability (15% PRD) → W_RELIABILITY placeholder (always 0.5 until
#     intro_outcomes tracking is implemented)
#   - Path length (10% PRD) → HOP_DECAY applied at path level
#   - Timing / recency (5% PRD) → W_RECENCY
#
# Effective per-edge weights (reliability placeholder included at neutral):

W_STRENGTH: float = 0.40
W_RECENCY: float = 0.05
W_FREQUENCY: float = 0.25
W_CHANNEL: float = 0.15
W_RELIABILITY: float = 0.15

# Default reliability score used until intro_outcomes tracking is available.
DEFAULT_RELIABILITY: float = 0.5

RECENCY_HALFLIFE_DAYS: float = 90.0
HOP_DECAY: float = 0.85

MAX_HOPS_HARD_CAP: int = 3
MAX_PATHS_HARD_CAP: int = 50

# Path scores are scaled to 0–100 for display (PRD Section 4.2).
SCORE_SCALE: float = 100.0


# ---------------------------------------------------------------------------
# Pure scoring functions (unit-testable, no DB dependency)
# ---------------------------------------------------------------------------

def recency_score(last_interaction_iso: str | None) -> float:
    """Exponential decay based on days since last interaction.

    Returns 1.0 for today, ~0.37 at ``RECENCY_HALFLIFE_DAYS``.
    """
    if not last_interaction_iso:
        return 0.0
    try:
        last_dt = datetime.fromisoformat(last_interaction_iso)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=UTC)
        days = max(0.0, (datetime.now(UTC) - last_dt).total_seconds() / 86400.0)
    except (ValueError, TypeError):
        return 0.0
    return math.exp(-days / RECENCY_HALFLIFE_DAYS)


def frequency_score(interaction_count: int) -> float:
    """Log-scaled frequency; saturates at 10 interactions."""
    if interaction_count <= 0:
        return 0.0
    return min(1.0, math.log(1.0 + interaction_count) / math.log(11.0))


def channel_diversity_score(channels: list[str] | None) -> float:
    """Multi-channel bonus; full score at 3+ distinct channels."""
    if not channels:
        return 0.0
    return min(1.0, len(set(channels)) / 3.0)


def edge_warmth(
    strength: float,
    last_interaction_at: str | None,
    interaction_count: int,
    channels: list[str] | None,
    reliability: float | None = None,
) -> float:
    """Composite warmth score for a single relationship edge."""
    rel = reliability if reliability is not None else DEFAULT_RELIABILITY
    return (
        W_STRENGTH * strength
        + W_RECENCY * recency_score(last_interaction_at)
        + W_FREQUENCY * frequency_score(interaction_count)
        + W_CHANNEL * channel_diversity_score(channels)
        + W_RELIABILITY * rel
    )


def compute_path_score(edge_warmths: list[float], hop_count: int) -> float:
    """Multiply edge warmths with a per-hop decay penalty.

    Returns a score in the 0–100 range (PRD Section 4.2).
    """
    if not edge_warmths:
        return 0.0
    product = 1.0
    for w in edge_warmths:
        product *= w
    raw = product * (HOP_DECAY ** max(0, hop_count - 1))
    return round(raw * SCORE_SCALE, 2)


def _most_recent_interaction(path: dict[str, Any]) -> str:
    """Return the most recent last_interaction_at across all edges in a path."""
    best = ""
    for e in path.get("path_edges", []):
        ts = e.get("last_interaction_at") or ""
        if ts > best:
            best = ts
    return best


# ---------------------------------------------------------------------------
# Sorting: tiebreaker logic (PRD Section 7)
# ---------------------------------------------------------------------------
# Primary: path_score DESC
# Tiebreak 1: shorter path preferred (hop_count ASC)
# Tiebreak 2: most recent connector interaction (last_interaction_at DESC)

def _sort_key(path: dict[str, Any]) -> tuple:
    return (
        -path["path_score"],
        path["hop_count"],
        _most_recent_interaction(path),  # later timestamps sort higher lexically
    )


def _sort_paths(paths: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort paths by score DESC, then hop_count ASC, then recency DESC."""
    return sorted(paths, key=lambda p: (_sort_key(p)[0], _sort_key(p)[1], -ord(_sort_key(p)[2][0]) if _sort_key(p)[2] else 0))


# Simpler and correct: use a tuple that sorts properly.
def _rank_paths(paths: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rank paths: highest score first, then shorter, then most recent."""
    return sorted(
        paths,
        key=lambda p: (
            -p["path_score"],
            p["hop_count"],
            # Negate recency: later ISO strings are lexically larger, we want DESC
            "".join(chr(255 - ord(c)) for c in _most_recent_interaction(p))
            if _most_recent_interaction(p)
            else "",
        ),
    )


# ---------------------------------------------------------------------------
# Connector deduplication (PRD Section 7)
# ---------------------------------------------------------------------------
# "A connector appears in multiple paths → show them once in the recommended
#  path. Do not show them again in alternatives — surface different connectors
#  for diversity."

def _connector_ids(path: dict[str, Any]) -> set[str]:
    """Return entity_ids of intermediate nodes (connectors, excluding origin and target)."""
    nodes = path.get("path_nodes", [])
    if len(nodes) <= 2:
        return set()
    return {n["entity_id"] for n in nodes[1:-1]}


def _deduplicate_connectors(paths: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first (highest-scored) path per connector set.

    The recommended path (index 0) is always kept. For alternatives, a path
    is only included if it introduces at least one connector not already seen.
    """
    if len(paths) <= 1:
        return paths

    result = [paths[0]]
    seen_connectors: set[str] = _connector_ids(paths[0])

    for path in paths[1:]:
        connectors = _connector_ids(path)
        if not connectors:
            # Direct path (1-hop) — always include
            result.append(path)
        elif connectors - seen_connectors:
            # Has at least one new connector
            result.append(path)
            seen_connectors |= connectors

    return result


# ---------------------------------------------------------------------------
# Cache abstraction for warm-path query results
# ---------------------------------------------------------------------------


class WarmPathCache(ABC):
    """Interface for warm-path query caching."""

    @abstractmethod
    def get(self, key: str) -> Any | None: ...

    @abstractmethod
    def put(self, key: str, value: Any) -> None: ...

    @abstractmethod
    def invalidate_tenant(self, tenant_id: str) -> int: ...

    @abstractmethod
    def clear(self) -> None: ...

    @property
    @abstractmethod
    def size(self) -> int: ...


class InMemoryWarmPathCache(WarmPathCache):
    """Thread-safe LRU cache with per-entry TTL expiration (in-process)."""

    def __init__(self, max_size: int = 256, ttl_seconds: int = 300) -> None:
        self._max_size = max(1, max_size)
        self._ttl = ttl_seconds
        self._lock = Lock()
        self._data: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.monotonic() > expires_at:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return value

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (time.monotonic() + self._ttl, value)
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)

    def invalidate_tenant(self, tenant_id: str) -> int:
        with self._lock:
            keys = [k for k in self._data if k.startswith(f"{tenant_id}:")]
            for k in keys:
                del self._data[k]
            return len(keys)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    @property
    def size(self) -> int:
        return len(self._data)


class UpstashRedisWarmPathCache(WarmPathCache):
    """Upstash Redis (REST API) backed cache with TTL per key."""

    _KEY_PREFIX = "warm_path:"

    def __init__(self, *, url: str, token: str, ttl_seconds: int = 300) -> None:
        from upstash_redis import Redis

        self._redis = Redis(url=url, token=token)
        self._ttl = ttl_seconds

    def _prefixed(self, key: str) -> str:
        return f"{self._KEY_PREFIX}{key}"

    def get(self, key: str) -> Any | None:
        raw = self._redis.get(self._prefixed(key))
        if raw is None:
            return None
        if isinstance(raw, str):
            return json.loads(raw)
        return raw

    def put(self, key: str, value: Any) -> None:
        self._redis.set(self._prefixed(key), json.dumps(value), ex=self._ttl)

    def invalidate_tenant(self, tenant_id: str) -> int:
        pattern = f"{self._KEY_PREFIX}{tenant_id}:*"
        cursor, count = "0", 0
        while True:
            cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                self._redis.delete(*keys)
                count += len(keys)
            if str(cursor) == "0":
                break
        return count

    def clear(self) -> None:
        pattern = f"{self._KEY_PREFIX}*"
        cursor = "0"
        while True:
            cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                self._redis.delete(*keys)
            if str(cursor) == "0":
                break

    @property
    def size(self) -> int:
        pattern = f"{self._KEY_PREFIX}*"
        cursor, count = "0", 0
        while True:
            cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
            count += len(keys)
            if str(cursor) == "0":
                break
        return count


# Backward-compatible alias used in tests
_TTLCache = InMemoryWarmPathCache


def create_warm_path_cache(settings: Settings) -> WarmPathCache:
    """Factory: Upstash Redis when configured, in-memory fallback otherwise."""
    if settings.upstash_redis_rest_url and settings.upstash_redis_rest_token:
        logger.info("warm_path cache: Upstash Redis at %s", settings.upstash_redis_rest_url)
        return UpstashRedisWarmPathCache(
            url=settings.upstash_redis_rest_url,
            token=settings.upstash_redis_rest_token,
            ttl_seconds=settings.warm_path_cache_ttl_seconds,
        )
    logger.info("warm_path cache: in-memory (Upstash Redis not configured)")
    return InMemoryWarmPathCache(
        max_size=settings.warm_path_cache_max_size,
        ttl_seconds=settings.warm_path_cache_ttl_seconds,
    )


# ---------------------------------------------------------------------------
# Cypher query templates
# ---------------------------------------------------------------------------

# The edge-warmth expression is computed in Python (not Cypher) so we fetch
# raw edge properties and score in the application layer.  This keeps Cypher
# simple and the scoring logic testable.
#
# Disconnected connector filtering: we exclude Person nodes that have
# is_active = false (set when a user disconnects from Kue). This prevents
# disconnected connectors from appearing in any path recommendation.

_TARGETED_QUERY = """
MATCH path = (origin:Person {tenant_id: $tenant_id, entity_id: $origin_id})
             -[:KNOWS*1..$max_hops]->
             (target:Person {tenant_id: $tenant_id, entity_id: $target_id})
WHERE ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
  AND ALL(n IN nodes(path) WHERE coalesce(n.is_active, true) = true)
RETURN
  [n IN nodes(path) | {
    entity_id: n.entity_id,
    name: n.name,
    primary_email: n.primary_email,
    company: n.domain
  }] AS path_nodes,
  [r IN relationships(path) | {
    from_entity_id: startNode(r).entity_id,
    to_entity_id: endNode(r).entity_id,
    strength: r.strength,
    interaction_count: r.interaction_count,
    last_interaction_at: r.last_interaction_at,
    channels: r.channels
  }] AS path_edges,
  length(path) AS hop_count
ORDER BY reduce(s = 1.0, r IN relationships(path) | s * coalesce(r.strength, 0)) DESC
LIMIT $max_paths
"""

_DISCOVER_QUERY = """
MATCH path = (origin:Person {tenant_id: $tenant_id, entity_id: $origin_id})
             -[:KNOWS*1..$max_hops]->
             (target:Person)
WHERE target <> origin
  AND target.tenant_id = $tenant_id
  AND ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
  AND ALL(n IN nodes(path) WHERE coalesce(n.is_active, true) = true)
RETURN
  [n IN nodes(path) | {
    entity_id: n.entity_id,
    name: n.name,
    primary_email: n.primary_email,
    company: n.domain
  }] AS path_nodes,
  [r IN relationships(path) | {
    from_entity_id: startNode(r).entity_id,
    to_entity_id: endNode(r).entity_id,
    strength: r.strength,
    interaction_count: r.interaction_count,
    last_interaction_at: r.last_interaction_at,
    channels: r.channels
  }] AS path_edges,
  length(path) AS hop_count
ORDER BY reduce(s = 1.0, r IN relationships(path) | s * coalesce(r.strength, 0)) DESC
LIMIT $max_paths
"""

# Large-network variants: only traverse KNOWS edges with strength >= threshold.
# Used when the tenant's Person count exceeds LARGE_NETWORK_THRESHOLD.

_TARGETED_QUERY_STRONG = """
MATCH path = (origin:Person {tenant_id: $tenant_id, entity_id: $origin_id})
             -[:KNOWS*1..$max_hops]->
             (target:Person {tenant_id: $tenant_id, entity_id: $target_id})
WHERE ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
  AND ALL(n IN nodes(path) WHERE coalesce(n.is_active, true) = true)
  AND ALL(r IN relationships(path) WHERE r.strength >= $min_strength)
RETURN
  [n IN nodes(path) | {
    entity_id: n.entity_id,
    name: n.name,
    primary_email: n.primary_email,
    company: n.domain
  }] AS path_nodes,
  [r IN relationships(path) | {
    from_entity_id: startNode(r).entity_id,
    to_entity_id: endNode(r).entity_id,
    strength: r.strength,
    interaction_count: r.interaction_count,
    last_interaction_at: r.last_interaction_at,
    channels: r.channels
  }] AS path_edges,
  length(path) AS hop_count
ORDER BY reduce(s = 1.0, r IN relationships(path) | s * coalesce(r.strength, 0)) DESC
LIMIT $max_paths
"""

_DISCOVER_QUERY_STRONG = """
MATCH path = (origin:Person {tenant_id: $tenant_id, entity_id: $origin_id})
             -[:KNOWS*1..$max_hops]->
             (target:Person)
WHERE target <> origin
  AND target.tenant_id = $tenant_id
  AND ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
  AND ALL(n IN nodes(path) WHERE coalesce(n.is_active, true) = true)
  AND ALL(r IN relationships(path) WHERE r.strength >= $min_strength)
RETURN
  [n IN nodes(path) | {
    entity_id: n.entity_id,
    name: n.name,
    primary_email: n.primary_email,
    company: n.domain
  }] AS path_nodes,
  [r IN relationships(path) | {
    from_entity_id: startNode(r).entity_id,
    to_entity_id: endNode(r).entity_id,
    strength: r.strength,
    interaction_count: r.interaction_count,
    last_interaction_at: r.last_interaction_at,
    channels: r.channels
  }] AS path_edges,
  length(path) AS hop_count
ORDER BY reduce(s = 1.0, r IN relationships(path) | s * coalesce(r.strength, 0)) DESC
LIMIT $max_paths
"""

_NETWORK_SIZE_QUERY = """
MATCH (p:Person {tenant_id: $tenant_id})
RETURN count(p) AS person_count
"""

_RESOLVE_ORIGIN_QUERY = """
MATCH (p:Person {tenant_id: $tenant_id, primary_email: $email})
RETURN p.entity_id AS entity_id
LIMIT 1
"""

# Default thresholds (can be overridden via Settings)
LARGE_NETWORK_THRESHOLD: int = 10_000
LARGE_NETWORK_MIN_STRENGTH: float = 0.3


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

def _score_raw_path(raw: dict[str, Any]) -> dict[str, Any]:
    """Take a raw Cypher result row and attach computed warmth scores."""
    edges = raw.get("path_edges", [])
    hop_count = int(raw.get("hop_count", len(edges)))

    scored_edges: list[dict[str, Any]] = []
    warmths: list[float] = []
    for e in edges:
        w = edge_warmth(
            strength=float(e.get("strength") or 0.0),
            last_interaction_at=e.get("last_interaction_at"),
            interaction_count=int(e.get("interaction_count") or 0),
            channels=e.get("channels") or [],
        )
        warmths.append(w)
        scored_edges.append({**e, "edge_warmth": round(w, 4)})

    return {
        "path_nodes": raw.get("path_nodes", []),
        "path_edges": scored_edges,
        "path_score": compute_path_score(warmths, hop_count),
        "hop_count": hop_count,
        "is_direct": hop_count == 1,
    }


@dataclass
class WarmPathService:
    graph_store: GraphStore
    _cache: WarmPathCache = field(default_factory=InMemoryWarmPathCache)
    _large_network_threshold: int = LARGE_NETWORK_THRESHOLD
    _large_network_min_strength: float = LARGE_NETWORK_MIN_STRENGTH

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_large_network(self, tenant_id: str) -> bool:
        """Check if a tenant's network exceeds the large-network threshold."""
        cache_key = f"{tenant_id}:__network_size__"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached >= self._large_network_threshold

        rows = self.graph_store.run_read_query(
            _NETWORK_SIZE_QUERY, tenant_id=tenant_id
        )
        count = int(rows[0]["person_count"]) if rows else 0
        self._cache.put(cache_key, count)
        return count >= self._large_network_threshold

    def _cache_key(self, *parts: str | int | float) -> str:
        return ":".join(str(p) for p in parts)

    def invalidate_tenant_cache(self, tenant_id: str) -> int:
        """Clear all cached results for a tenant. Call after pipeline refresh."""
        return self._cache.invalidate_tenant(tenant_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_paths_to_target(
        self,
        *,
        tenant_id: str,
        origin_entity_id: str,
        target_entity_id: str,
        max_hops: int = 3,
        max_paths: int = 5,
    ) -> list[dict[str, Any]]:
        max_hops = min(max(1, max_hops), MAX_HOPS_HARD_CAP)
        max_paths = min(max(1, max_paths), MAX_PATHS_HARD_CAP)

        key = self._cache_key(
            tenant_id, "targeted", origin_entity_id, target_entity_id, max_hops, max_paths
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        large = self._is_large_network(tenant_id)
        query = _TARGETED_QUERY_STRONG if large else _TARGETED_QUERY
        params: dict[str, Any] = dict(
            tenant_id=tenant_id,
            origin_id=origin_entity_id,
            target_id=target_entity_id,
            max_hops=max_hops,
            max_paths=max_paths * 3,
        )
        if large:
            params["min_strength"] = self._large_network_min_strength

        rows = self.graph_store.run_read_query(query, **params)

        scored = _rank_paths([_score_raw_path(r) for r in rows])
        deduped = _deduplicate_connectors(scored)
        result = deduped[:max_paths]
        self._cache.put(key, result)
        return result

    def discover_warmest_paths(
        self,
        *,
        tenant_id: str,
        origin_entity_id: str,
        max_hops: int = 3,
        max_paths: int = 20,
        min_score: float = 0.0,
    ) -> list[dict[str, Any]]:
        max_hops = min(max(1, max_hops), MAX_HOPS_HARD_CAP)
        max_paths = min(max(1, max_paths), MAX_PATHS_HARD_CAP)

        key = self._cache_key(
            tenant_id, "discover", origin_entity_id, max_hops, max_paths, min_score
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        large = self._is_large_network(tenant_id)
        query = _DISCOVER_QUERY_STRONG if large else _DISCOVER_QUERY
        params: dict[str, Any] = dict(
            tenant_id=tenant_id,
            origin_id=origin_entity_id,
            max_hops=max_hops,
            max_paths=max_paths * 3,
        )
        if large:
            params["min_strength"] = self._large_network_min_strength

        rows = self.graph_store.run_read_query(query, **params)

        scored = _rank_paths([_score_raw_path(r) for r in rows])
        if min_score > 0:
            scored = [p for p in scored if p["path_score"] >= min_score]
        deduped = _deduplicate_connectors(scored)
        result = deduped[:max_paths]
        self._cache.put(key, result)
        return result

    def resolve_origin(self, *, tenant_id: str, email: str) -> str | None:
        rows = self.graph_store.run_read_query(
            _RESOLVE_ORIGIN_QUERY,
            tenant_id=tenant_id,
            email=email.strip().lower(),
        )
        if rows:
            return str(rows[0].get("entity_id"))
        return None
