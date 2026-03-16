from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any


@dataclass
class CacheEntry:
    namespace: str
    key: str
    value: dict[str, Any]
    version: str
    expires_at: datetime
    created_at: datetime


class InMemoryCacheRegistry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._store: dict[str, CacheEntry] = {}

    def _compound_key(self, namespace: str, version: str, key: str) -> str:
        return f"{namespace}:{version}:{key}"

    def put(
        self,
        *,
        namespace: str,
        key: str,
        value: dict[str, Any],
        version: str = "v1",
        ttl_seconds: int = 60 * 60 * 24 * 90,
    ) -> None:
        now = datetime.now(UTC)
        with self._lock:
            self._evict_locked(now)
            compound = self._compound_key(namespace, version, key)
            self._store[compound] = CacheEntry(
                namespace=namespace,
                key=key,
                value=value,
                version=version,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )

    def get(
        self, *, namespace: str, key: str, version: str = "v1"
    ) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        with self._lock:
            self._evict_locked(now)
            compound = self._compound_key(namespace, version, key)
            entry = self._store.get(compound)
            if entry is None:
                return None
            return dict(entry.value)

    def stats(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        with self._lock:
            self._evict_locked(now)
            by_namespace: dict[str, int] = {}
            for entry in self._store.values():
                by_namespace[entry.namespace] = by_namespace.get(entry.namespace, 0) + 1
            return {
                "total_entries": len(self._store),
                "namespaces": by_namespace,
            }

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def _evict_locked(self, now: datetime) -> None:
        expired = [k for k, v in self._store.items() if v.expires_at <= now]
        for key in expired:
            self._store.pop(key, None)


cache_registry = InMemoryCacheRegistry()
