from __future__ import annotations

import sqlite3
from pathlib import Path

_TURSO_REPLICA_PATH = "data/turso_replica.db"


class _TursoConnection:
    """Thin wrapper around libsql Connection that adds sqlite3-compatible context manager support."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._conn.commit()
        else:
            try:
                self._conn.rollback()
            except Exception:
                pass
        return False

    def execute(self, sql, parameters=()):
        return self._conn.execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self._conn.executemany(sql, parameters)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value


def get_connection(db_path: str):
    """Return a database connection.

    Uses Turso (libsql) when turso_url and turso_auth_token are configured,
    otherwise falls back to local SQLite at db_path.
    """
    from app.core.config import settings

    if settings.turso_url and settings.turso_auth_token:
        return _turso_connect(settings.turso_url, settings.turso_auth_token)
    return sqlite3.connect(db_path)


def _turso_connect(url: str, token: str) -> _TursoConnection:
    import libsql_experimental as libsql

    Path(_TURSO_REPLICA_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = libsql.connect(
        _TURSO_REPLICA_PATH,
        sync_url=url,
        auth_token=token,
    )
    return _TursoConnection(conn)
