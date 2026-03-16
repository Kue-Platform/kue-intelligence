from __future__ import annotations

import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from app.ingestion.db import get_connection


_lock = Lock()


def _ensure_tables(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL UNIQUE,
                tenant_id TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                name TEXT,
                google_sub TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS otp_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_otp_email_code ON otp_codes(email, code)"
        )
        conn.commit()


def _db_path() -> str:
    from app.core.config import settings

    return settings.auth_db_path


def generate_otp(email: str) -> str:
    """Store a 6-digit OTP for the given email. Returns the code."""
    from app.core.config import settings

    code = str(secrets.randbelow(900000) + 100000)  # 100000-999999
    db = _db_path()
    _ensure_tables(db)
    now = datetime.now(UTC)
    expires_at = now + timedelta(seconds=settings.otp_expiry_seconds)

    with _lock:
        with get_connection(db) as conn:
            # Invalidate previous codes for this email
            conn.execute(
                "UPDATE otp_codes SET used = 1 WHERE email = ? AND used = 0", (email,)
            )
            conn.execute(
                "INSERT INTO otp_codes (email, code, expires_at, created_at) VALUES (?, ?, ?, ?)",
                (email, code, expires_at.isoformat(), now.isoformat()),
            )
            conn.commit()
    return code


def verify_otp(email: str, code: str) -> bool:
    """Return True and mark used if the OTP is valid and unexpired. False otherwise."""
    db = _db_path()
    _ensure_tables(db)
    now = datetime.now(UTC)

    with _lock:
        with get_connection(db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, expires_at FROM otp_codes
                WHERE email = ? AND code = ? AND used = 0
                ORDER BY id DESC LIMIT 1
                """,
                (email, code),
            )
            row = cursor.fetchone()
            if not row:
                return False
            expires_at = datetime.fromisoformat(
                str(row["expires_at"]).replace("Z", "+00:00")
            )
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            if now > expires_at:
                return False
            conn.execute("UPDATE otp_codes SET used = 1 WHERE id = ?", (row["id"],))
            conn.commit()
    return True


def get_or_create_user(
    *, email: str, name: str | None = None, google_sub: str | None = None
) -> dict[str, Any]:
    """Return existing user by email or create a new one. Returns user dict with user_id and tenant_id."""
    db = _db_path()
    _ensure_tables(db)
    now = datetime.now(UTC).isoformat()

    with _lock:
        with get_connection(db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM users WHERE email = ?", (email,))
            row = cursor.fetchone()
            if row:
                # Update name/google_sub if provided
                if name or google_sub:
                    updates: list[str] = ["updated_at = ?"]
                    params: list[Any] = [now]
                    if name:
                        updates.append("name = ?")
                        params.append(name)
                    if google_sub:
                        updates.append("google_sub = ?")
                        params.append(google_sub)
                    params.append(int(row["id"]))
                    conn.execute(
                        f"UPDATE users SET {', '.join(updates)} WHERE id = ?",
                        params,
                    )
                    conn.commit()
                return {
                    "user_id": str(row["user_id"]),
                    "tenant_id": str(row["tenant_id"]),
                    "email": str(row["email"]),
                    "name": row["name"],
                }

            user_id = uuid4().hex
            tenant_id = user_id  # personal tenant
            conn.execute(
                """
                INSERT INTO users (user_id, tenant_id, email, name, google_sub, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, tenant_id, email, name, google_sub, now, now),
            )
            conn.commit()
    return {"user_id": user_id, "tenant_id": tenant_id, "email": email, "name": name}
