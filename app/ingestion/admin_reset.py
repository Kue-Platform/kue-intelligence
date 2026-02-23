from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings


@dataclass
class ResetResult:
    ok: bool
    mode: str
    details: dict[str, Any]


def reset_all_data(settings: Settings) -> ResetResult:
    if settings.supabase_url and (settings.supabase_service_role_key or settings.supabase_anon_key):
        api_key = settings.supabase_service_role_key or settings.supabase_anon_key
        if not api_key:
            raise RuntimeError("Supabase API key is missing for reset.")
        url = f"{settings.supabase_url.rstrip('/')}/rest/v1/rpc/kue_admin_reset_data"
        response = httpx.post(
            url,
            headers={
                "apikey": api_key,
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={},
            timeout=30.0,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Supabase reset RPC failed ({response.status_code}): {response.text}")
        payload = response.json() if response.text else {}
        return ResetResult(ok=True, mode="supabase_rpc", details={"response": payload})

    removed: list[str] = []
    for db_path in [settings.raw_events_db_path, settings.canonical_events_db_path, settings.pipeline_db_path]:
        target = Path(db_path)
        if target.exists():
            target.unlink()
            removed.append(str(target))
    return ResetResult(ok=True, mode="local_sqlite_delete", details={"deleted_files": removed})
