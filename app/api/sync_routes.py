from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from app.auth.deps import get_current_user
from app.core.config import settings
from app.ingestion.google_connector import (
    GoogleConnectorError,
    GoogleOAuthConnector,
    GoogleOAuthContext,
)
from app.ingestion.pipeline_store import create_pipeline_store
from app.ingestion.raw_store import create_raw_event_store
from app.ingestion.source_connection_store import create_source_connection_store
from app.schemas import IngestionSource

router = APIRouter(prefix="/v1/sync", tags=["sync"])


def _get_google_access_token(tenant_id: str, user_id: str) -> str:
    """Look up the stored Google access token for this user. Raises HTTPException if none."""
    store = create_source_connection_store(settings)
    connections = store.get_connections_for_user(tenant_id, user_id)
    google_sources = {
        IngestionSource.GMAIL,
        IngestionSource.GOOGLE_CONTACTS,
        IngestionSource.GOOGLE_CALENDAR,
    }
    for conn in connections:
        if conn.source in google_sources:
            token = conn.token_json.get("access_token") or ""
            if token:
                return str(token)
    raise HTTPException(
        status_code=404,
        detail="No Google connection found. Connect your Google account first via /v1/ingestion/google/oauth/callback",
    )


def _persist_and_ingest(
    *,
    tenant_id: str,
    user_id: str,
    trace_id: str,
    source_events: list,
    source_label: str,
) -> dict[str, Any]:
    pipeline_store = create_pipeline_store(settings)
    raw_store = create_raw_event_store(settings)

    pipeline_store.ensure_tenant_user(tenant_id, user_id)
    run_id = pipeline_store.create_pipeline_run(
        trace_id=trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source=source_events[0].source if source_events else source_label,
        trigger_type="kue/sync.triggered",
        source_event_id=str(source_events[0].source_event_id)
        if source_events
        else None,
        metadata={"ingest_path": f"sync/{source_label}"},
    )
    raw_store.persist_source_events(source_events, run_id=run_id, ingest_version="v1")
    return {"run_id": run_id, "trace_id": trace_id, "event_count": len(source_events)}


@router.post("/google/contacts")
async def sync_google_contacts(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Fetch Google Contacts and ingest them into the pipeline."""
    tenant_id: str = current_user["tenant_id"]
    user_id: str = current_user["sub"]
    access_token = _get_google_access_token(tenant_id, user_id)

    trace_id = f"trace_{uuid4().hex}"
    ctx = GoogleOAuthContext(tenant_id=tenant_id, user_id=user_id, trace_id=trace_id)
    connector = GoogleOAuthConnector()

    import httpx as _httpx

    try:
        async with _httpx.AsyncClient(timeout=20.0) as client:
            contacts = await connector._fetch_contacts(
                client=client, access_token=access_token
            )
        events = connector._to_contact_events(context=ctx, contacts=contacts)
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _persist_and_ingest(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=trace_id,
        source_events=events,
        source_label="google_contacts",
    )


@router.post("/google/gmail")
async def sync_google_gmail(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Fetch Gmail messages and ingest them into the pipeline."""
    tenant_id: str = current_user["tenant_id"]
    user_id: str = current_user["sub"]
    access_token = _get_google_access_token(tenant_id, user_id)

    trace_id = f"trace_{uuid4().hex}"
    ctx = GoogleOAuthContext(tenant_id=tenant_id, user_id=user_id, trace_id=trace_id)
    connector = GoogleOAuthConnector()

    import httpx as _httpx

    try:
        async with _httpx.AsyncClient(timeout=30.0) as client:
            messages = await connector._fetch_gmail_messages(
                client=client, access_token=access_token
            )
        events = connector._to_gmail_events(context=ctx, messages=messages)
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _persist_and_ingest(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=trace_id,
        source_events=events,
        source_label="gmail",
    )


@router.post("/google/calendar")
async def sync_google_calendar(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Fetch Google Calendar events and ingest them into the pipeline."""
    tenant_id: str = current_user["tenant_id"]
    user_id: str = current_user["sub"]
    access_token = _get_google_access_token(tenant_id, user_id)

    trace_id = f"trace_{uuid4().hex}"
    ctx = GoogleOAuthContext(tenant_id=tenant_id, user_id=user_id, trace_id=trace_id)
    connector = GoogleOAuthConnector()

    import httpx as _httpx

    try:
        async with _httpx.AsyncClient(timeout=20.0) as client:
            cal_events = await connector._fetch_calendar_events(
                client=client, access_token=access_token
            )
        events = connector._to_calendar_events(context=ctx, events=cal_events)
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _persist_and_ingest(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=trace_id,
        source_events=events,
        source_label="google_calendar",
    )


@router.post("/google/all")
async def sync_google_all(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Fetch Google Contacts, Gmail, and Calendar in parallel and ingest all."""
    tenant_id: str = current_user["tenant_id"]
    user_id: str = current_user["sub"]
    access_token = _get_google_access_token(tenant_id, user_id)

    trace_id = f"trace_{uuid4().hex}"
    ctx = GoogleOAuthContext(tenant_id=tenant_id, user_id=user_id, trace_id=trace_id)
    connector = GoogleOAuthConnector()

    import httpx as _httpx

    try:
        async with _httpx.AsyncClient(timeout=30.0) as client:
            contacts_task = connector._fetch_contacts(
                client=client, access_token=access_token
            )
            gmail_task = connector._fetch_gmail_messages(
                client=client, access_token=access_token
            )
            calendar_task = connector._fetch_calendar_events(
                client=client, access_token=access_token
            )
            contacts, messages, cal_events = await asyncio.gather(
                contacts_task, gmail_task, calendar_task
            )
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    contact_events = connector._to_contact_events(context=ctx, contacts=contacts)
    gmail_events = connector._to_gmail_events(context=ctx, messages=messages)
    calendar_events = connector._to_calendar_events(context=ctx, events=cal_events)
    all_events = contact_events + gmail_events + calendar_events

    result = _persist_and_ingest(
        tenant_id=tenant_id,
        user_id=user_id,
        trace_id=trace_id,
        source_events=all_events,
        source_label="google_all",
    )
    result["breakdown"] = {
        "contacts": len(contact_events),
        "gmail": len(gmail_events),
        "calendar": len(calendar_events),
    }
    return result
