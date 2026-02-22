from __future__ import annotations

import logging
from urllib.parse import urlparse
from typing import Any

import inngest

from app.core.config import settings
from app.ingestion.google_connector import GoogleOAuthConnector, GoogleOAuthContext
from app.ingestion.canonical_store import create_canonical_event_store
from app.ingestion.parsers import parse_raw_events
from app.ingestion.raw_store import create_raw_event_store
from app.schemas import GoogleMockSourceType, SourceEvent


def _effective_signing_key() -> str | None:
    parsed = urlparse(settings.inngest_base_url)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1"}:
        # In local inngest dev mode, disable cloud signing verification.
        return None
    return settings.inngest_signing_key or None


inngest_client = inngest.Inngest(
    app_id=settings.inngest_source_app,
    event_key=settings.inngest_event_key or None,
    signing_key=_effective_signing_key(),
    logger=logging.getLogger("uvicorn"),
)


def _validate_source_events(events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    if not events_payload:
        raise ValueError("source_events cannot be empty")
    events = [SourceEvent.model_validate(item) for item in events_payload]
    return {"count": len(events)}


def _validate_oauth_payload(data: dict[str, Any]) -> dict[str, Any]:
    required = ["trace_id", "tenant_id", "user_id"]
    missing = [field for field in required if not data.get(field)]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")
    return {"ok": True}


async def _fetch_google_source_events_from_oauth(data: dict[str, Any]) -> dict[str, Any]:
    connector = GoogleOAuthConnector()
    source_events = await connector.handle_callback(
        code=str(data["code"]),
        context=GoogleOAuthContext(
            tenant_id=str(data["tenant_id"]),
            user_id=str(data["user_id"]),
            trace_id=str(data["trace_id"]),
        ),
    )
    return {
        "count": len(source_events),
        "source_events": [event.model_dump(mode="json") for event in source_events],
    }


def _fetch_google_source_events_from_mock(data: dict[str, Any]) -> dict[str, Any]:
    connector = GoogleOAuthConnector()
    source_events = connector.handle_mock_callback(
        context=GoogleOAuthContext(
            tenant_id=str(data["tenant_id"]),
            user_id=str(data["user_id"]),
            trace_id=str(data["trace_id"]),
        ),
        source_type=GoogleMockSourceType(str(data["source_type"])),
        payload=dict(data["payload"]),
    )
    return {
        "count": len(source_events),
        "source_events": [event.model_dump(mode="json") for event in source_events],
    }


def _persist_raw_events(events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    events = [SourceEvent.model_validate(item) for item in events_payload]
    store = create_raw_event_store(settings)
    result = store.persist_source_events(events)
    trace_id = events[0].trace_id if events else None
    return {
        "stored_count": result.stored_count,
        "captured_at": result.captured_at.isoformat(),
        "store": store.store_name,
        "trace_id": trace_id,
    }


def _fetch_raw_events(trace_id: str) -> dict[str, Any]:
    store = create_raw_event_store(settings)
    raw_events = store.list_by_trace_id(trace_id)
    return {
        "count": len(raw_events),
        "events": [event.model_dump(mode="json") for event in raw_events],
    }


def _parse_raw_events(raw_events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    # Convert payload dictionaries into the same model expected by the parser.
    # We reuse Pydantic validation on RawCapturedEvent through model_validate.
    from app.schemas import RawCapturedEvent  # local import to avoid cycle on module load

    typed_raw_events = [RawCapturedEvent.model_validate(item) for item in raw_events_payload]
    parse_result = parse_raw_events(typed_raw_events)
    return {
        "parsed_count": len(parse_result.parsed_events),
        "failed_count": len(parse_result.failures),
        "parsed_events": [
            {
                "raw_event_id": item.raw_event_id,
                "tenant_id": item.tenant_id,
                "user_id": item.user_id,
                "trace_id": item.trace_id,
                "source": str(item.source),
                "source_event_id": item.source_event_id,
                "occurred_at": item.occurred_at.isoformat(),
                "event_type": str(item.event_type),
                "normalized": item.normalized,
                "parse_warnings": item.parse_warnings,
            }
            for item in parse_result.parsed_events
        ],
        "failures": [
            {
                "raw_event_id": item.raw_event_id,
                "source_event_id": item.source_event_id,
                "reason": item.reason,
            }
            for item in parse_result.failures
        ],
    }


def _persist_canonical(parsed_payload: dict[str, Any], parser_version: str) -> dict[str, Any]:
    from app.ingestion.parsers import ParsedCanonicalEvent
    from app.schemas import CanonicalEventType, IngestionSource
    from datetime import datetime

    parsed_events: list[ParsedCanonicalEvent] = []
    for item in parsed_payload.get("parsed_events", []):
        parsed_events.append(
            ParsedCanonicalEvent(
                raw_event_id=int(item["raw_event_id"]),
                tenant_id=str(item["tenant_id"]),
                user_id=str(item["user_id"]),
                trace_id=str(item["trace_id"]),
                source=IngestionSource(str(item["source"])),
                source_event_id=str(item["source_event_id"]),
                occurred_at=datetime.fromisoformat(str(item["occurred_at"])),
                event_type=CanonicalEventType(str(item["event_type"])),
                normalized=dict(item["normalized"]),
                parse_warnings=list(item.get("parse_warnings", [])),
            )
        )

    store = create_canonical_event_store(settings)
    persist_result = store.persist_parsed_events(parsed_events, parser_version)
    return {
        "stored_count": persist_result.stored_count,
        "store": store.store_name,
        "parser_version": parser_version,
        "parsed_at": persist_result.parsed_at.isoformat(),
    }


@inngest_client.create_function(
    fn_id="ingestion-pipeline-run",
    trigger=inngest.TriggerEvent(event="pipeline/run.requested"),
)
async def ingestion_pipeline_run(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    trace_id = str(data["trace_id"])
    parser_version = str(data.get("parser_version") or settings.parser_version)
    source_events_payload = list(data.get("source_events", []))

    await ctx.step.run("stage.raw_capture.layer.validate", _validate_source_events, source_events_payload)
    raw_result = await ctx.step.run("stage.raw_capture.layer.persist", _persist_raw_events, source_events_payload)

    raw_fetch = await ctx.step.run("stage.canonicalization.layer.fetch_raw", _fetch_raw_events, trace_id)
    parse_result = await ctx.step.run(
        "stage.canonicalization.layer.parse",
        _parse_raw_events,
        list(raw_fetch.get("events", [])),
    )
    canonical_result = await ctx.step.run(
        "stage.canonicalization.layer.persist",
        _persist_canonical,
        parse_result,
        parser_version,
    )

    return {
        "trace_id": trace_id,
        "status": "completed",
        "raw_capture": raw_result,
        "canonicalization": {
            "raw_count": raw_fetch.get("count", 0),
            "parsed_count": parse_result.get("parsed_count", 0),
            "failed_count": parse_result.get("failed_count", 0),
            **canonical_result,
        },
    }


@inngest_client.create_function(
    fn_id="ingestion-user-connected",
    trigger=inngest.TriggerEvent(event="kue/user.connected"),
)
async def ingestion_user_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)
    trace_id = str(data["trace_id"])

    await ctx.step.run("stage.intake.layer.validate_payload", _validate_oauth_payload, data)
    fetched = await ctx.step.run("stage.intake.layer.fetch_google", _fetch_google_source_events_from_oauth, data)
    source_events_payload = list(fetched.get("source_events", []))

    await ctx.step.run("stage.raw_capture.layer.validate", _validate_source_events, source_events_payload)
    raw_result = await ctx.step.run("stage.raw_capture.layer.persist", _persist_raw_events, source_events_payload)

    raw_fetch = await ctx.step.run("stage.canonicalization.layer.fetch_raw", _fetch_raw_events, trace_id)
    parse_result = await ctx.step.run(
        "stage.canonicalization.layer.parse",
        _parse_raw_events,
        list(raw_fetch.get("events", [])),
    )
    canonical_result = await ctx.step.run(
        "stage.canonicalization.layer.persist",
        _persist_canonical,
        parse_result,
        parser_version,
    )

    return {
        "trace_id": trace_id,
        "status": "completed",
        "fetched_count": fetched.get("count", 0),
        "raw_capture": raw_result,
        "canonicalization": {
            "raw_count": raw_fetch.get("count", 0),
            "parsed_count": parse_result.get("parsed_count", 0),
            "failed_count": parse_result.get("failed_count", 0),
            **canonical_result,
        },
    }


@inngest_client.create_function(
    fn_id="ingestion-user-mock-connected",
    trigger=inngest.TriggerEvent(event="kue/user.mock_connected"),
)
async def ingestion_user_mock_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)
    trace_id = str(data["trace_id"])

    await ctx.step.run("stage.intake.layer.validate_payload", _validate_oauth_payload, data)
    fetched = await ctx.step.run("stage.intake.layer.fetch_mock", _fetch_google_source_events_from_mock, data)
    source_events_payload = list(fetched.get("source_events", []))

    await ctx.step.run("stage.raw_capture.layer.validate", _validate_source_events, source_events_payload)
    raw_result = await ctx.step.run("stage.raw_capture.layer.persist", _persist_raw_events, source_events_payload)

    raw_fetch = await ctx.step.run("stage.canonicalization.layer.fetch_raw", _fetch_raw_events, trace_id)
    parse_result = await ctx.step.run(
        "stage.canonicalization.layer.parse",
        _parse_raw_events,
        list(raw_fetch.get("events", [])),
    )
    canonical_result = await ctx.step.run(
        "stage.canonicalization.layer.persist",
        _persist_canonical,
        parse_result,
        parser_version,
    )
    return {
        "trace_id": trace_id,
        "status": "completed",
        "fetched_count": fetched.get("count", 0),
        "raw_capture": raw_result,
        "canonicalization": {
            "raw_count": raw_fetch.get("count", 0),
            "parsed_count": parse_result.get("parsed_count", 0),
            "failed_count": parse_result.get("failed_count", 0),
            **canonical_result,
        },
    }


@inngest_client.create_function(
    fn_id="ingestion-stage-canonicalization-replay",
    trigger=inngest.TriggerEvent(event="pipeline/stage.canonicalization.replay.requested"),
)
async def replay_canonicalization(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    trace_id = str(data["trace_id"])
    parser_version = str(data.get("parser_version") or settings.parser_version)

    raw_fetch = await ctx.step.run("stage.canonicalization.layer.fetch_raw", _fetch_raw_events, trace_id)
    parse_result = await ctx.step.run(
        "stage.canonicalization.layer.parse",
        _parse_raw_events,
        list(raw_fetch.get("events", [])),
    )
    canonical_result = await ctx.step.run(
        "stage.canonicalization.layer.persist",
        _persist_canonical,
        parse_result,
        parser_version,
    )
    return {
        "trace_id": trace_id,
        "raw_count": raw_fetch.get("count", 0),
        "parsed_count": parse_result.get("parsed_count", 0),
        "failed_count": parse_result.get("failed_count", 0),
        **canonical_result,
    }


inngest_functions = [
    ingestion_user_connected,
    ingestion_user_mock_connected,
    ingestion_pipeline_run,
    replay_canonicalization,
]
