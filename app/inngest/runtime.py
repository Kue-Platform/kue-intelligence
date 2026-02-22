from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import inngest

from app.core.config import settings
from app.ingestion.canonical_store import create_canonical_event_store
from app.ingestion.google_connector import GoogleOAuthConnector, GoogleOAuthContext
from app.ingestion.parsers import parse_raw_events
from app.ingestion.pipeline_store import PipelineStore, create_pipeline_store
from app.ingestion.raw_store import create_raw_event_store
from app.schemas import GoogleMockSourceType, IngestionSource, SourceEvent


def _effective_signing_key() -> str | None:
    parsed = urlparse(settings.inngest_base_url)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1"}:
        return None
    return settings.inngest_signing_key or None


inngest_client = inngest.Inngest(
    app_id=settings.inngest_source_app,
    event_key=settings.inngest_event_key or None,
    signing_key=_effective_signing_key(),
    logger=logging.getLogger("uvicorn"),
)


@lru_cache(maxsize=1)
def _pipeline_store() -> PipelineStore:
    return create_pipeline_store(settings)


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


def _persist_raw_events(run_id: str, events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    events = [SourceEvent.model_validate(item) for item in events_payload]
    store = create_raw_event_store(settings)
    result = store.persist_source_events(
        events,
        run_id=run_id,
        ingest_version="v1",
    )
    trace_id = events[0].trace_id if events else None
    return {
        "stored_count": result.stored_count,
        "captured_at": result.captured_at.isoformat(),
        "store": store.store_name,
        "trace_id": trace_id,
        "run_id": run_id,
    }


def _fetch_raw_events(trace_id: str) -> dict[str, Any]:
    store = create_raw_event_store(settings)
    raw_events = store.list_by_trace_id(trace_id)
    return {
        "count": len(raw_events),
        "events": [event.model_dump(mode="json") for event in raw_events],
    }


def _parse_raw_events(raw_events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    from app.schemas import RawCapturedEvent

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


def _persist_canonical(run_id: str, parsed_payload: dict[str, Any], parser_version: str) -> dict[str, Any]:
    from datetime import datetime

    from app.ingestion.parsers import ParsedCanonicalEvent
    from app.schemas import CanonicalEventType, IngestionSource

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
    persist_result = store.persist_parsed_events(
        parsed_events,
        parser_version,
        run_id=run_id,
        schema_version="v1",
        parse_status="parsed",
    )
    return {
        "stored_count": persist_result.stored_count,
        "store": store.store_name,
        "parser_version": parser_version,
        "parsed_at": persist_result.parsed_at.isoformat(),
        "run_id": run_id,
    }


def _resolve_source_hint(data: dict[str, Any], source_events_payload: list[dict[str, Any]]) -> IngestionSource | None:
    if source_events_payload:
        first_source = source_events_payload[0].get("source")
        if first_source:
            return IngestionSource(str(first_source))

    source_type = data.get("source_type")
    if source_type == "contacts":
        return IngestionSource.GOOGLE_CONTACTS
    if source_type == "gmail":
        return IngestionSource.GMAIL
    if source_type == "calendar":
        return IngestionSource.GOOGLE_CALENDAR

    source = data.get("source")
    if source:
        return IngestionSource(str(source))
    return None


def _ensure_pipeline_run(
    data: dict[str, Any],
    trigger_type: str,
    source_events_payload: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    tenant_id = str(data["tenant_id"])
    user_id = str(data["user_id"])
    trace_id = str(data["trace_id"])
    source = _resolve_source_hint(data, source_events_payload or [])
    source_event_id = None
    if source_events_payload:
        source_event_id = str(source_events_payload[0].get("source_event_id") or "")
    source_event_id = source_event_id or (str(data.get("source_event_id")) if data.get("source_event_id") else None)

    store = _pipeline_store()
    store.ensure_tenant_user(tenant_id, user_id)
    run_id = store.create_pipeline_run(
        trace_id=trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        source=source,
        trigger_type=trigger_type,
        source_event_id=source_event_id,
        metadata={"event_name": trigger_type},
    )
    return {"run_id": run_id, "trace_id": trace_id, "tenant_id": tenant_id, "user_id": user_id}


def _run_layer_internal(
    *,
    run_id: str,
    stage_key: str,
    layer_key: str,
    records_in: int | None,
    op: str,
    payload: dict[str, Any],
) -> Any:
    store = _pipeline_store()
    stage_run_id = store.start_stage_run(
        run_id=run_id,
        stage_key=stage_key,
        layer_key=layer_key,
        records_in=records_in,
    )
    try:
        if op == "validate_source_events":
            result = _validate_source_events(list(payload["source_events"]))
        elif op == "persist_raw_events":
            result = _persist_raw_events(run_id, list(payload["source_events"]))
        elif op == "fetch_raw_events":
            result = _fetch_raw_events(str(payload["trace_id"]))
        elif op == "parse_raw_events":
            result = _parse_raw_events(list(payload["raw_events"]))
        elif op == "persist_canonical":
            result = _persist_canonical(
                run_id,
                dict(payload["parse_result"]),
                str(payload["parser_version"]),
            )
        else:
            raise ValueError(f"Unsupported layer operation: {op}")

        records_out = None
        if isinstance(result, dict):
            for key in ("count", "stored_count", "parsed_count"):
                value = result.get(key)
                if isinstance(value, int):
                    records_out = value
                    break
        store.finish_stage_run(stage_run_id=stage_run_id, status="succeeded", records_out=records_out)
        return result
    except Exception as exc:
        store.finish_stage_run(
            stage_run_id=stage_run_id,
            status="failed",
            error_json={"message": str(exc), "type": exc.__class__.__name__},
        )
        raise


def _layer_validate_source_events(run_id: str, source_events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="validate",
        records_in=len(source_events_payload),
        op="validate_source_events",
        payload={"source_events": source_events_payload},
    )


def _layer_persist_raw_events(run_id: str, source_events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="persist",
        records_in=len(source_events_payload),
        op="persist_raw_events",
        payload={"source_events": source_events_payload},
    )


def _layer_fetch_raw_events(run_id: str, trace_id: str) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="canonicalization",
        layer_key="fetch_raw",
        records_in=None,
        op="fetch_raw_events",
        payload={"trace_id": trace_id},
    )


def _layer_parse_raw_events(run_id: str, raw_events: list[dict[str, Any]]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="canonicalization",
        layer_key="parse",
        records_in=len(raw_events),
        op="parse_raw_events",
        payload={"raw_events": raw_events},
    )


def _layer_persist_canonical(
    run_id: str,
    parse_result: dict[str, Any],
    parser_version: str,
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="canonicalization",
        layer_key="persist",
        records_in=int(parse_result.get("parsed_count", 0)),
        op="persist_canonical",
        payload={"parse_result": parse_result, "parser_version": parser_version},
    )


def _mark_run_succeeded(run_id: str, metadata: dict[str, Any]) -> None:
    store = _pipeline_store()
    store.mark_pipeline_status(run_id=run_id, status="succeeded", metadata=metadata)


def _mark_run_failed(run_id: str, exc: Exception) -> None:
    store = _pipeline_store()
    store.mark_pipeline_status(
        run_id=run_id,
        status="failed",
        error_json={"message": str(exc), "type": exc.__class__.__name__},
    )


async def _run_pipeline_core(
    ctx: inngest.Context,
    *,
    data: dict[str, Any],
    trigger_type: str,
    parser_version: str,
    source_events_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    run_info = await ctx.step.run(
        "stage.orchestration.layer.ensure_run",
        _ensure_pipeline_run,
        data,
        trigger_type,
        source_events_payload,
    )
    run_id = str(run_info["run_id"])
    trace_id = str(run_info["trace_id"])
    try:
        await ctx.step.run(
            "stage.raw_capture.layer.validate",
            _layer_validate_source_events,
            run_id,
            source_events_payload,
        )
        raw_result = await ctx.step.run(
            "stage.raw_capture.layer.persist",
            _layer_persist_raw_events,
            run_id,
            source_events_payload,
        )

        raw_fetch = await ctx.step.run(
            "stage.canonicalization.layer.fetch_raw",
            _layer_fetch_raw_events,
            run_id,
            trace_id,
        )
        parse_result = await ctx.step.run(
            "stage.canonicalization.layer.parse",
            _layer_parse_raw_events,
            run_id,
            list(raw_fetch.get("events", [])),
        )
        canonical_result = await ctx.step.run(
            "stage.canonicalization.layer.persist",
            _layer_persist_canonical,
            run_id,
            parse_result,
            parser_version,
        )
        summary = {
            "trace_id": trace_id,
            "run_id": run_id,
            "status": "completed",
            "raw_capture": raw_result,
            "canonicalization": {
                "raw_count": raw_fetch.get("count", 0),
                "parsed_count": parse_result.get("parsed_count", 0),
                "failed_count": parse_result.get("failed_count", 0),
                **canonical_result,
            },
        }
        await ctx.step.run("stage.orchestration.layer.complete_run", _mark_run_succeeded, run_id, summary)
        return summary
    except Exception as exc:
        await ctx.step.run("stage.orchestration.layer.fail_run", _mark_run_failed, run_id, exc)
        raise


@inngest_client.create_function(
    fn_id="ingestion-pipeline-run",
    trigger=inngest.TriggerEvent(event="pipeline/run.requested"),
)
async def ingestion_pipeline_run(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)
    source_events_payload = list(data.get("source_events", []))
    return await _run_pipeline_core(
        ctx,
        data=data,
        trigger_type="pipeline/run.requested",
        parser_version=parser_version,
        source_events_payload=source_events_payload,
    )


@inngest_client.create_function(
    fn_id="ingestion-user-connected",
    trigger=inngest.TriggerEvent(event="kue/user.connected"),
)
async def ingestion_user_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)

    await ctx.step.run("stage.intake.layer.validate_payload", _validate_oauth_payload, data)
    fetched = await ctx.step.run("stage.intake.layer.fetch_google", _fetch_google_source_events_from_oauth, data)
    return await _run_pipeline_core(
        ctx,
        data=data,
        trigger_type="kue/user.connected",
        parser_version=parser_version,
        source_events_payload=list(fetched.get("source_events", [])),
    )


@inngest_client.create_function(
    fn_id="ingestion-user-mock-connected",
    trigger=inngest.TriggerEvent(event="kue/user.mock_connected"),
)
async def ingestion_user_mock_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)

    await ctx.step.run("stage.intake.layer.validate_payload", _validate_oauth_payload, data)
    fetched = await ctx.step.run("stage.intake.layer.fetch_mock", _fetch_google_source_events_from_mock, data)
    return await _run_pipeline_core(
        ctx,
        data=data,
        trigger_type="kue/user.mock_connected",
        parser_version=parser_version,
        source_events_payload=list(fetched.get("source_events", [])),
    )


@inngest_client.create_function(
    fn_id="ingestion-stage-canonicalization-replay",
    trigger=inngest.TriggerEvent(event="pipeline/stage.canonicalization.replay.requested"),
)
async def replay_canonicalization(ctx: inngest.Context) -> dict[str, Any]:
    data = ctx.event.data or {}
    parser_version = str(data.get("parser_version") or settings.parser_version)

    run_info = await ctx.step.run(
        "stage.orchestration.layer.ensure_run",
        _ensure_pipeline_run,
        data,
        "pipeline/stage.canonicalization.replay.requested",
        [],
    )
    run_id = str(run_info["run_id"])
    trace_id = str(run_info["trace_id"])
    try:
        raw_fetch = await ctx.step.run(
            "stage.canonicalization.layer.fetch_raw",
            _layer_fetch_raw_events,
            run_id,
            trace_id,
        )
        parse_result = await ctx.step.run(
            "stage.canonicalization.layer.parse",
            _layer_parse_raw_events,
            run_id,
            list(raw_fetch.get("events", [])),
        )
        canonical_result = await ctx.step.run(
            "stage.canonicalization.layer.persist",
            _layer_persist_canonical,
            run_id,
            parse_result,
            parser_version,
        )
        summary = {
            "trace_id": trace_id,
            "run_id": run_id,
            "raw_count": raw_fetch.get("count", 0),
            "parsed_count": parse_result.get("parsed_count", 0),
            "failed_count": parse_result.get("failed_count", 0),
            **canonical_result,
        }
        await ctx.step.run("stage.orchestration.layer.complete_run", _mark_run_succeeded, run_id, summary)
        return summary
    except Exception as exc:
        await ctx.step.run("stage.orchestration.layer.fail_run", _mark_run_failed, run_id, exc)
        raise


inngest_functions = [
    ingestion_user_connected,
    ingestion_user_mock_connected,
    ingestion_pipeline_run,
    replay_canonicalization,
]
