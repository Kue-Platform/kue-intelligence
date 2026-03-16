from __future__ import annotations

from typing import Any

import inngest

from app.core.config import settings
from app.inngest.client import inngest_client, _alert_on_failure
from app.inngest.operations import (
    _validate_oauth_payload,
    _fetch_google_source_events_from_oauth,
    _fetch_google_source_events_from_mock,
)
from app.inngest.pipeline import (
    _run_pipeline_core,
    _run_post_ingest_steps,
    _build_pipeline_summary,
)
from app.inngest.layers import (
    _layer_ingest_and_process,
    _mark_run_succeeded,
    _mark_run_failed,
)
from app.inngest.operations import _ensure_pipeline_run


@inngest_client.create_function(
    fn_id="ingestion-pipeline-run",
    trigger=inngest.TriggerEvent(event="pipeline/run.requested"),
    retries=settings.inngest_max_retries,
    on_failure=_alert_on_failure,
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
    retries=settings.inngest_max_retries,
    on_failure=_alert_on_failure,
)
async def ingestion_user_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = dict(ctx.event.data or {})
    parser_version = str(data.get("parser_version") or settings.parser_version)

    await ctx.step.run(
        "stage.intake.layer.validate_payload", _validate_oauth_payload, data
    )
    fetched = await ctx.step.run(
        "stage.intake.layer.fetch_google", _fetch_google_source_events_from_oauth, data
    )
    data["source_count"] = fetched.get("count", 0)
    data["pre_stored"] = fetched.get("pre_stored", False)
    return await _run_pipeline_core(
        ctx,
        data=data,
        trigger_type="kue/user.connected",
        parser_version=parser_version,
        source_events_payload=[],
    )


@inngest_client.create_function(
    fn_id="ingestion-user-mock-connected",
    trigger=inngest.TriggerEvent(event="kue/user.mock_connected"),
    retries=settings.inngest_max_retries,
    on_failure=_alert_on_failure,
)
async def ingestion_user_mock_connected(ctx: inngest.Context) -> dict[str, Any]:
    data = dict(ctx.event.data or {})
    parser_version = str(data.get("parser_version") or settings.parser_version)

    if not data.get("pre_stored"):
        await ctx.step.run(
            "stage.intake.layer.validate_payload", _validate_oauth_payload, data
        )
        fetched = await ctx.step.run(
            "stage.intake.layer.fetch_mock", _fetch_google_source_events_from_mock, data
        )
        data["source_count"] = fetched.get("count", 0)
        data["pre_stored"] = fetched.get("pre_stored", False)

    return await _run_pipeline_core(
        ctx,
        data=data,
        trigger_type="kue/user.mock_connected",
        parser_version=parser_version,
        source_events_payload=[],
    )


@inngest_client.create_function(
    fn_id="ingestion-stage-canonicalization-replay",
    trigger=inngest.TriggerEvent(
        event="pipeline/stage.canonicalization.replay.requested"
    ),
    retries=settings.inngest_max_retries,
    on_failure=_alert_on_failure,
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
    tenant_id = str(run_info["tenant_id"])
    user_id = str(run_info["user_id"])

    try:
        ingest_result = await ctx.step.run(
            "stage.canonicalization.layer.ingest_and_process",
            _layer_ingest_and_process,
            run_id,
            trace_id,
            parser_version,
        )

        steps = await _run_post_ingest_steps(
            ctx,
            run_id=run_id,
            trace_id=trace_id,
            tenant_id=tenant_id,
            user_id=user_id,
            ingest_result=ingest_result,
        )

        raw_result = {
            "stored_count": ingest_result.get("raw_count", 0),
            "trace_id": trace_id,
            "run_id": run_id,
            "store": "replay",
        }
        summary = _build_pipeline_summary(
            trace_id=trace_id,
            run_id=run_id,
            raw_result=raw_result,
            ingest_result=ingest_result,
            **steps,
        )
        await ctx.step.run(
            "stage.orchestration.layer.complete_run",
            _mark_run_succeeded,
            run_id,
            summary,
        )
        return summary
    except Exception as exc:
        await ctx.step.run(
            "stage.orchestration.layer.fail_run", _mark_run_failed, run_id, exc
        )
        raise


inngest_functions = [
    ingestion_user_connected,
    ingestion_user_mock_connected,
    ingestion_pipeline_run,
    replay_canonicalization,
]
