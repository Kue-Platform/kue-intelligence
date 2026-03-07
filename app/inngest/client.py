from __future__ import annotations

import logging
from functools import lru_cache
from urllib.parse import urlparse

import httpx
import inngest

from app.core.config import settings
from app.ingestion.pipeline_store import PipelineStore, create_pipeline_store
from app.ingestion.step_payload_store import StepPayloadStore, create_step_payload_store


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


@lru_cache(maxsize=1)
def _step_payload_store() -> StepPayloadStore:
    return create_step_payload_store(settings)


async def _alert_on_failure(ctx: inngest.Context) -> None:
    data = dict(ctx.event.data or {})
    function_id = data.get("function_id")
    run_id = data.get("run_id")
    attempt = data.get("attempt")
    error = data.get("error")

    alert_payload = {
        "type": "pipeline_function_failed",
        "service": settings.app_name,
        "function_id": function_id,
        "run_id": run_id,
        "attempt": attempt,
        "max_retries": settings.inngest_max_retries,
        "error": error,
        "event": data,
    }

    if settings.alert_webhook_url:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(settings.alert_webhook_url, json=alert_payload)
            response.raise_for_status()
    else:
        logging.getLogger("uvicorn").error(
            "Pipeline failure after retries exhausted: %s",
            alert_payload,
        )
