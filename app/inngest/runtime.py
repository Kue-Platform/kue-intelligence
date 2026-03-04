from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import inngest
import httpx

from app.core.config import settings
from app.ingestion.canonical_store import create_canonical_event_store
from app.ingestion.enrichment import clean_and_enrich_events
from app.ingestion.entity_resolution import extract_entity_candidates, merge_entity_candidates
from app.ingestion.entity_store import create_entity_store
from app.ingestion.graph_projection import create_graph_projection_service
from app.ingestion.graph_store import create_graph_store
from app.ingestion.metadata_extraction import extract_metadata_candidates
from app.ingestion.relationship_extraction import compute_relationship_strength, extract_interactions
from app.ingestion.relationship_store import create_relationship_store
from app.ingestion.semantic_prep import build_semantic_documents
from app.ingestion.search_document_store import create_search_document_store
from app.ingestion.embeddings import (
    EmbeddingRequest,
    EmbeddingVectorRecord,
    build_embedding_requests,
    embedding_cache_lookup,
    embedding_cache_store,
    generate_embeddings,
)
from app.ingestion.embedding_store import create_embedding_store
from app.ingestion.cache_registry import cache_registry
from app.ingestion.search_indexing import build_hybrid_signals
from app.ingestion.search_index_store import create_search_index_store
from app.ingestion.google_connector import GoogleOAuthConnector, GoogleOAuthContext
from app.ingestion.parsers import parse_raw_events
from app.ingestion.pipeline_store import PipelineStore, create_pipeline_store
from app.ingestion.raw_store import create_raw_event_store
from app.ingestion.validators import validate_parsed_events
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


def _persist_entities(merged_payload: dict[str, Any]) -> dict[str, Any]:
    from app.ingestion.entity_resolution import EntityCandidate

    resolved_entities = [
        EntityCandidate.model_validate(item)
        for item in list(merged_payload.get("resolved_entities", []))
    ]
    store = create_entity_store(settings)
    persist_result = store.upsert_entities(resolved_entities)
    return {
        "resolved_count": persist_result.resolved_count,
        "created_entities": persist_result.created_entities,
        "updated_entities": persist_result.updated_entities,
        "identities_upserted": persist_result.identities_upserted,
        "store": store.store_name,
    }


def _persist_relationships(
    extract_payload: dict[str, Any],
    strength_payload: dict[str, Any],
) -> dict[str, Any]:
    from app.ingestion.relationship_extraction import InteractionCandidate, RelationshipAggregate

    interactions = [
        InteractionCandidate.model_validate(item)
        for item in list(extract_payload.get("interactions", []))
    ]
    relationships = [
        RelationshipAggregate.model_validate(item)
        for item in list(strength_payload.get("relationships", []))
    ]
    store = create_relationship_store(settings)
    result = store.persist(interactions, relationships)
    return {
        "interaction_count": result.interaction_count,
        "relationship_count": result.relationship_count,
        "relationships_upserted": result.relationships_upserted,
        "store": store.store_name,
    }


def _persist_metadata(metadata_payload: dict[str, Any]) -> dict[str, Any]:
    from app.ingestion.metadata_extraction import MetadataCandidate

    candidates = [
        MetadataCandidate.model_validate(item)
        for item in list(metadata_payload.get("candidates", []))
    ]
    store = create_entity_store(settings)
    updated_count = store.upsert_metadata(candidates)
    return {
        "candidate_count": len(candidates),
        "updated_count": updated_count,
        "store": store.store_name,
    }


def _persist_semantic_documents(semantic_payload: dict[str, Any]) -> dict[str, Any]:
    from app.ingestion.semantic_prep import SemanticDocument

    docs = [
        SemanticDocument.model_validate(item)
        for item in list(semantic_payload.get("documents", []))
    ]
    store = create_search_document_store(settings)
    result = store.persist_documents(docs)
    return {
        "candidate_count": result.candidate_count,
        "stored_count": result.stored_count,
        "skipped_no_entity": result.skipped_no_entity,
        "store": store.store_name,
    }


def _embedding_cache_lookup(semantic_payload: dict[str, Any]) -> dict[str, Any]:
    requests = build_embedding_requests(semantic_payload)
    lookup = embedding_cache_lookup(requests)
    return lookup.model_dump(mode="json")


def _generate_embedding_vectors(cache_lookup_payload: dict[str, Any]) -> dict[str, Any]:
    misses = [
        EmbeddingRequest.model_validate(item)
        for item in list(cache_lookup_payload.get("misses", []))
    ]
    generated = generate_embeddings(misses)
    hits = [
        EmbeddingVectorRecord.model_validate(item)
        for item in list(cache_lookup_payload.get("hits", []))
    ]
    all_records = hits + generated.generated
    return {
        "candidate_count": int(cache_lookup_payload.get("candidate_count", 0)),
        "cache_hit_count": int(cache_lookup_payload.get("cache_hit_count", 0)),
        "cache_miss_count": int(cache_lookup_payload.get("cache_miss_count", 0)),
        "generated_count": generated.generated_count,
        "records": [item.model_dump(mode="json") for item in all_records],
    }


def _embedding_cache_store(vectors_payload: dict[str, Any]) -> dict[str, Any]:
    records = [
        EmbeddingVectorRecord.model_validate(item)
        for item in list(vectors_payload.get("records", []))
    ]
    stored = embedding_cache_store(records)
    return {"cache_store_count": stored}


def _persist_embeddings(vectors_payload: dict[str, Any]) -> dict[str, Any]:
    records = [
        EmbeddingVectorRecord.model_validate(item)
        for item in list(vectors_payload.get("records", []))
    ]
    store = create_embedding_store(settings)
    result = store.persist_vectors(records)
    return {
        "persisted_count": result.persisted_count,
        "skipped_no_entity": result.skipped_no_entity,
        "store": store.store_name,
    }


def _cache_enrichment(enrichment_payload: dict[str, Any]) -> dict[str, Any]:
    parsed_events = list(enrichment_payload.get("parsed_events", []))
    cached = 0
    for event in parsed_events:
        source_event_id = str(event.get("source_event_id") or "").strip()
        tenant_id = str(event.get("tenant_id") or "").strip()
        if not source_event_id or not tenant_id:
            continue
        key = f"{tenant_id}:{source_event_id}"
        cache_registry.put(
            namespace="enrichment",
            version="v1",
            key=key,
            value=dict(event),
        )
        cached += 1
    return {"cached_count": cached}


def _cache_embeddings(vectors_payload: dict[str, Any]) -> dict[str, Any]:
    records = list(vectors_payload.get("records", []))
    cached = 0
    for record in records:
        tenant_id = str(record.get("tenant_id") or "").strip()
        doc_type = str(record.get("doc_type") or "").strip()
        content = str(record.get("content") or "").strip()
        if not tenant_id or not doc_type or not content:
            continue
        key = f"{tenant_id}:{doc_type}:{content}"
        cache_registry.put(
            namespace="embedding",
            version="v1",
            key=key,
            value={"embedding": list(record.get("embedding", []))},
        )
        cached += 1
    return {"cached_count": cached}


def _cache_metrics_snapshot() -> dict[str, Any]:
    stats = cache_registry.stats()
    return {"stats": stats}


def _persist_search_index(signals_payload: dict[str, Any]) -> dict[str, Any]:
    from app.ingestion.search_indexing import HybridSignal

    signals = [
        HybridSignal.model_validate(item)
        for item in list(signals_payload.get("signals", []))
    ]
    store = create_search_index_store(settings)
    result = store.apply_hybrid_signals(signals)
    tenant_id = str(signals[0].tenant_id) if signals else ""
    health = store.health_check(tenant_id) if tenant_id else {"index_ready": False}
    return {
        "applied_count": result.applied_count,
        "skipped_no_entity": result.skipped_no_entity,
        "health": health,
        "store": store.store_name,
    }


def _graph_prepare(tenant_id: str, user_id: str, trace_id: str) -> dict[str, Any]:
    graph_store = create_graph_store(settings)
    schema_result = graph_store.ensure_schema()
    service = create_graph_projection_service(settings)
    snapshot = service.prepare_snapshot(tenant_id=tenant_id, user_id=user_id, trace_id=trace_id)
    return {
        "enabled": bool(snapshot.get("enabled", False)) and bool(schema_result.get("enabled", False)),
        "schema": schema_result,
        "batch_stats": snapshot.get("batch_stats", {}),
        "snapshot": snapshot,
        "snapshot_checksum": snapshot.get("snapshot_checksum"),
        "store": graph_store.store_name,
    }


def _graph_project_nodes(prepare_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(prepare_result.get("snapshot", {}))
    graph_store = create_graph_store(settings)
    result = graph_store.upsert_nodes(snapshot)
    result["store"] = graph_store.store_name
    return result


def _graph_project_edges(prepare_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(prepare_result.get("snapshot", {}))
    graph_store = create_graph_store(settings)
    result = graph_store.upsert_edges(snapshot)
    result["store"] = graph_store.store_name
    return result


def _graph_finalize(
    run_id: str,
    trace_id: str,
    tenant_id: str,
    user_id: str,
    prepare_result: dict[str, Any],
    node_result: dict[str, Any],
    edge_result: dict[str, Any],
) -> dict[str, Any]:
    snapshot = dict(prepare_result.get("snapshot", {}))
    graph_store = create_graph_store(settings)
    verification = graph_store.verify(snapshot)
    service = create_graph_projection_service(settings)
    mirror = service.persist_mirror_state(
        run_id=run_id,
        trace_id=trace_id,
        tenant_id=tenant_id,
        user_id=user_id,
        snapshot=snapshot,
        verification=verification,
        node_result=node_result,
        edge_result=edge_result,
    )
    return {
        "verification": verification,
        "mirror": mirror,
        "store": graph_store.store_name,
        "snapshot_checksum": prepare_result.get("snapshot_checksum"),
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
        elif op == "validate_canonical":
            result = validate_parsed_events(dict(payload["parse_result"])).model_dump(mode="json")
        elif op == "clean_and_enrich":
            result = clean_and_enrich_events(dict(payload["validation_result"])).model_dump(mode="json")
        elif op == "entity_exact_match":
            result = extract_entity_candidates(dict(payload["enrichment_result"])).model_dump(mode="json")
        elif op == "entity_merge":
            result = merge_entity_candidates(dict(payload["exact_match_result"])).model_dump(mode="json")
        elif op == "entity_persist":
            result = _persist_entities(dict(payload["merge_result"]))
        elif op == "extract_interactions":
            result = extract_interactions(dict(payload["enrichment_result"])).model_dump(mode="json")
        elif op == "compute_relationship_strength":
            result = compute_relationship_strength(dict(payload["extract_result"])).model_dump(mode="json")
        elif op == "persist_relationships":
            result = _persist_relationships(
                dict(payload["extract_result"]),
                dict(payload["strength_result"]),
            )
        elif op == "extract_metadata":
            result = extract_metadata_candidates(dict(payload["enrichment_result"])).model_dump(mode="json")
        elif op == "persist_metadata":
            result = _persist_metadata(dict(payload["metadata_result"]))
        elif op == "build_semantic_documents":
            result = build_semantic_documents(dict(payload["enrichment_result"])).model_dump(mode="json")
        elif op == "persist_semantic_documents":
            result = _persist_semantic_documents(dict(payload["semantic_result"]))
        elif op == "embedding_cache_lookup":
            result = _embedding_cache_lookup(dict(payload["semantic_result"]))
        elif op == "embedding_generate":
            result = _generate_embedding_vectors(dict(payload["cache_lookup_result"]))
        elif op == "embedding_cache_store":
            result = _embedding_cache_store(dict(payload["vectors_result"]))
        elif op == "embedding_persist":
            result = _persist_embeddings(dict(payload["vectors_result"]))
        elif op == "cache_enrichment":
            result = _cache_enrichment(dict(payload["enrichment_result"]))
        elif op == "cache_embeddings":
            result = _cache_embeddings(dict(payload["vectors_result"]))
        elif op == "cache_metrics_snapshot":
            result = _cache_metrics_snapshot()
        elif op == "build_hybrid_signals":
            result = build_hybrid_signals(dict(payload["vectors_result"])).model_dump(mode="json")
        elif op == "persist_search_index":
            result = _persist_search_index(dict(payload["signals_result"]))
        elif op == "graph_prepare":
            result = _graph_prepare(
                str(payload["tenant_id"]),
                str(payload["user_id"]),
                str(payload["trace_id"]),
            )
        elif op == "graph_project_nodes":
            result = _graph_project_nodes(dict(payload["prepare_result"]))
        elif op == "graph_project_edges":
            result = _graph_project_edges(dict(payload["prepare_result"]))
        elif op == "graph_finalize":
            result = _graph_finalize(
                str(payload["run_id"]),
                str(payload["trace_id"]),
                str(payload["tenant_id"]),
                str(payload["user_id"]),
                dict(payload["prepare_result"]),
                dict(payload["node_result"]),
                dict(payload["edge_result"]),
            )
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
    parsed_count = int(parse_result.get("parsed_count", 0))
    if parsed_count == 0:
        parsed_count = len(list(parse_result.get("parsed_events", [])))
    return _run_layer_internal(
        run_id=run_id,
        stage_key="canonicalization",
        layer_key="persist",
        records_in=parsed_count,
        op="persist_canonical",
        payload={"parse_result": parse_result, "parser_version": parser_version},
    )


def _layer_validate_canonical(run_id: str, parse_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="validation",
        layer_key="schema_enforcement",
        records_in=int(parse_result.get("parsed_count", 0)),
        op="validate_canonical",
        payload={"parse_result": parse_result},
    )


def _layer_clean_and_enrich(run_id: str, validation_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="cleaning_enrichment",
        layer_key="normalize_and_enrich",
        records_in=int(validation_result.get("valid_count", 0)),
        op="clean_and_enrich",
        payload={"validation_result": validation_result},
    )


def _layer_entity_exact_match(run_id: str, enrichment_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="entity_resolution",
        layer_key="exact_match",
        records_in=int(enrichment_result.get("enriched_count", 0)),
        op="entity_exact_match",
        payload={"enrichment_result": enrichment_result},
    )


def _layer_entity_merge(run_id: str, exact_match_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="entity_resolution",
        layer_key="merge_entities",
        records_in=int(exact_match_result.get("candidate_count", 0)),
        op="entity_merge",
        payload={"exact_match_result": exact_match_result},
    )


def _layer_entity_persist(run_id: str, merge_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="entity_resolution",
        layer_key="persist_entities",
        records_in=int(merge_result.get("resolved_count", 0)),
        op="entity_persist",
        payload={"merge_result": merge_result},
    )


def _layer_extract_interactions(run_id: str, enrichment_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="relationship_extraction",
        layer_key="extract_interactions",
        records_in=int(enrichment_result.get("enriched_count", 0)),
        op="extract_interactions",
        payload={"enrichment_result": enrichment_result},
    )


def _layer_compute_relationship_strength(run_id: str, extract_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="relationship_extraction",
        layer_key="compute_strength",
        records_in=int(extract_result.get("interaction_count", 0)),
        op="compute_relationship_strength",
        payload={"extract_result": extract_result},
    )


def _layer_persist_relationships(
    run_id: str,
    extract_result: dict[str, Any],
    strength_result: dict[str, Any],
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="relationship_extraction",
        layer_key="persist_relationships",
        records_in=int(strength_result.get("relationship_count", 0)),
        op="persist_relationships",
        payload={"extract_result": extract_result, "strength_result": strength_result},
    )


def _layer_extract_metadata(run_id: str, enrichment_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="metadata_extraction",
        layer_key="extract_tags",
        records_in=int(enrichment_result.get("enriched_count", 0)),
        op="extract_metadata",
        payload={"enrichment_result": enrichment_result},
    )


def _layer_persist_metadata(run_id: str, metadata_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="metadata_extraction",
        layer_key="persist_metadata",
        records_in=int(metadata_result.get("candidate_count", 0)),
        op="persist_metadata",
        payload={"metadata_result": metadata_result},
    )


def _layer_build_semantic_documents(run_id: str, enrichment_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="semantic_prep",
        layer_key="build_search_documents",
        records_in=int(enrichment_result.get("enriched_count", 0)),
        op="build_semantic_documents",
        payload={"enrichment_result": enrichment_result},
    )


def _layer_persist_semantic_documents(run_id: str, semantic_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="semantic_prep",
        layer_key="persist_search_documents",
        records_in=int(semantic_result.get("document_count", 0)),
        op="persist_semantic_documents",
        payload={"semantic_result": semantic_result},
    )


def _layer_embedding_cache_lookup(run_id: str, semantic_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="cache_lookup",
        records_in=int(semantic_result.get("document_count", 0)),
        op="embedding_cache_lookup",
        payload={"semantic_result": semantic_result},
    )


def _layer_embedding_generate(run_id: str, cache_lookup_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="generate_vectors",
        records_in=int(cache_lookup_result.get("cache_miss_count", 0)),
        op="embedding_generate",
        payload={"cache_lookup_result": cache_lookup_result},
    )


def _layer_embedding_cache_store(run_id: str, vectors_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="cache_store",
        records_in=int(vectors_result.get("generated_count", 0)),
        op="embedding_cache_store",
        payload={"vectors_result": vectors_result},
    )


def _layer_embedding_persist(run_id: str, vectors_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="persist_vectors",
        records_in=int(vectors_result.get("candidate_count", 0)),
        op="embedding_persist",
        payload={"vectors_result": vectors_result},
    )


def _layer_cache_enrichment(run_id: str, enrichment_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="caching",
        layer_key="write_enrichment_cache",
        records_in=int(enrichment_result.get("enriched_count", 0)),
        op="cache_enrichment",
        payload={"enrichment_result": enrichment_result},
    )


def _layer_cache_embeddings(run_id: str, vectors_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="caching",
        layer_key="write_embedding_cache",
        records_in=int(vectors_result.get("candidate_count", 0)),
        op="cache_embeddings",
        payload={"vectors_result": vectors_result},
    )


def _layer_cache_metrics(run_id: str) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="caching",
        layer_key="cache_metrics_snapshot",
        records_in=None,
        op="cache_metrics_snapshot",
        payload={},
    )


def _layer_build_hybrid_signals(run_id: str, vectors_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="search_indexing",
        layer_key="materialize_hybrid_signals",
        records_in=int(vectors_result.get("candidate_count", 0)),
        op="build_hybrid_signals",
        payload={"vectors_result": vectors_result},
    )


def _layer_persist_search_index(run_id: str, signals_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="search_indexing",
        layer_key="index_health_check",
        records_in=int(signals_result.get("signal_count", 0)),
        op="persist_search_index",
        payload={"signals_result": signals_result},
    )


def _layer_graph_prepare(run_id: str, tenant_id: str, user_id: str, trace_id: str) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="prepare",
        records_in=None,
        op="graph_prepare",
        payload={"tenant_id": tenant_id, "user_id": user_id, "trace_id": trace_id},
    )


def _layer_graph_project_nodes(run_id: str, prepare_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(prepare_result.get("snapshot", {}))
    records_in = (
        len(list(snapshot.get("persons", [])))
        + len(list(snapshot.get("users", [])))
        + len(list(snapshot.get("companies", [])))
        + len(list(snapshot.get("topics", [])))
    )
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="project_nodes",
        records_in=records_in,
        op="graph_project_nodes",
        payload={"prepare_result": prepare_result},
    )


def _layer_graph_project_edges(run_id: str, prepare_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(prepare_result.get("snapshot", {}))
    records_in = (
        len(list(snapshot.get("knows_edges", [])))
        + len(list(snapshot.get("interacted_edges", [])))
        + len(list(snapshot.get("works_at_edges", [])))
        + len(list(snapshot.get("member_of_edges", [])))
        + len(list(snapshot.get("has_topic_edges", [])))
        + len(list(snapshot.get("intro_path_edges", [])))
    )
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="project_edges",
        records_in=records_in,
        op="graph_project_edges",
        payload={"prepare_result": prepare_result},
    )


def _layer_graph_finalize(
    run_id: str,
    trace_id: str,
    tenant_id: str,
    user_id: str,
    prepare_result: dict[str, Any],
    node_result: dict[str, Any],
    edge_result: dict[str, Any],
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="finalize",
        records_in=None,
        op="graph_finalize",
        payload={
            "run_id": run_id,
            "trace_id": trace_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "prepare_result": prepare_result,
            "node_result": node_result,
            "edge_result": edge_result,
        },
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
    tenant_id = str(run_info["tenant_id"])
    user_id = str(run_info["user_id"])
    tenant_id = str(run_info["tenant_id"])
    user_id = str(run_info["user_id"])
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
        validation_result = await ctx.step.run(
            "stage.validation.layer.schema_enforcement",
            _layer_validate_canonical,
            run_id,
            parse_result,
        )
        enrichment_result = await ctx.step.run(
            "stage.cleaning_enrichment.layer.normalize_and_enrich",
            _layer_clean_and_enrich,
            run_id,
            validation_result,
        )
        canonical_result = await ctx.step.run(
            "stage.canonicalization.layer.persist",
            _layer_persist_canonical,
            run_id,
            enrichment_result,
            parser_version,
        )
        entity_exact = await ctx.step.run(
            "stage.entity_resolution.layer.exact_match",
            _layer_entity_exact_match,
            run_id,
            enrichment_result,
        )
        entity_merge = await ctx.step.run(
            "stage.entity_resolution.layer.merge_entities",
            _layer_entity_merge,
            run_id,
            entity_exact,
        )
        entity_persist = await ctx.step.run(
            "stage.entity_resolution.layer.persist_entities",
            _layer_entity_persist,
            run_id,
            entity_merge,
        )
        metadata_result = await ctx.step.run(
            "stage.metadata_extraction.layer.extract_tags",
            _layer_extract_metadata,
            run_id,
            enrichment_result,
        )
        metadata_persist = await ctx.step.run(
            "stage.metadata_extraction.layer.persist_metadata",
            _layer_persist_metadata,
            run_id,
            metadata_result,
        )
        semantic_result = await ctx.step.run(
            "stage.semantic_prep.layer.build_search_documents",
            _layer_build_semantic_documents,
            run_id,
            enrichment_result,
        )
        semantic_persist = await ctx.step.run(
            "stage.semantic_prep.layer.persist_search_documents",
            _layer_persist_semantic_documents,
            run_id,
            semantic_result,
        )
        embedding_lookup = await ctx.step.run(
            "stage.embedding.layer.cache_lookup",
            _layer_embedding_cache_lookup,
            run_id,
            semantic_result,
        )
        embedding_vectors = await ctx.step.run(
            "stage.embedding.layer.generate_vectors",
            _layer_embedding_generate,
            run_id,
            embedding_lookup,
        )
        embedding_cache_written = await ctx.step.run(
            "stage.embedding.layer.cache_store",
            _layer_embedding_cache_store,
            run_id,
            embedding_vectors,
        )
        embedding_persist = await ctx.step.run(
            "stage.embedding.layer.persist_vectors",
            _layer_embedding_persist,
            run_id,
            embedding_vectors,
        )
        caching_enrichment = await ctx.step.run(
            "stage.caching.layer.write_enrichment_cache",
            _layer_cache_enrichment,
            run_id,
            enrichment_result,
        )
        caching_embeddings = await ctx.step.run(
            "stage.caching.layer.write_embedding_cache",
            _layer_cache_embeddings,
            run_id,
            embedding_vectors,
        )
        caching_metrics = await ctx.step.run(
            "stage.caching.layer.cache_metrics_snapshot",
            _layer_cache_metrics,
            run_id,
        )
        hybrid_signals = await ctx.step.run(
            "stage.search_indexing.layer.materialize_hybrid_signals",
            _layer_build_hybrid_signals,
            run_id,
            embedding_vectors,
        )
        search_index_result = await ctx.step.run(
            "stage.search_indexing.layer.index_health_check",
            _layer_persist_search_index,
            run_id,
            hybrid_signals,
        )
        interactions = await ctx.step.run(
            "stage.relationship_extraction.layer.extract_interactions",
            _layer_extract_interactions,
            run_id,
            enrichment_result,
        )
        strengths = await ctx.step.run(
            "stage.relationship_extraction.layer.compute_strength",
            _layer_compute_relationship_strength,
            run_id,
            interactions,
        )
        relationship_persist = await ctx.step.run(
            "stage.relationship_extraction.layer.persist_relationships",
            _layer_persist_relationships,
            run_id,
            interactions,
            strengths,
        )
        graph_prepare = await ctx.step.run(
            "stage.graph_projection.layer.prepare",
            _layer_graph_prepare,
            run_id,
            tenant_id,
            user_id,
            trace_id,
        )
        graph_nodes = await ctx.step.run(
            "stage.graph_projection.layer.project_nodes",
            _layer_graph_project_nodes,
            run_id,
            graph_prepare,
        )
        graph_edges = await ctx.step.run(
            "stage.graph_projection.layer.project_edges",
            _layer_graph_project_edges,
            run_id,
            graph_prepare,
        )
        graph_finalize = await ctx.step.run(
            "stage.graph_projection.layer.finalize",
            _layer_graph_finalize,
            run_id,
            trace_id,
            tenant_id,
            user_id,
            graph_prepare,
            graph_nodes,
            graph_edges,
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
            "validation": {
                "valid_count": validation_result.get("valid_count", 0),
                "invalid_count": validation_result.get("invalid_count", 0),
                "invalid_events": validation_result.get("invalid_events", []),
            },
            "cleaning_enrichment": {
                "enriched_count": enrichment_result.get("enriched_count", 0),
            },
            "entity_resolution": {
                "candidate_count": entity_exact.get("candidate_count", 0),
                "resolved_count": entity_merge.get("resolved_count", 0),
                **entity_persist,
            },
            "metadata_extraction": {
                **metadata_persist,
            },
            "semantic_prep": {
                "document_count": semantic_result.get("document_count", 0),
                **semantic_persist,
            },
            "embedding": {
                "cache_hit_count": embedding_lookup.get("cache_hit_count", 0),
                "cache_miss_count": embedding_lookup.get("cache_miss_count", 0),
                "generated_count": embedding_vectors.get("generated_count", 0),
                **embedding_cache_written,
                **embedding_persist,
            },
            "caching": {
                "enrichment_cached_count": caching_enrichment.get("cached_count", 0),
                "embedding_cached_count": caching_embeddings.get("cached_count", 0),
                **caching_metrics,
            },
            "search_indexing": {
                "signal_count": hybrid_signals.get("signal_count", 0),
                **search_index_result,
            },
            "relationship_extraction": {
                "interaction_count": interactions.get("interaction_count", 0),
                "relationship_count": strengths.get("relationship_count", 0),
                **relationship_persist,
            },
            "graph_projection": {
                "batch_stats": graph_prepare.get("batch_stats", {}),
                "nodes_by_label": graph_nodes.get("nodes_by_label", {}),
                "edges_by_type": graph_edges.get("edges_by_type", {}),
                "verification": graph_finalize.get("verification", {}),
                "mirror": graph_finalize.get("mirror", {}),
                "snapshot_checksum": graph_finalize.get("snapshot_checksum"),
                "store": graph_finalize.get("store"),
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
    retries=settings.inngest_max_retries,
    on_failure=_alert_on_failure,
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
        validation_result = await ctx.step.run(
            "stage.validation.layer.schema_enforcement",
            _layer_validate_canonical,
            run_id,
            parse_result,
        )
        enrichment_result = await ctx.step.run(
            "stage.cleaning_enrichment.layer.normalize_and_enrich",
            _layer_clean_and_enrich,
            run_id,
            validation_result,
        )
        canonical_result = await ctx.step.run(
            "stage.canonicalization.layer.persist",
            _layer_persist_canonical,
            run_id,
            enrichment_result,
            parser_version,
        )
        entity_exact = await ctx.step.run(
            "stage.entity_resolution.layer.exact_match",
            _layer_entity_exact_match,
            run_id,
            enrichment_result,
        )
        entity_merge = await ctx.step.run(
            "stage.entity_resolution.layer.merge_entities",
            _layer_entity_merge,
            run_id,
            entity_exact,
        )
        entity_persist = await ctx.step.run(
            "stage.entity_resolution.layer.persist_entities",
            _layer_entity_persist,
            run_id,
            entity_merge,
        )
        metadata_result = await ctx.step.run(
            "stage.metadata_extraction.layer.extract_tags",
            _layer_extract_metadata,
            run_id,
            enrichment_result,
        )
        metadata_persist = await ctx.step.run(
            "stage.metadata_extraction.layer.persist_metadata",
            _layer_persist_metadata,
            run_id,
            metadata_result,
        )
        semantic_result = await ctx.step.run(
            "stage.semantic_prep.layer.build_search_documents",
            _layer_build_semantic_documents,
            run_id,
            enrichment_result,
        )
        semantic_persist = await ctx.step.run(
            "stage.semantic_prep.layer.persist_search_documents",
            _layer_persist_semantic_documents,
            run_id,
            semantic_result,
        )
        embedding_lookup = await ctx.step.run(
            "stage.embedding.layer.cache_lookup",
            _layer_embedding_cache_lookup,
            run_id,
            semantic_result,
        )
        embedding_vectors = await ctx.step.run(
            "stage.embedding.layer.generate_vectors",
            _layer_embedding_generate,
            run_id,
            embedding_lookup,
        )
        embedding_cache_written = await ctx.step.run(
            "stage.embedding.layer.cache_store",
            _layer_embedding_cache_store,
            run_id,
            embedding_vectors,
        )
        embedding_persist = await ctx.step.run(
            "stage.embedding.layer.persist_vectors",
            _layer_embedding_persist,
            run_id,
            embedding_vectors,
        )
        caching_enrichment = await ctx.step.run(
            "stage.caching.layer.write_enrichment_cache",
            _layer_cache_enrichment,
            run_id,
            enrichment_result,
        )
        caching_embeddings = await ctx.step.run(
            "stage.caching.layer.write_embedding_cache",
            _layer_cache_embeddings,
            run_id,
            embedding_vectors,
        )
        caching_metrics = await ctx.step.run(
            "stage.caching.layer.cache_metrics_snapshot",
            _layer_cache_metrics,
            run_id,
        )
        hybrid_signals = await ctx.step.run(
            "stage.search_indexing.layer.materialize_hybrid_signals",
            _layer_build_hybrid_signals,
            run_id,
            embedding_vectors,
        )
        search_index_result = await ctx.step.run(
            "stage.search_indexing.layer.index_health_check",
            _layer_persist_search_index,
            run_id,
            hybrid_signals,
        )
        interactions = await ctx.step.run(
            "stage.relationship_extraction.layer.extract_interactions",
            _layer_extract_interactions,
            run_id,
            enrichment_result,
        )
        strengths = await ctx.step.run(
            "stage.relationship_extraction.layer.compute_strength",
            _layer_compute_relationship_strength,
            run_id,
            interactions,
        )
        relationship_persist = await ctx.step.run(
            "stage.relationship_extraction.layer.persist_relationships",
            _layer_persist_relationships,
            run_id,
            interactions,
            strengths,
        )
        graph_prepare = await ctx.step.run(
            "stage.graph_projection.layer.prepare",
            _layer_graph_prepare,
            run_id,
            tenant_id,
            user_id,
            trace_id,
        )
        graph_nodes = await ctx.step.run(
            "stage.graph_projection.layer.project_nodes",
            _layer_graph_project_nodes,
            run_id,
            graph_prepare,
        )
        graph_edges = await ctx.step.run(
            "stage.graph_projection.layer.project_edges",
            _layer_graph_project_edges,
            run_id,
            graph_prepare,
        )
        graph_finalize = await ctx.step.run(
            "stage.graph_projection.layer.finalize",
            _layer_graph_finalize,
            run_id,
            trace_id,
            tenant_id,
            user_id,
            graph_prepare,
            graph_nodes,
            graph_edges,
        )
        summary = {
            "trace_id": trace_id,
            "run_id": run_id,
            "raw_count": raw_fetch.get("count", 0),
            "parsed_count": parse_result.get("parsed_count", 0),
            "failed_count": parse_result.get("failed_count", 0),
            "validation": {
                "valid_count": validation_result.get("valid_count", 0),
                "invalid_count": validation_result.get("invalid_count", 0),
                "invalid_events": validation_result.get("invalid_events", []),
            },
            "cleaning_enrichment": {
                "enriched_count": enrichment_result.get("enriched_count", 0),
            },
            "entity_resolution": {
                "candidate_count": entity_exact.get("candidate_count", 0),
                "resolved_count": entity_merge.get("resolved_count", 0),
                **entity_persist,
            },
            "metadata_extraction": {
                **metadata_persist,
            },
            "semantic_prep": {
                "document_count": semantic_result.get("document_count", 0),
                **semantic_persist,
            },
            "embedding": {
                "cache_hit_count": embedding_lookup.get("cache_hit_count", 0),
                "cache_miss_count": embedding_lookup.get("cache_miss_count", 0),
                "generated_count": embedding_vectors.get("generated_count", 0),
                **embedding_cache_written,
                **embedding_persist,
            },
            "caching": {
                "enrichment_cached_count": caching_enrichment.get("cached_count", 0),
                "embedding_cached_count": caching_embeddings.get("cached_count", 0),
                **caching_metrics,
            },
            "search_indexing": {
                "signal_count": hybrid_signals.get("signal_count", 0),
                **search_index_result,
            },
            "relationship_extraction": {
                "interaction_count": interactions.get("interaction_count", 0),
                "relationship_count": strengths.get("relationship_count", 0),
                **relationship_persist,
            },
            "graph_projection": {
                "batch_stats": graph_prepare.get("batch_stats", {}),
                "nodes_by_label": graph_nodes.get("nodes_by_label", {}),
                "edges_by_type": graph_edges.get("edges_by_type", {}),
                "verification": graph_finalize.get("verification", {}),
                "mirror": graph_finalize.get("mirror", {}),
                "snapshot_checksum": graph_finalize.get("snapshot_checksum"),
                "store": graph_finalize.get("store"),
            },
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
