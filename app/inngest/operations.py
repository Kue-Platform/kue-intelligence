from __future__ import annotations

from typing import Any

from app.core.config import settings
from app.ingestion.canonical_store import create_canonical_event_store
from app.ingestion.enrichment import clean_and_enrich_events
from app.ingestion.entity_resolution import (
    MergeResult,
    extract_entity_candidates,
    resolve_entities_multi_signal,
)
from app.ingestion.entity_store import create_entity_store
from app.ingestion.graph_projection import create_graph_projection_service
from app.ingestion.graph_store import create_graph_store
from app.ingestion.metadata_extraction import extract_metadata_candidates
from app.ingestion.relationship_extraction import compute_relationship_strength, extract_interactions
from app.ingestion.relationship_store import create_relationship_store
from app.ingestion.semantic_prep import build_semantic_documents
from app.ingestion.search_document_store import create_search_document_store
from app.ingestion.embeddings import (
    EmbeddingVectorRecord,
    build_embedding_requests,
    generate_embeddings,
)
from app.ingestion.embedding_store import create_embedding_store
from app.ingestion.search_indexing import build_hybrid_signals
from app.ingestion.search_index_store import create_search_index_store
from app.ingestion.google_connector import GoogleOAuthConnector, GoogleOAuthContext
from app.ingestion.parsers import parse_raw_events
from app.ingestion.raw_store import create_raw_event_store
from app.ingestion.source_connection_store import SourceConnectionRecord, create_source_connection_store
from app.ingestion.validators import validate_parsed_events
from app.schemas import GoogleMockSourceType, IngestionSource, SourceEvent

from app.inngest.client import _pipeline_store, _step_payload_store


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
    """Fetch Google source events and persist them before pipeline execution."""
    connector = GoogleOAuthConnector()
    context = GoogleOAuthContext(
        tenant_id=str(data["tenant_id"]),
        user_id=str(data["user_id"]),
        trace_id=str(data["trace_id"]),
    )
    source_events, connection = await connector.handle_callback_with_connection(
        code=str(data["code"]),
        context=context,
    )
    _pipeline_store().ensure_tenant_user(context.tenant_id, context.user_id)
    connected_sources = sorted({event.source for event in source_events}, key=str)
    create_source_connection_store(settings).upsert_connections(
        [
            SourceConnectionRecord(
                tenant_id=context.tenant_id,
                user_id=context.user_id,
                source=source,
                external_account_id=connection.external_account_id,
                scopes=connection.scopes,
                token_json=connection.token_json,
                token_expires_at=connection.token_expires_at,
                status="active",
                last_sync_at=None,
                last_error=None,
            )
            for source in connected_sources
        ]
    )
    store = create_raw_event_store(settings)
    store.persist_source_events(source_events, ingest_version="v1")
    return {"count": len(source_events), "pre_stored": True}


def _fetch_google_source_events_from_mock(data: dict[str, Any]) -> dict[str, Any]:
    """Fetch mock source events and persist them before pipeline execution."""
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
    store = create_raw_event_store(settings)
    store.persist_source_events(source_events, ingest_version="v1")
    return {"count": len(source_events), "pre_stored": True}


def _persist_raw_events(run_id: str, events_payload: list[dict[str, Any]]) -> dict[str, Any]:
    events = [SourceEvent.model_validate(item) for item in events_payload]
    store = create_raw_event_store(settings)
    result = store.persist_source_events(events, run_id=run_id, ingest_version="v1")
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


def _process_enrichment(enrichment_payload: dict[str, Any]) -> dict[str, Any]:
    """Run pure-Python enrichment transforms in one pass."""
    entity_exact = extract_entity_candidates(enrichment_payload)

    # Multi-signal resolution: Union-Find based matching across email, phone,
    # LinkedIn URL, name+company, name+domain signals.
    entity_resolved = resolve_entities_multi_signal(
        entity_exact.candidates,
        auto_merge_threshold=settings.entity_merge_auto_threshold,
    )

    # Backward-compat: downstream stages expect MergeResult shape
    entity_merged = MergeResult(
        resolved_count=entity_resolved.resolved_count,
        resolved_entities=[g.canonical for g in entity_resolved.groups],
    )

    metadata = extract_metadata_candidates(enrichment_payload)
    semantic = build_semantic_documents(enrichment_payload)

    interactions = extract_interactions(enrichment_payload)
    relationships = compute_relationship_strength(interactions.model_dump(mode="json"))

    return {
        "merge_result":    entity_merged.model_dump(mode="json"),
        "metadata_result": metadata.model_dump(mode="json"),
        "semantic_result": semantic.model_dump(mode="json"),
        "extract_result":  interactions.model_dump(mode="json"),
        "strength_result": relationships.model_dump(mode="json"),
        "entity_candidate_count": entity_exact.candidate_count,
        "resolved_count":         entity_merged.resolved_count,
        "merge_candidate_count":  entity_resolved.merge_candidate_count,
        "merge_candidates":       entity_resolved.merge_candidates,
        "metadata_count":         metadata.candidate_count,
        "document_count":         semantic.document_count,
        "interaction_count":      interactions.interaction_count,
        "relationship_count":     relationships.relationship_count,
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


def _generate_embedding_vectors(semantic_payload: dict[str, Any]) -> dict[str, Any]:
    """Generate embedding vectors and store them in StepPayloadStore."""
    run_id = str(semantic_payload.get("run_id", ""))
    requests = build_embedding_requests(semantic_payload)
    generated = generate_embeddings(requests)

    sps = _step_payload_store()
    vectors_ref = sps.write(run_id, "embedding_generate", {
        "records": [item.model_dump(mode="json") for item in generated.generated],
    })

    return {
        "run_id":          run_id,
        "generated_count": generated.generated_count,
        "total_count":     generated.generated_count,
        "vectors_ref":     vectors_ref,
    }


def _persist_embeddings(vectors_ref_or_payload: dict[str, Any]) -> dict[str, Any]:
    """Persist embedding vectors, loading from StepPayloadStore when a vectors_ref is present."""
    vectors_ref = str(vectors_ref_or_payload.get("vectors_ref", ""))
    if vectors_ref:
        vectors_data = _step_payload_store().read(vectors_ref)
    else:
        vectors_data = vectors_ref_or_payload
    records = [
        EmbeddingVectorRecord.model_validate(item)
        for item in list(vectors_data.get("records", []))
    ]
    store = create_embedding_store(settings)
    result = store.persist_vectors(records)
    return {
        "persisted_count": result.persisted_count,
        "skipped_no_entity": result.skipped_no_entity,
        "store": store.store_name,
    }


def _build_signals(embedding_vectors: dict[str, Any]) -> dict[str, Any]:
    """Build hybrid search signals from embedding vectors."""
    vectors_ref = str(embedding_vectors.get("vectors_ref", ""))
    if vectors_ref:
        vectors_data = _step_payload_store().read(vectors_ref)
    else:
        vectors_data = embedding_vectors
    signals = build_hybrid_signals(vectors_data).model_dump(mode="json")
    return {
        "signals": signals.get("signals", []),
        "signal_count": signals.get("signal_count", 0),
    }


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


def _ingest_and_process(trace_id: str, run_id: str, parser_version: str) -> dict[str, Any]:
    """Fetch, canonicalize, enrich, and persist intermediate outputs."""
    raw = _fetch_raw_events(trace_id)
    raw_events = list(raw.get("events", []))

    parsed = _parse_raw_events(raw_events)
    validated = validate_parsed_events(dict(parsed)).model_dump(mode="json")
    enriched = clean_and_enrich_events(dict(validated)).model_dump(mode="json")

    canonical = _persist_canonical(run_id, enriched, parser_version)
    process = _process_enrichment(enriched)

    valid_count = int(validated.get("valid_count", 0))
    invalid_count = int(validated.get("invalid_count", 0))
    invalid_events = list(validated.get("invalid_events", []))

    sps = _step_payload_store()
    enrichment_ref = sps.write(run_id, "ingest_and_process", {
        "merge_result":    process["merge_result"],
        "metadata_result": process["metadata_result"],
        "semantic_result": process["semantic_result"],
        "extract_result":  process["extract_result"],
        "strength_result": process["strength_result"],
    })

    del enriched, validated

    return {
        "raw_count":      raw.get("count", 0),
        "parsed_count":   parsed.get("parsed_count", 0),
        "failed_count":   parsed.get("failed_count", 0),
        "valid_count":    valid_count,
        "invalid_count":  invalid_count,
        "invalid_events": invalid_events,
        "enriched_count": process.get("entity_candidate_count", 0),
        **canonical,
        "enrichment_ref": enrichment_ref,
        "entity_candidate_count": process["entity_candidate_count"],
        "resolved_count":         process["resolved_count"],
        "metadata_count":         process["metadata_count"],
        "document_count":         process["document_count"],
        "interaction_count":      process["interaction_count"],
        "relationship_count":     process["relationship_count"],
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


def _project_graph(prepare_result: dict[str, Any]) -> dict[str, Any]:
    """Project graph nodes and edges from a prepared snapshot."""
    node_result = _graph_project_nodes(prepare_result)
    edge_result = _graph_project_edges(prepare_result)
    return {
        "node_result": node_result,
        "edge_result": edge_result,
        "nodes_by_label": node_result.get("nodes_by_label", {}),
        "edges_by_type": edge_result.get("edges_by_type", {}),
    }


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

    existing_run_id = data.get("run_id")
    if existing_run_id:
        return {
            "run_id": str(existing_run_id),
            "trace_id": trace_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "pre_stored": True,
        }

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
    return {"run_id": run_id, "trace_id": trace_id, "tenant_id": tenant_id, "user_id": user_id, "pre_stored": False}
