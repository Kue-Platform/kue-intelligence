from __future__ import annotations

from typing import Any

import inngest

from app.inngest.client import _step_payload_store
from app.inngest.layers import (
    _layer_entity_persist,
    _layer_persist_metadata,
    _layer_persist_semantic_documents,
    _layer_embedding_generate,
    _layer_embedding_persist,
    _layer_build_signals,
    _layer_persist_search_index,
    _layer_persist_relationships,
    _layer_graph_prepare,
    _layer_project_graph,
    _layer_graph_finalize,
    _layer_validate_source_events,
    _layer_persist_raw_events,
    _layer_ingest_and_process,
    _mark_run_succeeded,
    _mark_run_failed,
)
from app.inngest.operations import _ensure_pipeline_run


def _build_pipeline_summary(
    *,
    trace_id: str,
    run_id: str,
    raw_result: dict[str, Any],
    ingest_result: dict[str, Any],
    entity_persist: dict[str, Any],
    metadata_persist: dict[str, Any],
    semantic_persist: dict[str, Any],
    embedding_vectors: dict[str, Any],
    embedding_persist: dict[str, Any],
    signals_result: dict[str, Any],
    search_index_result: dict[str, Any],
    relationship_persist: dict[str, Any],
    graph_prepare: dict[str, Any],
    graph_projection: dict[str, Any],
    graph_finalize: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_id": trace_id,
        "run_id": run_id,
        "status": "completed",
        "raw_capture": raw_result,
        "canonicalization": {
            "raw_count": ingest_result.get("raw_count", 0),
            "parsed_count": ingest_result.get("parsed_count", 0),
            "failed_count": ingest_result.get("failed_count", 0),
            **{
                k: v
                for k, v in ingest_result.items()
                if k in ("stored_count", "captured_at", "store")
            },
        },
        "validation": {
            "valid_count": ingest_result.get("valid_count", 0),
            "invalid_count": ingest_result.get("invalid_count", 0),
            "invalid_events": ingest_result.get("invalid_events", []),
        },
        "cleaning_enrichment": {
            "enriched_count": ingest_result.get("enriched_count", 0),
        },
        "entity_resolution": {
            "candidate_count": ingest_result.get("entity_candidate_count", 0),
            "resolved_count": ingest_result.get("resolved_count", 0),
            **entity_persist,
        },
        "metadata_extraction": {**metadata_persist},
        "semantic_prep": {
            "document_count": ingest_result.get("document_count", 0),
            **semantic_persist,
        },
        "embedding": {
            "generated_count": embedding_vectors.get("generated_count", 0),
            **embedding_persist,
        },
        "search_indexing": {
            "signal_count": signals_result.get("signal_count", 0),
            **search_index_result,
        },
        "relationship_extraction": {
            "interaction_count": ingest_result.get("interaction_count", 0),
            "relationship_count": ingest_result.get("relationship_count", 0),
            **relationship_persist,
        },
        "graph_projection": {
            "batch_stats": graph_prepare.get("batch_stats", {}),
            "nodes_by_label": graph_projection.get("nodes_by_label", {}),
            "edges_by_type": graph_projection.get("edges_by_type", {}),
            "verification": graph_finalize.get("verification", {}),
            "mirror": graph_finalize.get("mirror", {}),
            "snapshot_checksum": graph_finalize.get("snapshot_checksum"),
            "store": graph_finalize.get("store"),
        },
    }


async def _run_post_ingest_steps(
    ctx: inngest.Context,
    *,
    run_id: str,
    trace_id: str,
    tenant_id: str,
    user_id: str,
    ingest_result: dict[str, Any],
) -> dict[str, Any]:
    """Execute all pipeline steps after ingest_and_process and return their results."""
    enrichment_slices = _step_payload_store().read(ingest_result["enrichment_ref"])
    semantic_result_with_run_id = {
        **enrichment_slices["semantic_result"],
        "run_id": run_id,
    }

    entity_persist = await ctx.step.run(
        "stage.entity_resolution.layer.persist_entities",
        _layer_entity_persist,
        run_id,
        enrichment_slices["merge_result"],
    )
    metadata_persist = await ctx.step.run(
        "stage.metadata_extraction.layer.persist_metadata",
        _layer_persist_metadata,
        run_id,
        enrichment_slices["metadata_result"],
    )
    semantic_persist = await ctx.step.run(
        "stage.semantic_prep.layer.persist_search_documents",
        _layer_persist_semantic_documents,
        run_id,
        enrichment_slices["semantic_result"],
    )
    embedding_vectors = await ctx.step.run(
        "stage.embedding.layer.generate_vectors",
        _layer_embedding_generate,
        run_id,
        semantic_result_with_run_id,
    )
    embedding_persist = await ctx.step.run(
        "stage.embedding.layer.persist_vectors",
        _layer_embedding_persist,
        run_id,
        embedding_vectors,
    )
    signals_result = await ctx.step.run(
        "stage.search_indexing.layer.build_signals",
        _layer_build_signals,
        run_id,
        embedding_vectors,
    )
    search_index_result = await ctx.step.run(
        "stage.search_indexing.layer.index_health_check",
        _layer_persist_search_index,
        run_id,
        signals_result,
    )
    relationship_persist = await ctx.step.run(
        "stage.relationship_extraction.layer.persist_relationships",
        _layer_persist_relationships,
        run_id,
        enrichment_slices["extract_result"],
        enrichment_slices["strength_result"],
    )
    graph_prepare = await ctx.step.run(
        "stage.graph_projection.layer.prepare",
        _layer_graph_prepare,
        run_id,
        tenant_id,
        user_id,
        trace_id,
    )
    graph_projection = await ctx.step.run(
        "stage.graph_projection.layer.project",
        _layer_project_graph,
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
        graph_projection["node_result"],
        graph_projection["edge_result"],
    )

    return {
        "entity_persist": entity_persist,
        "metadata_persist": metadata_persist,
        "semantic_persist": semantic_persist,
        "embedding_vectors": embedding_vectors,
        "embedding_persist": embedding_persist,
        "signals_result": signals_result,
        "search_index_result": search_index_result,
        "relationship_persist": relationship_persist,
        "graph_prepare": graph_prepare,
        "graph_projection": graph_projection,
        "graph_finalize": graph_finalize,
    }


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
    pre_stored = bool(run_info.get("pre_stored", False)) or bool(
        data.get("pre_stored", False)
    )

    try:
        if pre_stored:
            raw_result: dict[str, Any] = {
                "stored_count": int(data.get("source_count", 0)),
                "trace_id": trace_id,
                "run_id": run_id,
                "store": "pre_stored",
            }
        else:
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
