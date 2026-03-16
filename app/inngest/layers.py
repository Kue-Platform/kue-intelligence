from __future__ import annotations

from typing import Any

from app.inngest.client import _pipeline_store
from app.inngest.operations import (
    _validate_source_events,
    _persist_raw_events,
    _fetch_raw_events,
    _parse_raw_events,
    _ingest_and_process,
    _persist_entities,
    _persist_relationships,
    _persist_metadata,
    _persist_semantic_documents,
    _generate_embedding_vectors,
    _persist_embeddings,
    _build_signals,
    _persist_search_index,
    _graph_prepare,
    _project_graph,
    _graph_finalize,
)


_OP_DISPATCH: dict[str, Any] = {
    "validate_source_events": lambda payload, run_id: _validate_source_events(
        list(payload["source_events"])
    ),
    "persist_raw_events": lambda payload, run_id: _persist_raw_events(
        run_id, list(payload["source_events"])
    ),
    "fetch_raw_events": lambda payload, run_id: _fetch_raw_events(
        str(payload["trace_id"])
    ),
    "parse_raw_events": lambda payload, run_id: _parse_raw_events(
        list(payload["raw_events"])
    ),
    "ingest_and_process": lambda payload, run_id: _ingest_and_process(
        str(payload["trace_id"]),
        str(payload["run_id"]),
        str(payload["parser_version"]),
    ),
    "entity_persist": lambda payload, run_id: _persist_entities(
        dict(payload["merge_result"])
    ),
    "persist_relationships": lambda payload, run_id: _persist_relationships(
        dict(payload["extract_result"]),
        dict(payload["strength_result"]),
    ),
    "persist_metadata": lambda payload, run_id: _persist_metadata(
        dict(payload["metadata_result"])
    ),
    "persist_semantic_documents": lambda payload, run_id: _persist_semantic_documents(
        dict(payload["semantic_result"])
    ),
    "embedding_generate": lambda payload, run_id: _generate_embedding_vectors(
        dict(payload["semantic_result"])
    ),
    "embedding_persist": lambda payload, run_id: _persist_embeddings(
        dict(payload["vectors_result"])
    ),
    "build_signals": lambda payload, run_id: _build_signals(
        dict(payload["embedding_vectors"])
    ),
    "persist_search_index": lambda payload, run_id: _persist_search_index(
        dict(payload["signals_result"])
    ),
    "graph_prepare": lambda payload, run_id: _graph_prepare(
        str(payload["tenant_id"]),
        str(payload["user_id"]),
        str(payload["trace_id"]),
    ),
    "project_graph": lambda payload, run_id: _project_graph(
        dict(payload["prepare_result"])
    ),
    "graph_finalize": lambda payload, run_id: _graph_finalize(
        str(payload["run_id"]),
        str(payload["trace_id"]),
        str(payload["tenant_id"]),
        str(payload["user_id"]),
        dict(payload["prepare_result"]),
        dict(payload["node_result"]),
        dict(payload["edge_result"]),
    ),
}


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
        handler = _OP_DISPATCH.get(op)
        if handler is None:
            raise ValueError(f"Unsupported layer operation: {op}")
        result = handler(payload, run_id)

        records_out = None
        if isinstance(result, dict):
            for key in ("count", "stored_count", "parsed_count"):
                value = result.get(key)
                if isinstance(value, int):
                    records_out = value
                    break
        store.finish_stage_run(
            stage_run_id=stage_run_id, status="succeeded", records_out=records_out
        )
        return result
    except Exception as exc:
        store.finish_stage_run(
            stage_run_id=stage_run_id,
            status="failed",
            error_json={"message": str(exc), "type": exc.__class__.__name__},
        )
        raise


def _layer_validate_source_events(
    run_id: str, source_events_payload: list[dict[str, Any]]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="validate",
        records_in=len(source_events_payload),
        op="validate_source_events",
        payload={"source_events": source_events_payload},
    )


def _layer_persist_raw_events(
    run_id: str, source_events_payload: list[dict[str, Any]]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="persist",
        records_in=len(source_events_payload),
        op="persist_raw_events",
        payload={"source_events": source_events_payload},
    )


def _layer_ingest_and_process(
    run_id: str, trace_id: str, parser_version: str
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="canonicalization",
        layer_key="ingest_and_process",
        records_in=None,
        op="ingest_and_process",
        payload={
            "trace_id": trace_id,
            "run_id": run_id,
            "parser_version": parser_version,
        },
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


def _layer_persist_metadata(
    run_id: str, metadata_result: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="metadata_extraction",
        layer_key="persist_metadata",
        records_in=int(metadata_result.get("candidate_count", 0)),
        op="persist_metadata",
        payload={"metadata_result": metadata_result},
    )


def _layer_persist_semantic_documents(
    run_id: str, semantic_result: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="semantic_prep",
        layer_key="persist_search_documents",
        records_in=int(semantic_result.get("document_count", 0)),
        op="persist_semantic_documents",
        payload={"semantic_result": semantic_result},
    )


def _layer_embedding_generate(
    run_id: str, semantic_result: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="generate_vectors",
        records_in=int(semantic_result.get("document_count", 0)),
        op="embedding_generate",
        payload={"semantic_result": semantic_result},
    )


def _layer_embedding_persist(
    run_id: str, vectors_result: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="embedding",
        layer_key="persist_vectors",
        records_in=int(vectors_result.get("candidate_count", 0)),
        op="embedding_persist",
        payload={"vectors_result": vectors_result},
    )


def _layer_build_signals(
    run_id: str, embedding_vectors: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="search_indexing",
        layer_key="build_signals",
        records_in=int(embedding_vectors.get("generated_count", 0)),
        op="build_signals",
        payload={"embedding_vectors": embedding_vectors},
    )


def _layer_persist_search_index(
    run_id: str, signals_result: dict[str, Any]
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="search_indexing",
        layer_key="index_health_check",
        records_in=int(signals_result.get("signal_count", 0)),
        op="persist_search_index",
        payload={"signals_result": signals_result},
    )


def _layer_graph_prepare(
    run_id: str, tenant_id: str, user_id: str, trace_id: str
) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="prepare",
        records_in=None,
        op="graph_prepare",
        payload={"tenant_id": tenant_id, "user_id": user_id, "trace_id": trace_id},
    )


def _layer_project_graph(run_id: str, prepare_result: dict[str, Any]) -> dict[str, Any]:
    return _run_layer_internal(
        run_id=run_id,
        stage_key="graph_projection",
        layer_key="project",
        records_in=None,
        op="project_graph",
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
    _pipeline_store().mark_pipeline_status(
        run_id=run_id, status="succeeded", metadata=metadata
    )


def _mark_run_failed(run_id: str, exc: Exception) -> None:
    _pipeline_store().mark_pipeline_status(
        run_id=run_id,
        status="failed",
        error_json={"message": str(exc), "type": exc.__class__.__name__},
    )
