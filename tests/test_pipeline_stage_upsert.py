import tempfile

from app.ingestion.pipeline_store import SqlitePipelineStore


def test_start_stage_run_is_idempotent_for_same_stage_attempt() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_pipeline_")
    store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    store.ensure_tenant_user("tenant_1", "user_1")
    run_id = store.create_pipeline_run(
        trace_id="trace_stage_upsert",
        tenant_id="tenant_1",
        user_id="user_1",
        source=None,
        trigger_type="test",
        source_event_id=None,
    )

    first = store.start_stage_run(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="persist",
        attempt=1,
        records_in=1,
    )
    second = store.start_stage_run(
        run_id=run_id,
        stage_key="raw_capture",
        layer_key="persist",
        attempt=1,
        records_in=1,
    )

    assert first == second


def test_create_pipeline_run_reuses_existing_trace_run() -> None:
    tmpdir = tempfile.mkdtemp(prefix="kue_pipeline_")
    store = SqlitePipelineStore(db_path=f"{tmpdir}/pipeline.db")
    store.ensure_tenant_user("tenant_1", "user_1")

    run1 = store.create_pipeline_run(
        trace_id="trace_same",
        tenant_id="tenant_1",
        user_id="user_1",
        source=None,
        trigger_type="test_a",
        source_event_id="evt_a",
    )
    run2 = store.create_pipeline_run(
        trace_id="trace_same",
        tenant_id="tenant_1",
        user_id="user_1",
        source=None,
        trigger_type="test_b",
        source_event_id="evt_b",
    )

    assert run1 == run2
