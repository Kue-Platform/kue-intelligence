-- Migration: 202603060001_step_payloads.sql
-- Creates the step_payloads table used by StepPayloadStore to offload large
-- Inngest step return values (enrichment slices, embedding vectors) so they
-- never hit the 256 KB serialization limit.

CREATE TABLE IF NOT EXISTS step_payloads (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    step_name   TEXT        NOT NULL,
    payload_json JSONB      NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT step_payloads_run_step_unique UNIQUE (run_id, step_name)
);

CREATE INDEX IF NOT EXISTS idx_step_payloads_run_id
    ON step_payloads (run_id);

COMMENT ON TABLE step_payloads IS
    'Transient store for large Inngest step intermediate payloads (enrichment '
    'slices, embedding vectors). Rows are written by one step and read by the next; '
    'they can be deleted after the pipeline run completes.';

-- RLS: service role bypasses; anon/authenticated roles have no access
ALTER TABLE step_payloads ENABLE ROW LEVEL SECURITY;

-- Service role (used by the backend) can do everything
CREATE POLICY step_payloads_service_all ON step_payloads
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
