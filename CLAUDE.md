# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (uses uv)
make sync          # uv sync --group dev

# Run the dev server
make run           # uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run all tests
make test          # uv run pytest

# Run a single test file
uv run pytest tests/test_layer6_entity_resolution.py -v

# Run a single test by name
uv run pytest tests/test_layer6_entity_resolution.py::test_layer6_resolution_dedups_by_email -v

# Lock dependencies
make lock          # uv lock
```

Inngest requires its own dev server running alongside the FastAPI app:
```bash
npx inngest-cli@latest dev -u http://localhost:8000/api/inngest
```

## Architecture

**kue-intelligence** is a 12-stage data intelligence pipeline that ingests professional contact and communication data (Google Contacts, Gmail, Google Calendar), resolves entities, extracts relationships, and builds a searchable graph. Inngest drives the async orchestration; Supabase is the canonical data store; Neo4j holds the derived graph.

### Request Flow

```
Client → POST /mock or GET /google/oauth/callback
       → FastAPI (ingestion_routes.py) sends Inngest event
       → Inngest function (runtime.py) triggers _run_pipeline_core()
       → pipeline.py orchestrates 11 pipeline stages via layers.py
       → Each stage calls domain functions in operations.py / ingestion/
       → Results persisted to Supabase tables (and Neo4j for graph)
```

### Key Modules

| Path | Role |
|------|------|
| `app/main.py` | FastAPI app bootstrap; mounts router + Inngest |
| `app/api/ingestion_routes.py` | All REST endpoints; store factories via DI |
| `app/inngest/runtime.py` | Four Inngest function definitions (triggers, retries, failure handlers) |
| `app/inngest/pipeline.py` | `_run_pipeline_core()` — top-level stage orchestration |
| `app/inngest/layers.py` | `_run_layer_internal()` wraps each stage with `pipeline_stage_runs` tracking |
| `app/inngest/operations.py` | Pure domain functions for every pipeline operation |
| `app/ingestion/` | One file per pipeline concern: parsers, validators, enrichment, entity/relationship/search/embedding stores, graph projection, connectors |
| `app/schemas.py` | Pydantic DTOs shared across all layers |
| `app/core/config.py` | `settings` — single Pydantic Settings instance loaded from env |

### Pipeline Stages (in order)

1. **raw_capture** — Store raw source events verbatim (`raw_events` table)
2. **canonicalization** — Parse raw JSON → `CanonicalEvent` Pydantic models (`canonical_events` table)
3. **entity_resolution** — Deduplicate contacts by email → `entities` + `entity_identities`
4. **metadata_extraction** — Tag extraction → `entities.tags`
5. **semantic_prep** — Build free-text semantic documents → `search_documents`
6. **embedding** — Generate vectors → `embedding_store`
7. **search_indexing** — Hybrid signal construction → `search_index_signals`
8. **relationship_extraction** — Build interaction facts + strength scores → `relationships`
9. **graph_projection** — Mirror Supabase graph into Neo4j (`graph_projection_runs/nodes/edges`)

### Inngest Functions

| fn_id | Trigger event | Purpose |
|-------|---------------|---------|
| `ingestion-pipeline-run` | `pipeline/run.requested` | Main pipeline entry point |
| `ingestion-user-connected` | `kue/user.connected` | Real OAuth → fetch → pipeline |
| `ingestion-user-mock-connected` | `kue/user.mock_connected` | Mock data → pipeline |
| `ingestion-stage-canonicalization-replay` | `pipeline/stage.canonicalization.replay.requested` | Replay canonicalization for existing trace |

All functions share `_run_pipeline_core()`. Failure alerts post to `ALERT_WEBHOOK_URL`.

### Large Payload Offloading

Inngest has a 256 KB step-result size limit. `StepPayloadStore` (`step_payloads` table) is used to offload large intermediate results (enrichment slices, embeddings). Affected operations serialize to Supabase and pass only a reference key between steps.

### Data Stores

Each `*_store.py` in `app/ingestion/` targets Supabase by default and falls back to SQLite for local dev when Supabase env vars are absent. Stores are injected as FastAPI dependencies in `ingestion_routes.py`.

### Testing Approach

Tests use `TestClient` from `httpx` against the FastAPI app directly. Each pipeline layer has a dedicated test file (`test_layer2_*` through `test_layer12_*`). Tests call the REST layer-dispatch endpoints (`/layer*/...`) rather than importing domain functions directly — integration-style, not unit-style.

### Environment Variables

Copy `.env.example` to `.env`. Required for full functionality:
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` — Supabase project
- `INNGEST_EVENT_KEY`, `INNGEST_SIGNING_KEY` — Inngest app credentials
- `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` — Google OAuth
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` — Neo4j (optional; graph stage no-ops if absent)
