# kue-intelligence

AI/Data service scaffold built with Python 3.11+, FastAPI, staged ingestion orchestration, Supabase, and pytest.

## Stack
- API: FastAPI
- Orchestration events: Inngest
- Testing: pytest

## Quick Start

1. Create and activate a Python 3.11+ virtual environment.
2. Install deps:

```bash
pip install -e ".[dev]"
```

3. Create env file:

```bash
cp .env.example .env
```

4. Update `.env` with your Supabase + Inngest settings.

5. Start API:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

6. Ensure your Inngest event key is configured to stream stage/layer execution events.

## API Endpoints
- `GET /health`: health check.
- `POST /v1/ingestion/mock`: trigger mock source connector inputs (`google_contacts`, `gmail`, `linkedin`).
- `GET /v1/ingestion/google/oauth/callback`: receive Google callback and emit `kue/user.connected` event.
- `POST /v1/ingestion/google/oauth/callback/mock`: emit `kue/user.mock_connected` event.
- `GET /v1/ingestion/raw-events/{trace_id}`: fetch Layer 2 captured raw events by trace id.
- `POST /v1/ingestion/layer2/capture`: run stage pipeline from explicit source events.
- `POST /v1/ingestion/layer3/parse/{trace_id}`: dispatch canonicalization replay in Inngest.
- `POST /v1/ingestion/stage/canonicalization/replay/{trace_id}`: replay canonicalization stage.
- `GET /v1/ingestion/layer3/events/{trace_id}`: fetch persisted Layer 3 canonical events by trace id.

### Layer 1 Test Endpoint Example

```bash
curl -X POST http://localhost:8000/v1/ingestion/mock \
  -H "Content-Type: application/json" \
  -d '{
    "source": "google_contacts",
    "trigger_type": "manual",
    "payload": {
      "contacts": [{"name": "Alan Turing"}]
    }
  }'
```

### Google OAuth Callback Example

```bash
curl "http://localhost:8000/v1/ingestion/google/oauth/callback?code=<google_auth_code>&tenant_id=tenant_123&user_id=user_123"
```

Callbacks emit Inngest events and Inngest functions execute stage orchestration:
- `kue/user.connected` (real OAuth path)
- `kue/user.mock_connected` (mock path)

Inngest function then performs:
- intake fetch from Google APIs (or mock payload adaptation)
- `stage.raw_capture` (validate + persist raw events)
- `stage.canonicalization` (fetch raw + parse + persist canonical)

Each stage emits Inngest events:
- `pipeline.run.started|completed|failed`
- `stage.raw_capture.started|completed`
- `stage.canonicalization.started|completed`
- `*.layer.started|completed` for layer-level visibility

Inngest functions are served from this API app via `inngest.fast_api.serve(...)`.

### Google OAuth Callback Mock Example

```bash
curl -X POST http://localhost:8000/v1/ingestion/google/oauth/callback/mock \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "contacts",
    "tenant_id": "tenant_123",
    "user_id": "user_123",
    "payload": {
      "connections": [
        {"resourceName": "people/c_1", "names": [{"displayName": "Alan Turing"}]}
      ]
    }
  }'
```

## Run Tests

```bash
pytest
```

## Supabase Table (Layer 2)

Create this table in Supabase for raw capture:

```sql
create table if not exists public.raw_events (
  id bigserial primary key,
  tenant_id text not null,
  user_id text not null,
  source text not null,
  source_event_id text not null,
  occurred_at timestamptz not null,
  trace_id text not null,
  payload_json jsonb not null,
  captured_at timestamptz not null default now()
);

create index if not exists idx_raw_events_trace_id on public.raw_events(trace_id);
create index if not exists idx_raw_events_tenant_source on public.raw_events(tenant_id, source, occurred_at);
```

## Supabase Table (Layer 3)

Create this table in Supabase for parsed canonical storage:

```sql
create table if not exists public.canonical_events (
  id bigserial primary key,
  raw_event_id bigint not null,
  tenant_id text not null,
  user_id text not null,
  trace_id text not null,
  source text not null,
  source_event_id text not null,
  occurred_at timestamptz not null,
  event_type text not null,
  normalized_json jsonb not null,
  parse_warnings_json jsonb not null default '[]'::jsonb,
  parser_version text not null,
  parsed_at timestamptz not null default now()
);
```
