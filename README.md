# kue-intelligence

AI/Data service scaffold built with Python 3.11+, FastAPI, Celery, Upstash Redis, pandas/numpy, and pytest.

## Stack
- API: FastAPI
- Async workers: Celery
- Queue/Result store: Redis (Upstash-compatible)
- Data processing: pandas + numpy
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

4. Update `.env` with your Upstash Redis URL.

5. Start API:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

6. Start Celery worker:

```bash
celery -A app.celery_app.celery_app worker --loglevel=info
```

## API Endpoints
- `GET /health`: health check.
- `POST /v1/jobs/analyze`: enqueue numeric analysis task.
- `GET /v1/jobs/{task_id}`: fetch Celery task status/result.
- `POST /v1/ingestion/mock`: trigger mock source connector inputs (`google_contacts`, `gmail`, `linkedin`).
- `GET /v1/ingestion/google/oauth/callback`: complete Google OAuth callback and emit normalized source events for contacts, gmail, and calendar.

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

Response includes `source_events` in this shape:
- `tenant_id`
- `user_id`
- `source`
- `source_event_id`
- `occurred_at`
- `trace_id`

## Run Tests

```bash
pytest
```
