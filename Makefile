.PHONY: sync run test lint lock

sync:
	uv sync --group dev

run:
	uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

test:
	uv run pytest

lock:
	uv lock
