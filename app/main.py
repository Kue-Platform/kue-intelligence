from fastapi import FastAPI

from app.api.ingestion_routes import router as ingestion_router
from app.api.routes import router as jobs_router
from app.core.config import settings

app = FastAPI(title=settings.app_name)
app.include_router(jobs_router)
app.include_router(ingestion_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}
