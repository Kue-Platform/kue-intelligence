from fastapi import FastAPI
import inngest.fast_api

from app.api.ingestion_routes import router as ingestion_router
from app.core.config import settings
from app.inngest.runtime import inngest_client, inngest_functions

app = FastAPI(title=settings.app_name)
app.include_router(ingestion_router)
inngest.fast_api.serve(app, inngest_client, inngest_functions)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}
