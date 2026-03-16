from fastapi import FastAPI
import inngest.fast_api

from app.api.auth_routes import router as auth_router
from app.api.ingestion_routes import router as ingestion_router
from app.api.network_routes import router as network_router
from app.api.search_routes import router as search_router
from app.api.sync_routes import router as sync_router
from app.core.config import settings
from app.inngest.runtime import inngest_client, inngest_functions

app = FastAPI(title=settings.app_name)
app.include_router(auth_router)
app.include_router(ingestion_router)
app.include_router(network_router)
app.include_router(search_router)
app.include_router(sync_router)
inngest.fast_api.serve(app, inngest_client, inngest_functions)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}
