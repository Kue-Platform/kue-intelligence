from collections import Counter
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from app.ingestion.connectors import trigger_mock_connector
from app.ingestion.google_connector import (
    GoogleConnectorError,
    GoogleOAuthConnector,
    GoogleOAuthContext,
    resolve_google_state,
)
from app.schemas import (
    GoogleOAuthMockCallbackRequest,
    GoogleOAuthCallbackResponse,
    IngestionMockTriggerRequest,
    IngestionMockTriggerResponse,
)

router = APIRouter(prefix="/v1/ingestion", tags=["ingestion"])


def get_google_connector() -> GoogleOAuthConnector:
    return GoogleOAuthConnector()


@router.post("/mock", response_model=IngestionMockTriggerResponse)
def trigger_ingestion_mock(payload: IngestionMockTriggerRequest) -> IngestionMockTriggerResponse:
    try:
        result = trigger_mock_connector(
            source=payload.source,
            trigger_type=payload.trigger_type,
            payload=payload.payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return IngestionMockTriggerResponse(
        connector_event_id=result.connector_event_id,
        trace_id=result.trace_id,
        source=result.source,
        trigger_type=result.trigger_type,
        accepted_at=result.accepted_at,
        records_detected=result.records_detected,
        payload_shape=result.payload_shape,
    )


@router.get("/google/oauth/callback", response_model=GoogleOAuthCallbackResponse)
async def google_oauth_callback(
    code: str | None = None,
    state: str | None = None,
    tenant_id: str | None = None,
    user_id: str | None = None,
    error: str | None = None,
    connector: GoogleOAuthConnector = Depends(get_google_connector),
) -> GoogleOAuthCallbackResponse:
    if error:
        raise HTTPException(status_code=400, detail=f"Google OAuth returned an error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing OAuth authorization code.")

    try:
        resolved_tenant_id, resolved_user_id = resolve_google_state(
            state=state,
            tenant_id=tenant_id,
            user_id=user_id,
        )
        trace_id = f"trace_{uuid4().hex}"
        source_events = await connector.handle_callback(
            code=code,
            context=GoogleOAuthContext(
                tenant_id=resolved_tenant_id,
                user_id=resolved_user_id,
                trace_id=trace_id,
            ),
        )
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Google connector failure: {exc}") from exc

    counts = Counter(str(event.source) for event in source_events)
    return GoogleOAuthCallbackResponse(
        tenant_id=resolved_tenant_id,
        user_id=resolved_user_id,
        trace_id=trace_id,
        source_events=source_events,
        counts=dict(counts),
    )


@router.post("/google/oauth/callback/mock", response_model=GoogleOAuthCallbackResponse)
async def google_oauth_callback_mock(
    request: GoogleOAuthMockCallbackRequest,
    connector: GoogleOAuthConnector = Depends(get_google_connector),
) -> GoogleOAuthCallbackResponse:
    try:
        resolved_tenant_id, resolved_user_id = resolve_google_state(
            state=request.state,
            tenant_id=request.tenant_id,
            user_id=request.user_id,
        )
        trace_id = request.trace_id or f"trace_{uuid4().hex}"
        source_events = connector.handle_mock_callback(
            context=GoogleOAuthContext(
                tenant_id=resolved_tenant_id,
                user_id=resolved_user_id,
                trace_id=trace_id,
            ),
            source_type=request.source_type,
            payload=request.payload,
        )
    except GoogleConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    counts = Counter(str(event.source) for event in source_events)
    return GoogleOAuthCallbackResponse(
        tenant_id=resolved_tenant_id,
        user_id=resolved_user_id,
        trace_id=trace_id,
        source_events=source_events,
        counts=dict(counts),
    )
