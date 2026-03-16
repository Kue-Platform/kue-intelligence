from __future__ import annotations

import base64
import json
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.auth import jwt as jwt_utils
from app.auth import otp as otp_store
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"


class OTPRequestBody(BaseModel):
    email: str


class OTPVerifyBody(BaseModel):
    email: str
    code: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    tenant_id: str
    email: str


@router.post("/otp/request")
def request_otp(body: OTPRequestBody) -> dict[str, Any]:
    """Generate and store an OTP for the given email.

    In production, send the code via email. In development (no mail config),
    the code is returned in the response body.
    """
    code = otp_store.generate_otp(str(body.email))
    response: dict[str, Any] = {"message": "OTP sent", "email": str(body.email)}
    if settings.app_env == "development":
        response["code"] = code  # expose code only in dev
    return response


@router.post("/otp/verify", response_model=TokenResponse)
def verify_otp(body: OTPVerifyBody) -> TokenResponse:
    """Verify OTP and return a JWT access token."""
    valid = otp_store.verify_otp(str(body.email), body.code)
    if not valid:
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    user = otp_store.get_or_create_user(email=str(body.email))
    token = jwt_utils.create_access_token(
        user_id=user["user_id"],
        tenant_id=user["tenant_id"],
        email=user["email"],
    )
    return TokenResponse(
        access_token=token,
        user_id=user["user_id"],
        tenant_id=user["tenant_id"],
        email=user["email"],
    )


@router.get("/google")
def google_login() -> dict[str, str]:
    """Return the Google OAuth authorization URL for user login."""
    redirect_uri = (
        settings.auth_google_redirect_uri or settings.google_oauth_redirect_uri
    )
    if not settings.google_oauth_client_id or not redirect_uri:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured")

    state_data = json.dumps({"flow": "auth"})
    state = base64.urlsafe_b64encode(state_data.encode()).decode().rstrip("=")

    params = {
        "client_id": settings.google_oauth_client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "state": state,
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return {"auth_url": f"{GOOGLE_AUTH_URL}?{query}"}


@router.get("/google/callback", response_model=TokenResponse)
async def google_callback(
    code: str | None = None,
    error: str | None = None,
) -> TokenResponse:
    """Exchange Google auth code for a Kue JWT."""
    if error:
        raise HTTPException(status_code=400, detail=f"Google OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing code parameter")

    redirect_uri = (
        settings.auth_google_redirect_uri or settings.google_oauth_redirect_uri
    )
    if (
        not settings.google_oauth_client_id
        or not settings.google_oauth_client_secret
        or not redirect_uri
    ):
        raise HTTPException(status_code=503, detail="Google OAuth is not configured")

    async with httpx.AsyncClient(timeout=20.0) as client:
        token_resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.google_oauth_client_id,
                "client_secret": settings.google_oauth_client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
    if token_resp.status_code >= 400:
        raise HTTPException(
            status_code=502, detail=f"Google token exchange failed: {token_resp.text}"
        )

    token_data = token_resp.json()
    access_token_google = str(token_data.get("access_token", ""))
    if not access_token_google:
        raise HTTPException(
            status_code=502, detail="Google token exchange returned no access_token"
        )

    async with httpx.AsyncClient(timeout=20.0) as client:
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token_google}"},
        )
    if userinfo_resp.status_code >= 400:
        raise HTTPException(status_code=502, detail="Failed to fetch Google user info")

    userinfo = userinfo_resp.json()
    email = str(userinfo.get("email") or "")
    name = str(userinfo.get("name") or "")
    google_sub = str(userinfo.get("sub") or "")

    if not email:
        raise HTTPException(
            status_code=502, detail="Google did not return an email address"
        )

    user = otp_store.get_or_create_user(
        email=email, name=name or None, google_sub=google_sub or None
    )
    jwt_token = jwt_utils.create_access_token(
        user_id=user["user_id"],
        tenant_id=user["tenant_id"],
        email=user["email"],
    )
    return TokenResponse(
        access_token=jwt_token,
        user_id=user["user_id"],
        tenant_id=user["tenant_id"],
        email=user["email"],
    )
