from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx

from app.core.config import settings
from app.schemas import GoogleMockSourceType, IngestionSource, SourceEvent

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_CONTACTS_URL = "https://people.googleapis.com/v1/people/me/connections"
GOOGLE_GMAIL_LIST_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
GOOGLE_GMAIL_GET_URL_TMPL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
GOOGLE_CALENDAR_EVENTS_URL = "https://www.googleapis.com/calendar/v3/calendars/primary/events"


class GoogleConnectorError(RuntimeError):
    """Raised when the Google connector cannot complete callback processing."""


@dataclass
class GoogleOAuthContext:
    tenant_id: str
    user_id: str
    trace_id: str


def _parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    parsed = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(parsed)
    except ValueError:
        return datetime.now(UTC)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def resolve_google_state(
    *,
    state: str | None,
    tenant_id: str | None,
    user_id: str | None,
) -> tuple[str, str]:
    if tenant_id and user_id:
        return tenant_id, user_id
    if not state:
        raise GoogleConnectorError("Missing tenant_id/user_id. Provide query params or encoded state.")

    try:
        decoded = base64.urlsafe_b64decode(state + "=" * (-len(state) % 4)).decode("utf-8")
        parsed = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as exc:
        raise GoogleConnectorError("Invalid OAuth state. Expected base64url JSON.") from exc

    parsed_tenant_id = parsed.get("tenant_id")
    parsed_user_id = parsed.get("user_id")
    if not parsed_tenant_id or not parsed_user_id:
        raise GoogleConnectorError("OAuth state must include tenant_id and user_id.")
    return str(parsed_tenant_id), str(parsed_user_id)


class GoogleOAuthConnector:
    async def handle_callback(
        self,
        *,
        code: str,
        context: GoogleOAuthContext,
    ) -> list[SourceEvent]:
        access_token = await self._exchange_code_for_token(code=code)

        async with httpx.AsyncClient(timeout=20.0) as client:
            userinfo_task = self._fetch_userinfo(client=client, access_token=access_token)
            contacts_task = self._fetch_contacts(client=client, access_token=access_token)
            gmail_task = self._fetch_gmail_messages(client=client, access_token=access_token)
            calendar_task = self._fetch_calendar_events(client=client, access_token=access_token)
            _, contacts, gmail_messages, calendar_events = await asyncio.gather(
                userinfo_task,
                contacts_task,
                gmail_task,
                calendar_task,
            )

        source_events: list[SourceEvent] = []
        source_events.extend(self._to_contact_events(context=context, contacts=contacts))
        source_events.extend(self._to_gmail_events(context=context, messages=gmail_messages))
        source_events.extend(self._to_calendar_events(context=context, events=calendar_events))
        return source_events

    def handle_mock_callback(
        self,
        *,
        context: GoogleOAuthContext,
        source_type: GoogleMockSourceType,
        payload: dict[str, Any],
    ) -> list[SourceEvent]:
        if source_type == GoogleMockSourceType.CONTACTS:
            contacts = self._normalize_contacts_payload(payload)
            return self._to_contact_events(context=context, contacts=contacts)
        if source_type == GoogleMockSourceType.GMAIL:
            messages = self._normalize_gmail_payload(payload)
            return self._to_gmail_events(context=context, messages=messages)
        events = self._normalize_calendar_payload(payload)
        return self._to_calendar_events(context=context, events=events)

    async def _exchange_code_for_token(self, *, code: str) -> str:
        if (
            not settings.google_oauth_client_id
            or not settings.google_oauth_client_secret
            or not settings.google_oauth_redirect_uri
        ):
            raise GoogleConnectorError("Google OAuth settings are missing in environment.")

        payload = {
            "code": code,
            "client_id": settings.google_oauth_client_id,
            "client_secret": settings.google_oauth_client_secret,
            "redirect_uri": settings.google_oauth_redirect_uri,
            "grant_type": "authorization_code",
        }

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(GOOGLE_TOKEN_URL, data=payload)
        if response.status_code >= 400:
            raise GoogleConnectorError(f"Token exchange failed with status {response.status_code}.")
        token = response.json().get("access_token")
        if not token:
            raise GoogleConnectorError("Token exchange succeeded but access_token is missing.")
        return token

    async def _fetch_userinfo(self, *, client: httpx.AsyncClient, access_token: str) -> dict[str, Any]:
        response = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code >= 400:
            raise GoogleConnectorError(f"Failed to fetch user info: {response.status_code}.")
        return response.json()

    async def _fetch_contacts(self, *, client: httpx.AsyncClient, access_token: str) -> list[dict[str, Any]]:
        response = await client.get(
            GOOGLE_CONTACTS_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "personFields": "names,emailAddresses,phoneNumbers,organizations,metadata",
                "pageSize": 200,
            },
        )
        if response.status_code >= 400:
            raise GoogleConnectorError(f"Failed to fetch contacts: {response.status_code}.")
        return response.json().get("connections", [])

    async def _fetch_gmail_messages(self, *, client: httpx.AsyncClient, access_token: str) -> list[dict[str, Any]]:
        list_response = await client.get(
            GOOGLE_GMAIL_LIST_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"maxResults": 25},
        )
        if list_response.status_code >= 400:
            raise GoogleConnectorError(f"Failed to list gmail messages: {list_response.status_code}.")
        message_refs = list_response.json().get("messages", [])
        if not message_refs:
            return []

        async def fetch_message_detail(message_id: str) -> dict[str, Any]:
            detail_url = GOOGLE_GMAIL_GET_URL_TMPL.format(message_id=message_id)
            detail_response = await client.get(
                detail_url,
                headers={"Authorization": f"Bearer {access_token}"},
                params={"format": "metadata", "metadataHeaders": "Date"},
            )
            if detail_response.status_code >= 400:
                return {"id": message_id}
            return detail_response.json()

        tasks = [fetch_message_detail(item["id"]) for item in message_refs if item.get("id")]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    async def _fetch_calendar_events(self, *, client: httpx.AsyncClient, access_token: str) -> list[dict[str, Any]]:
        response = await client.get(
            GOOGLE_CALENDAR_EVENTS_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "maxResults": 100,
                "singleEvents": "true",
                "orderBy": "updated",
            },
        )
        if response.status_code >= 400:
            raise GoogleConnectorError(f"Failed to fetch calendar events: {response.status_code}.")
        return response.json().get("items", [])

    def _normalize_contacts_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if "connections" in payload and isinstance(payload["connections"], list):
            return payload["connections"]
        if "contacts" in payload and isinstance(payload["contacts"], list):
            return payload["contacts"]
        if payload:
            return [payload]
        raise GoogleConnectorError("Mock contacts payload must contain 'connections' or contact object data.")

    def _normalize_gmail_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if "messages" in payload and isinstance(payload["messages"], list):
            return payload["messages"]
        if payload:
            return [payload]
        raise GoogleConnectorError("Mock gmail payload must contain 'messages' or message object data.")

    def _normalize_calendar_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if "items" in payload and isinstance(payload["items"], list):
            return payload["items"]
        if "events" in payload and isinstance(payload["events"], list):
            return payload["events"]
        if payload:
            return [payload]
        raise GoogleConnectorError("Mock calendar payload must contain 'items' or event object data.")

    def _to_contact_events(
        self,
        *,
        context: GoogleOAuthContext,
        contacts: list[dict[str, Any]],
    ) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        for item in contacts:
            source_event_id = item.get("resourceName") or f"contact_{uuid4().hex}"
            metadata = item.get("metadata", {})
            source_entries = metadata.get("sources", [])
            occurred_at = _parse_iso_datetime(
                source_entries[0].get("updateTime") if source_entries else None
            )
            events.append(
                SourceEvent(
                    tenant_id=context.tenant_id,
                    user_id=context.user_id,
                    source=IngestionSource.GOOGLE_CONTACTS,
                    source_event_id=source_event_id,
                    occurred_at=occurred_at,
                    trace_id=context.trace_id,
                    payload=item,
                )
            )
        return events

    def _to_gmail_events(
        self,
        *,
        context: GoogleOAuthContext,
        messages: list[dict[str, Any]],
    ) -> list[SourceEvent]:
        events: list[SourceEvent] = []
        for item in messages:
            message_id = item.get("id") or f"message_{uuid4().hex}"
            internal_date = item.get("internalDate")
            if isinstance(internal_date, str) and internal_date.isdigit():
                occurred_at = datetime.fromtimestamp(int(internal_date) / 1000, tz=UTC)
            else:
                occurred_at = datetime.now(UTC)
            events.append(
                SourceEvent(
                    tenant_id=context.tenant_id,
                    user_id=context.user_id,
                    source=IngestionSource.GMAIL,
                    source_event_id=message_id,
                    occurred_at=occurred_at,
                    trace_id=context.trace_id,
                    payload=item,
                )
            )
        return events

    def _to_calendar_events(
        self,
        *,
        context: GoogleOAuthContext,
        events: list[dict[str, Any]],
    ) -> list[SourceEvent]:
        output: list[SourceEvent] = []
        for item in events:
            event_id = item.get("id") or f"calendar_{uuid4().hex}"
            occurred_at = _parse_iso_datetime(item.get("updated") or item.get("created"))
            output.append(
                SourceEvent(
                    tenant_id=context.tenant_id,
                    user_id=context.user_id,
                    source=IngestionSource.GOOGLE_CALENDAR,
                    source_event_id=event_id,
                    occurred_at=occurred_at,
                    trace_id=context.trace_id,
                    payload=item,
                )
            )
        return output
