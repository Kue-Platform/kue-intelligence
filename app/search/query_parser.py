from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class QueryIntent(str, Enum):
    PERSON = "person_search"
    COMPANY = "company_search"
    INTRO_PATH = "intro_path"
    GENERAL = "general"


@dataclass
class ParsedQuery:
    raw: str
    intent: QueryIntent
    name_tokens: list[str] = field(default_factory=list)
    email: str | None = None
    company: str | None = None
    domain: str | None = None
    # For intro path: "intro to <name>" or "how to meet <name>"
    target_name: str | None = None


_INTRO_RE = re.compile(
    r"\b(?:intro(?:duce me)?\s+to|how\s+(?:do\s+i\s+)?meet|connect\s+me\s+(?:with|to))\b\s+(.+)",
    re.IGNORECASE,
)
_COMPANY_RE = re.compile(
    r"\b(?:people\s+at|contacts?\s+at|works?\s+at|from|employees?\s+(?:at|of))\b\s+(.+)",
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_DOMAIN_RE = re.compile(r"\b([\w\-]+\.(?:com|io|ai|co|net|org|dev))\b", re.IGNORECASE)


def parse_query(query: str) -> ParsedQuery:
    q = query.strip()

    # Check for intro path intent first
    intro_match = _INTRO_RE.search(q)
    if intro_match:
        target = intro_match.group(1).strip()
        return ParsedQuery(
            raw=q,
            intent=QueryIntent.INTRO_PATH,
            target_name=target,
            name_tokens=target.lower().split(),
        )

    # Check for company search
    company_match = _COMPANY_RE.search(q)
    if company_match:
        company_name = company_match.group(1).strip()
        return ParsedQuery(
            raw=q,
            intent=QueryIntent.COMPANY,
            company=company_name,
        )

    # Check for email
    email_match = _EMAIL_RE.search(q)
    if email_match:
        email = email_match.group(0)
        domain = email.split("@")[1] if "@" in email else None
        return ParsedQuery(
            raw=q,
            intent=QueryIntent.PERSON,
            email=email,
            domain=domain,
        )

    # Check for domain-only search
    domain_match = _DOMAIN_RE.search(q)
    if domain_match and len(q.split()) <= 3:
        domain = domain_match.group(1).lower()
        # Could be company or person
        return ParsedQuery(
            raw=q,
            intent=QueryIntent.COMPANY,
            domain=domain,
        )

    # Default: person name search
    # Strip common stop words to get name tokens
    stop_words = {"find", "search", "show", "me", "who", "is", "the", "a", "an"}
    tokens = [t.lower() for t in q.split() if t.lower() not in stop_words]
    if tokens:
        return ParsedQuery(
            raw=q,
            intent=QueryIntent.PERSON,
            name_tokens=tokens,
        )

    return ParsedQuery(raw=q, intent=QueryIntent.GENERAL)
