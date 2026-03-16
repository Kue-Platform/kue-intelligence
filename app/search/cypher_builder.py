from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.search.query_parser import ParsedQuery, QueryIntent


@dataclass
class CypherQuery:
    cypher: str
    params: dict[str, Any]
    result_type: str  # "persons" | "companies" | "mixed"


def build_cypher(
    parsed: ParsedQuery,
    *,
    tenant_id: str,
    user_id: str,
    limit: int = 20,
) -> CypherQuery:
    if parsed.intent == QueryIntent.PERSON:
        return _person_search(parsed, tenant_id=tenant_id, user_id=user_id, limit=limit)
    if parsed.intent == QueryIntent.COMPANY:
        return _company_search(parsed, tenant_id=tenant_id, limit=limit)
    if parsed.intent == QueryIntent.INTRO_PATH:
        return _intro_path_search(
            parsed, tenant_id=tenant_id, user_id=user_id, limit=limit
        )
    return _general_search(parsed, tenant_id=tenant_id, user_id=user_id, limit=limit)


def _person_search(
    parsed: ParsedQuery,
    *,
    tenant_id: str,
    user_id: str,
    limit: int,
) -> CypherQuery:
    conditions: list[str] = []
    params: dict[str, Any] = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "limit": limit,
    }

    if parsed.email:
        conditions.append("toLower(p.primary_email) = toLower($email)")
        params["email"] = parsed.email
    elif parsed.domain:
        conditions.append(
            "toLower(p.domain) = toLower($domain) OR toLower(p.primary_email) CONTAINS toLower($domain)"
        )
        params["domain"] = parsed.domain
    elif parsed.name_tokens:
        name_q = " ".join(parsed.name_tokens)
        conditions.append(
            "(toLower(p.name) CONTAINS toLower($name_q) OR toLower(p.primary_email) CONTAINS toLower($name_q))"
        )
        params["name_q"] = name_q

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    cypher = f"""
    MATCH (u:User {{tenant_id: $tenant_id, user_id: $user_id}})
    MATCH (p:Person {{tenant_id: $tenant_id}})
    {where_clause}
    OPTIONAL MATCH (u)-[k:KNOWS {{tenant_id: $tenant_id}}]->(p)
    OPTIONAL MATCH (p)-[w:WORKS_AT {{tenant_id: $tenant_id}}]->(c:Company)
    WITH p, k, c,
         size([(p)-[:KNOWS {{tenant_id: $tenant_id}}]-() | 1]) AS degree,
         size([(u)-[:KNOWS {{tenant_id: $tenant_id}}]->()-[:KNOWS {{tenant_id: $tenant_id}}]->(p) | 1]) AS shared_connections
    RETURN
        p.entity_id AS entity_id,
        p.name AS name,
        p.primary_email AS primary_email,
        p.domain AS domain,
        p.seniority AS seniority,
        p.location AS location,
        c.name AS company_name,
        c.domain AS company_domain,
        k.strength AS strength,
        k.interaction_count AS interaction_count,
        k.last_interaction_at AS last_interaction_at,
        degree,
        shared_connections
    ORDER BY k.strength DESC, shared_connections DESC
    LIMIT $limit
    """
    return CypherQuery(cypher=cypher, params=params, result_type="persons")


def _company_search(
    parsed: ParsedQuery,
    *,
    tenant_id: str,
    limit: int,
) -> CypherQuery:
    conditions: list[str] = []
    params: dict[str, Any] = {"tenant_id": tenant_id, "limit": limit}

    if parsed.domain:
        conditions.append("toLower(c.domain) = toLower($domain)")
        params["domain"] = parsed.domain
    elif parsed.company:
        conditions.append("toLower(c.name) CONTAINS toLower($company_q)")
        params["company_q"] = parsed.company

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    cypher = f"""
    MATCH (c:Company {{tenant_id: $tenant_id}})
    {where_clause}
    OPTIONAL MATCH (p:Person {{tenant_id: $tenant_id}})-[:WORKS_AT {{tenant_id: $tenant_id}}]->(c)
    RETURN
        c.company_id AS company_id,
        c.name AS name,
        c.domain AS domain,
        c.industry AS industry,
        c.size AS size,
        c.location AS location,
        count(p) AS employee_count
    ORDER BY employee_count DESC
    LIMIT $limit
    """
    return CypherQuery(cypher=cypher, params=params, result_type="companies")


def _intro_path_search(
    parsed: ParsedQuery,
    *,
    tenant_id: str,
    user_id: str,
    limit: int,
) -> CypherQuery:
    """Find Person nodes matching target_name, plus the shortest intro path from the current user."""
    params: dict[str, Any] = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "target_q": parsed.target_name or parsed.raw,
        "limit": limit,
    }
    cypher = """
    MATCH (u:User {tenant_id: $tenant_id, user_id: $user_id})
    MATCH (p:Person {tenant_id: $tenant_id})
    WHERE toLower(p.name) CONTAINS toLower($target_q)
    OPTIONAL MATCH (u)-[k:KNOWS {tenant_id: $tenant_id}]->(p)
    OPTIONAL MATCH path = shortestPath((u)-[:KNOWS*1..3 {tenant_id: $tenant_id}]->(p))
    WITH p, k, path,
         size([(p)-[:KNOWS {tenant_id: $tenant_id}]-() | 1]) AS degree,
         size([(u)-[:KNOWS {tenant_id: $tenant_id}]->()-[:KNOWS {tenant_id: $tenant_id}]->(p) | 1]) AS shared_connections
    OPTIONAL MATCH (p)-[:WORKS_AT {tenant_id: $tenant_id}]->(c:Company)
    RETURN
        p.entity_id AS entity_id,
        p.name AS name,
        p.primary_email AS primary_email,
        c.name AS company_name,
        k.strength AS strength,
        k.interaction_count AS interaction_count,
        k.last_interaction_at AS last_interaction_at,
        length(path) AS path_length,
        degree,
        shared_connections
    ORDER BY path_length ASC NULLS LAST, k.strength DESC NULLS LAST
    LIMIT $limit
    """
    return CypherQuery(cypher=cypher, params=params, result_type="persons")


def _general_search(
    parsed: ParsedQuery,
    *,
    tenant_id: str,
    user_id: str,
    limit: int,
) -> CypherQuery:
    """Fallback: search both persons and companies."""
    params: dict[str, Any] = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "q": parsed.raw,
        "limit": limit,
    }
    cypher = """
    MATCH (u:User {tenant_id: $tenant_id, user_id: $user_id})
    MATCH (p:Person {tenant_id: $tenant_id})
    WHERE toLower(p.name) CONTAINS toLower($q)
       OR toLower(p.primary_email) CONTAINS toLower($q)
    OPTIONAL MATCH (u)-[k:KNOWS {tenant_id: $tenant_id}]->(p)
    OPTIONAL MATCH (p)-[:WORKS_AT {tenant_id: $tenant_id}]->(c:Company)
    WITH p, k, c,
         size([(p)-[:KNOWS {tenant_id: $tenant_id}]-() | 1]) AS degree,
         size([(u)-[:KNOWS {tenant_id: $tenant_id}]->()-[:KNOWS {tenant_id: $tenant_id}]->(p) | 1]) AS shared_connections
    RETURN
        p.entity_id AS entity_id,
        p.name AS name,
        p.primary_email AS primary_email,
        p.domain AS domain,
        p.seniority AS seniority,
        p.location AS location,
        c.name AS company_name,
        k.strength AS strength,
        k.interaction_count AS interaction_count,
        k.last_interaction_at AS last_interaction_at,
        degree,
        shared_connections
    ORDER BY k.strength DESC NULLS LAST
    LIMIT $limit
    """
    return CypherQuery(cypher=cypher, params=params, result_type="persons")
