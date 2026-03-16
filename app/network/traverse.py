from __future__ import annotations

from typing import Any

from app.ingestion.graph_store import GraphStore


def second_degree_connections(
    graph_store: GraphStore,
    *,
    tenant_id: str,
    entity_id: str,
    limit: int = 25,
) -> list[dict[str, Any]]:
    """Return Person nodes reachable via exactly 2 KNOWS hops from entity_id."""
    return graph_store._run_query(
        """
        MATCH (p:Person {tenant_id: $tenant_id, entity_id: $entity_id})
        MATCH (p)-[:KNOWS {tenant_id: $tenant_id}]->(mid:Person)-[k2:KNOWS {tenant_id: $tenant_id}]->(target:Person)
        WHERE target.entity_id <> $entity_id
          AND NOT (p)-[:KNOWS {tenant_id: $tenant_id}]->(target)
        OPTIONAL MATCH (mid)-[k_mid:KNOWS {tenant_id: $tenant_id}]->(p)
        OPTIONAL MATCH (target)-[:WORKS_AT {tenant_id: $tenant_id}]->(c:Company)
        WITH DISTINCT target,
             collect(DISTINCT mid.entity_id)[0..5] AS via,
             max(k2.strength) AS max_path_strength,
             c
        RETURN
            target.entity_id AS entity_id,
            target.name AS name,
            target.primary_email AS primary_email,
            target.domain AS domain,
            target.seniority AS seniority,
            c.name AS company_name,
            via AS via_entity_ids,
            max_path_strength AS path_strength
        ORDER BY max_path_strength DESC NULLS LAST
        LIMIT $limit
        """,
        tenant_id=tenant_id,
        entity_id=entity_id,
        limit=limit,
    )


def intro_path(
    graph_store: GraphStore,
    *,
    tenant_id: str,
    from_entity_id: str,
    to_entity_id: str,
) -> dict[str, Any]:
    """Find the shortest introduction path between two Person nodes."""
    rows = graph_store._run_query(
        """
        MATCH (src:Person {tenant_id: $tenant_id, entity_id: $from_id}),
              (dst:Person {tenant_id: $tenant_id, entity_id: $to_id})
        MATCH path = shortestPath((src)-[:KNOWS*1..4 {tenant_id: $tenant_id}]->(dst))
        WITH path, nodes(path) AS path_nodes, relationships(path) AS path_rels
        RETURN
            [n IN path_nodes | {entity_id: n.entity_id, name: n.name}] AS nodes,
            length(path) AS path_length,
            [r IN path_rels | r.strength] AS strengths
        LIMIT 1
        """,
        tenant_id=tenant_id,
        from_id=from_entity_id,
        to_id=to_entity_id,
    )
    if not rows:
        return {"found": False, "path_length": None, "nodes": [], "strengths": []}
    row = rows[0]
    return {
        "found": True,
        "path_length": row.get("path_length"),
        "nodes": row.get("nodes", []),
        "strengths": row.get("strengths", []),
    }


def network_stats(
    graph_store: GraphStore,
    *,
    tenant_id: str,
) -> dict[str, Any]:
    """Return high-level stats for the tenant graph."""
    node_counts = graph_store._run_query(
        """
        MATCH (n {tenant_id: $tenant_id})
        WHERE n:Person OR n:User OR n:Company OR n:Topic
        RETURN labels(n)[0] AS label, count(n) AS cnt
        """,
        tenant_id=tenant_id,
    )
    rel_counts = graph_store._run_query(
        """
        MATCH ()-[r:KNOWS|INTERACTED_WITH|WORKS_AT|MEMBER_OF|HAS_TOPIC]->()
        WHERE r.tenant_id = $tenant_id
        RETURN type(r) AS rel_type, count(r) AS cnt
        """,
        tenant_id=tenant_id,
    )

    # Top connectors: persons with the highest KNOWS degree
    top_connectors = graph_store._run_query(
        """
        MATCH (p:Person {tenant_id: $tenant_id})-[k:KNOWS {tenant_id: $tenant_id}]->()
        WITH p, count(k) AS degree
        ORDER BY degree DESC
        LIMIT 5
        RETURN p.entity_id AS entity_id, p.name AS name, degree
        """,
        tenant_id=tenant_id,
    )

    return {
        "tenant_id": tenant_id,
        "nodes": {str(r["label"]): int(r["cnt"]) for r in node_counts},
        "relationships": {str(r["rel_type"]): int(r["cnt"]) for r in rel_counts},
        "top_connectors": [dict(r) for r in top_connectors],
    }
