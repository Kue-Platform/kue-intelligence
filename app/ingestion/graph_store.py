from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.core.config import Settings

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover - optional dependency in test env
    GraphDatabase = None  # type: ignore[assignment]


class GraphStore(ABC):
    @property
    @abstractmethod
    def store_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def ensure_schema(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def upsert_nodes(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def upsert_edges(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def verify(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        raise NotImplementedError


class NoopGraphStore(GraphStore):
    @property
    def store_name(self) -> str:
        return "graph:disabled"

    def ensure_schema(self) -> dict[str, Any]:
        return {"enabled": False, "schema_applied": False}

    def upsert_nodes(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        del snapshot
        return {
            "enabled": False,
            "nodes_by_label": {"Person": 0, "User": 0, "Company": 0, "Topic": 0},
            "total_upserted": 0,
            "failed_rows": 0,
        }

    def upsert_edges(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        del snapshot
        return {
            "enabled": False,
            "edges_by_type": {
                "KNOWS": 0,
                "INTERACTED_WITH": 0,
                "WORKS_AT": 0,
                "MEMBER_OF": 0,
                "HAS_TOPIC": 0,
                "INTRO_PATH": 0,
            },
            "total_upserted": 0,
            "skipped": 0,
        }

    def verify(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        del snapshot
        return {"enabled": False, "ok": True, "counts": {}, "mismatches": []}

    def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        return []


class Neo4jGraphStore(GraphStore):
    def __init__(
        self,
        *,
        uri: str,
        username: str,
        password: str,
        database: str,
        batch_size: int,
    ) -> None:
        if GraphDatabase is None:
            raise RuntimeError("neo4j package is not available")
        self._driver = GraphDatabase.driver(uri, auth=(username, password))
        self._database = database
        self._batch_size = max(1, int(batch_size))

    @property
    def store_name(self) -> str:
        return "neo4j"

    def _run_write(self, query: str, rows: list[dict[str, Any]]) -> int:
        total = 0
        with self._driver.session(database=self._database) as session:
            for idx in range(0, len(rows), self._batch_size):
                chunk = rows[idx : idx + self._batch_size]
                result = session.run(query, rows=chunk)
                row = result.single()
                if row and row.get("affected") is not None:
                    total += int(row["affected"])
        return total

    def _run_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        with self._driver.session(database=self._database) as session:
            result = session.run(query, **params)
            return [dict(r) for r in result]

    def ensure_schema(self) -> dict[str, Any]:
        queries = [
            # Node uniqueness constraints (back MERGE statements)
            "CREATE CONSTRAINT person_entity_id IF NOT EXISTS FOR (p:Person) REQUIRE (p.tenant_id, p.entity_id) IS UNIQUE",
            "CREATE CONSTRAINT user_user_id IF NOT EXISTS FOR (u:User) REQUIRE (u.tenant_id, u.user_id) IS UNIQUE",
            "CREATE CONSTRAINT company_company_id IF NOT EXISTS FOR (c:Company) REQUIRE (c.tenant_id, c.company_id) IS UNIQUE",
            "CREATE CONSTRAINT topic_topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE (t.tenant_id, t.topic_id) IS UNIQUE",
            # Node property indexes
            "CREATE INDEX person_primary_email IF NOT EXISTS FOR (p:Person) ON (p.primary_email)",
            "CREATE INDEX person_domain IF NOT EXISTS FOR (p:Person) ON (p.domain)",
            "CREATE INDEX company_domain IF NOT EXISTS FOR (c:Company) ON (c.domain)",
            "CREATE INDEX topic_name IF NOT EXISTS FOR (t:Topic) ON (t.name)",
            # Relationship property indexes on tenant_id
            # Allows WHERE r.tenant_id = $x to use an index instead of full scan
            "CREATE INDEX rel_knows_tenant IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r.tenant_id)",
            "CREATE INDEX rel_interacted_tenant IF NOT EXISTS FOR ()-[r:INTERACTED_WITH]-() ON (r.tenant_id)",
            "CREATE INDEX rel_works_at_tenant IF NOT EXISTS FOR ()-[r:WORKS_AT]-() ON (r.tenant_id)",
            "CREATE INDEX rel_member_of_tenant IF NOT EXISTS FOR ()-[r:MEMBER_OF]-() ON (r.tenant_id)",
            "CREATE INDEX rel_has_topic_tenant IF NOT EXISTS FOR ()-[r:HAS_TOPIC]-() ON (r.tenant_id)",
            "CREATE INDEX rel_intro_path_tenant IF NOT EXISTS FOR ()-[r:INTRO_PATH]-() ON (r.tenant_id)",
            # source_event_id index for INTERACTED_WITH dedup via MERGE
            "CREATE INDEX rel_interacted_source IF NOT EXISTS FOR ()-[r:INTERACTED_WITH]-() ON (r.source_event_id)",
        ]
        with self._driver.session(database=self._database) as session:
            for query in queries:
                session.run(query)
        return {"enabled": True, "schema_applied": True, "executed": len(queries)}

    def upsert_nodes(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        people = list(snapshot.get("persons", []))
        users = list(snapshot.get("users", []))
        companies = list(snapshot.get("companies", []))
        topics = list(snapshot.get("topics", []))

        person_q = """
        UNWIND $rows AS row
        MERGE (p:Person {tenant_id: row.tenant_id, entity_id: row.entity_id})
        ON CREATE SET p.created_at = row.created_at
        SET p.updated_at = row.updated_at,
            p.name = row.name,
            p.primary_email = row.primary_email,
            p.domain = row.domain,
            p.seniority = row.seniority,
            p.location = row.location
        RETURN count(*) AS affected
        """
        user_q = """
        UNWIND $rows AS row
        MERGE (u:User {tenant_id: row.tenant_id, user_id: row.user_id})
        ON CREATE SET u.created_at = row.created_at
        SET u.updated_at = row.updated_at,
            u.name = row.name,
            u.email = row.email,
            u.role = row.role
        RETURN count(*) AS affected
        """
        company_q = """
        UNWIND $rows AS row
        MERGE (c:Company {tenant_id: row.tenant_id, company_id: row.company_id})
        ON CREATE SET c.created_at = row.created_at
        SET c.updated_at = row.updated_at,
            c.name = row.name,
            c.domain = row.domain,
            c.industry = row.industry,
            c.size = row.size,
            c.location = row.location
        RETURN count(*) AS affected
        """
        topic_q = """
        UNWIND $rows AS row
        MERGE (t:Topic {tenant_id: row.tenant_id, topic_id: row.topic_id})
        ON CREATE SET t.created_at = row.created_at
        SET t.updated_at = row.updated_at,
            t.name = row.name
        RETURN count(*) AS affected
        """

        by_label = {
            "Person": self._run_write(person_q, people) if people else 0,
            "User": self._run_write(user_q, users) if users else 0,
            "Company": self._run_write(company_q, companies) if companies else 0,
            "Topic": self._run_write(topic_q, topics) if topics else 0,
        }
        total = sum(by_label.values())
        return {
            "enabled": True,
            "nodes_by_label": by_label,
            "total_upserted": total,
            "failed_rows": 0,
        }

    def upsert_edges(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        knows = list(snapshot.get("knows_edges", []))
        interacted = list(snapshot.get("interacted_edges", []))
        works_at = list(snapshot.get("works_at_edges", []))
        member_of = list(snapshot.get("member_of_edges", []))
        has_topic = list(snapshot.get("has_topic_edges", []))
        intro = list(snapshot.get("intro_path_edges", []))

        knows_q = """
        UNWIND $rows AS row
        MATCH (a:Person {tenant_id: row.tenant_id, entity_id: row.from_entity_id})
        MATCH (b:Person {tenant_id: row.tenant_id, entity_id: row.to_entity_id})
        MERGE (a)-[r:KNOWS {tenant_id: row.tenant_id, relationship_type: row.relationship_type}]->(b)
        SET r.strength = row.strength,
            r.interaction_count = row.interaction_count,
            r.first_interaction_at = row.first_interaction_at,
            r.last_interaction_at = row.last_interaction_at,
            r.channels = row.channels,
            r.evidence_json = row.evidence_json,
            r.updated_at = row.updated_at
        RETURN count(*) AS affected
        """

        interacted_q = """
        UNWIND $rows AS row
        MATCH (a:Person {tenant_id: row.tenant_id, entity_id: row.actor_entity_id})
        MATCH (b:Person {tenant_id: row.tenant_id, entity_id: row.target_entity_id})
        MERGE (a)-[r:INTERACTED_WITH {tenant_id: row.tenant_id, source_event_id: row.source_event_id}]->(b)
        SET r.channel = row.channel,
            r.interaction_type = row.interaction_type,
            r.occurred_at = row.occurred_at
        RETURN count(*) AS affected
        """

        works_at_q = """
        UNWIND $rows AS row
        MATCH (p:Person {tenant_id: row.tenant_id, entity_id: row.entity_id})
        MATCH (c:Company {tenant_id: row.tenant_id, company_id: row.company_id})
        MERGE (p)-[r:WORKS_AT {tenant_id: row.tenant_id}]->(c)
        SET r.title = row.title,
            r.department = row.department,
            r.seniority = row.seniority,
            r.start_date = row.start_date,
            r.end_date = row.end_date,
            r.is_current = row.is_current,
            r.updated_at = row.updated_at
        RETURN count(*) AS affected
        """

        member_of_q = """
        UNWIND $rows AS row
        MATCH (u:User {tenant_id: row.tenant_id, user_id: row.user_id})
        MATCH (c:Company {tenant_id: row.tenant_id, company_id: row.company_id})
        MERGE (u)-[r:MEMBER_OF {tenant_id: row.tenant_id}]->(c)
        SET r.role = row.role,
            r.joined_at = row.joined_at
        RETURN count(*) AS affected
        """

        has_topic_q = """
        UNWIND $rows AS row
        MATCH (p:Person {tenant_id: row.tenant_id, entity_id: row.entity_id})
        MATCH (t:Topic {tenant_id: row.tenant_id, topic_id: row.topic_id})
        MERGE (p)-[r:HAS_TOPIC {tenant_id: row.tenant_id}]->(t)
        SET r.confidence = row.confidence,
            r.source = row.source
        RETURN count(*) AS affected
        """

        intro_q = """
        UNWIND $rows AS row
        MATCH (u:User {tenant_id: row.tenant_id, user_id: row.user_id})
        MATCH (p:Person {tenant_id: row.tenant_id, entity_id: row.target_entity_id})
        MERGE (u)-[r:INTRO_PATH {tenant_id: row.tenant_id}]->(p)
        SET r.via_person_id = row.via_person_id,
            r.path_length = row.path_length,
            r.intro_likelihood = row.intro_likelihood,
            r.updated_at = row.updated_at
        RETURN count(*) AS affected
        """

        by_type = {
            "KNOWS": self._run_write(knows_q, knows) if knows else 0,
            "INTERACTED_WITH": self._run_write(interacted_q, interacted) if interacted else 0,
            "WORKS_AT": self._run_write(works_at_q, works_at) if works_at else 0,
            "MEMBER_OF": self._run_write(member_of_q, member_of) if member_of else 0,
            "HAS_TOPIC": self._run_write(has_topic_q, has_topic) if has_topic else 0,
            "INTRO_PATH": self._run_write(intro_q, intro) if intro else 0,
        }
        return {
            "enabled": True,
            "edges_by_type": by_type,
            "total_upserted": sum(by_type.values()),
            "skipped": 0,
        }

    def verify(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(snapshot.get("tenant_id") or "")
        if not tenant_id:
            return {"enabled": True, "ok": False, "counts": {}, "mismatches": ["missing_tenant_id"]}

        node_counts = self._run_query(
            """
            MATCH (n)
            WHERE n.tenant_id = $tenant_id AND (n:Person OR n:User OR n:Company OR n:Topic)
            RETURN labels(n)[0] AS label, count(n) AS cnt
            """,
            tenant_id=tenant_id,
        )
        rel_counts = self._run_query(
            """
            MATCH ()-[r:KNOWS|INTERACTED_WITH|WORKS_AT|MEMBER_OF|HAS_TOPIC|INTRO_PATH]->()
            WHERE r.tenant_id = $tenant_id
            RETURN type(r) AS rel_type, count(r) AS cnt
            """,
            tenant_id=tenant_id,
        )

        expected_nodes = {
            "Person": len(list(snapshot.get("persons", []))),
            "User": len(list(snapshot.get("users", []))),
            "Company": len(list(snapshot.get("companies", []))),
            "Topic": len(list(snapshot.get("topics", []))),
        }
        expected_edges = {
            "KNOWS": len(list(snapshot.get("knows_edges", []))),
            "INTERACTED_WITH": len(list(snapshot.get("interacted_edges", []))),
            "WORKS_AT": len(list(snapshot.get("works_at_edges", []))),
            "MEMBER_OF": len(list(snapshot.get("member_of_edges", []))),
            "HAS_TOPIC": len(list(snapshot.get("has_topic_edges", []))),
            "INTRO_PATH": len(list(snapshot.get("intro_path_edges", []))),
        }

        actual_nodes = {str(row["label"]): int(row["cnt"]) for row in node_counts}
        actual_edges = {str(row["rel_type"]): int(row["cnt"]) for row in rel_counts}

        mismatches: list[str] = []
        for key, expected in expected_nodes.items():
            if actual_nodes.get(key, 0) < expected:
                mismatches.append(f"node:{key}:expected>={expected}:actual={actual_nodes.get(key, 0)}")
        for key, expected in expected_edges.items():
            if actual_edges.get(key, 0) < expected:
                mismatches.append(f"edge:{key}:expected>={expected}:actual={actual_edges.get(key, 0)}")

        return {
            "enabled": True,
            "ok": len(mismatches) == 0,
            "counts": {
                "actual_nodes": actual_nodes,
                "actual_edges": actual_edges,
                "expected_nodes": expected_nodes,
                "expected_edges": expected_edges,
            },
            "mismatches": mismatches,
        }

    def run_read_query(self, query: str, **params: Any) -> list[dict[str, Any]]:
        with self._driver.session(database=self._database) as session:
            result = session.run(query, timeout=10.0, **params)
            return [dict(r) for r in result]


def create_graph_store(settings: Settings) -> GraphStore:
    if not settings.neo4j_uri or not settings.neo4j_username or not settings.neo4j_password:
        return NoopGraphStore()
    return Neo4jGraphStore(
        uri=settings.neo4j_uri,
        username=settings.neo4j_username,
        password=settings.neo4j_password,
        database=settings.neo4j_database,
        batch_size=settings.graph_projection_batch_size,
    )
