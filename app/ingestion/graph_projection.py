from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.config import Settings


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].strip().lower() or None


def _slug(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in value).strip("_")


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


@dataclass
class GraphProjectionService:
    settings: Settings

    @property
    def _enabled(self) -> bool:
        return bool(self.settings.supabase_url and (self.settings.supabase_service_role_key or self.settings.supabase_anon_key))

    @property
    def _api_key(self) -> str:
        return self.settings.supabase_service_role_key or self.settings.supabase_anon_key

    def _url(self, table: str) -> str:
        return f"{self.settings.supabase_url.rstrip('/')}/rest/v1/{table}"

    def _headers(self, prefer_repr: bool = False, upsert: bool = False) -> dict[str, str]:
        headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        if upsert:
            # PostgREST requires this header to treat on_conflict as an upsert
            headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        elif prefer_repr:
            headers["Prefer"] = "return=representation"
        return headers

    def _fetch_all(self, table: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._enabled:
            return []
        all_rows: list[dict[str, Any]] = []
        limit = max(1, int(self.settings.graph_projection_batch_size))
        offset = 0

        while True:
            page_params = dict(params)
            page_params["limit"] = str(limit)
            page_params["offset"] = str(offset)
            response = httpx.get(
                self._url(table),
                headers=self._headers(),
                params=page_params,
                timeout=30.0,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Supabase {table} fetch failed ({response.status_code}): {response.text}"
                )
            rows = response.json()
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < limit:
                break
            offset += limit

        return all_rows

    @staticmethod
    def _is_missing_relation(response: httpx.Response) -> bool:
        body = (response.text or "").lower()
        return response.status_code in {400, 404} and (
            "does not exist" in body or "42p01" in body or "relation" in body
        )

    def _extract_topics(self, metadata_json: dict[str, Any]) -> list[dict[str, Any]]:
        topics: list[dict[str, Any]] = []
        for key in ("topics", "skills", "interests", "tags"):
            raw = metadata_json.get(key)
            if isinstance(raw, list):
                for item in raw:
                    name = _norm_text(item)
                    if not name:
                        continue
                    topics.append({"name": name, "source": key})
        return topics

    def prepare_snapshot(self, *, tenant_id: str, user_id: str, trace_id: str) -> dict[str, Any]:
        if not self._enabled:
            return {
                "enabled": False,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "trace_id": trace_id,
                "batch_stats": {},
                "persons": [],
                "users": [],
                "companies": [],
                "topics": [],
                "knows_edges": [],
                "interacted_edges": [],
                "works_at_edges": [],
                "member_of_edges": [],
                "has_topic_edges": [],
                "intro_path_edges": [],
                "snapshot_checksum": "",
            }

        entities = self._fetch_all(
            "entities",
            {
                "tenant_id": f"eq.{tenant_id}",
                "select": "entity_id,display_name,primary_email,title_norm,location_norm,metadata_json,company_norm,created_at,updated_at",
            },
        )
        relationships = self._fetch_all(
            "relationships",
            {
                "tenant_id": f"eq.{tenant_id}",
                "select": "from_entity_id,to_entity_id,relationship_type,strength,interaction_count,first_interaction_at,last_interaction_at,evidence_json,updated_at",
            },
        )
        interactions = self._fetch_all(
            "interaction_facts",
            {
                "tenant_id": f"eq.{tenant_id}",
                "select": "actor_entity_id,target_entity_id,touchpoint_type,occurred_at,source,raw_event_id,canonical_event_id",
            },
        )
        users = self._fetch_all(
            "tenant_users",
            {
                "tenant_id": f"eq.{tenant_id}",
                "select": "user_id,email,display_name,role,created_at,updated_at",
            },
        )

        person_nodes: list[dict[str, Any]] = []
        company_by_name: dict[str, dict[str, Any]] = {}
        topic_by_key: dict[str, dict[str, Any]] = {}
        works_at_edges: list[dict[str, Any]] = []
        has_topic_edges: list[dict[str, Any]] = []

        for row in entities:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            email = _norm_text(row.get("primary_email"))
            domain = _email_domain(email)
            metadata_json = row.get("metadata_json") if isinstance(row.get("metadata_json"), dict) else {}
            seniority = _norm_text(metadata_json.get("seniority") or row.get("title_norm"))
            person_nodes.append(
                {
                    "tenant_id": tenant_id,
                    "entity_id": entity_id,
                    "name": _norm_text(row.get("display_name")) or "Unknown",
                    "primary_email": email,
                    "domain": domain,
                    "seniority": seniority,
                    "location": _norm_text(row.get("location_norm")),
                    "created_at": _norm_text(row.get("created_at")) or _utc_now_iso(),
                    "updated_at": _norm_text(row.get("updated_at")) or _utc_now_iso(),
                }
            )

            company_name = _norm_text(row.get("company_norm"))
            if company_name:
                company_id = f"cmp_{_slug(company_name)}"
                company = company_by_name.setdefault(
                    company_name.lower(),
                    {
                        "tenant_id": tenant_id,
                        "company_id": company_id,
                        "name": company_name,
                        "domain": domain,
                        "industry": None,
                        "size": None,
                        "location": _norm_text(row.get("location_norm")),
                        "created_at": _norm_text(row.get("created_at")) or _utc_now_iso(),
                        "updated_at": _norm_text(row.get("updated_at")) or _utc_now_iso(),
                    },
                )
                if not company.get("domain") and domain:
                    company["domain"] = domain
                works_at_edges.append(
                    {
                        "tenant_id": tenant_id,
                        "entity_id": entity_id,
                        "company_id": company_id,
                        "title": _norm_text(row.get("title_norm")),
                        "department": None,
                        "seniority": seniority,
                        "start_date": None,
                        "end_date": None,
                        "is_current": True,
                        "updated_at": _norm_text(row.get("updated_at")) or _utc_now_iso(),
                    }
                )

            topics = self._extract_topics(metadata_json)
            for topic in topics:
                topic_name = topic["name"]
                key = topic_name.lower()
                topic_id = f"top_{_slug(topic_name)}"
                topic_by_key.setdefault(
                    key,
                    {
                        "tenant_id": tenant_id,
                        "topic_id": topic_id,
                        "name": topic_name,
                        "created_at": _utc_now_iso(),
                        "updated_at": _utc_now_iso(),
                    },
                )
                has_topic_edges.append(
                    {
                        "tenant_id": tenant_id,
                        "entity_id": entity_id,
                        "topic_id": topic_id,
                        "confidence": 0.7,
                        "source": topic.get("source"),
                    }
                )

        user_nodes: list[dict[str, Any]] = []
        member_of_edges: list[dict[str, Any]] = []
        company_domain_to_id = {
            str(c.get("domain")).lower(): str(c["company_id"])
            for c in company_by_name.values()
            if c.get("domain")
        }
        for row in users:
            uid = str(row.get("user_id") or "")
            if not uid:
                continue
            email = _norm_text(row.get("email"))
            user_nodes.append(
                {
                    "tenant_id": tenant_id,
                    "user_id": uid,
                    "name": _norm_text(row.get("display_name")) or uid,
                    "email": email,
                    "role": _norm_text(row.get("role")) or "member",
                    "created_at": _norm_text(row.get("created_at")) or _utc_now_iso(),
                    "updated_at": _norm_text(row.get("updated_at")) or _utc_now_iso(),
                }
            )
            domain = _email_domain(email)
            if domain and domain.lower() in company_domain_to_id:
                member_of_edges.append(
                    {
                        "tenant_id": tenant_id,
                        "user_id": uid,
                        "company_id": company_domain_to_id[domain.lower()],
                        "role": _norm_text(row.get("role")) or "member",
                        "joined_at": _norm_text(row.get("created_at")) or _utc_now_iso(),
                    }
                )

        knows_edges: list[dict[str, Any]] = []
        for row in relationships:
            frm = _norm_text(row.get("from_entity_id"))
            to = _norm_text(row.get("to_entity_id"))
            if not frm or not to:
                continue
            evidence = row.get("evidence_json") if isinstance(row.get("evidence_json"), list) else []
            channels = sorted({str(item.get("touchpoint_type")) for item in evidence if isinstance(item, dict) and item.get("touchpoint_type")})
            knows_edges.append(
                {
                    "tenant_id": tenant_id,
                    "from_entity_id": frm,
                    "to_entity_id": to,
                    "relationship_type": _norm_text(row.get("relationship_type")) or "direct",
                    "strength": float(row.get("strength") or 0.0),
                    "interaction_count": int(row.get("interaction_count") or 0),
                    "first_interaction_at": _norm_text(row.get("first_interaction_at")),
                    "last_interaction_at": _norm_text(row.get("last_interaction_at")),
                    "channels": channels,
                    "evidence_json": _json_text(evidence),
                    "updated_at": _norm_text(row.get("updated_at")) or _utc_now_iso(),
                }
            )

        interacted_edges: list[dict[str, Any]] = []
        for row in interactions:
            actor = _norm_text(row.get("actor_entity_id"))
            target = _norm_text(row.get("target_entity_id"))
            if not actor or not target:
                continue
            occurred = _norm_text(row.get("occurred_at")) or _utc_now_iso()
            source_event_id = str(row.get("canonical_event_id") or row.get("raw_event_id") or f"evt_{hash((actor, target, occurred))}")
            interacted_edges.append(
                {
                    "tenant_id": tenant_id,
                    "actor_entity_id": actor,
                    "target_entity_id": target,
                    "channel": _norm_text(row.get("source")) or "unknown",
                    "interaction_type": _norm_text(row.get("touchpoint_type")) or "interaction",
                    "occurred_at": occurred,
                    "source_event_id": source_event_id,
                }
            )

        # DEPRECATED: intro_path_edges are superseded by the query-time warm-path
        # engine in app/ingestion/warm_path.py and the /v1/warm-path/ endpoints.
        # Kept for backward compatibility; will be removed in a follow-up PR.
        intro_path_edges: list[dict[str, Any]] = []
        email_to_entity = {
            str(item.get("primary_email")).lower(): str(item.get("entity_id"))
            for item in person_nodes
            if item.get("primary_email")
        }
        user_email = None
        for user in user_nodes:
            if str(user.get("user_id")) == user_id:
                user_email = _norm_text(user.get("email"))
                break
        if user_email and user_email.lower() in email_to_entity:
            user_entity_id = email_to_entity[user_email.lower()]
            adjacency: dict[str, list[dict[str, Any]]] = {}
            for edge in knows_edges:
                adjacency.setdefault(str(edge["from_entity_id"]), []).append(edge)

            best_paths: dict[str, dict[str, Any]] = {}
            for e1 in adjacency.get(user_entity_id, []):
                target = str(e1["to_entity_id"])
                likelihood = float(e1.get("strength") or 0.0)
                best_paths[target] = {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "target_entity_id": target,
                    "via_person_id": target,
                    "path_length": 1,
                    "intro_likelihood": round(likelihood, 4),
                    "updated_at": _utc_now_iso(),
                }

                for e2 in adjacency.get(target, []):
                    second_target = str(e2["to_entity_id"])
                    if second_target == user_entity_id:
                        continue
                    score = likelihood * float(e2.get("strength") or 0.0)
                    current = best_paths.get(second_target)
                    if current is None or float(current.get("intro_likelihood") or 0.0) < score:
                        best_paths[second_target] = {
                            "tenant_id": tenant_id,
                            "user_id": user_id,
                            "target_entity_id": second_target,
                            "via_person_id": target,
                            "path_length": 2,
                            "intro_likelihood": round(score, 4),
                            "updated_at": _utc_now_iso(),
                        }

            intro_path_edges = list(best_paths.values())

        companies = list(company_by_name.values())
        topics = list(topic_by_key.values())

        snapshot_for_hash = {
            "persons": person_nodes,
            "users": user_nodes,
            "companies": companies,
            "topics": topics,
            "knows_edges": knows_edges,
            "interacted_edges": interacted_edges,
            "works_at_edges": works_at_edges,
            "member_of_edges": member_of_edges,
            "has_topic_edges": has_topic_edges,
            "intro_path_edges": intro_path_edges,
        }
        checksum = hashlib.sha256(
            json.dumps(snapshot_for_hash, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        return {
            "enabled": True,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "trace_id": trace_id,
            "batch_stats": {
                "entities": len(entities),
                "relationships": len(relationships),
                "interactions": len(interactions),
                "tenant_users": len(users),
            },
            **snapshot_for_hash,
            "snapshot_checksum": checksum,
        }

    def persist_mirror_state(
        self,
        *,
        run_id: str,
        trace_id: str,
        tenant_id: str,
        user_id: str,
        snapshot: dict[str, Any],
        verification: dict[str, Any],
        node_result: dict[str, Any],
        edge_result: dict[str, Any],
    ) -> dict[str, Any]:
        if not self._enabled:
            return {
                "mirror_store": "disabled",
                "run_row_upserted": False,
                "node_rows_upserted": 0,
                "edge_rows_upserted": 0,
            }

        run_payload = {
            "run_id": run_id,
            "trace_id": trace_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "status": "succeeded" if verification.get("ok", True) else "failed",
            "node_counts_json": node_result.get("nodes_by_label", {}),
            "edge_counts_json": edge_result.get("edges_by_type", {}),
            "verification_json": verification,
            "snapshot_checksum": snapshot.get("snapshot_checksum"),
            "updated_at": _utc_now_iso(),
        }

        run_resp = httpx.post(
            self._url("graph_projection_runs"),
            headers=self._headers(upsert=True),
            params={"on_conflict": "run_id"},
            json=[run_payload],
            timeout=30.0,
        )
        if run_resp.status_code >= 400:
            if self._is_missing_relation(run_resp):
                return {
                    "mirror_store": "supabase:graph_projection_unavailable",
                    "run_row_upserted": False,
                    "node_rows_upserted": 0,
                    "edge_rows_upserted": 0,
                }
            raise RuntimeError(
                f"Supabase graph_projection_runs upsert failed ({run_resp.status_code}): {run_resp.text}"
            )

        # Deduplicate on (tenant_id, node_label, node_key) — the ON CONFLICT key.
        deduped_nodes: dict[tuple, dict] = {}
        for label, rows in (
            ("Person", list(snapshot.get("persons", []))),
            ("User", list(snapshot.get("users", []))),
            ("Company", list(snapshot.get("companies", []))),
            ("Topic", list(snapshot.get("topics", []))),
        ):
            key_name = {
                "Person": "entity_id",
                "User": "user_id",
                "Company": "company_id",
                "Topic": "topic_id",
            }[label]
            for row in rows:
                node_key = str(row.get(key_name) or "")
                if not node_key:
                    continue
                row_hash = hashlib.sha256(
                    json.dumps(row, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()
                deduped_nodes[(tenant_id, label, node_key)] = {
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "node_label": label,
                    "node_key": node_key,
                    "node_hash": row_hash,
                    "projected_at": _utc_now_iso(),
                }

        node_rows = list(deduped_nodes.values())

        # Deduplicate on (tenant_id, edge_type, edge_key) — the ON CONFLICT key.
        edge_configs = [
            ("KNOWS", "knows_edges", lambda r: f"{r.get('from_entity_id')}->{r.get('to_entity_id')}:{r.get('relationship_type') or 'direct'}"),
            ("INTERACTED_WITH", "interacted_edges", lambda r: f"{r.get('actor_entity_id')}->{r.get('target_entity_id')}:{r.get('source_event_id')}"),
            ("WORKS_AT", "works_at_edges", lambda r: f"{r.get('entity_id')}->{r.get('company_id')}"),
            ("MEMBER_OF", "member_of_edges", lambda r: f"{r.get('user_id')}->{r.get('company_id')}"),
            ("HAS_TOPIC", "has_topic_edges", lambda r: f"{r.get('entity_id')}->{r.get('topic_id')}"),
            ("INTRO_PATH", "intro_path_edges", lambda r: f"{r.get('user_id')}->{r.get('target_entity_id')}"),
        ]
        deduped_edges: dict[tuple, dict] = {}
        for edge_type, key, key_fn in edge_configs:
            for row in list(snapshot.get(key, [])):
                edge_key = key_fn(row)
                if not edge_key:
                    continue
                row_hash = hashlib.sha256(
                    json.dumps(row, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest()
                deduped_edges[(tenant_id, edge_type, edge_key)] = {
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "edge_type": edge_type,
                    "edge_key": edge_key,
                    "edge_hash": row_hash,
                    "projected_at": _utc_now_iso(),
                }

        edge_rows = list(deduped_edges.values())

        if node_rows:
            node_resp = httpx.post(
                self._url("graph_projection_nodes"),
                headers=self._headers(upsert=True),
                params={"on_conflict": "tenant_id,node_label,node_key"},
                json=node_rows,
                timeout=30.0,
            )
            if node_resp.status_code >= 400:
                if self._is_missing_relation(node_resp):
                    return {
                        "mirror_store": "supabase:graph_projection_unavailable",
                        "run_row_upserted": True,
                        "node_rows_upserted": 0,
                        "edge_rows_upserted": 0,
                    }
                raise RuntimeError(
                    f"Supabase graph_projection_nodes upsert failed ({node_resp.status_code}): {node_resp.text}"
                )

        if edge_rows:
            edge_resp = httpx.post(
                self._url("graph_projection_edges"),
                headers=self._headers(upsert=True),
                params={"on_conflict": "tenant_id,edge_type,edge_key"},
                json=edge_rows,
                timeout=30.0,
            )
            if edge_resp.status_code >= 400:
                if self._is_missing_relation(edge_resp):
                    return {
                        "mirror_store": "supabase:graph_projection_unavailable",
                        "run_row_upserted": True,
                        "node_rows_upserted": len(node_rows),
                        "edge_rows_upserted": 0,
                    }
                raise RuntimeError(
                    f"Supabase graph_projection_edges upsert failed ({edge_resp.status_code}): {edge_resp.text}"
                )

        return {
            "mirror_store": "supabase:graph_projection",
            "run_row_upserted": True,
            "node_rows_upserted": len(node_rows),
            "edge_rows_upserted": len(edge_rows),
        }


def create_graph_projection_service(settings: Settings) -> GraphProjectionService:
    return GraphProjectionService(settings=settings)
