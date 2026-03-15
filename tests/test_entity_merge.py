from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from app.ingestion.entity_resolution import EntityCandidate
from app.ingestion.entity_store import SqliteEntityStore


def _make_candidate(**overrides) -> EntityCandidate:
    defaults = {
        "raw_event_id": 1,
        "tenant_id": "t1",
        "user_id": "u1",
        "source": "google_contacts",
        "source_event_id": "se1",
        "display_name": "Unknown",
    }
    defaults.update(overrides)
    return EntityCandidate(**defaults)


def _make_store(tmp_path: Path) -> SqliteEntityStore:
    db = str(tmp_path / "test.db")
    store = SqliteEntityStore(db_path=db)
    # Create downstream tables that execute_merge touches
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS search_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                content TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS relationships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                from_entity_id TEXT NOT NULL,
                to_entity_id TEXT NOT NULL,
                relationship_type TEXT NOT NULL DEFAULT 'knows',
                strength REAL NOT NULL DEFAULT 0.0,
                UNIQUE(tenant_id, from_entity_id, to_entity_id, relationship_type)
            )
            """
        )
        conn.commit()
    return store


class TestCrossRunMatching:
    def test_linkedin_url_cross_run_match(self, tmp_path):
        store = _make_store(tmp_path)
        url = "https://linkedin.com/in/alice"

        # First import: Google Contacts with email + linkedin
        c1 = _make_candidate(
            source_event_id="gc1",
            display_name="Alice",
            primary_email="alice@example.com",
            linkedin_url=url,
        )
        r1 = store.upsert_entities([c1])
        assert r1.created_entities == 1

        # Second import: LinkedIn with same URL, no email
        c2 = _make_candidate(
            raw_event_id=2,
            source="linkedin",
            source_event_id="li1",
            display_name="Alice Smith",
            linkedin_url=url,
        )
        r2 = store.upsert_entities([c2])
        assert r2.created_entities == 0
        assert r2.updated_entities == 1

    def test_phone_cross_run_match(self, tmp_path):
        store = _make_store(tmp_path)

        c1 = _make_candidate(
            source_event_id="gc1",
            display_name="Bob",
            primary_email="bob@example.com",
            phones=["(555) 123-4567"],
        )
        r1 = store.upsert_entities([c1])
        assert r1.created_entities == 1

        # Second import with same phone, different format
        c2 = _make_candidate(
            raw_event_id=2,
            source="linkedin",
            source_event_id="li2",
            display_name="Bob Jones",
            phones=["+15551234567"],
        )
        r2 = store.upsert_entities([c2])
        assert r2.created_entities == 0
        assert r2.updated_entities == 1

    def test_name_company_cross_run_match(self, tmp_path):
        store = _make_store(tmp_path)

        c1 = _make_candidate(
            source_event_id="gc1",
            display_name="Jane Doe",
            primary_email="jane@stripe.com",
            company_norm="Stripe",
            name_norm="jane doe",
        )
        store.upsert_entities([c1])

        c2 = _make_candidate(
            raw_event_id=2,
            source="linkedin",
            source_event_id="li3",
            display_name="Jane Doe",
            company_norm="Stripe",
            name_norm="jane doe",
        )
        r2 = store.upsert_entities([c2])
        assert r2.created_entities == 0
        assert r2.updated_entities == 1

    def test_no_false_cross_run_match(self, tmp_path):
        store = _make_store(tmp_path)

        c1 = _make_candidate(
            source_event_id="gc1",
            display_name="John Smith",
            primary_email="john@google.com",
            company_norm="Google",
            name_norm="john smith",
        )
        store.upsert_entities([c1])

        # Different company — should NOT match
        c2 = _make_candidate(
            raw_event_id=2,
            source="linkedin",
            source_event_id="li4",
            display_name="John Smith",
            company_norm="Meta",
            name_norm="john smith",
        )
        r2 = store.upsert_entities([c2])
        assert r2.created_entities == 1
        assert r2.updated_entities == 0


class TestExecuteMerge:
    def test_merge_moves_identities_and_deletes_merged(self, tmp_path):
        store = _make_store(tmp_path)

        c1 = _make_candidate(source_event_id="s1", display_name="Alice", primary_email="alice@a.com")
        c2 = _make_candidate(
            raw_event_id=2, source="linkedin", source_event_id="s2", display_name="Alice L"
        )
        store.upsert_entities([c1, c2])

        # Get the entity_ids
        with sqlite3.connect(store._db_path) as conn:
            rows = conn.execute("SELECT entity_id FROM entities ORDER BY created_at").fetchall()
        assert len(rows) == 2
        surviving_id = rows[0][0]
        merged_id = rows[1][0]

        result = store.execute_merge(
            tenant_id="t1",
            surviving_entity_id=surviving_id,
            merged_entity_id=merged_id,
        )
        assert result.identities_moved == 1

        # Merged entity should be gone
        with sqlite3.connect(store._db_path) as conn:
            remaining = conn.execute("SELECT entity_id FROM entities").fetchall()
        assert len(remaining) == 1
        assert remaining[0][0] == surviving_id

    def test_merge_repoints_relationships(self, tmp_path):
        store = _make_store(tmp_path)

        c1 = _make_candidate(source_event_id="s1", display_name="A", primary_email="a@test.com")
        c2 = _make_candidate(
            raw_event_id=2, source="linkedin", source_event_id="s2", display_name="A2"
        )
        store.upsert_entities([c1, c2])

        with sqlite3.connect(store._db_path) as conn:
            rows = conn.execute("SELECT entity_id FROM entities ORDER BY created_at").fetchall()
            surviving_id, merged_id = rows[0][0], rows[1][0]

            # Insert a relationship pointing to the merged entity
            conn.execute(
                "INSERT INTO relationships (tenant_id, from_entity_id, to_entity_id, relationship_type, strength) VALUES (?, ?, ?, ?, ?)",
                ("t1", merged_id, "other_entity", "knows", 0.5),
            )
            conn.commit()

        result = store.execute_merge(
            tenant_id="t1",
            surviving_entity_id=surviving_id,
            merged_entity_id=merged_id,
        )
        assert result.relationships_repointed == 1

        with sqlite3.connect(store._db_path) as conn:
            rel = conn.execute("SELECT from_entity_id FROM relationships").fetchone()
            assert rel[0] == surviving_id

    def test_merge_coalesces_null_fields(self, tmp_path):
        store = _make_store(tmp_path)

        # Surviving entity has no linkedin_url
        c1 = _make_candidate(source_event_id="s1", display_name="Eve", primary_email="eve@test.com")
        # Merged entity has linkedin_url
        c2 = _make_candidate(
            raw_event_id=2,
            source="linkedin",
            source_event_id="s2",
            display_name="Eve L",
            linkedin_url="https://linkedin.com/in/eve",
        )
        store.upsert_entities([c1, c2])

        with sqlite3.connect(store._db_path) as conn:
            rows = conn.execute("SELECT entity_id FROM entities ORDER BY created_at").fetchall()
            surviving_id, merged_id = rows[0][0], rows[1][0]

        store.execute_merge(
            tenant_id="t1",
            surviving_entity_id=surviving_id,
            merged_entity_id=merged_id,
        )

        with sqlite3.connect(store._db_path) as conn:
            row = conn.execute(
                "SELECT linkedin_url FROM entities WHERE entity_id = ?", (surviving_id,)
            ).fetchone()
            assert row[0] == "https://linkedin.com/in/eve"
