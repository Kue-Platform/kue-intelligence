from __future__ import annotations

from app.ingestion.entity_resolution import (
    EntityCandidate,
    resolve_entities_multi_signal,
)


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


def test_exact_email_match_merges():
    a = _make_candidate(raw_event_id=1, source_event_id="s1", primary_email="alice@example.com", display_name="Alice")
    b = _make_candidate(raw_event_id=2, source_event_id="s2", primary_email="alice@example.com", display_name="Alice A")
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 2
    assert any(s.signal_type == "email" for s in result.groups[0].signals)
    assert result.groups[0].min_confidence == 0.99


def test_name_company_match():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Jane Doe",
        name_norm="jane doe", company_norm="Acme Inc",
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Jane Doe",
        name_norm="jane doe", company_norm="Acme Inc",
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert result.groups[0].min_confidence == 0.90
    assert any(s.signal_type == "name_company" for s in result.groups[0].signals)


def test_name_domain_match():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Bob Smith",
        name_norm="bob smith", email_domain="acme.com", primary_email="bob@acme.com",
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Bob Smith",
        name_norm="bob smith", email_domain="acme.com", primary_email="bob.smith@acme.com",
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert any(s.signal_type == "name_domain" for s in result.groups[0].signals)


def test_phone_match():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Charlie",
        phones=["(555) 123-4567"],
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Charlie B",
        phones=["5551234567"],
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert result.groups[0].min_confidence == 0.85
    assert any(s.signal_type == "phone" for s in result.groups[0].signals)


def test_linkedin_url_match():
    url = "https://linkedin.com/in/alice"
    a = _make_candidate(raw_event_id=1, source_event_id="s1", linkedin_url=url, display_name="Alice")
    b = _make_candidate(raw_event_id=2, source_event_id="s2", linkedin_url=url, display_name="Alice X")
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert any(s.signal_type == "linkedin_url" for s in result.groups[0].signals)
    assert result.groups[0].min_confidence == 0.99


def test_no_false_merge_different_company():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="John Smith",
        name_norm="john smith", company_norm="Google",
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="John Smith",
        name_norm="john smith", company_norm="Meta",
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 2
    for g in result.groups:
        assert len(g.members) == 1


def test_transitive_merge():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Eve",
        primary_email="eve@example.com",
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Eve B",
        primary_email="eve@example.com", phones=["5559876543"],
    )
    c = _make_candidate(
        raw_event_id=3, source_event_id="s3", display_name="Eve C",
        phones=["555-987-6543"],
    )
    result = resolve_entities_multi_signal([a, b, c])
    assert len(result.groups) == 1
    assert len(result.groups[0].members) == 3


def test_secondary_email_bridges():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Frank",
        primary_email="frank@work.com", secondary_emails=["frank.personal@gmail.com"],
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Frank P",
        primary_email="frank.personal@gmail.com",
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert any(s.signal_type == "email" for s in result.groups[0].signals)


def test_below_threshold_goes_to_merge_candidates():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Grace",
        phones=["5551112222"],
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Grace H",
        phones=["(555) 111-2222"],
    )
    result = resolve_entities_multi_signal([a, b], auto_merge_threshold=0.95)
    assert len(result.groups) == 0
    assert result.merge_candidate_count == 1
    assert len(result.merge_candidates) == 1
    assert result.merge_candidates[0]["min_confidence"] == 0.85


def test_backward_compat_email_only():
    a = _make_candidate(raw_event_id=1, source_event_id="s1", primary_email="old@test.com", display_name="Old")
    b = _make_candidate(raw_event_id=2, source_event_id="s2", primary_email="old@test.com", display_name="Old User")
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert result.resolved_count == 2


def test_canonical_selection_prefers_email():
    a = _make_candidate(
        raw_event_id=1, source_event_id="s1", display_name="Hank",
        name_norm="hank", company_norm="Acme",
    )
    b = _make_candidate(
        raw_event_id=2, source_event_id="s2", display_name="Hank",
        name_norm="hank", company_norm="Acme", primary_email="hank@acme.com",
        email_domain="acme.com",
    )
    result = resolve_entities_multi_signal([a, b])
    assert len(result.groups) == 1
    assert result.groups[0].canonical.primary_email == "hank@acme.com"


def test_empty_candidates():
    result = resolve_entities_multi_signal([])
    assert len(result.groups) == 0
    assert result.resolved_count == 0
    assert result.merge_candidate_count == 0
    assert result.merge_candidates == []
