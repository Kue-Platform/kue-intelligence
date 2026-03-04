-- ── Storage Optimizations ─────────────────────────────────────────────────────
-- 1. Unique constraint on entities(tenant_id, primary_email)
--    Prevents duplicate entity rows from concurrent pipeline runs.
--    Also enables native PostgREST on_conflict upsert on the entities table.
create unique index if not exists idx_entities_tenant_email_unique
  on public.entities(tenant_id, primary_email)
  where primary_email is not null;

-- ── 2. Deduplication index on interaction_facts ────────────────────────────────
--    Prevents duplicate rows from pipeline retries and re-runs.
--    Partial index: only applies when both entity IDs are resolved.
create unique index if not exists idx_interaction_facts_unique
  on public.interaction_facts(tenant_id, actor_entity_id, target_entity_id, touchpoint_type, occurred_at)
  where actor_entity_id is not null and target_entity_id is not null;

-- ── 3. Replace ivfflat vector index with hnsw ──────────────────────────────────
--    ivfflat with lists=100 is only effective at 300k+ rows.
--    hnsw has no minimum row count requirement and gives better recall at all
--    dataset sizes with no tuning needed.
drop index if exists public.idx_search_documents_embedding_ivfflat;

create index if not exists idx_search_documents_embedding_hnsw
  on public.search_documents using hnsw (embedding vector_cosine_ops)
  with (m = 16, ef_construction = 64);
