-- ── Multi-Signal Entity Resolution ──────────────────────────────────────────
-- Extends entities with multi-signal matching fields and adds merge tracking
-- tables for the Union-Find based cross-source entity resolution system.

-- ── 1. Extend entities table ────────────────────────────────────────────────
-- phones: array of phone numbers (vs single primary_phone already present)
-- secondary_emails: additional email addresses beyond primary_email
-- linkedin_url: LinkedIn profile URL for cross-source matching
alter table public.entities
  add column if not exists phones text[] not null default '{}',
  add column if not exists secondary_emails text[] not null default '{}',
  add column if not exists linkedin_url text;

-- Index for LinkedIn URL matching during cross-run entity resolution
create unique index if not exists idx_entities_tenant_linkedin_url
  on public.entities(tenant_id, linkedin_url)
  where linkedin_url is not null;

-- Index for name+company matching during cross-run resolution
create index if not exists idx_entities_tenant_name_company
  on public.entities(tenant_id, lower(display_name), company_norm)
  where company_norm is not null;

-- GIN index on phones array for cross-run phone matching
create index if not exists idx_entities_phones_gin
  on public.entities using gin (phones)
  where phones != '{}';

-- ── 2. Merge candidates table ───────────────────────────────────────────────
-- Stores ambiguous entity matches below the auto-merge confidence threshold
-- for manual review or future resolution.
create table if not exists public.merge_candidates (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  source_entity_id uuid not null,
  target_entity_id uuid not null,
  match_signals_json jsonb not null default '{}'::jsonb,
  confidence numeric(5,4) not null,
  status text not null default 'pending',  -- pending | accepted | rejected
  resolved_at timestamptz,
  created_at timestamptz not null default now(),
  unique (tenant_id, source_entity_id, target_entity_id),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (source_entity_id) references public.entities(entity_id) on delete cascade,
  foreign key (target_entity_id) references public.entities(entity_id) on delete cascade
);

create index if not exists idx_merge_candidates_tenant_status
  on public.merge_candidates(tenant_id, status, confidence desc);

-- ── 3. Entity merge log ─────────────────────────────────────────────────────
-- Audit trail for executed merges, with rollback snapshot of the merged entity.
create table if not exists public.entity_merge_log (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  surviving_entity_id uuid not null,
  merged_entity_id uuid not null,
  match_signal text not null,
  confidence numeric(5,4) not null,
  merged_at timestamptz not null default now(),
  rollback_payload_json jsonb,
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade
  -- No FK on entity IDs: merged entity is deleted after merge
);

create index if not exists idx_entity_merge_log_tenant
  on public.entity_merge_log(tenant_id, merged_at desc);

create index if not exists idx_entity_merge_log_surviving
  on public.entity_merge_log(surviving_entity_id, merged_at desc);

-- ── 4. Unique constraint on interaction_facts for email-based upsert ────────
-- The pipeline persists interactions by actor_email/target_email before entity
-- resolution links them to entity IDs. This constraint enables idempotent
-- upsert via PostgREST on_conflict.
-- Only add if actor_email/target_email columns exist (added by relationship store).
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public'
      and table_name = 'interaction_facts'
      and column_name = 'actor_email'
  ) then
    -- Add columns if not present
    begin
      alter table public.interaction_facts
        add column if not exists actor_email text,
        add column if not exists target_email text;
    exception when others then null;
    end;

    -- Create unique index for email-based upsert
    create unique index if not exists idx_interaction_facts_email_dedup
      on public.interaction_facts(tenant_id, source, actor_email, target_email, touchpoint_type, occurred_at)
      where actor_email is not null and target_email is not null;
  end if;
end $$;
