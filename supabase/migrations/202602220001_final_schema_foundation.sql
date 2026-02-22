-- Final ingestion/search-ready schema foundation for Kue MVP+
-- Additive migration: keeps existing layer-2/layer-3 tables compatible.

create extension if not exists pgcrypto;
create extension if not exists vector;

do $$
begin
  if not exists (select 1 from pg_type where typname = 'source_type') then
    create type public.source_type as enum (
      'google_contacts',
      'gmail',
      'google_calendar',
      'linkedin',
      'twitter',
      'csv_import'
    );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_type where typname = 'pipeline_status') then
    create type public.pipeline_status as enum (
      'queued',
      'running',
      'succeeded',
      'failed',
      'partial'
    );
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_type where typname = 'connection_status') then
    create type public.connection_status as enum (
      'active',
      'revoked',
      'error'
    );
  end if;
end $$;

create table if not exists public.tenants (
  tenant_id text primary key,
  name text not null,
  plan text not null default 'free',
  settings_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.tenant_users (
  tenant_id text not null,
  user_id text not null,
  email text,
  display_name text,
  role text not null default 'member',
  status text not null default 'active',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, user_id),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade
);

create index if not exists idx_tenant_users_email
  on public.tenant_users(email);

create table if not exists public.source_connections (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  user_id text not null,
  source public.source_type not null,
  external_account_id text not null,
  scopes text[] not null default '{}',
  token_json jsonb not null default '{}'::jsonb,
  token_expires_at timestamptz,
  status public.connection_status not null default 'active',
  last_sync_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id, source, external_account_id),
  foreign key (tenant_id, user_id)
    references public.tenant_users(tenant_id, user_id) on delete cascade
);

create index if not exists idx_source_connections_tenant_source
  on public.source_connections(tenant_id, source, status);

create table if not exists public.sync_checkpoints (
  id bigserial primary key,
  tenant_id text not null,
  user_id text not null,
  source public.source_type not null,
  cursor_type text not null,
  cursor_value text,
  cursor_json jsonb not null default '{}'::jsonb,
  last_event_at timestamptz,
  last_successful_sync_at timestamptz,
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id, source, cursor_type),
  foreign key (tenant_id, user_id)
    references public.tenant_users(tenant_id, user_id) on delete cascade
);

create index if not exists idx_sync_checkpoints_tenant_source
  on public.sync_checkpoints(tenant_id, source, updated_at desc);

create table if not exists public.pipeline_runs (
  run_id uuid primary key default gen_random_uuid(),
  trace_id text not null,
  tenant_id text not null,
  user_id text not null,
  source public.source_type,
  trigger_type text not null,
  source_event_id text,
  status public.pipeline_status not null default 'queued',
  requested_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  error_json jsonb,
  metadata_json jsonb not null default '{}'::jsonb,
  unique (tenant_id, trace_id),
  foreign key (tenant_id, user_id)
    references public.tenant_users(tenant_id, user_id) on delete cascade
);

create index if not exists idx_pipeline_runs_tenant_requested
  on public.pipeline_runs(tenant_id, requested_at desc);

create index if not exists idx_pipeline_runs_status
  on public.pipeline_runs(status, requested_at desc);

create table if not exists public.pipeline_stage_runs (
  id bigserial primary key,
  run_id uuid not null,
  stage_key text not null,
  layer_key text not null,
  status public.pipeline_status not null default 'queued',
  attempt int not null default 1,
  started_at timestamptz,
  completed_at timestamptz,
  duration_ms int,
  records_in int,
  records_out int,
  input_ref text,
  output_ref text,
  error_json jsonb,
  llm_prompt_name text,
  llm_model text,
  llm_prompt_tokens int,
  llm_completion_tokens int,
  llm_total_tokens int,
  llm_cost_usd numeric(12,6),
  created_at timestamptz not null default now(),
  unique (run_id, stage_key, layer_key, attempt),
  foreign key (run_id) references public.pipeline_runs(run_id) on delete cascade
);

create index if not exists idx_pipeline_stage_runs_run
  on public.pipeline_stage_runs(run_id, created_at asc);

create index if not exists idx_pipeline_stage_runs_stage_status
  on public.pipeline_stage_runs(stage_key, status, created_at desc);

-- Extend existing layer-2 table with orchestration metadata (kept nullable for compatibility).
alter table public.raw_events
  add column if not exists run_id uuid,
  add column if not exists headers_json jsonb not null default '{}'::jsonb,
  add column if not exists ingest_version text not null default 'v1',
  add column if not exists payload_size_bytes int generated always as (octet_length(payload_json::text)) stored;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_raw_events_pipeline_run'
  ) then
    alter table public.raw_events
      add constraint fk_raw_events_pipeline_run
      foreign key (run_id) references public.pipeline_runs(run_id) on delete set null;
  end if;
end $$;

create index if not exists idx_raw_events_run_id
  on public.raw_events(run_id);

create index if not exists idx_raw_events_trace_id_occurred
  on public.raw_events(trace_id, occurred_at desc);

-- Extend existing layer-3 table with richer parse metadata.
alter table public.canonical_events
  add column if not exists run_id uuid,
  add column if not exists parse_status text not null default 'parsed',
  add column if not exists schema_version text not null default 'v1',
  add column if not exists canonical_hash text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_canonical_events_pipeline_run'
  ) then
    alter table public.canonical_events
      add constraint fk_canonical_events_pipeline_run
      foreign key (run_id) references public.pipeline_runs(run_id) on delete set null;
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'fk_canonical_events_raw_event'
  ) then
    alter table public.canonical_events
      add constraint fk_canonical_events_raw_event
      foreign key (raw_event_id) references public.raw_events(id) on delete cascade;
  end if;
end $$;

create index if not exists idx_canonical_events_run_id
  on public.canonical_events(run_id);

create index if not exists idx_canonical_events_canonical_hash
  on public.canonical_events(canonical_hash);

create table if not exists public.entities (
  entity_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  display_name text not null,
  primary_email text,
  primary_phone text,
  company_norm text,
  title_norm text,
  location_norm text,
  metadata_json jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade
);

create index if not exists idx_entities_tenant_name
  on public.entities(tenant_id, display_name);

create index if not exists idx_entities_tenant_email
  on public.entities(tenant_id, primary_email);

create table if not exists public.entity_identities (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  entity_id uuid not null,
  source public.source_type not null,
  source_identity text not null,
  email text,
  phone text,
  profile_url text,
  confidence numeric(5,4) not null default 1.0,
  is_primary boolean not null default false,
  raw_event_id bigint,
  created_at timestamptz not null default now(),
  unique (tenant_id, source, source_identity),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (entity_id) references public.entities(entity_id) on delete cascade,
  foreign key (raw_event_id) references public.raw_events(id) on delete set null
);

create index if not exists idx_entity_identities_entity
  on public.entity_identities(entity_id, source);

create table if not exists public.relationships (
  id bigserial primary key,
  tenant_id text not null,
  from_entity_id uuid not null,
  to_entity_id uuid not null,
  relationship_type text not null default 'knows',
  strength numeric(5,4) not null default 0.0,
  first_interaction_at timestamptz,
  last_interaction_at timestamptz,
  interaction_count int not null default 0,
  evidence_json jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now(),
  unique (tenant_id, from_entity_id, to_entity_id, relationship_type),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (from_entity_id) references public.entities(entity_id) on delete cascade,
  foreign key (to_entity_id) references public.entities(entity_id) on delete cascade
);

create index if not exists idx_relationships_tenant_from_strength
  on public.relationships(tenant_id, from_entity_id, strength desc);

create index if not exists idx_relationships_tenant_to_strength
  on public.relationships(tenant_id, to_entity_id, strength desc);

create table if not exists public.interaction_facts (
  id bigserial primary key,
  tenant_id text not null,
  source public.source_type not null,
  actor_entity_id uuid,
  target_entity_id uuid,
  touchpoint_type text not null,
  occurred_at timestamptz not null,
  topic text,
  payload_json jsonb not null default '{}'::jsonb,
  raw_event_id bigint,
  canonical_event_id bigint,
  created_at timestamptz not null default now(),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (actor_entity_id) references public.entities(entity_id) on delete set null,
  foreign key (target_entity_id) references public.entities(entity_id) on delete set null,
  foreign key (raw_event_id) references public.raw_events(id) on delete set null,
  foreign key (canonical_event_id) references public.canonical_events(id) on delete set null
);

create index if not exists idx_interaction_facts_tenant_time
  on public.interaction_facts(tenant_id, occurred_at desc);

create index if not exists idx_interaction_facts_entities
  on public.interaction_facts(tenant_id, actor_entity_id, target_entity_id, occurred_at desc);

create table if not exists public.search_documents (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  entity_id uuid not null,
  doc_type text not null,
  content text not null,
  metadata_json jsonb not null default '{}'::jsonb,
  embedding vector(1536),
  content_tsv tsvector generated always as (
    to_tsvector('english', coalesce(content, ''))
  ) stored,
  source_updated_at timestamptz,
  indexed_at timestamptz not null default now(),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (entity_id) references public.entities(entity_id) on delete cascade
);

create index if not exists idx_search_documents_tenant_entity
  on public.search_documents(tenant_id, entity_id, indexed_at desc);

create index if not exists idx_search_documents_tsv
  on public.search_documents using gin (content_tsv);

create index if not exists idx_search_documents_embedding_ivfflat
  on public.search_documents using ivfflat (embedding vector_cosine_ops)
  with (lists = 100);

create table if not exists public.query_intents (
  id bigserial primary key,
  tenant_id text not null,
  user_id text not null,
  trace_id text not null,
  raw_query text not null,
  rewritten_query text,
  intent_json jsonb not null default '{}'::jsonb,
  filters_json jsonb not null default '{}'::jsonb,
  ranking_explain_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, user_id)
    references public.tenant_users(tenant_id, user_id) on delete cascade
);

create index if not exists idx_query_intents_tenant_created
  on public.query_intents(tenant_id, created_at desc);

create index if not exists idx_query_intents_trace_id
  on public.query_intents(trace_id);

create table if not exists public.ai_call_logs (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  run_id uuid,
  stage_run_id bigint,
  provider text not null,
  model text not null,
  prompt_name text,
  prompt_text text,
  response_text text,
  prompt_tokens int,
  completion_tokens int,
  total_tokens int,
  latency_ms int,
  cost_usd numeric(12,6),
  created_at timestamptz not null default now(),
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade,
  foreign key (run_id) references public.pipeline_runs(run_id) on delete set null,
  foreign key (stage_run_id) references public.pipeline_stage_runs(id) on delete set null
);

create index if not exists idx_ai_call_logs_tenant_time
  on public.ai_call_logs(tenant_id, created_at desc);

create index if not exists idx_ai_call_logs_run
  on public.ai_call_logs(run_id, stage_run_id);

create or replace view public.v_pipeline_observability as
select
  pr.run_id,
  pr.trace_id,
  pr.tenant_id,
  pr.user_id,
  pr.source,
  pr.trigger_type,
  pr.status as run_status,
  pr.requested_at,
  pr.started_at,
  pr.completed_at,
  psr.stage_key,
  psr.layer_key,
  psr.status as stage_status,
  psr.attempt,
  psr.records_in,
  psr.records_out,
  psr.duration_ms,
  psr.llm_model,
  psr.llm_prompt_name,
  psr.llm_total_tokens,
  psr.llm_cost_usd,
  psr.error_json
from public.pipeline_runs pr
left join public.pipeline_stage_runs psr
  on psr.run_id = pr.run_id;
