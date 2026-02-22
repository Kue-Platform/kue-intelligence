-- Final schema RLS patch
-- Enforces tenant boundaries while allowing service-role orchestration.

create or replace function public.current_tenant_id()
returns text
language sql
stable
as $$
  select coalesce(
    nullif(auth.jwt() ->> 'tenant_id', ''),
    nullif(auth.jwt() -> 'app_metadata' ->> 'tenant_id', '')
  );
$$;

create or replace function public.current_user_id()
returns text
language sql
stable
as $$
  select coalesce(
    nullif(auth.jwt() ->> 'sub', ''),
    nullif(auth.jwt() ->> 'user_id', '')
  );
$$;

create or replace function public.tenant_matches(row_tenant_id text)
returns boolean
language sql
stable
as $$
  select current_tenant_id() is not null and current_tenant_id() = row_tenant_id;
$$;

create or replace function public.user_matches(row_user_id text)
returns boolean
language sql
stable
as $$
  select current_user_id() is not null and current_user_id() = row_user_id;
$$;

-- Enable RLS on all multi-tenant tables.
alter table public.tenants enable row level security;
alter table public.tenant_users enable row level security;
alter table public.source_connections enable row level security;
alter table public.sync_checkpoints enable row level security;
alter table public.pipeline_runs enable row level security;
alter table public.pipeline_stage_runs enable row level security;
alter table public.raw_events enable row level security;
alter table public.canonical_events enable row level security;
alter table public.entities enable row level security;
alter table public.entity_identities enable row level security;
alter table public.relationships enable row level security;
alter table public.interaction_facts enable row level security;
alter table public.search_documents enable row level security;
alter table public.query_intents enable row level security;
alter table public.ai_call_logs enable row level security;

-- Remove old policies if present.
drop policy if exists tenants_service_role_all on public.tenants;
drop policy if exists tenants_authenticated_select on public.tenants;
drop policy if exists tenant_users_service_role_all on public.tenant_users;
drop policy if exists tenant_users_authenticated_select on public.tenant_users;
drop policy if exists source_connections_service_role_all on public.source_connections;
drop policy if exists source_connections_authenticated_select on public.source_connections;
drop policy if exists sync_checkpoints_service_role_all on public.sync_checkpoints;
drop policy if exists sync_checkpoints_authenticated_select on public.sync_checkpoints;
drop policy if exists pipeline_runs_service_role_all on public.pipeline_runs;
drop policy if exists pipeline_runs_authenticated_select on public.pipeline_runs;
drop policy if exists pipeline_stage_runs_service_role_all on public.pipeline_stage_runs;
drop policy if exists pipeline_stage_runs_authenticated_select on public.pipeline_stage_runs;
drop policy if exists raw_events_service_role_all on public.raw_events;
drop policy if exists raw_events_authenticated_none on public.raw_events;
drop policy if exists raw_events_anon_none on public.raw_events;
drop policy if exists canonical_events_service_role_all on public.canonical_events;
drop policy if exists canonical_events_authenticated_none on public.canonical_events;
drop policy if exists canonical_events_anon_none on public.canonical_events;
drop policy if exists canonical_events_authenticated_select on public.canonical_events;
drop policy if exists entities_service_role_all on public.entities;
drop policy if exists entities_authenticated_select on public.entities;
drop policy if exists entity_identities_service_role_all on public.entity_identities;
drop policy if exists entity_identities_authenticated_select on public.entity_identities;
drop policy if exists relationships_service_role_all on public.relationships;
drop policy if exists relationships_authenticated_select on public.relationships;
drop policy if exists interaction_facts_service_role_all on public.interaction_facts;
drop policy if exists interaction_facts_authenticated_select on public.interaction_facts;
drop policy if exists search_documents_service_role_all on public.search_documents;
drop policy if exists search_documents_authenticated_select on public.search_documents;
drop policy if exists query_intents_service_role_all on public.query_intents;
drop policy if exists query_intents_authenticated_select on public.query_intents;
drop policy if exists ai_call_logs_service_role_all on public.ai_call_logs;
drop policy if exists ai_call_logs_authenticated_none on public.ai_call_logs;

-- Service role full access for backend workers/functions.
create policy tenants_service_role_all
  on public.tenants for all to service_role
  using (true) with check (true);

create policy tenant_users_service_role_all
  on public.tenant_users for all to service_role
  using (true) with check (true);

create policy source_connections_service_role_all
  on public.source_connections for all to service_role
  using (true) with check (true);

create policy sync_checkpoints_service_role_all
  on public.sync_checkpoints for all to service_role
  using (true) with check (true);

create policy pipeline_runs_service_role_all
  on public.pipeline_runs for all to service_role
  using (true) with check (true);

create policy pipeline_stage_runs_service_role_all
  on public.pipeline_stage_runs for all to service_role
  using (true) with check (true);

create policy raw_events_service_role_all
  on public.raw_events for all to service_role
  using (true) with check (true);

create policy canonical_events_service_role_all
  on public.canonical_events for all to service_role
  using (true) with check (true);

create policy entities_service_role_all
  on public.entities for all to service_role
  using (true) with check (true);

create policy entity_identities_service_role_all
  on public.entity_identities for all to service_role
  using (true) with check (true);

create policy relationships_service_role_all
  on public.relationships for all to service_role
  using (true) with check (true);

create policy interaction_facts_service_role_all
  on public.interaction_facts for all to service_role
  using (true) with check (true);

create policy search_documents_service_role_all
  on public.search_documents for all to service_role
  using (true) with check (true);

create policy query_intents_service_role_all
  on public.query_intents for all to service_role
  using (true) with check (true);

create policy ai_call_logs_service_role_all
  on public.ai_call_logs for all to service_role
  using (true) with check (true);

-- Authenticated tenant-scoped read access (app-facing observability/search).
create policy tenants_authenticated_select
  on public.tenants for select to authenticated
  using (tenant_matches(tenant_id));

create policy tenant_users_authenticated_select
  on public.tenant_users for select to authenticated
  using (tenant_matches(tenant_id));

create policy source_connections_authenticated_select
  on public.source_connections for select to authenticated
  using (tenant_matches(tenant_id) and user_matches(user_id));

create policy sync_checkpoints_authenticated_select
  on public.sync_checkpoints for select to authenticated
  using (tenant_matches(tenant_id) and user_matches(user_id));

create policy pipeline_runs_authenticated_select
  on public.pipeline_runs for select to authenticated
  using (tenant_matches(tenant_id) and user_matches(user_id));

create policy pipeline_stage_runs_authenticated_select
  on public.pipeline_stage_runs for select to authenticated
  using (
    exists (
      select 1
      from public.pipeline_runs pr
      where pr.run_id = pipeline_stage_runs.run_id
        and tenant_matches(pr.tenant_id)
        and user_matches(pr.user_id)
    )
  );

create policy canonical_events_authenticated_select
  on public.canonical_events for select to authenticated
  using (tenant_matches(tenant_id));

create policy entities_authenticated_select
  on public.entities for select to authenticated
  using (tenant_matches(tenant_id));

create policy entity_identities_authenticated_select
  on public.entity_identities for select to authenticated
  using (tenant_matches(tenant_id));

create policy relationships_authenticated_select
  on public.relationships for select to authenticated
  using (tenant_matches(tenant_id));

create policy interaction_facts_authenticated_select
  on public.interaction_facts for select to authenticated
  using (tenant_matches(tenant_id));

create policy search_documents_authenticated_select
  on public.search_documents for select to authenticated
  using (tenant_matches(tenant_id));

create policy query_intents_authenticated_select
  on public.query_intents for select to authenticated
  using (tenant_matches(tenant_id) and user_matches(user_id));

-- Keep sensitive tables backend-only for now.
create policy raw_events_authenticated_none
  on public.raw_events for all to authenticated
  using (false) with check (false);

create policy raw_events_anon_none
  on public.raw_events for all to anon
  using (false) with check (false);

create policy canonical_events_anon_none
  on public.canonical_events for all to anon
  using (false) with check (false);

create policy ai_call_logs_authenticated_none
  on public.ai_call_logs for all to authenticated
  using (false) with check (false);
