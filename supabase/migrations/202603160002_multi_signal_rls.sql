-- ── RLS for multi-signal entity resolution tables ──────────────────────────
-- merge_candidates and entity_merge_log were created in 202603160001 without
-- row-level security. This migration adds the missing policies.

alter table public.merge_candidates enable row level security;
alter table public.entity_merge_log enable row level security;

-- Service role full access (backend pipeline workers).
create policy merge_candidates_service_role_all
  on public.merge_candidates for all to service_role
  using (true) with check (true);

create policy entity_merge_log_service_role_all
  on public.entity_merge_log for all to service_role
  using (true) with check (true);

-- Authenticated tenant-scoped read access.
create policy merge_candidates_authenticated_select
  on public.merge_candidates for select to authenticated
  using (tenant_matches(tenant_id));

create policy entity_merge_log_authenticated_select
  on public.entity_merge_log for select to authenticated
  using (tenant_matches(tenant_id));

notify pgrst, 'reload schema';
