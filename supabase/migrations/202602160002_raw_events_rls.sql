-- Layer 2 security: RLS for raw_events
-- Goal: backend-only access via service role.

alter table public.raw_events enable row level security;

-- Cleanly recreate policies for repeatable deployments.
drop policy if exists raw_events_service_role_all on public.raw_events;
drop policy if exists raw_events_authenticated_none on public.raw_events;
drop policy if exists raw_events_anon_none on public.raw_events;

-- Service role can read/write all rows.
create policy raw_events_service_role_all
  on public.raw_events
  for all
  to service_role
  using (true)
  with check (true);

-- Explicit deny policies for client-facing roles.
create policy raw_events_authenticated_none
  on public.raw_events
  for all
  to authenticated
  using (false)
  with check (false);

create policy raw_events_anon_none
  on public.raw_events
  for all
  to anon
  using (false)
  with check (false);
