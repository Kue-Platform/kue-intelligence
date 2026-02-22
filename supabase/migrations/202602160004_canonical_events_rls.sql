-- Layer 3 security: RLS for canonical_events

alter table public.canonical_events enable row level security;

drop policy if exists canonical_events_service_role_all on public.canonical_events;
drop policy if exists canonical_events_authenticated_none on public.canonical_events;
drop policy if exists canonical_events_anon_none on public.canonical_events;

create policy canonical_events_service_role_all
  on public.canonical_events
  for all
  to service_role
  using (true)
  with check (true);

create policy canonical_events_authenticated_none
  on public.canonical_events
  for all
  to authenticated
  using (false)
  with check (false);

create policy canonical_events_anon_none
  on public.canonical_events
  for all
  to anon
  using (false)
  with check (false);
