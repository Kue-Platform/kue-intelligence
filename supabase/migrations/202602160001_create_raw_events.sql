-- Layer 2: Immutable raw event capture
-- Creates a durable raw_events table used by ingestion callbacks.

create table if not exists public.raw_events (
  id bigserial primary key,
  tenant_id text not null,
  user_id text not null,
  source text not null,
  source_event_id text not null,
  occurred_at timestamptz not null,
  trace_id text not null,
  payload_json jsonb not null,
  captured_at timestamptz not null default now()
);

-- Prevent duplicate writes for the same source event per tenant/user/source.
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'uq_raw_events_source_dedup'
  ) then
    alter table public.raw_events
      add constraint uq_raw_events_source_dedup
      unique (tenant_id, user_id, source, source_event_id);
  end if;
end $$;

create index if not exists idx_raw_events_trace_id
  on public.raw_events(trace_id);

create index if not exists idx_raw_events_tenant_source_occurred_at
  on public.raw_events(tenant_id, source, occurred_at desc);

create index if not exists idx_raw_events_captured_at
  on public.raw_events(captured_at desc);
