-- Layer 3: Parsed canonical events

create table if not exists public.canonical_events (
  id bigserial primary key,
  raw_event_id bigint not null,
  tenant_id text not null,
  user_id text not null,
  trace_id text not null,
  source text not null,
  source_event_id text not null,
  occurred_at timestamptz not null,
  event_type text not null,
  normalized_json jsonb not null,
  parse_warnings_json jsonb not null default '[]'::jsonb,
  parser_version text not null,
  parsed_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'uq_canonical_events_dedup'
  ) then
    alter table public.canonical_events
      add constraint uq_canonical_events_dedup
      unique (raw_event_id, event_type, parser_version);
  end if;
end $$;

create index if not exists idx_canonical_events_trace_id
  on public.canonical_events(trace_id);

create index if not exists idx_canonical_events_tenant_event_type
  on public.canonical_events(tenant_id, event_type, occurred_at desc);
