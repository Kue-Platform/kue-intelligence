-- Graph projection mirror tables for Stage 13 observability/idempotency.

create table if not exists public.graph_projection_runs (
  id bigserial primary key,
  run_id uuid not null,
  trace_id text not null,
  tenant_id text not null,
  user_id text not null,
  status text not null,
  node_counts_json jsonb not null default '{}'::jsonb,
  edge_counts_json jsonb not null default '{}'::jsonb,
  verification_json jsonb not null default '{}'::jsonb,
  snapshot_checksum text,
  error_json jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id),
  foreign key (run_id) references public.pipeline_runs(run_id) on delete cascade,
  foreign key (tenant_id, user_id) references public.tenant_users(tenant_id, user_id) on delete cascade
);

create index if not exists idx_graph_projection_runs_trace
  on public.graph_projection_runs(trace_id);

create index if not exists idx_graph_projection_runs_tenant_updated
  on public.graph_projection_runs(tenant_id, updated_at desc);

create table if not exists public.graph_projection_nodes (
  id bigserial primary key,
  run_id uuid,
  tenant_id text not null,
  node_label text not null,
  node_key text not null,
  node_hash text not null,
  projected_at timestamptz not null default now(),
  unique (tenant_id, node_label, node_key),
  foreign key (run_id) references public.pipeline_runs(run_id) on delete set null,
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade
);

create index if not exists idx_graph_projection_nodes_tenant_label
  on public.graph_projection_nodes(tenant_id, node_label);

create table if not exists public.graph_projection_edges (
  id bigserial primary key,
  run_id uuid,
  tenant_id text not null,
  edge_type text not null,
  edge_key text not null,
  edge_hash text not null,
  projected_at timestamptz not null default now(),
  unique (tenant_id, edge_type, edge_key),
  foreign key (run_id) references public.pipeline_runs(run_id) on delete set null,
  foreign key (tenant_id) references public.tenants(tenant_id) on delete cascade
);

create index if not exists idx_graph_projection_edges_tenant_type
  on public.graph_projection_edges(tenant_id, edge_type);

alter table public.graph_projection_runs enable row level security;
alter table public.graph_projection_nodes enable row level security;
alter table public.graph_projection_edges enable row level security;

drop policy if exists graph_projection_runs_service_role_all on public.graph_projection_runs;
drop policy if exists graph_projection_runs_authenticated_select on public.graph_projection_runs;
drop policy if exists graph_projection_nodes_service_role_all on public.graph_projection_nodes;
drop policy if exists graph_projection_nodes_authenticated_select on public.graph_projection_nodes;
drop policy if exists graph_projection_edges_service_role_all on public.graph_projection_edges;
drop policy if exists graph_projection_edges_authenticated_select on public.graph_projection_edges;

create policy graph_projection_runs_service_role_all
  on public.graph_projection_runs for all to service_role
  using (true) with check (true);

create policy graph_projection_nodes_service_role_all
  on public.graph_projection_nodes for all to service_role
  using (true) with check (true);

create policy graph_projection_edges_service_role_all
  on public.graph_projection_edges for all to service_role
  using (true) with check (true);

create policy graph_projection_runs_authenticated_select
  on public.graph_projection_runs for select to authenticated
  using (tenant_matches(tenant_id) and user_matches(user_id));

create policy graph_projection_nodes_authenticated_select
  on public.graph_projection_nodes for select to authenticated
  using (tenant_matches(tenant_id));

create policy graph_projection_edges_authenticated_select
  on public.graph_projection_edges for select to authenticated
  using (tenant_matches(tenant_id));
