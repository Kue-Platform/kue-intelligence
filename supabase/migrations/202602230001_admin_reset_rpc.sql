-- Admin reset RPC for non-production cleanup/testing.
-- Truncates Kue ingestion/search tables in FK-safe way.

create or replace function public.kue_admin_reset_data()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  tables text[] := array[
    'ai_call_logs',
    'query_intents',
    'search_documents',
    'interaction_facts',
    'relationships',
    'entity_identities',
    'entities',
    'canonical_events',
    'raw_events',
    'pipeline_stage_runs',
    'pipeline_runs',
    'sync_checkpoints',
    'source_connections',
    'tenant_users',
    'tenants'
  ];
  t text;
  cleared int := 0;
begin
  foreach t in array tables loop
    if to_regclass(format('public.%I', t)) is not null then
      execute format('truncate table public.%I restart identity cascade', t);
      cleared := cleared + 1;
    end if;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'tables_cleared', cleared,
    'timestamp', now()
  );
end;
$$;

revoke all on function public.kue_admin_reset_data() from public;
revoke all on function public.kue_admin_reset_data() from anon;
revoke all on function public.kue_admin_reset_data() from authenticated;
grant execute on function public.kue_admin_reset_data() to service_role;
