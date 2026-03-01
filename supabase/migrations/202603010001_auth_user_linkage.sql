-- Auth user linkage + org improvements
-- Adds auth.users FK to tenant_users for proper RLS, org branding/quota columns,
-- and an invitations table. Fully additive — existing pipeline rows are preserved.

-- ── 1. Extend tenants with org-level metadata ─────────────────────────────────

alter table public.tenants
  add column if not exists domain         text,          -- e.g. "stripe.com" for SSO
  add column if not exists logo_url       text,
  add column if not exists max_seats      int  not null default 5,
  add column if not exists metadata_json  jsonb not null default '{}'::jsonb;

comment on column public.tenants.domain    is 'Primary email domain for the org, used for auto-provisioning';
comment on column public.tenants.max_seats is 'Maximum number of active members allowed on this plan';

-- ── 2. Link tenant_users to auth.users ───────────────────────────────────────
-- auth_user_id is the Supabase-Auth UUID (auth.uid()).
-- user_id (text) is kept unchanged so the existing pipeline code does not break.
-- Service-role inserts (pipeline) can leave auth_user_id NULL.
-- Frontend/auth inserts must supply auth_user_id.

alter table public.tenant_users
  add column if not exists auth_user_id uuid
    references auth.users(id) on delete cascade;

-- Partial unique index: one auth user per tenant (ignores NULL rows from pipeline)
create unique index if not exists idx_tenant_users_auth_user_unique
  on public.tenant_users(tenant_id, auth_user_id)
  where auth_user_id is not null;

-- Speed up auth.uid() → tenant lookup (used heavily for RLS)
create index if not exists idx_tenant_users_auth_user_id
  on public.tenant_users(auth_user_id);

comment on column public.tenant_users.auth_user_id
  is 'Supabase auth.users UUID — set when user registers/accepts invite. NULL for service-role pipeline rows.';

-- ── 3. Org invitations table ─────────────────────────────────────────────────

create table if not exists public.org_invitations (
  id            uuid        primary key default gen_random_uuid(),
  tenant_id     text        not null references public.tenants(tenant_id) on delete cascade,
  invited_by    uuid        not null references auth.users(id) on delete cascade,
  email         text        not null,
  role          text        not null default 'member',
  token         text        not null unique default extensions.encode(extensions.gen_random_bytes(24), 'hex'),
  status        text        not null default 'pending',   -- pending | accepted | revoked | expired
  expires_at    timestamptz not null default (now() + interval '7 days'),
  accepted_at   timestamptz,
  created_at    timestamptz not null default now()
);

create index if not exists idx_org_invitations_tenant
  on public.org_invitations(tenant_id, status, expires_at desc);

create index if not exists idx_org_invitations_token
  on public.org_invitations(token) where status = 'pending';

create index if not exists idx_org_invitations_email
  on public.org_invitations(email, status);

comment on table public.org_invitations is
  'Pending/accepted invitations to join a tenant org. token is sent via email, expires in 7 days.';

-- ── 4. Updated RLS helper functions ──────────────────────────────────────────

-- Replace current_user_id() to use auth.uid() text cast (backward compatible)
create or replace function public.current_user_id()
returns text
language sql
stable
as $$
  select coalesce(
    auth.uid()::text,
    nullif(auth.jwt() ->> 'sub', ''),
    nullif(auth.jwt() ->> 'user_id', '')
  );
$$;

-- New function: resolves the caller's tenant_id from their auth_user_id membership
create or replace function public.current_tenant_id()
returns text
language sql
stable
as $$
  select coalesce(
    -- Prefer JWT claim (set by your app on sign-in via app_metadata)
    nullif(auth.jwt() -> 'app_metadata' ->> 'tenant_id', ''),
    nullif(auth.jwt() ->> 'tenant_id', ''),
    -- Fall back to DB lookup via auth_user_id
    (
      select tenant_id
      from public.tenant_users
      where auth_user_id = auth.uid()
      limit 1
    )
  );
$$;

-- True if the calling auth user is a member of the given tenant
create or replace function public.is_tenant_member(check_tenant_id text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from public.tenant_users
    where tenant_id     = check_tenant_id
      and auth_user_id  = auth.uid()
      and status        = 'active'
  );
$$;

-- True if the calling auth user is an admin of the given tenant
create or replace function public.is_tenant_admin(check_tenant_id text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from public.tenant_users
    where tenant_id     = check_tenant_id
      and auth_user_id  = auth.uid()
      and role          = 'admin'
      and status        = 'active'
  );
$$;

-- ── 5. RLS policies for new table + upgraded policies for tenant_users ────────

alter table public.org_invitations enable row level security;

-- Service role full access
create policy org_invitations_service_role_all
  on public.org_invitations for all to service_role
  using (true) with check (true);

-- Admins can manage invitations for their tenant
create policy org_invitations_admin_all
  on public.org_invitations for all to authenticated
  using (is_tenant_admin(tenant_id))
  with check (is_tenant_admin(tenant_id));

-- Members can view invitations in their tenant (read-only)
create policy org_invitations_member_select
  on public.org_invitations for select to authenticated
  using (is_tenant_member(tenant_id));

-- Upgrade tenant_users: members can now INSERT themselves (on invite acceptance)
-- and UPDATE their own profile row. Admins can manage all rows in their tenant.
drop policy if exists tenant_users_authenticated_insert on public.tenant_users;
drop policy if exists tenant_users_authenticated_update on public.tenant_users;
drop policy if exists tenant_users_admin_all on public.tenant_users;

create policy tenant_users_admin_all
  on public.tenant_users for all to authenticated
  using (is_tenant_admin(tenant_id))
  with check (is_tenant_admin(tenant_id));

-- Members can read other members in their tenant
-- (policy already exists as tenant_users_authenticated_select; no need to re-create)

-- Self-update: a user can update only their own row
create policy tenant_users_self_update
  on public.tenant_users for update to authenticated
  using  (auth_user_id = auth.uid())
  with check (auth_user_id = auth.uid());

-- Self-insert: used when accepting an invitation
create policy tenant_users_self_insert
  on public.tenant_users for insert to authenticated
  with check (auth_user_id = auth.uid());

-- ── 6. Convenience view for the current user's workspace context ─────────────

create or replace view public.v_my_workspace as
select
  t.tenant_id,
  t.name          as org_name,
  t.plan,
  t.domain,
  t.logo_url,
  t.max_seats,
  t.settings_json as org_settings,
  tu.user_id,
  tu.auth_user_id,
  tu.email,
  tu.display_name,
  tu.role,
  tu.status,
  tu.created_at   as member_since
from public.tenants t
join public.tenant_users tu
  on tu.tenant_id = t.tenant_id
 and tu.auth_user_id = auth.uid()
 and tu.status = 'active';

comment on view public.v_my_workspace is
  'Returns the calling user''s active tenant membership and org details. One row per org they belong to.';
