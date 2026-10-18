-- Private, service-mediated registry foundation. No client or anonymous schema grants.
-- Application services must validate canonical StrategySpec contracts before registration.
create schema if not exists stratosphere;
revoke all on schema stratosphere from public, anon, authenticated;
grant usage on schema stratosphere to service_role;
alter default privileges for role postgres in schema stratosphere
  revoke all on tables from public, anon, authenticated;
alter default privileges for role postgres in schema stratosphere
  revoke execute on functions from public, anon, authenticated;

create table stratosphere.profiles (
  id uuid primary key references auth.users(id),
  display_name text not null,
  created_at timestamptz not null default now()
);

create table stratosphere.strategies (
  id uuid primary key default gen_random_uuid(),
  public_id text not null unique check (public_id ~ '^[0-9]{24}$'),
  name text not null check (length(btrim(name)) > 0),
  formal_spec jsonb not null check (
    jsonb_typeof(formal_spec) = 'object' and formal_spec->>'schema_version' = 'strategy/1'
  ),
  canonical_spec text not null check (canonical_spec::jsonb = formal_spec),
  spec_sha256 text not null unique check (spec_sha256 ~ '^[a-f0-9]{64}$'),
  created_by uuid not null references stratosphere.profiles(id),
  created_at timestamptz not null default now()
);
create index strategies_created_by_idx on stratosphere.strategies(created_by);

create table stratosphere.strategy_lineage (
  parent_id uuid not null references stratosphere.strategies(id),
  child_id uuid not null references stratosphere.strategies(id),
  rationale text not null,
  created_at timestamptz not null default now(),
  primary key (parent_id, child_id),
  check (parent_id <> child_id)
);
create index lineage_child_idx on stratosphere.strategy_lineage(child_id);

create table stratosphere.source_snapshots (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  payload jsonb not null check (jsonb_typeof(payload) = 'object'),
  snapshot_sha256 text not null check (snapshot_sha256 ~ '^[a-f0-9]{64}$'),
  retrieved_at timestamptz not null default now(),
  unique (owner_id, snapshot_sha256),
  unique (owner_id, id)
);

create table stratosphere.strategy_sources (
  strategy_id uuid not null references stratosphere.strategies(id),
  owner_id uuid not null references stratosphere.profiles(id),
  source_id uuid not null,
  evidence jsonb not null default '[]'::jsonb check (jsonb_typeof(evidence) = 'array'),
  created_at timestamptz not null default now(),
  primary key (strategy_id, source_id),
  foreign key (owner_id, source_id) references stratosphere.source_snapshots(owner_id,id)
);
create index strategy_sources_owner_source_idx on stratosphere.strategy_sources(owner_id,source_id);

create table stratosphere.submissions (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.strategies(id),
  source_id uuid not null,
  resolution text not null check (resolution in ('new_strategy','exact_duplicate','variant')),
  created_at timestamptz not null default now(),
  foreign key (owner_id, source_id) references stratosphere.source_snapshots(owner_id,id)
);
create index submissions_owner_source_idx on stratosphere.submissions(owner_id,source_id);
create index submissions_strategy_idx on stratosphere.submissions(strategy_id);

create table stratosphere.agents (
  strategy_id uuid primary key references stratosphere.strategies(id),
  agent_id uuid not null unique default gen_random_uuid(),
  created_at timestamptz not null default now()
);
create table stratosphere.agent_sessions (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.agents(strategy_id),
  created_at timestamptz not null default now(),
  unique (owner_id,strategy_id),
  unique (owner_id,id)
);
create index agent_sessions_strategy_idx on stratosphere.agent_sessions(strategy_id);
create table stratosphere.agent_turns (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  session_id uuid not null,
  user_text text not null,
  assistant_text text not null,
  model_ref text not null,
  created_at timestamptz not null default now(),
  foreign key (owner_id,session_id) references stratosphere.agent_sessions(owner_id,id)
);
create index agent_turns_owner_session_idx on stratosphere.agent_turns(owner_id,session_id,created_at);

create table stratosphere.research_jobs (
  id uuid primary key,
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.strategies(id),
  plan_sha256 text not null check (plan_sha256 ~ '^[a-f0-9]{64}$'),
  state text not null check (state in (
    'extracting','needs_clarification','unsupported','awaiting_approval',
    'paused','running','completed','failed','canceled'
  )),
  updated_at timestamptz not null default now(),
  unique (owner_id,id),
  unique (owner_id,id,strategy_id,plan_sha256)
);
create index research_jobs_owner_idx on stratosphere.research_jobs(owner_id);
create index research_jobs_strategy_idx on stratosphere.research_jobs(strategy_id);

create table stratosphere.operations (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.strategies(id),
  action text not null,
  job_id uuid,
  status text not null check (status in ('running','awaiting_approval','completed','failed','canceled')),
  detail text not null default '',
  created_at timestamptz not null default now(),
  finished_at timestamptz,
  foreign key (owner_id,job_id) references stratosphere.research_jobs(owner_id,id)
);
create index operations_owner_job_idx on stratosphere.operations(owner_id,job_id);
create index operations_strategy_idx on stratosphere.operations(strategy_id);

create table stratosphere.reports (
  id text primary key check (id ~ '^[a-f0-9]{64}$'),
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.strategies(id),
  job_id uuid not null,
  plan_sha256 text not null,
  kind text not null check (kind in ('research','study','paper')),
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (owner_id,job_id),
  foreign key (owner_id,job_id,strategy_id,plan_sha256)
    references stratosphere.research_jobs(owner_id,id,strategy_id,plan_sha256)
);
create index reports_strategy_idx on stratosphere.reports(strategy_id);

create table stratosphere.assessments (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references stratosphere.profiles(id),
  strategy_id uuid not null references stratosphere.strategies(id),
  assessed_at timestamptz not null default now(),
  payload jsonb not null
);
create index assessments_owner_idx on stratosphere.assessments(owner_id);
create index assessments_strategy_idx on stratosphere.assessments(strategy_id,assessed_at);

-- Policies are ready for a future verified application session; the private schema has no
-- anon/authenticated USAGE or table grants. Login/HTTP deployment is deliberately not enabled.
alter table stratosphere.profiles enable row level security;
create policy profile_owner on stratosphere.profiles to authenticated
  using ((select auth.uid()) = id) with check ((select auth.uid()) = id);
alter table stratosphere.strategies enable row level security;
create policy catalog_definitions on stratosphere.strategies for select to authenticated using (true);
alter table stratosphere.strategy_lineage enable row level security;
create policy catalog_lineage on stratosphere.strategy_lineage for select to authenticated using (true);
alter table stratosphere.agents enable row level security;
create policy catalog_agent_identity on stratosphere.agents for select to authenticated using (true);

do $$
declare table_name text;
begin
  foreach table_name in array array[
    'source_snapshots','strategy_sources','submissions','agent_sessions','agent_turns',
    'research_jobs','operations','reports','assessments'
  ] loop
    execute format('alter table stratosphere.%I enable row level security',table_name);
    execute format(
      'create policy owner_access on stratosphere.%I to authenticated '
      'using ((select auth.uid()) = owner_id) with check ((select auth.uid()) = owner_id)',table_name
    );
  end loop;
end $$;

grant select, insert, update, delete on all tables in schema stratosphere to service_role;

create function stratosphere.canonical_json(value jsonb) returns text
language plpgsql immutable strict security invoker set search_path = '' as $$
declare result text;
begin
  case jsonb_typeof(value)
    when 'object' then
      select '{' || coalesce(string_agg(to_jsonb(key)::text || ':' ||
        stratosphere.canonical_json(val),',' order by key collate "C"),'') || '}'
      into result from jsonb_each(value) as entry(key,val);
    when 'array' then
      select '[' || coalesce(string_agg(stratosphere.canonical_json(val),',' order by ord),'') || ']'
      into result from jsonb_array_elements(value) with ordinality as entry(val,ord);
    else result := value::text;
  end case;
  return result;
end $$;

create function stratosphere.random_public_id() returns text
language plpgsql volatile security invoker set search_path = '' as $$
declare result text := ''; bytes bytea; i integer; digit integer;
begin
  while length(result) < 24 loop
    bytes := extensions.gen_random_bytes(24);
    for i in 0..23 loop
      digit := get_byte(bytes,i);
      if digit < 250 and length(result) < 24 then
        result := result || (digit % 10)::text;
      end if;
    end loop;
  end loop;
  return result;
end $$;

create function stratosphere.protect_strategy_identity() returns trigger
language plpgsql security invoker set search_path = '' as $$
begin
  if new.id <> old.id or new.public_id <> old.public_id or new.formal_spec <> old.formal_spec
    or new.canonical_spec <> old.canonical_spec or new.spec_sha256 <> old.spec_sha256
    or new.created_by <> old.created_by or new.created_at <> old.created_at then
    raise exception 'Strategy identity is immutable';
  end if;
  return new;
end $$;
create trigger strategy_identity before update on stratosphere.strategies
  for each row execute function stratosphere.protect_strategy_identity();

create function stratosphere.protect_report() returns trigger
language plpgsql security invoker set search_path = '' as $$
begin
  raise exception 'Completed report records are immutable';
end $$;
create trigger report_identity before update or delete on stratosphere.reports
  for each row execute function stratosphere.protect_report();

create function stratosphere.register_strategy(
  p_owner uuid, p_name text, p_canonical text, p_source jsonb,
  p_evidence jsonb default '[]'::jsonb, p_parent uuid default null, p_rationale text default ''
) returns table(strategy_id uuid, public_id text, agent_id uuid, resolution text)
language plpgsql security invoker set search_path = '' as $$
declare
  behavior jsonb := p_canonical::jsonb;
  digest text := encode(sha256(convert_to(p_canonical,'UTF8')),'hex');
  source_digest text := encode(sha256(convert_to(stratosphere.canonical_json(p_source),'UTF8')),'hex');
  stored stratosphere.strategies%rowtype;
  source_id uuid;
  outcome text;
  allocated boolean := false;
  attempt integer;
begin
  if current_user not in ('postgres','service_role') then
    raise insufficient_privilege using message='Registration requires the trusted application service';
  end if;
  if stratosphere.canonical_json(behavior) <> p_canonical then
    raise exception 'Specification must use the canonical wire representation';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(digest,0));
  select s.* into stored from stratosphere.strategies s where s.spec_sha256=digest;
  if found then
    if stored.canonical_spec <> p_canonical then
      raise exception 'Behavior digest collision; identity merge blocked';
    end if;
    outcome := 'exact_duplicate';
  else
    for attempt in 1..100 loop
      begin
        insert into stratosphere.strategies
          (public_id,name,formal_spec,canonical_spec,spec_sha256,created_by)
        values (stratosphere.random_public_id(),p_name,behavior,p_canonical,digest,p_owner)
        returning * into stored;
        allocated := true;
        exit;
      exception when unique_violation then
        -- Hash-specific transaction locking leaves random public-ID collision as the retry case.
      end;
    end loop;
    if not allocated then raise exception 'Public identity allocation failed'; end if;
    outcome := case when p_parent is null then 'new_strategy' else 'variant' end;
  end if;
  if p_parent = stored.id then raise exception 'Variant must change behavior'; end if;
  if p_parent is not null then
    insert into stratosphere.strategy_lineage(parent_id,child_id,rationale)
    values (p_parent,stored.id,p_rationale) on conflict do nothing;
  end if;
  insert into stratosphere.agents(strategy_id) values(stored.id) on conflict do nothing;
  insert into stratosphere.source_snapshots(owner_id,payload,snapshot_sha256)
  values (p_owner,p_source,source_digest) on conflict(owner_id,snapshot_sha256) do nothing;
  select s.id into source_id from stratosphere.source_snapshots s
    where s.owner_id=p_owner and s.snapshot_sha256=source_digest;
  if (select payload from stratosphere.source_snapshots where id=source_id) <> p_source then
    raise exception 'Source digest collision';
  end if;
  insert into stratosphere.strategy_sources(strategy_id,owner_id,source_id,evidence)
  values (stored.id,p_owner,source_id,p_evidence) on conflict do nothing;
  insert into stratosphere.submissions(owner_id,strategy_id,source_id,resolution)
  values (p_owner,stored.id,source_id,outcome);
  insert into stratosphere.agent_sessions(owner_id,strategy_id)
  values (p_owner,stored.id) on conflict do nothing;
  return query select stored.id,stored.public_id,a.agent_id,outcome
    from stratosphere.agents a where a.strategy_id=stored.id;
end $$;

-- Only the application service can call these routines; no definer privileges or public RPCs.
revoke execute on all functions in schema stratosphere from public, anon, authenticated;
grant execute on all functions in schema stratosphere to service_role;
