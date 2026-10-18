-- Controlled database conformance fixtures only; all data and temporary grants roll back.
begin;
do $verify$
declare
  alice uuid := gen_random_uuid();
  bob uuid := gen_random_uuid();
  a record; b record; c record; d record;
  first_job uuid := gen_random_uuid();
  second_job uuid := gen_random_uuid();
  alice_session uuid; bob_session uuid;
  count_value integer;
  blocked boolean;
  base_spec text := '{"assumptions":["Indicator periods count published observations, not calendar days.","The ECB reference rate is the close input; no intraday high/low or volume is inferred.","Reference availability is assumed at 17:00 UTC; this is not a point-in-time publication archive.","Entry requires flat exposure; exit requires long exposure; rules never reinforce.","Crosses above means previous fast <= slow and current fast > slow; below reverses the inequalities.","SMA uses exact rational arithmetic; warmup requires eight observations plus one prior comparison."],"data_requirements":{"availability":"event_timestamp","input":"reference_rate","warmup_bars":9},"entry_rules":[{"action":"open_long","condition":{"args":[{"args":[],"op":"indicator","value":"fast"},{"args":[],"op":"indicator","value":"slow"}],"op":"crosses_above","value":null},"id":"enter_long"}],"execution_requirements":{"max_positions":1,"order_type":"market","reinforcement":false,"session_end":"hold","shorting":false},"exit_rules":[{"action":"close_long","condition":{"args":[{"args":[],"op":"indicator","value":"fast"},{"args":[],"op":"indicator","value":"slow"}],"op":"crosses_below","value":null},"id":"exit_long"}],"indicators":[{"algorithm":"sma","algorithm_version":1,"id":"fast","input":"close","period":3,"timeframe_seconds":86400},{"algorithm":"sma","algorithm_version":1,"id":"slow","input":"close","period":8,"timeframe_seconds":86400}],"instrument_constraints":{"asset_classes":["forex"],"fixed_symbols":["EURUSD"],"selection":"fixed"},"market_conditions":[],"missing_data_policy":{"action":"abort","max_staleness_seconds":345600},"position_sizing":{"amount":"100","mode":"fixed_base"},"schema_version":"strategy/1","signal_timing":{"evaluate":"event","precedence":"exit_before_entry","submit":"next_observation"},"timeframes":[86400]}';
  variant_spec text := '{"assumptions":["Indicator periods count published observations, not calendar days.","The ECB reference rate is the close input; no intraday high/low or volume is inferred.","Reference availability is assumed at 17:00 UTC; this is not a point-in-time publication archive.","Entry requires flat exposure; exit requires long exposure; rules never reinforce.","Crosses above means previous fast <= slow and current fast > slow; below reverses the inequalities.","SMA uses exact rational arithmetic; warmup requires eight observations plus one prior comparison."],"data_requirements":{"availability":"event_timestamp","input":"reference_rate","warmup_bars":9},"entry_rules":[{"action":"open_long","condition":{"args":[{"args":[],"op":"indicator","value":"fast"},{"args":[],"op":"indicator","value":"slow"}],"op":"crosses_above","value":null},"id":"enter_long"}],"execution_requirements":{"max_positions":1,"order_type":"market","reinforcement":false,"session_end":"hold","shorting":false},"exit_rules":[{"action":"close_long","condition":{"args":[{"args":[],"op":"indicator","value":"fast"},{"args":[],"op":"indicator","value":"slow"}],"op":"crosses_below","value":null},"id":"exit_long"}],"indicators":[{"algorithm":"sma","algorithm_version":1,"id":"fast","input":"close","period":4,"timeframe_seconds":86400},{"algorithm":"sma","algorithm_version":1,"id":"slow","input":"close","period":8,"timeframe_seconds":86400}],"instrument_constraints":{"asset_classes":["forex"],"fixed_symbols":["EURUSD"],"selection":"fixed"},"market_conditions":[],"missing_data_policy":{"action":"abort","max_staleness_seconds":345600},"position_sizing":{"amount":"100","mode":"fixed_base"},"schema_version":"strategy/1","signal_timing":{"evaluate":"event","precedence":"exit_before_entry","submit":"next_observation"},"timeframes":[86400]}';
begin
  assert current_user = 'postgres', 'Conformance harness requires privileged trusted context';
  assert not has_schema_privilege('anon','stratosphere','usage');
  assert not has_schema_privilege('authenticated','stratosphere','usage');
  insert into auth.users(id) values(alice),(bob);
  insert into stratosphere.profiles(id,display_name) values(alice,'Harness Alice'),(bob,'Harness Bob');
  select * into a from stratosphere.register_strategy(
    alice,'Authored conformance',base_spec,'{"text":"private source A"}'::jsonb);
  select * into b from stratosphere.register_strategy(
    alice,'Different name',base_spec,'{"text":"private source B"}'::jsonb);
  select * into c from stratosphere.register_strategy(
    alice,'Variant',variant_spec,'{"text":"variant source"}'::jsonb,'[]'::jsonb,a.strategy_id,'Period 3 to 4');
  select * into d from stratosphere.register_strategy(
    bob,'Same shared definition',base_spec,'{"text":"Bob private source"}'::jsonb);
  assert a.strategy_id = b.strategy_id and a.agent_id = b.agent_id;
  assert a.agent_id = d.agent_id and a.strategy_id = d.strategy_id;
  assert a.public_id ~ '^[0-9]{24}$' and c.public_id <> a.public_id;
  assert c.agent_id <> a.agent_id and c.resolution='variant';
  assert b.resolution='exact_duplicate';
  assert (select spec_sha256 from stratosphere.strategies where id=a.strategy_id)='b155e4f23654825e82ef2d9634262fead388f3e0fd6e1379bc93e530bfff4617';
  assert (select count(*) from stratosphere.strategy_lineage where parent_id=a.strategy_id)=1;
  assert (select count(*) from stratosphere.submissions where owner_id=alice)=3;
  select id into alice_session from stratosphere.agent_sessions
    where owner_id=alice and strategy_id=a.strategy_id;
  select id into bob_session from stratosphere.agent_sessions
    where owner_id=bob and strategy_id=a.strategy_id;
  insert into stratosphere.agent_turns(owner_id,session_id,user_text,assistant_text,model_ref)
    values(alice,alice_session,'Alice private question','Alice reply','authored-fixture'),
          (bob,bob_session,'Bob private question','Bob reply','authored-fixture');
  insert into stratosphere.research_jobs(id,owner_id,strategy_id,plan_sha256,state)
    values(first_job,alice,a.strategy_id,repeat('a',64),'completed'),
          (second_job,bob,a.strategy_id,repeat('b',64),'completed');
  insert into stratosphere.reports(id,owner_id,strategy_id,job_id,plan_sha256,kind,payload)
    values(repeat('c',64),alice,a.strategy_id,first_job,repeat('a',64),'research','{"authored":true}'),
          (repeat('d',64),bob,a.strategy_id,second_job,repeat('b',64),'research','{"authored":true}');
  blocked := false;
  begin
    update stratosphere.strategies set formal_spec=variant_spec::jsonb where id=a.strategy_id;
  exception when raise_exception then blocked:=true;
  end;
  assert blocked, 'Behavior identity mutation was not rejected';
  blocked := false;
  begin
    update stratosphere.reports set payload='{}' where id=repeat('c',64);
  exception when raise_exception then blocked:=true;
  end;
  assert blocked, 'Completed evidence overwrite was not rejected';
  blocked := false;
  begin
    insert into stratosphere.agent_turns(owner_id,session_id,user_text,assistant_text,model_ref)
      values(alice,bob_session,'cross-owner','forbidden','fixture');
  exception when foreign_key_violation then blocked:=true;
  end;
  assert blocked, 'Cross-owner session association was not rejected';

  -- Temporary privileges exercise RLS in the unexposed schema, then roll back with this transaction.
  grant usage on schema stratosphere to authenticated;
  grant select,insert,update on all tables in schema stratosphere to authenticated;
  perform set_config('request.jwt.claims',json_build_object('sub',alice,'role','authenticated')::text,true);
  set local role authenticated;
  select count(*) into count_value from stratosphere.agent_turns;
  assert count_value=1, 'Alice could read another owner agent history';
  select count(*) into count_value from stratosphere.reports;
  assert count_value=1, 'Alice could read another owner reports';
  select count(*) into count_value from stratosphere.source_snapshots;
  assert count_value=3, 'Source ownership filter failed';
  blocked := false;
  begin
    update stratosphere.agent_turns set owner_id=bob where owner_id=alice;
  exception when insufficient_privilege then blocked:=true;
  end;
  assert blocked, 'Owner reassignment was not rejected';
  blocked := false;
  begin
    perform * from stratosphere.register_strategy(bob,'forged',base_spec,'{}');
  exception when insufficient_privilege then blocked:=true;
  end;
  assert blocked, 'Client could call privileged registration';
  reset role;
end $verify$;
rollback;
select 'passed' as registry_conformance, 'all fixtures and temporary grants rolled back' as cleanup;
