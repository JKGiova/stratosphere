-- Append-only dated findings remain separate from mutable agent conversations and instances.
create function stratosphere.protect_applicability_assessment() returns trigger
language plpgsql security invoker set search_path = '' as $$
begin
  raise check_violation using message='Applicability assessments are immutable dated snapshots';
end $$;

revoke all on function stratosphere.protect_applicability_assessment()
  from public, anon, authenticated;
grant execute on function stratosphere.protect_applicability_assessment() to service_role;

create trigger assessment_identity before update or delete on stratosphere.assessments
  for each row execute function stratosphere.protect_applicability_assessment();
