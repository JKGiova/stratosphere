-- Cover the full ownership/job/strategy/plan relationship used by report constraints.
create index reports_owner_job_strategy_plan_idx
  on stratosphere.reports(owner_id,job_id,strategy_id,plan_sha256);
