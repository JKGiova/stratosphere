# Retained hosted database code

The personal TUI uses local SQLite. Hosted user databases, application login and synchronization
are outside [the current plan](../plan.md).

This directory retains applied migration history and ownership/immutability SQL tests from earlier
work. Keep these files compatible; documentation cleanup does not edit migrations, grant access,
delete resources or prove current hosted-project health.

Future authorized database changes must append migrations instead of rewriting applied SQL.
Privileged service credentials must never reach the TUI, models or native workers. The existing
SQL harness uses authored fixtures with transactional rollback; it is not application-login or
provider-account verification. See [contracts](../docs/contracts.md).
