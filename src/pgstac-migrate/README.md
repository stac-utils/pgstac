# pgstac-migrate

Standalone PgSTAC migration CLI and Python API.

This package applies PgSTAC schema migrations to a PostgreSQL database from a bundled migration artifact.

## Install

```bash
pip install pgstac-migrate
```

## Quick start

```bash
pgstac-migrate --help
pgstac-migrate migrate
```

## CLI command reference

Top-level commands:

- migrate: apply migrations to a live database
- plan: show the migration plan without applying changes
- versions: list all versions available in the bundled artifact
- info: show artifact metadata and bundled migration file info
- build-artifact: build or refresh the local artifact from source SQL files

### migrate

Usage:

```bash
pgstac-migrate migrate [--dsn DSN] [-h HOST] [-p PORT] [-d DBNAME] [-U USER] [-W] [--to TARGET] [--dry-run]
```

Parameters:

- --dsn DSN
	- Full libpq connection string. When provided, it takes precedence over individual host/user/db flags.
- -h, --host HOST
	- Database host. Same meaning as PGHOST.
- -p, --port PORT
	- Database port. Same meaning as PGPORT.
- -d, --dbname DBNAME
	- Database name. Same meaning as PGDATABASE.
- -U, --user USER
	- Database user. Same meaning as PGUSER.
- -W, --password-prompt
	- Prompt for password interactively.
- --to TARGET
	- Target PgSTAC version to migrate to. If omitted, migrates to the latest version in the artifact.
- --dry-run
	- Computes and executes the migration plan, then rolls back before commit.

Examples:

```bash
pgstac-migrate migrate
pgstac-migrate migrate --to 0.9.11
pgstac-migrate migrate --dry-run
pgstac-migrate migrate --dsn "postgresql://user:pass@localhost:5432/postgis"
pgstac-migrate migrate --host localhost --port 5432 --dbname postgis --user username -W
```

### plan

Usage:

```bash
pgstac-migrate plan [--source SOURCE] [--to TARGET]
```

Parameters:

- --source SOURCE
	- Starting version for planning. Omit for fresh install planning.
- --to TARGET
	- Target version. If omitted, plans to the latest version in the artifact.

Examples:

```bash
pgstac-migrate plan
pgstac-migrate plan --source 0.9.10 --to 0.9.11
```

### versions

Usage:

```bash
pgstac-migrate versions
```

Prints all versions available in the bundled artifact catalog.

### info

Usage:

```bash
pgstac-migrate info
```

Prints artifact manifest metadata, plus checksums and sizes for bundled entries.

### build-artifact

Usage:

```bash
pgstac-migrate build-artifact
```

What it does:

- Reads PgSTAC SQL and migration sources from the repository source tree.
- Builds a compressed artifact file named migrations.tar.zst.
- Writes the artifact to src/pgstac-migrate/src/pgstac_migrate/migrations.tar.zst.

When to use it:

- During source-tree development after SQL or migration files change.
- Before testing commands like plan, versions, info, or migrate against local unreleased migration changes.

When you do not need it:

- Typical PyPI package usage, where an artifact is already bundled in the installed wheel.

## Connection parameters and environment variables

pgstac-migrate follows libpq/psql connection conventions.

Resolution order:

1. Explicit CLI arguments
2. libpq environment variables
3. libpq defaults

If --dsn is provided, it overrides individual connection flags.

Supported libpq environment variables for connection behavior include:

- PGHOST: database host name
- PGHOSTADDR: database host IP address
- PGPORT: database port
- PGDATABASE: database name
- PGUSER: database user
- PGPASSWORD: database password
- PGPASSFILE: password file path
- PGSERVICE: named service to load connection options
- PGSERVICEFILE: service file path
- PGCONNECT_TIMEOUT: connection timeout in seconds
- PGTARGETSESSIONATTRS: target session attributes for multi-host connection routing
- PGLOADBALANCEHOSTS: host load balancing policy

SSL and TLS environment variables:

- PGSSLMODE
- PGSSLROOTCERT
- PGSSLCERT
- PGSSLKEY
- PGSSLPASSWORD
- PGSSLCRL
- PGSSLCRLDIR
- PGSSLSNI
- PGSSLNEGOTIATION

Additional libpq environment variables commonly used with PostgreSQL are also honored by libpq. See PostgreSQL libpq connection settings for complete semantics.

## Python API reference

Module: pgstac_migrate.api

Functions:

- artifact_path() -> pathlib.Path
	- Returns the artifact path used by the package.
- normalize_target_version(target: str | None) -> str | None
	- Maps source-tree development targets like 0.9.11-dev to unreleased.
- migrate(...)
	- Applies migrations and returns an ApplyResult object.

migrate parameters:

- target: str | None = None
	- Target version. None means latest available.
- dry_run: bool = False
	- Run migration in rollback mode.
- conninfo: str | None = None
	- Full DSN/libpq conninfo string.
- host: str | None = None
- port: int | str | None = None
- dbname: str | None = None
- user: str | None = None
- password: str | None = None

Return value:

- final_version: resulting database version
- bootstrapped_from: base version used when bootstrapping from an empty state
- applied_steps: ordered list of migration steps applied

Example:

```python
from pgstac_migrate.api import migrate

result = migrate(
		target="0.9.11",
		dry_run=False,
		host="localhost",
		port=5432,
		dbname="postgis",
		user="username",
		password="password",
)

print(result.final_version)
print(result.bootstrapped_from)
print(result.applied_steps)
```

## Source checkout usage

```bash
uv run --directory src/pgstac-migrate pgstac-migrate build-artifact
uv run --directory src/pgstac-migrate pgstac-migrate info
uv run --directory src/pgstac-migrate pgstac-migrate versions
uv run --directory src/pgstac-migrate pgstac-migrate plan
uv run --directory src/pgstac-migrate pgstac-migrate migrate --dry-run
```

## Operational notes

- The `migrate` command is safe to re-run. If a database is already at target version, no migration steps are applied.
- Use `plan` before `migrate` when changing environments or moving between non-adjacent versions.
- Use `--dry-run` in CI or release validation to verify pathing and SQL execution without committing changes.

## Troubleshooting

### Connection/authentication errors

Symptoms:

- connection refused
- password authentication failed
- timeout expired

Checks:

- verify `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and credentials
- verify SSL settings (`PGSSLMODE`, certificate paths) when required
- try a known-good `psql` connection with the same DSN/env values

### Target version not found

Symptoms:

- requested `--to` version is rejected
- plan cannot reach target

Checks:

- run `pgstac-migrate versions` to see available targets
- run `pgstac-migrate info` to confirm artifact contents
- in source checkouts, run `pgstac-migrate build-artifact` after migration source changes

### No steps applied

If `migrate` reports no applied steps, this usually means either:

- database is already at target version, or
- source/target are equal for the selected plan

Use `plan` to confirm the expected path.

### Dry-run behavior

`--dry-run` executes the migration sequence and then rolls back.

- It is expected to report a final version in command output while leaving the database unchanged.
- Use this mode to validate migration viability, not to persist schema changes.
