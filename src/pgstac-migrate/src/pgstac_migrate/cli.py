"""CLI for PgSTAC migration artifacts."""

from __future__ import annotations

import argparse
import atexit
import shutil
import sys
import tempfile
from pathlib import Path

from pgpkg.artifact import LoadedArtifact, load_artifact
from pgpkg.catalog import Catalog, build_catalog
from pgpkg.cli import _add_db_args, _resolve_password
from pgpkg.config import ProjectConfig
from pgpkg.errors import PgpkgError
from pgpkg.planner import MigrationPlan, plan
from pgpkg.versioning import default_target

from pgstac_migrate.api import artifact_path as resolved_artifact_path
from pgstac_migrate.api import migrate as migrate_database
from pgstac_migrate.build import build_local_artifact


def _artifact_path() -> Path:
    return resolved_artifact_path()


def _catalog_from_artifact(artifact: LoadedArtifact) -> Catalog:
    tmp_root = Path(tempfile.mkdtemp(prefix="pgstac_migrate_"))
    atexit.register(shutil.rmtree, tmp_root, True)

    migrations_dir = tmp_root / "migrations"
    migrations_dir.mkdir()
    for name, data in artifact.migrations_files().items():
        (migrations_dir / Path(name).name).write_bytes(data)

    sql_dir = tmp_root / "sql"
    pre_dir = sql_dir / "pre"
    post_dir = sql_dir / "post"
    pre_dir.mkdir(parents=True)
    post_dir.mkdir(parents=True)

    config = ProjectConfig(
        project_name=artifact.manifest.project_name,
        prefix=artifact.manifest.prefix,
        sql_dir=sql_dir,
        migrations_dir=migrations_dir,
        pre_dir=pre_dir,
        post_dir=post_dir,
        project_root=tmp_root,
        version_source=artifact.manifest.version_source,
        tracking_schema=artifact.manifest.tracking_schema,
        tracking_table=artifact.manifest.tracking_table,
    )
    return build_catalog(config)


def _load_artifact_and_catalog() -> tuple[LoadedArtifact, Catalog]:
    artifact = load_artifact(_artifact_path())
    return artifact, _catalog_from_artifact(artifact)


def _render_plan(migration_plan: MigrationPlan) -> None:
    print(f"target:    {migration_plan.target}")
    print(f"source:    {migration_plan.source}")
    bootstrap = migration_plan.bootstrap_base
    print(f"bootstrap: {bootstrap.name if bootstrap else '(none)'}")
    print("steps:")
    if not migration_plan.steps:
        print("  (none)")
    for step in migration_plan.steps:
        print(f"  {step.from_version} -> {step.to_version}  [{step.file.name}]")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pgstac-migrate", add_help=False)
    parser.add_argument("--help", action="help", help="Show help and exit")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_migrate = sub.add_parser(
        "migrate",
        help="Apply baked PgSTAC migrations to a live DB",
        add_help=False,
    )
    p_migrate.add_argument("--help", action="help", help="Show help and exit")
    _add_db_args(p_migrate)
    p_migrate.add_argument(
        "--to", dest="target", help="Target version (default: highest)"
    )
    p_migrate.add_argument("--dry-run", action="store_true")

    sub.add_parser("info", help="Print baked artifact info")
    sub.add_parser("versions", help="List baked migration versions")
    sub.add_parser("build-artifact", help="Bake the local PgSTAC migration artifact")

    p_plan = sub.add_parser("plan", help="Show baked migration plan")
    p_plan.add_argument("--source", help="Source version (omit for fresh install)")
    p_plan.add_argument("--to", dest="target", help="Target version (default: highest)")

    args = parser.parse_args(argv)

    try:
        if args.cmd == "build-artifact":
            path = build_local_artifact()
            print(f"wrote {path}")
            return 0

        if args.cmd == "migrate":
            password = _resolve_password(args)
            result = migrate_database(
                target=args.target,
                dry_run=args.dry_run,
                conninfo=args.dsn,
                host=args.host,
                port=args.port,
                dbname=args.dbname,
                user=args.user,
                password=password,
            )
            if result.bootstrapped_from is not None:
                print(f"bootstrapped to {result.bootstrapped_from}")
            for from_version, to_version in result.applied_steps:
                print(f"applied {from_version} -> {to_version}")
            print(f"final version: {result.final_version}")
            if args.dry_run:
                print("(dry-run: rolled back)")
            return 0

        artifact, catalog = _load_artifact_and_catalog()
        if args.cmd == "info":
            print(f"project: {artifact.manifest.project_name}")
            print(f"prefix:  {artifact.manifest.prefix}")
            for entry in artifact.manifest.entries:
                print(f"  {entry.name}  {entry.sha256[:12]}  {entry.size}B")
            return 0

        if args.cmd == "versions":
            for version in catalog.versions:
                print(version)
            return 0

        if args.cmd == "plan":
            target = args.target or default_target(catalog.versions)
            if target is None:
                raise PgpkgError(
                    "Artifact catalog is empty; nothing to plan.", code="E_PLAN"
                )
            migration_plan = plan(catalog, source=args.source, target=target)
            _render_plan(migration_plan)
            return 0

        return 2
    except PgpkgError as exc:
        print(f"error [{exc.code}]: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
