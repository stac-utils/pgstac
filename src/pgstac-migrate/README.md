# pgstac-migrate

Apply baked PgSTAC migrations with `pgpkg`.

Source-tree development resolves `pgpkg>=0.1,<0.2` from PyPI by default.

Examples:

```bash
uv run --directory src/pgstac-migrate pgstac-migrate build-artifact
uv run --directory src/pgstac-migrate pgstac-migrate info
uv run --directory src/pgstac-migrate pgstac-migrate versions
uv run --directory src/pgstac-migrate pgstac-migrate migrate --help
```

Standalone post-release bootstrap helper:

```bash
uv run --script src/pgstac-migrate/scripts/build_artifact.py
```

That helper does not use `uv.lock`; it resolves its own inline dependency on `pgpkg>=0.1,<0.2` directly from PyPI.
