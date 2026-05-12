"""PgSTAC-specific version tracking integration for pgpkg."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pgpkg.tracking import current_tracking_version

if TYPE_CHECKING:
    import psycopg
    from pgpkg.config import ProjectConfig


class PgstacVersionSource:
    """Use pgstac.migrations as the authoritative installed version."""

    def _has_set_version(self, conn: psycopg.Connection) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regprocedure('pgstac.set_version(text)')")
            row = cur.fetchone()
            return row is not None and row[0] is not None

    def read_live_version(
        self,
        conn: psycopg.Connection,
        config: ProjectConfig,
    ) -> str | None:
        del config
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('pgstac.migrations')")
            row = cur.fetchone()
            if row is None or row[0] is None:
                return None
            cur.execute(
                """
                SELECT version
                FROM pgstac.migrations
                ORDER BY datetime DESC, version DESC
                LIMIT 1
                """,
            )
            version_row = cur.fetchone()
            return version_row[0] if version_row else None

    def record_applied(
        self,
        conn: psycopg.Connection,
        config: ProjectConfig,
        *,
        version: str,
        sha256: str,
        filename: str,
    ) -> None:
        del sha256, filename
        with conn.cursor() as cur:
            if self._has_set_version(conn):
                cur.execute("SELECT pgstac.set_version(%s)", (version,))
            else:
                cur.execute(
                    "INSERT INTO pgstac.migrations (version) VALUES (%s)", (version,)
                )

        tracking_version = current_tracking_version(
            conn,
            schema=config.tracking_schema,
            table=config.tracking_table,
        )
        if tracking_version != version:
            raise RuntimeError(
                f"pgpkg tracking version mismatch: expected {version!r}, got {tracking_version!r}",
            )

        live_version = self.read_live_version(conn, config)
        if live_version != version:
            raise RuntimeError(
                f"pgstac live version mismatch: expected {version!r}, got {live_version!r}",
            )
