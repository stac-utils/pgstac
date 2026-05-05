"""Compatibility helpers for callers that still import legacy migration types."""

from __future__ import annotations

import glob
import os
from collections import defaultdict
from collections.abc import Iterator

MIGRATION_PREFIX = "pgstac--"


def base_migration_filename(version: str) -> str:
    """Return the canonical base migration filename for a version."""
    return f"{MIGRATION_PREFIX}{version}.sql"


def incremental_migration_filename(from_version: str, to_version: str) -> str:
    """Return the canonical incremental migration filename for a version hop."""
    return f"{MIGRATION_PREFIX}{from_version}--{to_version}.sql"


class MigrationPath:
    """Calculate path from migration files to get from one version to the next."""

    def __init__(self, path: str, f: str, t: str) -> None:
        """Initialize MigrationPath."""
        self.path = path
        if f is None:
            f = "init"
        if t is None:
            raise Exception('Must set "to" version')
        if f == t:
            raise Exception("No Migration Necessary")

        self.f = f
        self.t = t

    def parse_filename(self, filename: str) -> list[str]:
        """Get version numbers from filename."""
        filename = os.path.splitext(os.path.basename(filename))[0].replace(
            MIGRATION_PREFIX,
            "",
            1,
        )
        return filename.split("--")

    def get_files(self) -> Iterator[str]:
        """Find all migration files available."""
        path = self.path.rstrip("/")
        return glob.iglob(f"{path}/*.sql")

    def build_graph(self) -> dict[str, list[str]]:
        """Build a graph to get from one version to another."""
        graph = defaultdict(list)
        for file in self.get_files():
            parts = self.parse_filename(file)
            if len(parts) == 2:
                graph[parts[0]].append(parts[1])
            else:
                graph["init"].append(parts[0])
        return graph

    def build_path(self) -> list[str] | None:
        """Create the path of ordered files needed to migrate."""
        graph = self.build_graph()
        explored: list[str] = []
        q = [[self.f]]

        while q:
            path = q.pop(0)
            node = path[-1]
            if node not in explored:
                neighbours = graph[node]
                for neighbour in neighbours:
                    new_path = list(path)
                    new_path.append(neighbour)
                    q.append(new_path)
                    if neighbour == self.t:
                        return new_path
                explored.append(node)
        return None

    def migrations(self) -> list[str]:
        """Return the list of migrations needed in order."""
        path = self.build_path()
        if path is None:
            raise Exception(
                f"Could not determine path to get from {self.f} to {self.t}.",
            )
        if len(path) == 1:
            return []
        files = []
        start_idx = 0
        if path[0] == "init":
            files.append(base_migration_filename(path[1]))
            start_idx = 1
        for idx in range(start_idx, len(path) - 1):
            files.append(incremental_migration_filename(path[idx], path[idx + 1]))
        return files
