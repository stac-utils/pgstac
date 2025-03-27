#!/usr/bin/env python
"""
Example script demonstrating how to load queryables into PgSTAC.

This script shows how to use the load_queryables function both from the command line
and programmatically.
"""

import sys
from pathlib import Path

# Add the parent directory to the path so we can import pypgstac
sys.path.append(str(Path(__file__).parent.parent))

from pypgstac.pypgstac import PgstacCLI


def load_for_specific_collections(
    cli, sample_file, collection_ids, delete_missing=False,
):
    """Load queryables for specific collections.

    Args:
        cli: PgstacCLI instance
        sample_file: Path to the queryables file
        collection_ids: List of collection IDs to apply queryables to
        delete_missing: If True, delete properties not present in the file
    """
    cli.load_queryables(
        str(sample_file), collection_ids=collection_ids, delete_missing=delete_missing,
    )


def main():
    """Demonstrate loading queryables into PgSTAC."""
    # Get the path to the sample queryables file
    sample_file = Path(__file__).parent / "sample_queryables.json"

    # Check if the file exists
    if not sample_file.exists():
        return

    # Create a PgstacCLI instance
    # This will use the standard PostgreSQL environment variables for connection
    cli = PgstacCLI()

    # Load queryables for all collections
    cli.load_queryables(str(sample_file))

    # Example of loading for specific collections
    load_for_specific_collections(cli, sample_file, ["landsat-8", "sentinel-2"])

    # Example of loading queryables with delete_missing=True
    # This will delete properties not present in the file
    cli.load_queryables(str(sample_file), delete_missing=True)

    # Example of loading for specific collections with delete_missing=True
    # This will delete properties not present in the file, but only for the specified collections
    load_for_specific_collections(
        cli, sample_file, ["landsat-8", "sentinel-2"], delete_missing=True,
    )


if __name__ == "__main__":
    main()
