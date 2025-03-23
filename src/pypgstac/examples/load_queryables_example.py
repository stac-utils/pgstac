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


def load_for_specific_collections(cli, sample_file, collection_ids):
    """Load queryables for specific collections."""
    cli.load_queryables(str(sample_file), collection_ids=collection_ids)


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
    # Uncomment the following line to test with specific collections
    load_for_specific_collections(cli, sample_file, "landsat-8,sentinel-2")



if __name__ == "__main__":
    main()
