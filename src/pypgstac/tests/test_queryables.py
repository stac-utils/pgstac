"""Tests for pypgstac queryables functionality."""

from pathlib import Path

import pytest

from pypgstac.db import PgstacDB
from pypgstac.load import Loader
from pypgstac.pypgstac import PgstacCLI

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "pgstac" / "tests" / "testdata"
TEST_COLLECTIONS_JSON = TEST_DATA_DIR / "collections.json"
TEST_QUERYABLES_JSON = HERE / "data-files" / "queryables" / "test_queryables.json"


def test_load_queryables_succeeds(db: PgstacDB) -> None:
    """Test pypgstac queryables loader."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables
    cli.load_queryables(str(TEST_QUERYABLES_JSON))

    # Verify that the queryables were loaded
    result = db.query(
        """
        SELECT name, property_wrapper, property_index_type
        FROM queryables
        WHERE name LIKE 'test:%'
        ORDER BY name;
        """,
    )

    # Convert result to a list of dictionaries for easier assertion
    queryables = [
        {"name": row[0], "property_wrapper": row[1], "property_index_type": row[2]}
        for row in result
    ]

    # Check that all test properties were loaded with correct wrappers
    assert len(queryables) == 5

    # Check string property
    string_prop = next(q for q in queryables if q["name"] == "test:string_prop")
    assert string_prop["property_wrapper"] == "to_text"
    assert string_prop["property_index_type"] == "BTREE"

    # Check number property
    number_prop = next(q for q in queryables if q["name"] == "test:number_prop")
    assert number_prop["property_wrapper"] == "to_float"
    assert number_prop["property_index_type"] == "BTREE"

    # Check integer property
    integer_prop = next(q for q in queryables if q["name"] == "test:integer_prop")
    assert integer_prop["property_wrapper"] == "to_int"
    assert integer_prop["property_index_type"] == "BTREE"

    # Check datetime property
    datetime_prop = next(q for q in queryables if q["name"] == "test:datetime_prop")
    assert datetime_prop["property_wrapper"] == "to_tstz"
    assert datetime_prop["property_index_type"] == "BTREE"

    # Check array property
    array_prop = next(q for q in queryables if q["name"] == "test:array_prop")
    assert array_prop["property_wrapper"] == "to_text_array"
    assert array_prop["property_index_type"] == "BTREE"


def test_load_queryables_with_collections(db: PgstacDB, loader: Loader) -> None:
    """Test pypgstac queryables loader with specific collections."""
    # Load test collections first
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode="insert",
    )

    # Get collection IDs from the database
    result = db.query("SELECT id FROM collections LIMIT 2;")
    collection_ids = [row[0] for row in result]

    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load queryables for specific collections
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        collection_ids=collection_ids,
    )

    # Verify that the queryables were loaded with the correct collection IDs
    result = db.query(
        """
        SELECT name, collection_ids
        FROM queryables
        WHERE name LIKE 'test:%'
        ORDER BY name;
        """,
    )

    # Convert result to a list of dictionaries for easier assertion
    queryables = [{"name": row[0], "collection_ids": row[1]} for row in result]

    # Check that all queryables have the correct collection IDs
    assert len(queryables) == 5
    for q in queryables:
        assert set(q["collection_ids"]) == set(collection_ids)


def test_load_queryables_update(db: PgstacDB) -> None:
    """Test updating existing queryables."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables
    cli.load_queryables(str(TEST_QUERYABLES_JSON))

    # Modify the test queryables file to change property wrappers
    # This is simulated by directly updating the database
    db.query(
        """
        UPDATE queryables
        SET property_wrapper = 'to_text'
        WHERE name = 'test:number_prop';
        """,
    )

    # Load the queryables again
    cli.load_queryables(str(TEST_QUERYABLES_JSON))

    # Verify that the property wrapper was updated
    result = db.query_one(
        """
        SELECT property_wrapper
        FROM queryables
        WHERE name = 'test:number_prop';
        """,
    )

    # The property wrapper should be back to to_float
    assert result == "to_float"


def test_load_queryables_invalid_json(db: PgstacDB) -> None:
    """Test loading queryables with invalid JSON."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Create a temporary file with invalid JSON
    invalid_json_file = HERE / "data-files" / "queryables" / "invalid.json"
    with open(invalid_json_file, "w") as f:
        f.write("{")

    # Loading should raise an exception
    with pytest.raises((ValueError, SyntaxError)):
        cli.load_queryables(str(invalid_json_file))

    # Clean up
    invalid_json_file.unlink()


def test_load_queryables_no_properties(db: PgstacDB) -> None:
    """Test loading queryables with no properties."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Create a temporary file with no properties
    no_props_file = HERE / "data-files" / "queryables" / "no_props.json"
    with open(no_props_file, "w") as f:
        f.write('{"type": "object", "title": "No Properties"}')

    # Loading should raise a ValueError
    with pytest.raises(
        ValueError, match="No properties found in queryables definition",
    ):
        cli.load_queryables(str(no_props_file))

    # Clean up
    no_props_file.unlink()
