"""Tests for pypgstac queryables functionality."""

from pathlib import Path
from unittest.mock import MagicMock, patch

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

    # Load the test queryables with index_fields specified for all fields
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        index_fields=[
            "test:string_prop",
            "test:number_prop",
            "test:integer_prop",
            "test:datetime_prop",
            "test:array_prop",
        ],
    )

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


def test_load_queryables_without_index_fields(db: PgstacDB) -> None:
    """Test pypgstac queryables loader without index_fields parameter."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables without index_fields
    cli.load_queryables(str(TEST_QUERYABLES_JSON))

    # Verify that the queryables were loaded without indexes
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

    # Check that all test properties were loaded with correct wrappers but no indexes
    assert len(queryables) == 5

    # Check that none of the properties have indexes
    for q in queryables:
        assert q["property_index_type"] is None


def test_load_queryables_with_specific_index_fields(db: PgstacDB) -> None:
    """Test pypgstac queryables loader with specific index_fields."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables with only specific index_fields
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        index_fields=["test:string_prop", "test:datetime_prop"],
    )

    # Verify that only the specified fields have indexes
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

    # Check that all properties are loaded
    assert len(queryables) == 5

    # Check that only the specified fields have indexes
    for q in queryables:
        if q["name"] in ["test:string_prop", "test:datetime_prop"]:
            assert q["property_index_type"] == "BTREE"
        else:
            assert q["property_index_type"] is None


def test_load_queryables_empty_index_fields(db: PgstacDB) -> None:
    """Test pypgstac queryables loader with empty index_fields."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables with empty index_fields
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        index_fields=[],
    )

    # Verify that no fields have indexes
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

    # Check that no fields have indexes
    for q in queryables:
        assert q["property_index_type"] is None


@patch("pypgstac.pypgstac.PgstacDB.connect")
def test_maintain_partitions_called_only_with_index_fields(mock_connect):
    """Test that maintain_partitions is only called when index_fields is provided."""
    # Mock the database connection
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Mock cursor
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Create a CLI instance with the mocked connection
    cli = PgstacCLI(dsn="mock_dsn")

    # Create a temporary file with test queryables
    test_file = HERE / "data-files" / "queryables" / "temp_test.json"
    with open(test_file, "w") as f:
        f.write(
            """
            {
                "type": "object",
                "title": "Test Properties",
                "properties": {
                    "test:prop1": {
                        "type": "string",
                        "title": "Test Property 1"
                    },
                    "test:prop2": {
                        "type": "integer",
                        "title": "Test Property 2"
                    }
                }
            }
            """,
        )

    # Case 1: With index_fields
    cli.load_queryables(
        str(test_file),
        index_fields=["test:prop1"],
    )

    # Check that maintain_partitions was called
    maintain_calls = [
        call_args
        for call_args in mock_cursor.execute.call_args_list
        if "maintain_partitions" in str(call_args)
    ]
    assert len(maintain_calls) == 1

    # Reset mock
    mock_cursor.reset_mock()

    # Case 2: Without index_fields
    cli.load_queryables(str(test_file))

    # Check that maintain_partitions was not called
    maintain_calls = [
        call_args
        for call_args in mock_cursor.execute.call_args_list
        if "maintain_partitions" in str(call_args)
    ]
    assert len(maintain_calls) == 0

    # Clean up
    test_file.unlink()


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
        index_fields=["test:string_prop"],
    )

    # Verify that the queryables were loaded with the correct collection IDs
    result = db.query(
        """
        SELECT name, collection_ids, property_index_type
        FROM queryables
        WHERE name LIKE 'test:%'
        ORDER BY name;
        """,
    )

    # Convert result to a list of dictionaries for easier assertion
    queryables = [
        {"name": row[0], "collection_ids": row[1], "property_index_type": row[2]}
        for row in result
    ]

    # Check that all queryables have the correct collection IDs
    assert len(queryables) == 5
    for q in queryables:
        assert set(q["collection_ids"]) == set(collection_ids)
        # Check that only test:string_prop has an index
        if q["name"] == "test:string_prop":
            assert q["property_index_type"] == "BTREE"
        else:
            assert q["property_index_type"] is None


def test_load_queryables_update(db: PgstacDB) -> None:
    """Test updating existing queryables."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # Load the test queryables with an index on number_prop
    cli.load_queryables(str(TEST_QUERYABLES_JSON), index_fields=["test:number_prop"])

    # Modify the test queryables file to change property wrappers
    # This is simulated by directly updating the database
    db.query(
        """
        UPDATE queryables
        SET property_wrapper = 'to_text'
        WHERE name = 'test:number_prop';
        """,
    )

    # Load the queryables again, but with a different index field
    cli.load_queryables(str(TEST_QUERYABLES_JSON), index_fields=["test:string_prop"])

    # Verify that the property wrapper was updated and index changed
    result = db.query(
        """
        SELECT name, property_wrapper, property_index_type
        FROM queryables
        WHERE name in ('test:number_prop', 'test:string_prop');
        """,
    )

    # Convert result to a list of dictionaries for easier assertion
    queryables = [
        {"name": row[0], "property_wrapper": row[1], "property_index_type": row[2]}
        for row in result
    ]

    # Find the properties
    number_prop = next(q for q in queryables if q["name"] == "test:number_prop")
    string_prop = next(q for q in queryables if q["name"] == "test:string_prop")

    # The property wrapper should be back to to_float
    assert number_prop["property_wrapper"] == "to_float"
    # The index should be removed from number_prop
    assert number_prop["property_index_type"] is None
    # The index should be added to string_prop
    assert string_prop["property_index_type"] == "BTREE"


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


def test_load_queryables_delete_missing(db: PgstacDB) -> None:
    """Test loading queryables with delete_missing=True."""
    # Create a CLI instance
    cli = PgstacCLI(dsn=db.dsn)

    # First, load the test queryables with indexes on all fields
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        index_fields=[
            "test:string_prop",
            "test:number_prop",
            "test:integer_prop",
            "test:datetime_prop",
            "test:array_prop",
        ],
    )

    # Create a temporary file with only one property
    partial_props_file = HERE / "data-files" / "queryables" / "partial_props.json"
    with open(partial_props_file, "w") as f:
        f.write(
            """
            {
                "type": "object",
                "title": "Partial Properties",
                "properties": {
                    "test:string_prop": {
                        "type": "string",
                        "title": "String Property"
                    }
                }
            }
            """,
        )

    # Load the partial queryables with delete_missing=True and index the string property
    cli.load_queryables(
        str(partial_props_file),
        delete_missing=True,
        index_fields=["test:string_prop"],
    )

    # Verify that only the string property remains and has an index
    result = db.query(
        """
        SELECT name, property_index_type
        FROM queryables
        WHERE name LIKE 'test:%'
        ORDER BY name;
        """,
    )

    # Convert result to a list of dictionaries
    queryables = [{"name": row[0], "property_index_type": row[1]} for row in result]

    # Check that only the string property remains and has an index
    assert len(queryables) == 1
    assert queryables[0]["name"] == "test:string_prop"
    assert queryables[0]["property_index_type"] == "BTREE"

    # Clean up
    partial_props_file.unlink()


def test_load_queryables_delete_missing_with_collections(
    db: PgstacDB,
    loader: Loader,
) -> None:
    """Test loading queryables with delete_missing=True and specific collections."""
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

    # First, load all test queryables for the specific collections with indexes
    cli.load_queryables(
        str(TEST_QUERYABLES_JSON),
        collection_ids=collection_ids,
        index_fields=[
            "test:string_prop",
            "test:number_prop",
            "test:integer_prop",
            "test:datetime_prop",
            "test:array_prop",
        ],
    )

    # Create a temporary file with only one property
    partial_props_file = HERE / "data-files" / "queryables" / "partial_props.json"
    with open(partial_props_file, "w") as f:
        f.write(
            """
            {
                "type": "object",
                "title": "Partial Properties",
                "properties": {
                    "test:string_prop": {
                        "type": "string",
                        "title": "String Property"
                    }
                }
            }
            """,
        )

    # Load the partial queryables with delete_missing=True for the specific collections
    # but without an index
    cli.load_queryables(
        str(partial_props_file),
        collection_ids=collection_ids,
        delete_missing=True,
    )

    # Verify that only the string property remains for the specific collections
    # and that it doesn't have an index
    result = db.query(
        """
        SELECT name, collection_ids, property_index_type
        FROM queryables
        WHERE name LIKE 'test:%'
        ORDER BY name;
        """,
    )

    # Convert result to a list of dictionaries
    queryables = [
        {"name": row[0], "collection_ids": row[1], "property_index_type": row[2]}
        for row in result
    ]

    # Filter queryables for the specific collections
    specific_queryables = [
        q
        for q in queryables
        if q["collection_ids"] and set(q["collection_ids"]) == set(collection_ids)
    ]

    # Check that only the string property remains for the specific collections
    assert len(specific_queryables) == 1
    assert specific_queryables[0]["name"] == "test:string_prop"
    # Verify it doesn't have an index
    assert specific_queryables[0]["property_index_type"] is None

    # Clean up
    partial_props_file.unlink()


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
        ValueError,
        match="No properties found in queryables definition",
    ):
        cli.load_queryables(str(no_props_file))

    # Clean up
    no_props_file.unlink()
