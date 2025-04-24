"""Integration tests for YDB MCP server.

These tests assume a YDB server is running at localhost:2136 with a database /local,
or will create a Docker container with YDB if none is available.
They directly test the YDBMCPServer methods without using HTTP.
"""

import json
import logging
import os
import time
import warnings

import pytest

# Fixtures are automatically imported by pytest from conftest.py
from tests.integration.conftest import call_mcp_tool

# Suppress the utcfromtimestamp deprecation warning from the YDB library
warnings.filterwarnings(
    "ignore", message="datetime.datetime.utcfromtimestamp.*", category=DeprecationWarning
)

# Table name used for tests - using timestamp to avoid conflicts
TEST_TABLE = f"mcp_integration_test_{int(time.time())}"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use loop_scope instead of scope for the asyncio marker
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_simple_query(mcp_server):
    """Test a basic YDB query."""
    result = await call_mcp_tool(mcp_server, "ydb_query", sql="SELECT 1+1 as result")

    assert "result_sets" in result, f"No result_sets in result: {result}"
    assert (
        len(result["result_sets"]) == 1
    ), f"Expected 1 result set, got {len(result['result_sets'])}"

    first_result = result["result_sets"][0]
    assert "columns" in first_result, f"No columns in result: {first_result}"
    assert "rows" in first_result, f"No rows in result: {first_result}"
    assert len(first_result["rows"]) > 0, f"Empty result set: {first_result}"
    assert (
        first_result["columns"][0] == "result"
    ), f"Unexpected column name: {first_result['columns'][0]}"
    assert first_result["rows"][0][0] == 2, f"Unexpected result value: {first_result['rows'][0][0]}"


async def test_create_table_and_query(mcp_server):
    """Test creating a table and executing queries against it."""
    # Generate a unique table name to avoid conflicts with other tests
    test_table_name = f"temp_test_table_{int(time.time())}"

    try:
        # Create table
        create_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                CREATE TABLE {test_table_name} (
                    id Uint64,
                    name Utf8,
                    PRIMARY KEY (id)
                );
            """,
        )
        assert "error" not in create_result, f"Error creating table: {create_result}"

        # Insert data
        insert_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                UPSERT INTO {test_table_name} (id, name)
                VALUES (1, 'Test 1'), (2, 'Test 2'), (3, 'Test 3');
            """,
        )
        assert "error" not in insert_result, f"Error inserting data: {insert_result}"

        # Query data
        query_result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"SELECT * FROM {test_table_name} ORDER BY id;"
        )

        assert "result_sets" in query_result, f"No result_sets in query result: {query_result}"
        assert (
            len(query_result["result_sets"]) == 1
        ), f"Expected 1 result set, got {len(query_result['result_sets'])}"

        first_result = query_result["result_sets"][0]
        assert "columns" in first_result, f"No columns in result: {first_result}"
        assert "rows" in first_result, f"No rows in result: {first_result}"
        assert len(first_result["rows"]) == 3, f"Expected 3 rows, got {len(first_result['rows'])}"

        # Check if 'id' and 'name' columns are present
        assert (
            "id" in first_result["columns"]
        ), f"Column 'id' not found in {first_result['columns']}"
        assert (
            "name" in first_result["columns"]
        ), f"Column 'name' not found in {first_result['columns']}"

        # Get column indexes
        id_idx = first_result["columns"].index("id")
        name_idx = first_result["columns"].index("name")

        # Verify values
        assert (
            first_result["rows"][0][id_idx] == 1
        ), f"Expected id=1, got {first_result['rows'][0][id_idx]}"

        # YDB may return strings as bytes, so handle both cases
        name_value = first_result["rows"][0][name_idx]
        if isinstance(name_value, bytes):
            assert (
                name_value.decode("utf-8") == "Test 1"
            ), f"Expected name='Test 1', got {name_value.decode('utf-8')}"
        else:
            assert name_value == "Test 1", f"Expected name='Test 1', got {name_value}"

    finally:
        # Cleanup - drop the table after test
        cleanup_result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"DROP TABLE {test_table_name};"
        )
        logger.debug(f"Table cleanup result: {cleanup_result}")


async def test_parameterized_query(mcp_server):
    """Test a parameterized query using the parameters feature of YDB."""
    # Test with a simple parameterized query
    result = await call_mcp_tool(
        mcp_server,
        "ydb_query_with_params",
        sql="""
            DECLARE $answer AS Int32;
            DECLARE $greeting AS Utf8;
            SELECT $answer as answer, $greeting as greeting
        """,
        params=json.dumps(
            {"answer": [-42, "Int32"], "greeting": "hello"}  # Explicitly specify the type as Int32
        ),
    )

    assert "result_sets" in result, f"No result_sets in result: {result}"
    assert (
        len(result["result_sets"]) == 1
    ), f"Expected 1 result set, got {len(result['result_sets'])}"

    first_result = result["result_sets"][0]
    assert "columns" in first_result, f"No columns in result: {first_result}"
    assert "rows" in first_result, f"No rows in result: {first_result}"
    assert len(first_result["rows"]) > 0, f"Empty result set: {first_result}"

    # Check column names
    assert (
        "answer" in first_result["columns"]
    ), f"Expected 'answer' column in result: {first_result['columns']}"
    assert (
        "greeting" in first_result["columns"]
    ), f"Expected 'greeting' column in result: {first_result['columns']}"

    # Check values
    answer_idx = first_result["columns"].index("answer")
    greeting_idx = first_result["columns"].index("greeting")

    assert (
        first_result["rows"][0][answer_idx] == -42
    ), f"Expected answer=-42, got {first_result['rows'][0][answer_idx]}"

    # YDB may return strings either as bytes or as strings depending on context
    greeting_value = first_result["rows"][0][greeting_idx]
    if isinstance(greeting_value, bytes):
        # If bytes, decode to string
        assert (
            greeting_value.decode("utf-8") == "hello"
        ), f"Expected greeting to decode to 'hello', got {greeting_value.decode('utf-8')}"
    else:
        # If already string
        assert greeting_value == "hello", f"Expected greeting to be 'hello', got {greeting_value}"


async def test_complex_query(mcp_server):
    """Test a more complex query with multiple result sets."""
    result = await call_mcp_tool(
        mcp_server,
        "ydb_query",
        sql="""
            SELECT 1 as value;
            SELECT 'test' as text, 2.5 as number;
        """,
    )

    # Check for result sets
    assert "result_sets" in result, f"No result_sets in result: {result}"
    assert (
        len(result["result_sets"]) == 2
    ), f"Expected 2 result sets, got {len(result['result_sets'])}"

    # Verify first result set
    first_result = result["result_sets"][0]
    assert "columns" in first_result
    assert first_result["columns"][0] == "value"
    assert first_result["rows"][0][0] == 1

    # Verify second result set
    second_result = result["result_sets"][1]
    assert "columns" in second_result
    assert len(second_result["columns"]) == 2
    assert second_result["columns"][0] == "text"
    assert second_result["columns"][1] == "number"
    # The text value is returned as a binary string, need to handle this
    text_value = second_result["rows"][0][0]
    if isinstance(text_value, bytes):
        assert text_value.decode("utf-8") == "test"
    else:
        assert text_value == "test"
    assert second_result["rows"][0][1] == 2.5


async def test_multiple_resultsets_with_tables(mcp_server):
    """Test multiple result sets involving table creation and queries."""
    # Create unique table names with timestamp to avoid conflicts
    test_table1 = f"temp_test_table1_{int(time.time())}"
    test_table2 = f"temp_test_table2_{int(time.time())}"

    try:
        # First, create tables - schema operations need to be separate
        setup_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                CREATE TABLE {test_table1} (id Uint64, name Utf8, PRIMARY KEY (id));
                CREATE TABLE {test_table2} (id Uint64, value Double, PRIMARY KEY (id));
            """,
        )
        assert "error" not in setup_result, f"Error creating tables: {setup_result}"

        # Then insert data - separate operation
        insert_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                UPSERT INTO {test_table1} (id, name) VALUES (1, 'First'), (2, 'Second'), (3, 'Third');
            """,
        )
        assert "error" not in insert_result, f"Error inserting data into table1: {insert_result}"

        insert_result2 = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                UPSERT INTO {test_table2} (id, value) VALUES (1, 10.5), (2, 20.75), (3, 30.25);
            """,
        )
        assert "error" not in insert_result2, f"Error inserting data into table2: {insert_result2}"

        # Now query both tables in a single request to test multiple result sets
        result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                SELECT * FROM {test_table1} ORDER BY id;
                SELECT * FROM {test_table2} ORDER BY id;
            """,
        )

        # Verify we have all result sets
        assert "result_sets" in result, f"No result_sets in result: {result}"
        assert (
            len(result["result_sets"]) == 2
        ), f"Expected 2 result sets, got {len(result['result_sets'])}"

        # Check first table results
        first_result = result["result_sets"][0]
        assert len(first_result["rows"]) == 3, "Expected 3 rows in first table"
        assert (
            "id" in first_result["columns"]
        ), f"Expected 'id' column in first table, got {first_result['columns']}"
        assert (
            "name" in first_result["columns"]
        ), f"Expected 'name' column in first table, got {first_result['columns']}"

        # Check second table results
        second_result = result["result_sets"][1]
        assert len(second_result["rows"]) == 3, "Expected 3 rows in second table"
        assert (
            "id" in second_result["columns"]
        ), f"Expected 'id' column in second table, got {second_result['columns']}"
        assert (
            "value" in second_result["columns"]
        ), f"Expected 'value' column in second table, got {second_result['columns']}"

        # Now test a join query - should return a single result set
        join_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                SELECT t1.id, t1.name, t2.value
                FROM {test_table1} t1
                JOIN {test_table2} t2 ON t1.id = t2.id
                ORDER BY t1.id;
            """,
        )

        # Validate join results
        assert "result_sets" in join_result, "Join query should return result_sets"
        assert (
            len(join_result["result_sets"]) == 1
        ), f"Expected 1 result set for join, got {len(join_result['result_sets'])}"

        first_join_result = join_result["result_sets"][0]
        assert "columns" in first_join_result, "Join query should return columns"
        assert "rows" in first_join_result, "Join query should return rows"
        assert (
            len(first_join_result["rows"]) == 3
        ), f"Expected 3 rows in join result, got {len(first_join_result['rows'])}"
        assert (
            len(first_join_result["columns"]) == 3
        ), f"Expected 3 columns in join result, got {len(first_join_result['columns'])}"

    finally:
        # Cleanup - drop the tables after test
        try:
            cleanup_result = await call_mcp_tool(
                mcp_server, "ydb_query", sql=f"DROP TABLE {test_table1};"
            )
            logger.debug(f"Table1 cleanup result: {cleanup_result}")

            cleanup_result2 = await call_mcp_tool(
                mcp_server, "ydb_query", sql=f"DROP TABLE {test_table2};"
            )
            logger.debug(f"Table2 cleanup result: {cleanup_result2}")
        except Exception as e:
            logger.warning(f"Failed to clean up test tables: {e}")


async def test_single_resultset_format(mcp_server):
    """Test that single result set queries use the new format with result_sets list."""
    result = await call_mcp_tool(mcp_server, "ydb_query", sql="SELECT 42 as answer")

    # Single result set should have result_sets key with one item
    assert "result_sets" in result, "Single result should include result_sets key"
    assert (
        len(result["result_sets"]) == 1
    ), f"Expected 1 result set, got {len(result['result_sets'])}"

    first_result = result["result_sets"][0]
    assert "columns" in first_result, "Should have columns in result set"
    assert "rows" in first_result, "Should have rows in result set"
    assert first_result["columns"][0] == "answer", "Expected 'answer' column"
    assert first_result["rows"][0][0] == 42, "Expected value 42"


async def test_data_types(mcp_server):
    """Test a very basic query with simple parameter types."""

    # Execute a simple query that doesn't use complex parameter types
    result = await call_mcp_tool(mcp_server, "ydb_query", sql="SELECT 1 AS value, 'test' AS text")

    # Basic result checks
    assert "result_sets" in result, f"No result_sets in result: {result}"
    assert len(result["result_sets"]) == 1, f"Expected 1 result set, got {len(result['result_sets'])}"

    first_result = result["result_sets"][0]
    assert "columns" in first_result, f"No columns in result: {first_result}"
    assert "rows" in first_result, f"No rows in result: {first_result}"
    assert len(first_result["columns"]) == 2, f"Expected 2 columns, got {len(first_result['columns'])}"
    assert len(first_result["rows"]) == 1, f"Expected 1 row, got {len(first_result['rows'])}"

    # Verify column names
    assert "value" in first_result["columns"], f"Expected column 'value', got {first_result['columns']}"
    assert "text" in first_result["columns"], f"Expected column 'text', got {first_result['columns']}"

    # Verify values
    row = first_result["rows"][0]
    value_idx = first_result["columns"].index("value")
    text_idx = first_result["columns"].index("text")

    assert row[value_idx] == 1, f"Expected value=1, got {row[value_idx]}"

    assert row[text_idx] == b"test", f"Expected text='test', got {row[text_idx]}"


async def test_all_data_types(mcp_server):
    """Test all supported YDB data types to ensure proper round-trip processing."""

    # Construct a query with literals of all supported data types
    result = await call_mcp_tool(
        mcp_server,
        "ydb_query",
        sql="""
            SELECT
                -- Boolean type
                true AS bool_true,
                false AS bool_false,

                -- Integer types (signed)
                -128 AS int8_min,
                127 AS int8_max,
                -32768 AS int16_min,
                32767 AS int16_max,
                -2147483648 AS int32_min,
                2147483647 AS int32_max,
                -9223372036854775808 AS int64_min,
                9223372036854775807 AS int64_max,

                -- Integer types (unsigned)
                0 AS uint8_min,
                255 AS uint8_max,
                0 AS uint16_min,
                65535 AS uint16_max,
                0 AS uint32_min,
                4294967295 AS uint32_max,
                0 AS uint64_min,
                18446744073709551615 AS uint64_max,

                -- Floating point types
                3.14 AS float_value,
                2.7182818284590452 AS double_value,

                -- String types
                "Hello, World!" AS string_value,
                "UTF8 строка" AS utf8_value,
                "00000000-0000-0000-0000-000000000000" AS uuid_value,
                '{"key": "value"}' AS json_value,

                -- Date and time types
                Date("2023-07-15") AS date_value,
                Datetime("2023-07-15T12:30:45Z") AS datetime_value,
                Timestamp("2023-07-15T12:30:45.123456Z") AS timestamp_value,
                INTERVAL("P1DT2H3M4.567S") AS interval_value,

                -- Decimal
                CAST("123.456789" AS Decimal(22,9)) AS decimal_value,

                -- Container types
                -- List containers
                AsList(1, 2, 3) AS int_list,
                AsList("a", "b", "c") AS string_list,

                -- Struct containers (similar to tuples)
                AsStruct(1 AS a, "x" AS b) AS simple_struct,

                -- Dictionary containers
                AsDict(
                    AsTuple("key1", 1),
                    AsTuple("key2", 2),
                    AsTuple("key3", 3)
                ) AS string_to_int_dict,

                -- Nested containers - list of structs
                AsList(
                    AsStruct(1 AS id, "Alice" AS name),
                    AsStruct(2 AS id, "Bob" AS name),
                    AsStruct(3 AS id, "Charlie" AS name)
                ) AS list_of_structs,

                -- Nested containers - struct with list
                AsStruct(
                    "users" AS collection_name,
                    AsList(1, 2, 3) AS ids,
                    true AS active
                ) AS struct_with_list,

                -- Dict with complex values
                AsDict(
                    AsTuple("person1", AsStruct(1 AS id, "Alice" AS name, AsList(25, 30, 28) AS scores)),
                    AsTuple("person2", AsStruct(2 AS id, "Bob" AS name, AsList(22, 27, 29) AS scores))
                ) AS complex_dict,

                -- Triple-nested container: list of structs with lists
                AsList(
                    AsStruct(
                        1 AS id,
                        "Team A" AS name,
                        AsList("Alice", "Bob") AS members
                    ),
                    AsStruct(
                        2 AS id,
                        "Team B" AS name,
                        AsList("Charlie", "David") AS members
                    )
                ) AS nested_list_struct_list,

                -- Tuple containers
                AsTuple(1, "a", true) AS mixed_tuple
        """,
    )

    # Basic result checks
    assert "result_sets" in result, f"No result_sets in result: {result}"
    assert (
        len(result["result_sets"]) == 1
    ), f"Expected 1 result set, got {len(result['result_sets'])}"

    first_result = result["result_sets"][0]
    assert "columns" in first_result, f"No columns in result: {first_result}"
    assert "rows" in first_result, f"No rows in result: {first_result}"
    assert len(first_result["rows"]) == 1, f"Expected 1 row, got {len(first_result['rows'])}"

    # Get the row
    row = first_result["rows"][0]

    # Helper function to get column index and value
    def get_value(column_name):
        try:
            idx = first_result["columns"].index(column_name)
            return row[idx]
        except ValueError:
            return None

    # Test each data type
    # Boolean values
    assert (
        get_value("bool_true") is True
    ), f"Expected bool_true to be True, got {get_value('bool_true')}"
    assert (
        get_value("bool_false") is False
    ), f"Expected bool_false to be False, got {get_value('bool_false')}"

    # Integer types (signed)
    assert (
        get_value("int8_min") == -128
    ), f"Expected int8_min to be -128, got {get_value('int8_min')}"
    assert get_value("int8_max") == 127, f"Expected int8_max to be 127, got {get_value('int8_max')}"
    assert (
        get_value("int16_min") == -32768
    ), f"Expected int16_min to be -32768, got {get_value('int16_min')}"
    assert (
        get_value("int16_max") == 32767
    ), f"Expected int16_max to be 32767, got {get_value('int16_max')}"
    assert (
        get_value("int32_min") == -2147483648
    ), f"Expected int32_min to be -2147483648, got {get_value('int32_min')}"
    assert (
        get_value("int32_max") == 2147483647
    ), f"Expected int32_max to be 2147483647, got {get_value('int32_max')}"
    assert (
        get_value("int64_min") == -9223372036854775808
    ), f"Expected int64_min to be -9223372036854775808, got {get_value('int64_min')}"
    assert (
        get_value("int64_max") == 9223372036854775807
    ), f"Expected int64_max to be 9223372036854775807, got {get_value('int64_max')}"

    # Integer types (unsigned)
    assert get_value("uint8_min") == 0, f"Expected uint8_min to be 0, got {get_value('uint8_min')}"
    assert (
        get_value("uint8_max") == 255
    ), f"Expected uint8_max to be 255, got {get_value('uint8_max')}"
    assert (
        get_value("uint16_min") == 0
    ), f"Expected uint16_min to be 0, got {get_value('uint16_min')}"
    assert (
        get_value("uint16_max") == 65535
    ), f"Expected uint16_max to be 65535, got {get_value('uint16_max')}"
    assert (
        get_value("uint32_min") == 0
    ), f"Expected uint32_min to be 0, got {get_value('uint32_min')}"
    assert (
        get_value("uint32_max") == 4294967295
    ), f"Expected uint32_max to be 4294967295, got {get_value('uint32_max')}"
    assert (
        get_value("uint64_min") == 0
    ), f"Expected uint64_min to be 0, got {get_value('uint64_min')}"
    assert (
        get_value("uint64_max") == 18446744073709551615
    ), f"Expected uint64_max to be 18446744073709551615, got {get_value('uint64_max')}"

    # Floating point types
    assert (
        abs(get_value("float_value") - 3.14) < 0.0001
    ), f"Expected float_value to be close to 3.14, got {get_value('float_value')}"
    assert (
        abs(get_value("double_value") - 2.7182818284590452) < 0.0000000000000001
    ), f"Expected double_value to be close to 2.7182818284590452, got {get_value('double_value')}"

    # String types - YDB returns these as binary strings
    string_value = get_value("string_value")
    assert string_value == b"Hello, World!", f"Expected string_value to be b'Hello, World!', got {string_value}"

    utf8_value = get_value("utf8_value")
    assert utf8_value == "UTF8 строка".encode('utf-8'), f"Expected utf8_value to be 'UTF8 строка', got {utf8_value}"

    uuid_value = get_value("uuid_value")
    assert uuid_value == b"00000000-0000-0000-0000-000000000000", f"Expected uuid_value to be b'00000000-0000-0000-0000-000000000000', got {uuid_value}"

    json_value = get_value("json_value")
    assert json_value == b'{"key": "value"}', f"Expected json_value to be b'{{'key': 'value'}}', got {json_value}"

    # Date and time types - YDB returns these as Python datetime objects
    import datetime

    date_value = get_value("date_value")
    assert isinstance(date_value, datetime.date), f"Expected date_value to be datetime.date, got {type(date_value)}"
    assert date_value == datetime.date(2023, 7, 15), f"Expected date_value to be 2023-07-15, got {date_value}"

    datetime_value = get_value("datetime_value")
    assert isinstance(datetime_value, datetime.datetime), f"Expected datetime_value to be datetime.datetime, got {type(datetime_value)}"
    # Compare timezone-aware datetime objects
    expected_datetime = datetime.datetime(2023, 7, 15, 12, 30, 45, tzinfo=datetime.UTC)
    if datetime_value.tzinfo is None:
        datetime_value = datetime_value.replace(tzinfo=datetime.UTC)
    else:
        datetime_value = datetime_value.astimezone(datetime.UTC)
    assert datetime_value == expected_datetime, f"Expected datetime_value to be {expected_datetime}, got {datetime_value}"

    timestamp_value = get_value("timestamp_value")
    assert isinstance(timestamp_value, datetime.datetime), f"Expected timestamp_value to be datetime.datetime, got {type(timestamp_value)}"
    expected_timestamp = datetime.datetime(2023, 7, 15, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    if timestamp_value.tzinfo is None:
        timestamp_value = timestamp_value.replace(tzinfo=datetime.UTC)
    else:
        timestamp_value = timestamp_value.astimezone(datetime.UTC)
    assert timestamp_value == expected_timestamp, f"Expected timestamp_value to be {expected_timestamp}, got {timestamp_value}"

    interval_value = get_value("interval_value")
    assert isinstance(interval_value, datetime.timedelta), f"Expected interval_value to be datetime.timedelta, got {type(interval_value)}"
    # P1DT2H3M4.567S equals 1 day, 2 hours, 3 minutes, 4.567 seconds
    expected_interval = datetime.timedelta(
        days=1, hours=2, minutes=3, seconds=4, microseconds=567000
    )
    assert interval_value.total_seconds() == expected_interval.total_seconds(), f"Expected interval_value to be {expected_interval}, got {interval_value}"

    # Decimal - YDB returns Decimal objects
    from decimal import Decimal

    decimal_value = get_value("decimal_value")
    assert isinstance(decimal_value, Decimal), f"Expected decimal_value to be Decimal, got {type(decimal_value)}"
    assert decimal_value == Decimal("123.456789"), f"Expected decimal_value to be Decimal('123.456789'), got {decimal_value}"

    # Container types
    # List containers
    int_list = get_value("int_list")
    assert isinstance(int_list, list), f"Expected int_list to be a list, got {type(int_list)}"
    assert int_list == [1, 2, 3], f"Expected int_list to be [1, 2, 3], got {int_list}"

    string_list = get_value("string_list")
    assert isinstance(string_list, list), f"Expected string_list to be a list, got {type(string_list)}"
    assert string_list == [b"a", b"b", b"c"], f"Expected string_list to be [b'a', b'b', b'c'], got {string_list}"

    # Struct containers (similar to Python dictionaries)
    simple_struct = get_value("simple_struct")
    assert isinstance(simple_struct, dict), f"Expected simple_struct to be a dict, got {type(simple_struct)}"
    assert "a" in simple_struct and "b" in simple_struct, f"Expected simple_struct to have keys 'a' and 'b', got {simple_struct}"
    assert simple_struct["a"] == 1, f"Expected simple_struct['a'] to be 1, got {simple_struct['a']}"
    assert simple_struct["b"] == b"x", f"Expected simple_struct['b'] to be b'x', got {simple_struct['b']}"

    # Dictionary containers
    string_to_int_dict = get_value("string_to_int_dict")
    assert isinstance(string_to_int_dict, dict), f"Expected string_to_int_dict to be a dict, got {type(string_to_int_dict)}"
    assert string_to_int_dict == {b"key1": 1, b"key2": 2, b"key3": 3}, f"Expected dict to be {{b'key1': 1, b'key2': 2, b'key3': 3}}, got {string_to_int_dict}"

    # Nested containers - list of structs
    list_of_structs = get_value("list_of_structs")
    assert isinstance(list_of_structs, list), f"Expected list_of_structs to be a list, got {type(list_of_structs)}"
    assert len(list_of_structs) == 3, f"Expected list_of_structs to have 3 items, got {len(list_of_structs)}"

    # Check first item in list of structs
    first_struct = list_of_structs[0]
    assert isinstance(first_struct, dict), f"Expected first_struct to be a dict, got {type(first_struct)}"
    assert first_struct == {"id": 1, "name": b"Alice"}, f"Expected first_struct to be {{'id': 1, 'name': b'Alice'}}, got {first_struct}"

    # Struct with list
    struct_with_list = get_value("struct_with_list")
    assert isinstance(struct_with_list, dict), f"Expected struct_with_list to be a dict, got {type(struct_with_list)}"
    assert struct_with_list == {
        "collection_name": b"users",
        "ids": [1, 2, 3],
        "active": True
    }, f"Expected struct_with_list to be {{'collection_name': b'users', 'ids': [1, 2, 3], 'active': True}}, got {struct_with_list}"

    # Complex dict
    complex_dict = get_value("complex_dict")
    assert isinstance(complex_dict, dict), f"Expected complex_dict to be a dict, got {type(complex_dict)}"
    expected_complex_dict = {
        b"person1": {"id": 1, "name": b"Alice", "scores": [25, 30, 28]},
        b"person2": {"id": 2, "name": b"Bob", "scores": [22, 27, 29]}
    }
    assert complex_dict == expected_complex_dict, f"Expected complex_dict to be {expected_complex_dict}, got {complex_dict}"

    # Triple-nested list
    nested_list = get_value("nested_list_struct_list")
    assert isinstance(nested_list, list), f"Expected nested_list to be a list, got {type(nested_list)}"
    assert len(nested_list) == 2, f"Expected nested_list to have 2 items, got {len(nested_list)}"

    expected_nested_list = [
        {"id": 1, "name": b"Team A", "members": [b"Alice", b"Bob"]},
        {"id": 2, "name": b"Team B", "members": [b"Charlie", b"David"]}
    ]
    assert nested_list == expected_nested_list, f"Expected nested_list to be {expected_nested_list}, got {nested_list}"

    # Tuple containers
    mixed_tuple = get_value("mixed_tuple")
    assert isinstance(mixed_tuple, (list, tuple)), f"Expected mixed_tuple to be a list or tuple, got {type(mixed_tuple)}"
    assert len(mixed_tuple) == 3, f"Expected mixed_tuple to have 3 items, got {len(mixed_tuple)}"
    expected_tuple = (1, b"a", True)
    # Convert to tuple if it's a list for comparison
    if isinstance(mixed_tuple, list):
        mixed_tuple = tuple(mixed_tuple)
    assert mixed_tuple == expected_tuple, f"Expected mixed_tuple to be {expected_tuple}, got {mixed_tuple}"


async def test_list_directory_integration(mcp_server):
    """Test listing directory contents in YDB."""
    # List the contents of the root directory - this should always exist
    result = await call_mcp_tool(mcp_server, "ydb_list_directory", path="/")

    # Parse the JSON result
    assert isinstance(result, list), f"Expected list result, got {type(result)}"
    assert len(result) > 0, "Expected non-empty result list"

    item = result[0]
    assert isinstance(item, dict), f"Expected dict item, got {type(item)}"
    assert "type" in item, f"Missing 'type' field in item: {item}"
    assert "text" in item, f"Missing 'text' field in item: {item}"

    # Parse the JSON string within the text field
    dir_data = json.loads(item["text"])

    # Verify the structure
    assert "path" in dir_data, f"Missing 'path' field in dir_data: {dir_data}"
    assert dir_data["path"] == "/", f"Expected path to be '/', got {dir_data['path']}"
    assert "items" in dir_data, f"Missing 'items' field in dir_data: {dir_data}"

    # Root directory should have at least some items
    assert isinstance(
        dir_data["items"], list
    ), f"Expected items to be a list, got {type(dir_data['items'])}"
    assert len(dir_data["items"]) > 0, f"Expected non-empty directory, got {dir_data['items']}"

    # Verify at least one item has expected properties
    assert "name" in dir_data["items"][0], f"Missing 'name' field in item: {dir_data['items'][0]}"
    assert "type" in dir_data["items"][0], f"Missing 'type' field in item: {dir_data['items'][0]}"
    assert "owner" in dir_data["items"][0], f"Missing 'owner' field in item: {dir_data['items'][0]}"

    logger.debug(f"Found {len(dir_data['items'])} items in root directory")
    for item in dir_data["items"]:
        logger.debug(f"Item: {item['name']}, Type: {item['type']}")


async def test_list_directory_nonexistent_integration(mcp_server):
    """Test listing a nonexistent directory in YDB."""
    # Generate a random path that should not exist
    nonexistent_path = f"/nonexistent_{int(time.time())}"

    # Try to list a nonexistent directory
    result = await call_mcp_tool(mcp_server, "ydb_list_directory", path=nonexistent_path)

    # Parse the result
    assert isinstance(result, list), f"Expected list result, got {type(result)}"
    assert len(result) > 0, "Expected non-empty result list"

    item = result[0]
    assert isinstance(item, dict), f"Expected dict item, got {type(item)}"
    assert "type" in item, f"Missing 'type' field in item: {item}"
    assert "text" in item, f"Missing 'text' field in item: {item}"

    # Should contain an error message
    assert "Error" in item["text"], f"Expected error message, got: {item['text']}"


async def test_describe_path_integration(mcp_server):
    """Test describing paths in YDB."""
    # 1. First create a test table to describe
    test_table_name = f"describe_test_table_{int(time.time())}"

    try:
        # Create a table with various column types
        create_result = await call_mcp_tool(
            mcp_server,
            "ydb_query",
            sql=f"""
                CREATE TABLE {test_table_name} (
                    id Uint64,
                    name Utf8,
                    value Double,
                    created Timestamp,
                    PRIMARY KEY (id)
                );
            """,
        )
        assert "error" not in create_result, f"Error creating table: {create_result}"

        # Wait a moment for the table to be fully created
        time.sleep(1)

        # 2. Now describe the table
        result = await call_mcp_tool(mcp_server, "ydb_describe_path", path=f"/{test_table_name}")

        # Parse the JSON result
        assert isinstance(result, list), f"Expected list result, got {type(result)}"
        assert len(result) > 0, "Expected non-empty result list"

        item = result[0]
        assert isinstance(item, dict), f"Expected dict item, got {type(item)}"
        assert "type" in item, f"Missing 'type' field in item: {item}"
        assert "text" in item, f"Missing 'text' field in item: {item}"

        # Parse the JSON string within the text field
        try:
            path_data = json.loads(item["text"])
        except json.JSONDecodeError:
            # If we can't parse the JSON, it might be an error message
            assert (
                "Error" in item["text"]
            ), f"Expected error message if not valid JSON, got: {item['text']}"
            return

        # If we get a valid JSON response
        if "path" in path_data:
            # Verify the structure
            assert (
                path_data["path"] == f"/{test_table_name}"
            ), f"Expected path to be '/{test_table_name}', got {path_data['path']}"
            assert "type" in path_data, f"Missing 'type' field in path_data: {path_data}"
            assert (
                path_data["type"] == "TABLE"
            ), f"Expected type to be 'TABLE', got {path_data['type']}"

            # Verify table information
            assert "table" in path_data, f"Missing 'table' field in path_data: {path_data}"

    finally:
        # Clean up - drop the table even if test fails
        cleanup_result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"DROP TABLE {test_table_name};"
        )
        logger.debug(f"Table cleanup result: {cleanup_result}")


async def test_describe_nonexistent_path_integration(mcp_server):
    """Test describing a nonexistent path in YDB."""
    # Generate a random path that should not exist
    nonexistent_path = f"/nonexistent_{int(time.time())}"

    # Try to describe a nonexistent path
    result = await call_mcp_tool(mcp_server, "ydb_describe_path", path=nonexistent_path)

    # Parse the result
    assert isinstance(result, list), f"Expected list result, got {type(result)}"
    assert len(result) > 0, "Expected non-empty result list"

    item = result[0]
    assert isinstance(item, dict), f"Expected dict item, got {type(item)}"
    assert "type" in item, f"Missing 'type' field in item: {item}"
    assert "text" in item, f"Missing 'text' field in item: {item}"

    # Should contain an error message
    assert "Error" in item["text"], f"Expected error message, got: {item['text']}"


async def test_ydb_status_integration(mcp_server):
    """Test getting YDB connection status."""
    result = await call_mcp_tool(mcp_server, "ydb_status")

    # Parse the JSON result
    assert isinstance(result, list), f"Expected list result, got {type(result)}"
    assert len(result) > 0, "Expected non-empty result list"

    item = result[0]
    assert isinstance(item, dict), f"Expected dict item, got {type(item)}"
    assert "type" in item, f"Missing 'type' field in item: {item}"
    assert "text" in item, f"Missing 'text' field in item: {item}"

    # Parse the JSON string within the text field
    status_data = json.loads(item["text"])

    # Verify the structure
    assert "status" in status_data, f"Missing 'status' field in status_data: {status_data}"
    assert "ydb_endpoint" in status_data, f"Missing 'ydb_endpoint' field in status_data: {status_data}"
    assert "ydb_database" in status_data, f"Missing 'ydb_database' field in status_data: {status_data}"
    assert "auth_mode" in status_data, f"Missing 'auth_mode' field in status_data: {status_data}"
    assert "ydb_connection" in status_data, f"Missing 'ydb_connection' field in status_data: {status_data}"

    # For a successful test run, we expect to be connected
    assert status_data["status"] == "running", f"Expected status to be 'running', got {status_data['status']}"
    assert status_data["ydb_connection"] == "connected", f"Expected ydb_connection to be 'connected', got {status_data['ydb_connection']}"
    assert status_data["error"] is None, f"Expected no error, got: {status_data.get('error')}"

    logger.info(f"YDB status check successful: {status_data}")
