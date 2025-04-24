"""Integration tests for YDB directory and table operations (list_directory, describe_path, table creation, and cleanup).

These tests validate the functionality of YDB directory listing, path description, and table operations including creation and cleanup.
They test real YDB interactions without mocks, requiring a running YDB instance.
"""

import asyncio
import json
import logging
import os
import time

import pytest

# Import from conftest
from tests.integration.conftest import call_mcp_tool
from ydb_mcp.connection import YDBConnection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark these tests as integration and asyncio tests
pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def parse_text_content(response):
    """Parse TextContent response into a dictionary."""
    if not response:
        return response

    # Handle direct dictionary response
    if isinstance(response, dict):
        return response

    # Handle list of TextContent
    if isinstance(response, list) and len(response) > 0:
        if isinstance(response[0], dict) and "type" in response[0] and "text" in response[0]:
            try:
                return json.loads(response[0]["text"])
            except json.JSONDecodeError:
                return response[0]["text"]

    return response


async def test_list_root_directory(mcp_server):
    """Test listing the contents of the root directory."""
    result = await call_mcp_tool(mcp_server, "ydb_list_directory", path="/")
    result = parse_text_content(result)
    assert "error" not in result, f"Error listing root directory: {result}"
    assert "items" in result, f"Root directory listing should contain items: {result}"
    assert len(result["items"]) > 0, f"Root directory should not be empty: {result}"

    # Verify the structure of items
    for item in result["items"]:
        assert "name" in item, f"Item should have a name: {item}"
        assert "type" in item, f"Item should have a type: {item}"
        assert "owner" in item, f"Item should have an owner: {item}"


async def test_list_directory_after_table_creation(mcp_server):
    """Test that a newly created table appears in the directory listing."""
    # Use the same logic as the server to parse endpoint and database
    ydb_endpoint = os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136/local")
    conn = YDBConnection(ydb_endpoint)
    _, db_path = conn._parse_endpoint_and_database()

    # Generate a unique table name to avoid conflicts
    test_table_name = f"test_table_{int(time.time())}"

    try:
        # Create a new table in the current database (not as an absolute path)
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
        logger.debug(f"Created table {test_table_name}")

        # Wait a moment for the table to be fully created and visible
        await asyncio.sleep(1)

        # List the database directory
        path = db_path
        found = False
        items = []
        for _ in range(5):
            dir_list = await call_mcp_tool(mcp_server, "ydb_list_directory", path=path)
            parsed_dir = parse_text_content(dir_list)
            items = parsed_dir.get("items", []) if isinstance(parsed_dir, dict) else []
            if any(test_table_name == item.get("name") for item in items):
                found = True
                break
            await asyncio.sleep(1)
        assert found, f"Table {test_table_name} not found in directory listing: {items}"

    finally:
        # Clean up - drop the table
        cleanup_result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"DROP TABLE {test_table_name};"
        )
        logger.debug(f"Table cleanup result: {cleanup_result}")


async def test_path_description(mcp_server):
    """Test describing each item in the root directory."""
    # List the root directory
    result = await call_mcp_tool(mcp_server, "ydb_list_directory", path="/")
    parsed = parse_text_content(result)
    assert "items" in parsed, f"Root directory listing missing items: {parsed}"
    # Describe each item
    for item in parsed["items"]:
        item_name = item["name"]
        item_path = f"/{item_name}"
        describe_result = await call_mcp_tool(mcp_server, "ydb_describe_path", path=item_path)
        path_data = parse_text_content(describe_result)
        assert "path" in path_data, f"Missing 'path' field in path data: {path_data}"
        assert path_data["path"] == item_path, f"Expected path to be '{item_path}', got {path_data['path']}"
        assert "type" in path_data, f"Missing 'type' field in path data: {path_data}"
        assert "name" in path_data, f"Missing 'name' field in path data: {path_data}"
        assert "owner" in path_data, f"Missing 'owner' field in path data: {path_data}"
        if path_data["type"] == "TABLE":
            assert "table" in path_data, f"Missing 'table' field for TABLE: {path_data}"
            assert "columns" in path_data["table"], f"Missing 'columns' field in table data: {path_data}"
            assert len(path_data["table"]["columns"]) > 0, f"Table should have at least one column: {path_data}"
