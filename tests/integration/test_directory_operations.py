"""Integration tests for YDB directory operations (list_directory and describe_path).

These tests validate the functionality of the YDB directory management operations.
They test real YDB interactions without mocks, requiring a running YDB instance.
"""

import asyncio
import json
import logging
import time

import pytest

# Import from conftest
from tests.integration.conftest import YDB_DATABASE, call_mcp_tool

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark these tests as integration and asyncio tests
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(scope="session")]


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
    # Generate a unique table name to avoid conflicts
    test_table_name = f"test_table_{int(time.time())}"

    try:
        # Create a new table
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

        # List the root directory
        list_result = await call_mcp_tool(mcp_server, "ydb_list_directory", path=YDB_DATABASE)
        list_result = parse_text_content(list_result)
        assert "error" not in list_result, f"Error listing directory: {list_result}"
        assert "items" in list_result, f"Directory listing should contain items: {list_result}"

        logger.debug(f"Directory listing items: {list_result['items']}")

        # Find our table in the listing
        table_found = False
        for item in list_result["items"]:
            logger.debug(f"Checking item: {item}")
            if item["name"] == test_table_name:
                table_found = True
                assert item["type"] == "TABLE", f"Expected type 'TABLE', got {item['type']}"
                break

        assert table_found, f"Table {test_table_name} not found in directory listing"

    finally:
        # Clean up - drop the table
        cleanup_result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"DROP TABLE {test_table_name};"
        )
        logger.debug(f"Table cleanup result: {cleanup_result}")
