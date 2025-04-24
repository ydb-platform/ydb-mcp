"""Integration tests for YDB directory operations (list_directory and describe_path).

These tests validate the functionality of the YDB directory management operations.
They test real YDB interactions without mocks, requiring a running YDB instance.
"""

import asyncio
import json
import logging
import os
import time
from urllib.parse import urlparse

import pytest

# Import from conftest
from tests.integration.conftest import YDB_DATABASE, call_mcp_tool
from ydb_mcp.connection import YDBConnection

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
