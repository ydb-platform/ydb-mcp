"""Integration tests for YDB directory and table operations.

These tests validate the functionality of directory listing and table description.
They test real YDB interactions without mocks, requiring a running YDB instance.
"""

import json
import logging

import pytest

# Import from conftest
from tests.integration.conftest import call_mcp_tool

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark these tests as integration and asyncio tests
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# We'll use the root directory which should already exist
ROOT_DIR = "/"


async def test_directory_listing(mcp_server):
    """Test listing a directory.

    This test lists the root directory and verifies the response structure.
    """
    # List the root directory
    logger.debug(f"Listing root directory contents")
    list_dir_result = await call_mcp_tool(mcp_server, "ydb_list_directory", path=ROOT_DIR)

    # Verify response structure for directory listing
    assert isinstance(list_dir_result, list), f"Expected list result, got {type(list_dir_result)}"
    assert len(list_dir_result) > 0, "Expected non-empty result list for directory listing"

    # Get the TextContent containing the directory data
    dir_content = list_dir_result[0]
    assert "text" in dir_content, f"Missing 'text' field in response: {dir_content}"

    # Parse the JSON response
    dir_data = json.loads(dir_content["text"])
    logger.debug(f"Root directory contents: {dir_data}")

    # Verify the directory data structure
    assert "path" in dir_data, f"Missing 'path' field in directory data: {dir_data}"
    assert dir_data["path"] == ROOT_DIR, f"Expected path to be '{ROOT_DIR}', got {dir_data['path']}"
    assert "items" in dir_data, f"Missing 'items' field in directory data: {dir_data}"

    # There should be at least one item in the root directory
    assert len(dir_data["items"]) > 0, "Root directory should not be empty"

    # Check the structure of the first item
    first_item = dir_data["items"][0]
    assert "name" in first_item, f"Missing 'name' field in item: {first_item}"
    assert "type" in first_item, f"Missing 'type' field in item: {first_item}"
    assert "owner" in first_item, f"Missing 'owner' field in item: {first_item}"

    # Return the directory data for potential use by other tests
    return dir_data


async def test_path_description(mcp_server):
    """Test describing a path.

    This test gets the root directory listing, chooses an item, and describes it.
    """
    # First, get the root directory listing
    dir_data = await test_directory_listing(mcp_server)

    # Choose an item to describe - preferably not the first one to avoid potential issues
    item_index = min(
        1, len(dir_data["items"]) - 1
    )  # Use index 1 if available, otherwise the last item
    item_to_describe = dir_data["items"][item_index]["name"]
    item_type = dir_data["items"][item_index]["type"]

    # Full path to the item
    item_path = f"{ROOT_DIR}{item_to_describe}"
    logger.debug(f"Found item to describe: {item_path} (type: {item_type})")

    # Describe the selected item
    logger.debug(f"Describing path: {item_path}")
    describe_result = await call_mcp_tool(mcp_server, "ydb_describe_path", path=item_path)

    # Verify response structure
    assert isinstance(describe_result, list), f"Expected list result, got {type(describe_result)}"
    assert len(describe_result) > 0, "Expected non-empty result list for path description"

    # Get the first result content
    result_content = describe_result[0]
    assert "text" in result_content, f"Missing 'text' field in result: {result_content}"

    result_text = result_content["text"]
    logger.debug(f"Describe result text: {result_text}")

    try:
        # Try to parse the JSON response
        path_data = json.loads(result_text)
        logger.debug(f"Path description: {path_data}")

        # Verify the path data
        assert "path" in path_data, f"Missing 'path' field in path data: {path_data}"
        assert (
            path_data["path"] == item_path
        ), f"Expected path to be '{item_path}', got {path_data['path']}"
        assert "type" in path_data, f"Missing 'type' field in path data: {path_data}"
        assert "name" in path_data, f"Missing 'name' field in path data: {path_data}"
        assert "owner" in path_data, f"Missing 'owner' field in path data: {path_data}"

        # Different checks depending on the type
        if path_data["type"] == "TABLE":
            assert "table" in path_data, f"Missing 'table' field for TABLE: {path_data}"

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        raise e
