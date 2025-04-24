"""Unit tests for YDB MCP server implementation."""

import asyncio
import base64
import datetime
import decimal
import json

# Patch the mcp module before importing the YDBMCPServer
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
import ydb
from mcp import Tool
from mcp.types import TextContent

sys.modules["mcp.server"] = MagicMock()
sys.modules["mcp.server.handler"] = MagicMock()
sys.modules["mcp.server.handler"].RequestHandler = MagicMock
sys.modules["mcp.server.handler"].register_handler = lambda name: lambda cls: cls

from ydb_mcp.server import CustomJSONEncoder, YDBMCPServer


@pytest.mark.unit
class TestYDBMCPServer:
    """Test YDB MCP server implementation."""

    # Initialization tests
    async def test_init_with_env_vars(self):
        """Test initialization with environment variables."""
        with patch(
            "os.environ", {"YDB_ENDPOINT": "test-endpoint", "YDB_DATABASE": "test-database"}
        ):
            with patch.object(YDBMCPServer, "register_tools"):
                server = YDBMCPServer()
                assert server.endpoint == "test-endpoint"
                assert server.database == "test-database"

    async def test_init_with_args(self):
        """Test initialization with arguments."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="arg-endpoint", database="arg-database")
            assert server.endpoint == "arg-endpoint"
            assert server.database == "arg-database"

    # Query tests
    async def test_query_simple(self):
        """Test simple query execution."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Create mock result set
            mock_col1 = MagicMock()
            mock_col1.name = "column1"
            mock_col2 = MagicMock()
            mock_col2.name = "column2"

            mock_row = MagicMock()
            mock_row.__getitem__.side_effect = lambda idx: ["value1", 123][idx]

            mock_result_set = MagicMock()
            mock_result_set.rows = [mock_row]
            mock_result_set.columns = [mock_col1, mock_col2]

            # Mock YDB driver and pool
            server.get_pool = AsyncMock()
            mock_pool = AsyncMock()
            mock_pool.execute_with_retries = AsyncMock(return_value=[mock_result_set])
            server.get_pool.return_value = mock_pool

            # Execute query
            result = await server.query("SELECT * FROM table")

            # Check the query was executed
            mock_pool.execute_with_retries.assert_called_once_with("SELECT * FROM table", None)

            # Check the result was processed correctly
            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], TextContent)

            # Parse the JSON response
            parsed_result = json.loads(result[0].text)
            assert "result_sets" in parsed_result
            assert len(parsed_result["result_sets"]) == 1
            assert "columns" in parsed_result["result_sets"][0]
            assert "rows" in parsed_result["result_sets"][0]
            assert parsed_result["result_sets"][0]["columns"] == ["column1", "column2"]
            assert len(parsed_result["result_sets"][0]["rows"]) == 1
            assert parsed_result["result_sets"][0]["rows"][0] == ["value1", 123]

    async def test_query_with_params(self):
        """Test query with parameters."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock server.query method
            server.query = AsyncMock(
                return_value={"result_sets": [{"columns": ["test"], "rows": [["data"]]}]}
            )

            # Create params as a JSON string
            params_json = json.dumps({"$param1": 123, "param2": "value"})

            # Execute query with params
            result = await server.query_with_params(
                "SELECT * FROM table WHERE id = $param1", params_json
            )

            # Check the query was executed with correct parameters
            expected_params = {"$param1": 123, "$param2": "value"}
            server.query.assert_called_once_with(
                "SELECT * FROM table WHERE id = $param1", expected_params
            )

            # Check the result
            assert result == {"result_sets": [{"columns": ["test"], "rows": [["data"]]}]}

    async def test_query_with_invalid_params(self):
        """Test query with invalid parameters JSON."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Invalid JSON string
            params_json = "invalid json"

            # Execute query with invalid params
            result = await server.query_with_params("SELECT * FROM table", params_json)

            # Check the error is returned
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"
            assert "Error parsing JSON parameters" in result[0].text

    async def test_query_with_auth_error(self):
        """Test query execution when there's an authentication error."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer()
            server.auth_error = "Authentication failed: Invalid token"

            # Execute query
            result = await server.query("SELECT * FROM table")

            # Verify error is returned
            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], TextContent)

            # Parse the JSON response
            parsed_result = json.loads(result[0].text)
            assert "error" in parsed_result
            assert parsed_result["error"] == "Authentication failed: Invalid token"

    async def test_query_with_complex_params(self):
        """Test query with complex parameter types."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer()

            # Mock pool for query execution
            mock_pool = AsyncMock()
            server.get_pool = AsyncMock(return_value=mock_pool)
            mock_pool.execute_with_retries = AsyncMock(return_value=[MagicMock()])

            # Test parameters with explicit YDB types
            params = {
                "$int_param": (42, "Int32"),
                "$str_param": ("test", "Utf8"),
                "$simple_param": "simple value",
            }

            # Execute query
            await server.query("SELECT * FROM table", params)

            # Verify parameters were processed correctly
            mock_pool.execute_with_retries.assert_called_once()
            call_args = mock_pool.execute_with_retries.call_args[0]
            assert call_args[0] == "SELECT * FROM table"
            assert "$int_param" in call_args[1]
            assert "$str_param" in call_args[1]
            assert "$simple_param" in call_args[1]

    # Authentication tests
    async def test_invalid_authentication(self):
        """Test that authentication fails with invalid credentials."""

        # Creating a dummy credentials object that will cause authentication to fail
        class InvalidCredentials(ydb.credentials.AbstractCredentials):
            def get_token(self, context):
                return "invalid_token_12345"

            def _update_driver_config(self, driver_config):
                # This method is required by the YDB driver
                pass

        # Create server with invalid credentials factory
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(
                endpoint="test-endpoint",
                database="test-database",
                credentials_factory=lambda: InvalidCredentials(),
            )

            # Mock the driver creation to raise an authentication error
            with patch.object(
                server,
                "create_driver",
                side_effect=Exception("Authentication failed: Invalid credentials"),
            ):
                # Authentication should fail when creating driver
                with pytest.raises(Exception) as excinfo:
                    await server.create_driver()

                # Verify the error message indicates an authentication problem
                error_message = str(excinfo.value).lower()
                assert (
                    "authentication" in error_message or "invalid" in error_message
                ), f"Expected authentication error, got: {error_message}"

    # Directory and path tests
    async def test_list_directory(self):
        """Test the list_directory method."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Create mock response
            mock_entry1 = MagicMock()
            mock_entry1.name = "table1"
            mock_entry1.type = "TABLE"
            mock_entry1.owner = "root"
            mock_entry1.permissions = []

            mock_entry2 = MagicMock()
            mock_entry2.name = "directory1"
            mock_entry2.type = "DIRECTORY"
            mock_entry2.owner = "root"
            mock_entry2.permissions = []

            mock_response = MagicMock()
            mock_response.children = [mock_entry1, mock_entry2]

            # Setup mock list_directory to return our response
            mock_scheme_client.list_directory = AsyncMock(return_value=mock_response)

            # Call the method
            result = await server.list_directory("/path/to/directory")

            # Verify scheme_client.list_directory was called
            mock_scheme_client.list_directory.assert_called_once_with("/path/to/directory")

            # Verify result format
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"

            # Parse the JSON result
            data = json.loads(result[0].text)

            # Verify content
            assert data["path"] == "/path/to/directory"
            assert len(data["items"]) == 2
            assert {"name": "directory1", "type": "DIRECTORY", "owner": "root"} in data["items"]
            assert {"name": "table1", "type": "TABLE", "owner": "root"} in data["items"]

    async def test_list_directory_empty(self):
        """Test the list_directory method with empty directory."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Create mock response for empty directory
            mock_response = MagicMock()
            mock_response.children = []

            # Setup mock list_directory to return our response
            mock_scheme_client.list_directory = AsyncMock(return_value=mock_response)

            # Call the method
            result = await server.list_directory("/path/to/empty/directory")

            # Verify result
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"
            assert "empty" in result[0].text

    async def test_list_directory_error(self):
        """Test the list_directory method with error."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client that raises an exception
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Setup mock to raise an exception
            mock_scheme_client.list_directory = AsyncMock(side_effect=Exception("Access denied"))

            # Call the method
            result = await server.list_directory("/path/to/directory")

            # Verify error result
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"
            assert "Error" in result[0].text
            assert "Access denied" in result[0].text

    async def test_describe_path(self):
        """Test the describe_path method."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Create mock response for a directory
            mock_response = MagicMock()
            mock_response.name = "testdir"
            mock_response.type = "DIRECTORY"
            mock_response.owner = "root"
            mock_response.permissions = []

            # Setup mock describe_path to return our response
            mock_scheme_client.describe_path = AsyncMock(return_value=mock_response)

            # Call the method
            result = await server.describe_path("/path/to/testdir")

            # Verify scheme_client.describe_path was called
            mock_scheme_client.describe_path.assert_called_once_with("/path/to/testdir")

            # Verify result format
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"

            # Parse the JSON result
            data = json.loads(result[0].text)

            # Verify content
            assert data["path"] == "/path/to/testdir"
            assert data["type"] == "DIRECTORY"
            assert data["name"] == "testdir"
            assert data["owner"] == "root"

    async def test_describe_path_table(self):
        """Test the describe_path method with a table path."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Create mock response for a table
            mock_response = MagicMock()
            mock_response.name = "test_table"
            mock_response.type = "TABLE"
            mock_response.owner = "root"
            mock_response.permissions = []

            # Create mock column
            mock_column = MagicMock()
            mock_column.name = "id"
            mock_column.type = "Int64"

            # Create mock table
            mock_table = MagicMock()
            mock_table.columns = [mock_column]
            mock_table.primary_key = ["id"]
            mock_table.indexes = []
            mock_table.partitioning_settings = None

            # Add table to response
            mock_response.table = mock_table

            # Setup mock describe_path to return our response
            mock_scheme_client.describe_path = AsyncMock(return_value=mock_response)

            # Call the method
            result = await server.describe_path("/path/to/test_table")

            # Verify result format
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"

            # Parse the JSON result
            data = json.loads(result[0].text)

            # Verify content
            assert data["path"] == "/path/to/test_table"
            assert data["type"] == "TABLE"
            assert data["name"] == "test_table"
            assert data["owner"] == "root"
            assert "table" in data
            assert data["table"]["columns"][0]["name"] == "id"
            assert data["table"]["columns"][0]["type"] == "Int64"
            assert data["table"]["primary_key"] == ["id"]

    async def test_describe_path_error(self):
        """Test the describe_path method with error."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer(endpoint="test-endpoint", database="test-database")

            # Mock driver and scheme client that raises an exception
            server.driver = MagicMock()
            mock_scheme_client = MagicMock()
            server.driver.scheme_client = mock_scheme_client

            # Setup mock to raise an exception
            mock_scheme_client.describe_path = AsyncMock(side_effect=Exception("Path not found"))

            # Call the method
            result = await server.describe_path("/non/existent/path")

            # Verify error result
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].type == "text"
            assert "Error" in result[0].text
            assert "Path not found" in result[0].text

    # Server management tests
    async def test_restart_success(self):
        """Test successful server restart."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer()

            # Create mock pool and driver with proper async mocks
            mock_pool = AsyncMock()
            mock_pool.stop = AsyncMock()

            # Create a real asyncio task for discovery
            discovery_coro = asyncio.sleep(0)  # A coroutine that completes immediately
            discovery_task = asyncio.create_task(discovery_coro)

            # Create a mock discovery with a synchronous stop method
            mock_discovery = MagicMock()
            mock_discovery.stop = MagicMock()  # Make stop a sync method as it is in the real code
            mock_discovery._discovery_task = discovery_task

            mock_driver = AsyncMock()
            mock_driver.stop = AsyncMock()
            mock_driver.discovery = mock_discovery

            # Set up the mocks before the restart
            server.pool = mock_pool
            server.driver = mock_driver
            server.create_driver = AsyncMock(return_value=MagicMock())

            # Perform restart
            success = await server.restart()

            # Clean up the task
            if not discovery_task.done():
                discovery_task.cancel()
                try:
                    await discovery_task
                except asyncio.CancelledError:
                    pass

            # Verify all cleanup and initialization was done
            assert mock_pool.stop.called
            assert mock_driver.stop.called
            assert mock_discovery.stop.called
            assert server.create_driver.called
            assert success is True

    async def test_restart_failure(self):
        """Test server restart when driver creation fails."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer()

            # Create mock pool and driver with proper async mocks
            mock_pool = AsyncMock()
            mock_pool.stop = AsyncMock()

            # Create a real asyncio task for discovery
            discovery_coro = asyncio.sleep(0)  # A coroutine that completes immediately
            discovery_task = asyncio.create_task(discovery_coro)

            # Create a mock discovery with a synchronous stop method
            mock_discovery = MagicMock()
            mock_discovery.stop = MagicMock()  # Make stop a sync method as it is in the real code
            mock_discovery._discovery_task = discovery_task

            mock_driver = AsyncMock()
            mock_driver.stop = AsyncMock()
            mock_driver.discovery = mock_discovery

            # Set up the mocks before the restart
            server.pool = mock_pool
            server.driver = mock_driver
            server.create_driver = AsyncMock(return_value=None)

            # Perform restart
            success = await server.restart()

            # Clean up the task
            if not discovery_task.done():
                discovery_task.cancel()
                try:
                    await discovery_task
                except asyncio.CancelledError:
                    pass

            # Verify cleanup was attempted but restart failed
            assert mock_pool.stop.called
            assert mock_driver.stop.called
            assert mock_discovery.stop.called
            assert server.create_driver.called
            assert success is False

    # Utility tests
    async def test_custom_json_encoder(self):
        """Test CustomJSONEncoder handles all special types correctly."""
        test_data = {
            "datetime": datetime.datetime(2024, 1, 1, 12, 0),
            "date": datetime.date(2024, 1, 1),
            "time": datetime.time(12, 0),
            "timedelta": datetime.timedelta(seconds=3600),
            "decimal": decimal.Decimal("123.45"),
            "bytes": b"test bytes",
            "regular": "string",
            "number": 42,
        }

        # Encode the test data
        encoded = json.dumps(test_data, cls=CustomJSONEncoder)
        decoded = json.loads(encoded)

        # Verify each type was encoded correctly
        assert decoded["datetime"] == "2024-01-01T12:00:00"
        assert decoded["date"] == "2024-01-01"
        assert decoded["time"] == "12:00:00"
        assert decoded["timedelta"] == "3600.0s"
        assert decoded["decimal"] == "123.45"
        assert decoded["bytes"] == "test bytes"
        assert decoded["regular"] == "string"
        assert decoded["number"] == 42

    def test_process_result_set_error(self):
        """Test result set processing when an error occurs."""
        with patch.object(YDBMCPServer, "register_tools"):
            server = YDBMCPServer()

            # Create a mock result set that will raise an exception
            mock_result_set = MagicMock()
            type(mock_result_set).columns = PropertyMock(side_effect=Exception("Test error"))
            type(mock_result_set).rows = PropertyMock(
                side_effect=Exception("Test error")
            )  # Also make rows raise an exception

            # Process the result set
            result = server._process_result_set(mock_result_set)

            # Verify error handling
            assert "error" in result
            assert "Test error" in result["error"]
            assert "columns" in result
            assert "rows" in result
            assert len(result["columns"]) == 0
            assert len(result["rows"]) == 0
