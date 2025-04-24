"""Tests for YDB MCP handler."""

import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Add mocks for mcp.server.handler
from tests.mocks import MockRequestHandler, mock_register_handler

# Mock the imports
sys.modules["mcp.server"] = MagicMock()
sys.modules["mcp.server.handler"] = MagicMock()
sys.modules["mcp.server.handler"].RequestHandler = MockRequestHandler
sys.modules["mcp.server.handler"].register_handler = mock_register_handler

# Import after mocking
from ydb_mcp.handler import YDBQueryHandler


class TestYDBQueryHandler(unittest.TestCase):
    """Test cases for YDBQueryHandler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.handler = YDBQueryHandler()

        # Mock the connection and executor
        self.handler.connection = MagicMock()
        self.handler.connection.connect = AsyncMock()
        self.handler.connection.close = AsyncMock()

        self.handler.executor = MagicMock()
        self.handler.executor.execute_query = AsyncMock()

        self.handler._connection_string = "grpc://ydb.server:2136/local"

    @patch("ydb_mcp.handler.YDBConnection")
    @patch("ydb_mcp.handler.QueryExecutor")
    async def test_initialize(self, mock_query_executor_class, mock_connection_class):
        """Test handler initialization."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_connection.connect = AsyncMock()
        mock_connection_class.return_value = mock_connection

        mock_executor = MagicMock()
        mock_query_executor_class.return_value = mock_executor

        # Reset handler to clean state
        self.handler.connection = None
        self.handler.executor = None
        self.handler._connection_string = None

        # Initialize handler
        await self.handler.initialize("grpc://ydb.server:2136/local")

        # Verify connection was created and initialized
        mock_connection_class.assert_called_once_with("grpc://ydb.server:2136/local")
        mock_connection.connect.assert_called_once()

        # Verify query executor was created
        mock_query_executor_class.assert_called_once_with(mock_connection)

        # Verify handler state
        self.assertEqual(self.handler.connection, mock_connection)
        self.assertEqual(self.handler.executor, mock_executor)
        self.assertEqual(self.handler._connection_string, "grpc://ydb.server:2136/local")

    @patch("ydb_mcp.handler.YDBConnection")
    @patch("ydb_mcp.handler.QueryExecutor")
    async def test_initialize_from_config(self, mock_query_executor_class, mock_connection_class):
        """Test handler initialization from config."""
        # Setup mocks
        mock_connection = MagicMock()
        mock_connection.connect = AsyncMock()
        mock_connection_class.return_value = mock_connection

        mock_executor = MagicMock()
        mock_query_executor_class.return_value = mock_executor

        # Set config
        self.handler.config = {"connection_string": "grpc://ydb.server:2136/local"}

        # Reset handler to clean state
        self.handler.connection = None
        self.handler.executor = None
        self.handler._connection_string = None

        # Initialize handler without connection string
        await self.handler.initialize()

        # Verify connection was created with config value
        mock_connection_class.assert_called_once_with("grpc://ydb.server:2136/local")
        mock_connection.connect.assert_called_once()

    async def test_initialize_no_connection_string(self):
        """Test initialization without connection string."""
        # Reset handler and config
        self.handler.connection = None
        self.handler.config = None
        self.handler._connection_string = None

        # Verify exception is raised
        with self.assertRaises(ValueError):
            await self.handler.initialize()

    async def test_handle_request(self):
        """Test request handling."""
        # Set up executor mock to return sample result
        self.handler.executor.execute_query.return_value = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"},
        ]

        # Skip initialization
        self.handler.initialize = AsyncMock()

        # Create request params
        params = {"sql": "SELECT * FROM test"}

        # Handle request
        result = await self.handler.handle_request(params)

        # Verify query was executed
        self.handler.executor.execute_query.assert_called_once_with("SELECT * FROM test")

        # Verify response format
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(result["result"]), 2)
        self.assertEqual(result["result"][0]["id"], 1)
        self.assertEqual(result["result"][1]["name"], "Test2")

    async def test_handle_request_no_sql(self):
        """Test handling request without SQL query."""
        # Handle request without SQL
        result = await self.handler.handle_request({})

        # Verify error response
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error"], "SQL query not provided")

    @patch("ydb_mcp.handler.logger.exception")
    async def test_handle_request_error(self, mock_logger_exception):
        """Test error handling in request handling."""
        # Skip initialization
        self.handler.initialize = AsyncMock()

        # Set up executor to raise exception
        self.handler.executor.execute_query.side_effect = Exception("Test error")

        # Handle request
        result = await self.handler.handle_request({"sql": "SELECT * FROM test"})

        # Verify error was logged
        mock_logger_exception.assert_called_once()

        # Verify error response
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error"], "Test error")

    async def test_shutdown(self):
        """Test handler shutdown."""
        # Call shutdown
        await self.handler.shutdown()

        # Verify connection was closed
        self.handler.connection.close.assert_called_once()


# Allow tests to run with asyncio
def run_async_test(test_case, test_func):
    """Run an async test function."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_func(test_case))


# Patch test methods to run with asyncio
for method_name in dir(TestYDBQueryHandler):
    if method_name.startswith("test_"):
        method = getattr(TestYDBQueryHandler, method_name)
        if asyncio.iscoroutinefunction(method):
            setattr(
                TestYDBQueryHandler, method_name, lambda self, m=method: run_async_test(self, m)
            )

if __name__ == "__main__":
    unittest.main()
