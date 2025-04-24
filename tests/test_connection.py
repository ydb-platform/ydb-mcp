"""Tests for YDB connection module."""

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
from ydb_mcp.connection import YDBConnection


class TestYDBConnection(unittest.TestCase):
    """Test cases for YDBConnection class."""

    def test_extract_database_path(self):
        """Test database path extraction from connection string."""
        # Test with simple format
        conn = YDBConnection("grpc://ydb.server:2136/local")
        self.assertEqual(conn._extract_database_path(), "/local")

        # Test with path containing multiple segments
        conn = YDBConnection("grpc://ydb.server:2136/my/database/path")
        self.assertEqual(conn._extract_database_path(), "/my/database/path")

        # Test with query parameters
        conn = YDBConnection("grpc://ydb.server:2136/local?ssl=true&timeout=60")
        self.assertEqual(conn._extract_database_path(), "/local")

        # Test with database:// prefix
        conn = YDBConnection("database://ydb.server:2136/local")
        self.assertEqual(conn._extract_database_path(), "/local")

    @patch("ydb.Driver")
    async def test_connect(self, mock_driver_class):
        """Test connection establishment."""
        # Setup mocks
        mock_driver = MagicMock()
        mock_driver.wait.return_value = True
        mock_driver_class.return_value = mock_driver

        with patch("ydb.SessionPool") as mock_session_pool_class:
            # Setup session pool mock
            mock_session_pool = MagicMock()
            mock_session_pool_class.return_value = mock_session_pool

            # Create connection and connect
            conn = YDBConnection("grpc://ydb.server:2136/local")
            await conn.connect()

            # Verify driver was created with correct parameters
            mock_driver_class.assert_called_once()
            self.assertEqual(mock_driver.wait.call_count, 1)

            # Verify session pool was created
            mock_session_pool_class.assert_called_once()

            # Verify driver and session pool were stored
            self.assertEqual(conn.driver, mock_driver)
            self.assertEqual(conn.session_pool, mock_session_pool)

    @patch("ydb.Driver")
    async def test_connect_failure(self, mock_driver_class):
        """Test connection failure handling."""
        # Setup driver mock to fail connection
        mock_driver = MagicMock()
        mock_driver.wait.return_value = False
        mock_driver_class.return_value = mock_driver

        # Create connection and try to connect
        conn = YDBConnection("grpc://ydb.server:2136/local")

        # Verify connection raises exception
        with self.assertRaises(RuntimeError):
            await conn.connect()

    @patch("asyncio.get_event_loop")
    async def test_close(self, mock_get_event_loop):
        """Test connection closure."""
        # Create connection with mock driver and session pool
        conn = YDBConnection("grpc://ydb.server:2136/local")
        conn.driver = MagicMock()
        conn.session_pool = MagicMock()

        # Set up mock loop
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        # Set up run_in_executor to run the stop method immediately
        def run_in_executor_side_effect(executor, func, *args, **kwargs):
            func(*args, **kwargs)
            return asyncio.Future()

        # Configure the mock
        mock_future = asyncio.Future()
        mock_future.set_result(None)
        mock_loop.run_in_executor.side_effect = lambda *args: mock_future

        # Close connection
        await conn.close()

        # Verify session pool and driver stop methods were passed to run_in_executor
        self.assertEqual(mock_loop.run_in_executor.call_count, 2)

        # Verify driver and session pool were cleared
        self.assertIsNone(conn.driver)
        self.assertIsNone(conn.session_pool)


# Allow tests to run with asyncio
def run_async_test(test_case, test_func):
    """Run an async test function."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_func(test_case))


# Patch test methods to run with asyncio
for method_name in dir(TestYDBConnection):
    if method_name.startswith("test_") and method_name != "test_extract_database_path":
        method = getattr(TestYDBConnection, method_name)
        if asyncio.iscoroutinefunction(method):
            setattr(TestYDBConnection, method_name, lambda self, m=method: run_async_test(self, m))

if __name__ == "__main__":
    unittest.main()
