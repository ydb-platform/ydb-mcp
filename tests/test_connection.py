"""Tests for YDB connection module."""

import asyncio
import sys
import unittest
from unittest.mock import ANY, AsyncMock, MagicMock, patch

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

    @patch("ydb.aio.Driver")
    async def test_connect(self, mock_driver_class):
        """Test connection establishment."""
        # Setup mocks
        mock_driver = AsyncMock()
        mock_driver.wait = AsyncMock(return_value=True)
        mock_driver.discovery_debug_details = MagicMock(
            return_value="Resolved endpoints: localhost:2136"
        )
        mock_driver_class.return_value = mock_driver

        with patch("ydb.aio.QuerySessionPool") as mock_session_pool_class:
            # Setup session pool mock
            mock_session_pool = MagicMock()
            mock_session_pool_class.return_value = mock_session_pool

            # Create connection and connect
            conn = YDBConnection("grpc://ydb.server:2136/local")
            driver, pool = await conn.connect()

            # Verify driver was created with correct parameters
            mock_driver_class.assert_called_once()
            mock_driver.wait.assert_called_once()
            mock_driver.discovery_debug_details.assert_called()

            # Verify session pool was created
            mock_session_pool_class.assert_called_once()

            # Verify driver and session pool were stored and returned
            assert conn.driver == mock_driver
            assert conn.session_pool == mock_session_pool
            assert driver == mock_driver
            assert pool == mock_session_pool

            # Reset mocks for next test
            mock_driver_class.reset_mock()
            mock_driver.wait.reset_mock()
            mock_driver.discovery_debug_details.reset_mock()

    @patch("ydb.aio.Driver")
    async def test_connect_with_database_in_endpoint(self, mock_driver_class):
        """Test connection with database specified in endpoint."""
        # Setup mocks
        mock_driver = AsyncMock()
        mock_driver.wait = AsyncMock(return_value=True)
        mock_driver.discovery_debug_details = MagicMock(
            return_value="Resolved endpoints: localhost:2136"
        )
        mock_driver_class.return_value = mock_driver

        with patch("ydb.aio.QuerySessionPool") as mock_session_pool_class:
            # Setup session pool mock
            mock_session_pool = MagicMock()
            mock_session_pool_class.return_value = mock_session_pool

            # Test cases for different endpoint formats
            test_cases = [
                ("grpc://ydb.server:2136/local", "grpc://ydb.server:2136", "/local"),
                ("grpcs://ydb.server:2136/local/test", "grpcs://ydb.server:2136", "/local/test"),
                ("ydb.server:2136/local", "grpc://ydb.server:2136", "/local"),
                ("grpc://ydb.server:2136/local", "grpc://ydb.server:2136", "/local"),
            ]

            for endpoint, expected_endpoint, expected_database in test_cases:
                # Create connection and connect
                conn = YDBConnection(endpoint)
                await conn.connect()

                # Verify driver was created with correct parameters
                mock_driver_class.assert_called_with(
                    endpoint=expected_endpoint, database=expected_database, credentials=ANY
                )
                mock_driver.wait.assert_called_once()
                mock_driver.discovery_debug_details.assert_called()

                # Reset mock call count
                mock_driver_class.reset_mock()
                mock_driver.wait.reset_mock()
                mock_driver.discovery_debug_details.reset_mock()

    @patch("ydb.aio.Driver")
    async def test_connect_with_explicit_database(self, mock_driver_class):
        """Test connection with explicitly provided database."""
        # Setup mocks
        mock_driver = AsyncMock()
        mock_driver.wait = AsyncMock(return_value=True)
        mock_driver.discovery_debug_details = MagicMock(
            return_value="Resolved endpoints: localhost:2136"
        )
        mock_driver_class.return_value = mock_driver

        with patch("ydb.aio.QuerySessionPool") as mock_session_pool_class:
            # Setup session pool mock
            mock_session_pool = MagicMock()
            mock_session_pool_class.return_value = mock_session_pool

            # Test cases for different endpoint formats with explicit database
            test_cases = [
                (
                    "grpc://ydb.server:2136/local",
                    "/explicit",
                    "grpc://ydb.server:2136",
                    "/explicit",
                ),
                (
                    "grpcs://ydb.server:2136/local",
                    "explicit",
                    "grpcs://ydb.server:2136",
                    "/explicit",
                ),
                ("ydb.server:2136/local", "/other", "grpc://ydb.server:2136", "/other"),
            ]

            for endpoint, database, expected_endpoint, expected_database in test_cases:
                # Create connection and connect
                conn = YDBConnection(endpoint, database=database)
                await conn.connect()

                # Verify driver was created with correct parameters
                mock_driver_class.assert_called_with(
                    endpoint=expected_endpoint, database=expected_database, credentials=ANY
                )
                mock_driver.wait.assert_called_once()
                mock_driver.discovery_debug_details.assert_called()

                # Reset mock call count
                mock_driver_class.reset_mock()
                mock_driver.wait.reset_mock()
                mock_driver.discovery_debug_details.reset_mock()


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
