"""Tests for YDB query module."""

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

# Import modules after mocking
from ydb_mcp.connection import YDBConnection
from ydb_mcp.query import QueryExecutor


class TestQueryExecutor(unittest.TestCase):
    """Test cases for QueryExecutor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = MagicMock(spec=YDBConnection)
        self.mock_connection.driver = MagicMock()
        self.mock_connection.session_pool = MagicMock()
        self.mock_connection.connect = AsyncMock()

        self.executor = QueryExecutor(self.mock_connection)

    @patch("asyncio.get_event_loop")
    async def test_execute_query(self, mock_get_event_loop):
        """Test execute_query method."""
        # Set up mocks
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        # Configure future for run_in_executor
        mock_future = asyncio.Future()
        mock_future.set_result([{"id": 1, "name": "Test1"}, {"id": 2, "name": "Test2"}])
        mock_loop.run_in_executor.return_value = mock_future

        # Execute query
        result = await self.executor.execute_query("SELECT * FROM test")

        # Verify expected interactions
        mock_get_event_loop.assert_called_once()
        mock_loop.run_in_executor.assert_called_once()

        # Verify the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[1]["name"], "Test2")

    @patch("asyncio.get_event_loop")
    async def test_execute_query_with_connection_init(self, mock_get_event_loop):
        """Test execute_query with connection initialization."""
        # Setup connection without driver and session_pool
        self.mock_connection.driver = None
        self.mock_connection.session_pool = None

        # Set up loop mock
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        # Configure future for run_in_executor
        mock_future = asyncio.Future()
        mock_future.set_result([{"result": "ok"}])
        mock_loop.run_in_executor.return_value = mock_future

        # Execute query
        result = await self.executor.execute_query("SELECT 1")

        # Verify connection was initialized
        self.mock_connection.connect.assert_called_once()

        # Verify result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["result"], "ok")

    @patch("asyncio.get_event_loop")
    async def test_execute_query_error(self, mock_get_event_loop):
        """Test error handling in execute_query."""
        # Make run_in_executor raise an exception
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        # Create a future that raises an exception
        mock_future = asyncio.Future()
        mock_future.set_exception(Exception("Test error"))
        mock_loop.run_in_executor.return_value = mock_future

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            await self.executor.execute_query("SELECT * FROM test")

        self.assertIn("Test error", str(context.exception))

    def test_execute_query_sync(self):
        """Test _execute_query_sync method."""
        # Setup session pool to return mock session
        mock_session = MagicMock()
        self.mock_connection.session_pool.retry_operation_sync.side_effect = (
            lambda callback: callback(mock_session)
        )

        # Setup transaction mock
        mock_transaction = MagicMock()
        mock_session.transaction.return_value = mock_transaction

        # Setup mock result sets
        mock_col1 = MagicMock()
        mock_col1.name = "id"
        mock_col2 = MagicMock()
        mock_col2.name = "name"

        mock_row1 = MagicMock()
        mock_row1.__getitem__.side_effect = lambda idx: [1, "Test1"][idx]
        mock_row2 = MagicMock()
        mock_row2.__getitem__.side_effect = lambda idx: [2, "Test2"][idx]

        mock_rs1 = MagicMock()
        mock_rs1.columns = [mock_col1, mock_col2]
        mock_rs1.rows = [mock_row1, mock_row2]

        mock_transaction.execute.return_value = [mock_rs1]

        # Mock the _convert_row_to_dict method to return dictionary with expected keys
        self.executor._convert_row_to_dict = MagicMock()
        self.executor._convert_row_to_dict.side_effect = lambda row, col_names=None: {
            "id": row[0],
            "name": row[1],
        }

        # Call the method
        self.executor._session_pool = self.mock_connection.session_pool
        result = self.executor._execute_query_sync("SELECT * FROM test")

        # Verify expected interactions
        mock_session.transaction.assert_called_once()
        mock_transaction.execute.assert_called_once()

        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[1]["name"], "Test2")

    def test_convert_row_to_dict(self):
        """Test _convert_row_to_dict method."""
        # Create a mock YDB row
        mock_row = MagicMock()
        mock_row.items.return_value = [("id", 1), ("name", "Test"), ("active", True)]

        # Convert row to dict
        result = self.executor._convert_row_to_dict(mock_row)

        # Verify result
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["name"], "Test")
        self.assertEqual(result["active"], True)

    def test_convert_ydb_value_basic_types(self):
        """Test _convert_ydb_value with basic types."""
        # Test None
        self.assertIsNone(self.executor._convert_ydb_value(None))

        # Test basic types that should be returned as is
        self.assertEqual(self.executor._convert_ydb_value(42), 42)
        self.assertEqual(self.executor._convert_ydb_value(3.14), 3.14)
        self.assertEqual(self.executor._convert_ydb_value(True), True)
        self.assertEqual(self.executor._convert_ydb_value(False), False)

    def test_convert_ydb_value_bytes(self):
        """Test _convert_ydb_value with bytes."""
        # Test UTF-8 bytes
        utf8_bytes = "Hello, World!".encode("utf-8")
        self.assertEqual(self.executor._convert_ydb_value(utf8_bytes), utf8_bytes)

        # Test UTF-8 bytes with non-ASCII characters
        utf8_complex = "UTF8 строка".encode("utf-8")
        self.assertEqual(self.executor._convert_ydb_value(utf8_complex), utf8_complex)

        # Test non-UTF8 bytes
        binary_data = bytes([0x80, 0x81, 0x82])  # Invalid UTF-8
        self.assertEqual(self.executor._convert_ydb_value(binary_data), binary_data)

    def test_convert_ydb_value_datetime(self):
        """Test _convert_ydb_value with datetime types."""
        import datetime

        # Test datetime
        dt = datetime.datetime(2023, 1, 1, 12, 0)
        self.assertEqual(self.executor._convert_ydb_value(dt), dt)

        # Test date
        d = datetime.date(2023, 1, 1)
        self.assertEqual(self.executor._convert_ydb_value(d), d)

        # Test time
        t = datetime.time(12, 0)
        self.assertEqual(self.executor._convert_ydb_value(t), t)

        # Test timedelta
        td = datetime.timedelta(days=1, hours=2)
        self.assertEqual(self.executor._convert_ydb_value(td), td)

    def test_convert_ydb_value_decimal(self):
        """Test _convert_ydb_value with Decimal type."""
        from decimal import Decimal

        # Test decimal values
        d = Decimal("123.456")
        self.assertEqual(self.executor._convert_ydb_value(d), d)

    def test_convert_ydb_value_containers(self):
        """Test _convert_ydb_value with container types."""
        # Test list with mixed types
        test_list = [1, "test".encode("utf-8"), True]
        converted_list = self.executor._convert_ydb_value(test_list)
        self.assertEqual(converted_list, [1, b"test", True])

        # Test dict with mixed types
        test_dict = {"key1".encode("utf-8"): "value1".encode("utf-8"), "key2".encode("utf-8"): 42}
        converted_dict = self.executor._convert_ydb_value(test_dict)
        self.assertEqual(converted_dict, {b"key1": b"value1", b"key2": 42})

        # Test tuple with mixed types
        test_tuple = (1, "test".encode("utf-8"), True)
        converted_tuple = self.executor._convert_ydb_value(test_tuple)
        self.assertEqual(converted_tuple, (1, b"test", True))

    def test_convert_ydb_value_nested_structures(self):
        """Test _convert_ydb_value with nested data structures."""
        # Create a complex nested structure
        nested_data = {
            "string".encode("utf-8"): "value".encode("utf-8"),
            "list".encode("utf-8"): [
                1,
                "item".encode("utf-8"),
                {"nested_key".encode("utf-8"): "nested_value".encode("utf-8")},
            ],
            "dict".encode("utf-8"): {
                "key1".encode("utf-8"): [1, 2, "three".encode("utf-8")],
                "key2".encode("utf-8"): {"inner".encode("utf-8"): "value".encode("utf-8")},
            },
        }

        expected_result = {
            b"string": b"value",
            b"list": [1, b"item", {b"nested_key": b"nested_value"}],
            b"dict": {b"key1": [1, 2, b"three"], b"key2": {b"inner": b"value"}},
        }

        converted = self.executor._convert_ydb_value(nested_data)
        self.assertEqual(converted, expected_result)


# Allow tests to run with asyncio
def run_async_test(test_case, test_func):
    """Run an async test function."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_func(test_case))


# Patch test methods to run with asyncio
for method_name in dir(TestQueryExecutor):
    if method_name.startswith("test_"):
        method = getattr(TestQueryExecutor, method_name)
        if asyncio.iscoroutinefunction(method):
            setattr(TestQueryExecutor, method_name, lambda self, m=method: run_async_test(self, m))

if __name__ == "__main__":
    unittest.main()
