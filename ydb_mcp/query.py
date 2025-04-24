import asyncio
import datetime
import decimal
import logging
import sys
from typing import Any, Dict, List

import ydb

from ydb_mcp.connection import YDBConnection

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executor for SQL queries against YDB."""

    def __init__(self, connection: YDBConnection):
        """Initialize the query executor.

        Args:
            connection: YDBConnection instance to use for executing queries
        """
        self.connection = connection
        self._session_pool = None

    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a read-only SQL query and return the results.

        Args:
            query: SQL query string to execute

        Returns:
            List of dictionaries representing rows of the query result

        Raises:
            Exception: If the query execution fails
        """
        if not self.connection.driver or not self.connection.session_pool:
            await self.connection.connect()

        self._session_pool = self.connection.session_pool

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._execute_query_sync, query)
            return result
        except Exception as e:
            # Only log real errors, not test errors
            if "Test error" not in str(e) or "pytest" not in sys.modules:
                logger.error(f"Error executing query: {e}")
            raise

    def _execute_query_sync(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query synchronously.

        This method is intended to be called by execute_query via run_in_executor.

        Args:
            query: SQL query string to execute

        Returns:
            List of dictionaries representing rows of the query result
        """

        def _execute_query(session):
            # Execute query and get result sets
            result_sets = session.transaction().execute(
                query,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
            )

            # Convert result sets to list of dictionaries
            result = []
            for rs in result_sets:
                for row in rs.rows:
                    result.append(self._convert_row_to_dict(row))
            return result

        return self._session_pool.retry_operation_sync(_execute_query)

    def _convert_row_to_dict(self, row: Any, col_names: List[str] = None) -> Dict[str, Any]:
        """Convert a YDB result row to a dictionary.

        Args:
            row: YDB result row
            col_names: Optional list of column names

        Returns:
            Dictionary representing the row data
        """
        result = {}
        for key, value in row.items():
            result[key] = self._convert_ydb_value(value)
        return result

    def _convert_ydb_value(self, value: Any) -> Any:
        """Convert YDB-specific types to Python types.

        Args:
            value: YDB value to convert

        Returns:
            Converted Python value
        """
        # Handle None/null values
        if value is None:
            return None

        # Handle bytes (strings in YDB are returned as bytes)
        if isinstance(value, bytes):
            # For now, keep all strings as bytes since we don't have type info
            return value

        # Handle date/time types
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
            return value

        # Handle Decimal type
        if isinstance(value, decimal.Decimal):
            return value

        # Handle container types
        if isinstance(value, list):
            return [self._convert_ydb_value(item) for item in value]
        if isinstance(value, dict):
            return {
                self._convert_ydb_value(k): self._convert_ydb_value(v) for k, v in value.items()
            }
        if isinstance(value, tuple):
            return tuple(self._convert_ydb_value(item) for item in value)

        # For all other types (int, float, bool), return as is
        return value
