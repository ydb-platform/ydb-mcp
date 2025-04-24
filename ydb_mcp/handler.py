"""MCP handlers for YDB operations."""

import logging
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from ydb_mcp.connection import YDBConnection
from ydb_mcp.query import QueryExecutor

logger = logging.getLogger(__name__)


class YDBQueryHandler:
    """Handler for executing YDB SQL queries."""

    def __init__(self):
        """Initialize the handler."""
        self.connection = None
        self.executor = None
        self._connection_string = None
        self.config = None

    async def initialize(self, connection_string: str = None) -> None:
        """Initialize YDB connection.

        Args:
            connection_string: YDB connection string. If not provided, uses
                the value from config.
        """
        if not connection_string and self.config:
            connection_string = self.config.get("connection_string")

        if not connection_string:
            raise ValueError("YDB connection string not provided")

        if connection_string != self._connection_string or not self.connection or not self.executor:
            # Close existing connection if connection string changed
            if self.connection and self._connection_string != connection_string:
                await self.connection.close()

            self._connection_string = connection_string
            self.connection = YDBConnection(connection_string)
            await self.connection.connect()
            self.executor = QueryExecutor(self.connection)

    async def handle_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle YDB query request.

        Args:
            params: Request parameters containing SQL query

        Returns:
            Query results
        """
        try:
            # Get SQL query from parameters
            sql = params.get("sql")
            if not sql:
                return {"error": "SQL query not provided", "status": "error"}

            # Initialize connection if not already initialized
            await self.initialize()

            # Execute query
            result = await self.executor.execute_query(sql)

            return {"result": result, "status": "success"}

        except Exception as e:
            logger.exception(f"Error handling YDB query request: {e}")
            return {"error": str(e), "status": "error"}

    async def shutdown(self) -> None:
        """Clean up resources on shutdown."""
        if self.connection:
            await self.connection.close()
