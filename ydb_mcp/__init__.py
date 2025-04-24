"""YDB MCP - Model Context Protocol server for YDB."""

__version__ = "0.1.0"

# Import order matters to avoid circular imports
from ydb_mcp.connection import YDBConnection
from ydb_mcp.query import QueryExecutor

__all__ = ["YDBConnection", "QueryExecutor"]
