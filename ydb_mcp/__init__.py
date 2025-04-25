"""YDB MCP - Model Context Protocol server for YDB."""

from .version import VERSION

__version__ = VERSION

# Import order matters to avoid circular imports
from ydb_mcp.connection import YDBConnection
from ydb_mcp.query import QueryExecutor

__all__ = ["YDBConnection", "QueryExecutor"]
