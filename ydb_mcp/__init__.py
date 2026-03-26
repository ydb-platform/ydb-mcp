"""YDB MCP - Model Context Protocol server for YDB."""

from .serialization import serialize_ydb_response
from .server import YDBMCPServer
from .tools import YDBGenericTool
from .version import VERSION

__version__ = VERSION
__all__ = ["YDBMCPServer", "YDBGenericTool", "serialize_ydb_response"]
