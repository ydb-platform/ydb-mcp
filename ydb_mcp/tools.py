"""Built-in generic MCP tools for YDB."""

from enum import Enum
from typing import TYPE_CHECKING, Any

from mcp.types import TextContent

from .params import _parse_params_str
from .serialization import serialize_ydb_response

if TYPE_CHECKING:
    from .server import YDBMCPServer


class YDBGenericTool(str, Enum):
    """Names of the built-in generic YDB tools."""

    QUERY = "ydb_query"
    QUERY_WITH_PARAMS = "ydb_query_with_params"
    EXPLAIN = "ydb_explain_query"
    EXPLAIN_WITH_PARAMS = "ydb_explain_query_with_params"
    STATUS = "ydb_status"
    LIST_DIRECTORY = "ydb_list_directory"
    DESCRIBE_PATH = "ydb_describe_path"


def register_generic_tools(server: "YDBMCPServer", enabled: set[YDBGenericTool]) -> None:
    """Register the built-in YDB tools with the FastMCP server.

    :param enabled: set of tools to register; empty set registers nothing.
    """

    async def ydb_query(sql: str) -> list[TextContent]:
        """Run a SQL query against YDB database."""
        return [TextContent(type="text", text=serialize_ydb_response({"result_sets": await server.execute(sql)}))]

    async def ydb_query_with_params(sql: str, params: str | dict) -> list[TextContent]:
        """Run a parameterized SQL query with JSON parameters."""
        result_sets = await server.execute(sql, _parse_params_str(params))
        return [TextContent(type="text", text=serialize_ydb_response({"result_sets": result_sets}))]

    async def ydb_explain_query(sql: str) -> list[TextContent]:
        """Explain a SQL query against YDB."""
        return [TextContent(type="text", text=serialize_ydb_response(await server.explain(sql)))]

    async def ydb_explain_query_with_params(sql: str, params: str | dict) -> list[TextContent]:
        """Explain a parameterized SQL query against YDB."""
        result = await server.explain(sql, _parse_params_str(params))
        return [TextContent(type="text", text=serialize_ydb_response(result))]

    async def ydb_status() -> list[TextContent]:
        """Get the current YDB connection status."""
        status: dict[str, Any] = {
            "status": "running",
            "ydb_endpoint": server.endpoint,
            "ydb_database": server.database,
            "auth_mode": server.auth_mode,
            "write_queries_enabled": server.allow_write,
        }
        try:
            await server._ensure_connected()
            assert server._driver is not None
            await server._driver.wait(timeout=5.0)
            status["ydb_connection"] = "connected"
        except Exception as e:
            status["ydb_connection"] = "error"
            status["error"] = str(e)
        return [TextContent(type="text", text=serialize_ydb_response(status))]

    async def ydb_list_directory(path: str) -> list[TextContent]:
        """List directory contents in YDB."""
        return [TextContent(type="text", text=serialize_ydb_response(await server.list_directory(path)))]

    async def ydb_describe_path(path: str) -> list[TextContent]:
        """Get detailed information about a YDB path (table, directory, etc.)."""
        return [TextContent(type="text", text=serialize_ydb_response(await server.describe_path(path)))]

    query_description = (
        "Run a read-only SQL query against YDB database"
        if not server.allow_write
        else "Run a SQL query against YDB database (writes enabled)"
    )
    parameterized_query_description = (
        "Run a read-only parameterized SQL query with JSON parameters"
        if not server.allow_write
        else "Run a parameterized SQL query with JSON parameters (writes enabled)"
    )

    for tool, fn, description in [
        (YDBGenericTool.QUERY, ydb_query, query_description),
        (YDBGenericTool.QUERY_WITH_PARAMS, ydb_query_with_params, parameterized_query_description),
        (YDBGenericTool.EXPLAIN, ydb_explain_query, "Explain a SQL query against YDB"),
        (
            YDBGenericTool.EXPLAIN_WITH_PARAMS,
            ydb_explain_query_with_params,
            "Explain a parameterized SQL query against YDB",
        ),
        (YDBGenericTool.STATUS, ydb_status, "Get the current YDB connection status"),
        (YDBGenericTool.LIST_DIRECTORY, ydb_list_directory, "List directory contents in YDB"),
        (YDBGenericTool.DESCRIBE_PATH, ydb_describe_path, "Get detailed information about a YDB path"),
    ]:
        if tool in enabled:
            server.add_tool(fn, name=tool.value, description=description)  # type: ignore[arg-type]
