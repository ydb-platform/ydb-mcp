"""Unit tests for YDBMCPServer."""

import json
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ydb_mcp.params import _build_ydb_params, _parse_params_str
from ydb_mcp.serialization import _process_result_set, _stringify_keys
from ydb_mcp.server import YDBMCPServer
from ydb_mcp.tools import YDBGenericTool

# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


class TestStringifyKeys:
    def test_flat_dict(self):
        assert _stringify_keys({1: "a", 2: "b"}) == {"1": "a", "2": "b"}

    def test_nested(self):
        result = _stringify_keys({1: {2: [3, {4: "v"}]}})
        assert result == {"1": {"2": [3, {"4": "v"}]}}

    def test_passthrough(self):
        assert _stringify_keys("hello") == "hello"
        assert _stringify_keys(42) == 42


class TestProcessResultSet:
    def _make_result_set(self, columns, rows):
        rs = MagicMock()
        rs.columns = [MagicMock(name=c) for c in columns]
        # MagicMock(name=...) sets the mock's own name attribute weirdly — fix:
        for mock_col, col_name in zip(rs.columns, columns):
            mock_col.name = col_name
        rs.rows = []
        for row in rows:
            mock_row = MagicMock()
            mock_row.__getitem__.side_effect = lambda i, r=row: r[i]
            rs.rows.append(mock_row)
        return rs

    def test_basic(self):
        rs = self._make_result_set(["id", "name"], [[1, "Alice"], [2, "Bob"]])
        result = _process_result_set(rs)
        assert result["columns"] == ["id", "name"]
        assert result["rows"] == [[1, "Alice"], [2, "Bob"]]

    def test_empty_rows(self):
        rs = self._make_result_set(["id"], [])
        result = _process_result_set(rs)
        assert result["columns"] == ["id"]
        assert result["rows"] == []

    def test_columns_error(self):
        rs = MagicMock()
        type(rs).columns = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
        result = _process_result_set(rs)
        assert "error" in result
        assert result["columns"] == []
        assert result["rows"] == []


class TestBuildYdbParams:
    def test_adds_dollar_prefix(self):
        result = _build_ydb_params({"id": 1})
        assert "$id" in result
        assert result["$id"] == 1

    def test_keeps_existing_dollar(self):
        result = _build_ydb_params({"$id": 1})
        assert "$id" in result
        assert "$$id" not in result

    def test_ydb_type_tuple(self):
        import ydb
        result = _build_ydb_params({"id": (42, "Int32")})
        assert result["$id"] == ydb.TypedValue(42, ydb.PrimitiveType.Int32)

    def test_unknown_type_tuple(self):
        result = _build_ydb_params({"id": (42, "NotAType")})
        assert result["$id"] == 42

    def test_mixed(self):
        result = _build_ydb_params({"$a": 1, "b": "hello"})
        assert result == {"$a": 1, "$b": "hello"}


class TestParseParamsStr:
    def test_empty(self):
        assert _parse_params_str("") == {}
        assert _parse_params_str("   ") == {}

    def test_basic(self):
        result = _parse_params_str('{"id": 42}')
        assert result == {"$id": 42}

    def test_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            _parse_params_str("not json")


# ---------------------------------------------------------------------------
# YDBMCPServer initialization
# ---------------------------------------------------------------------------


class TestYDBMCPServerInit:
    def test_defaults(self):
        s = YDBMCPServer(endpoint="grpc://localhost:2136", database="/local")
        assert s.endpoint == "grpc://localhost:2136"
        assert s.database == "/local"
        assert s.auth_mode == "anonymous"

    def test_env_fallback(self, monkeypatch):
        monkeypatch.setenv("YDB_ENDPOINT", "grpc://env-host:2136")
        monkeypatch.setenv("YDB_DATABASE", "/env-db")
        s = YDBMCPServer()
        assert s.endpoint == "grpc://env-host:2136"
        assert s.database == "/env-db"

    def test_invalid_auth_mode(self):
        with pytest.raises(ValueError, match="Unsupported auth mode"):
            YDBMCPServer(auth_mode="magic")

    def test_login_password_missing_creds(self):
        with pytest.raises(ValueError, match="login-password"):
            YDBMCPServer(auth_mode="login-password")

    def test_login_password_ok(self):
        s = YDBMCPServer(auth_mode="login-password", login="user", password="pass")
        assert s.auth_mode == "login-password"

    def test_access_token_missing(self):
        with pytest.raises(ValueError, match="access-token"):
            YDBMCPServer(auth_mode="access-token")

    def test_service_account_missing(self):
        with pytest.raises(ValueError, match="service-account"):
            YDBMCPServer(auth_mode="service-account")

    def test_disable_discovery_default_false(self):
        s = YDBMCPServer(endpoint="grpc://localhost:2136", database="/local")
        assert s.disable_discovery is False

    def test_disable_discovery_true(self):
        s = YDBMCPServer(endpoint="grpc://localhost:2136", database="/local", disable_discovery=True)
        assert s.disable_discovery is True


# ---------------------------------------------------------------------------
# __main__ CLI argument parsing
# ---------------------------------------------------------------------------


class TestMain:
    def _parse(self, argv):
        """Run main() with given argv, capturing the YDBMCPServer constructor call."""
        from ydb_mcp.__main__ import main

        with patch("ydb_mcp.__main__.YDBMCPServer") as mock_cls:
            mock_cls.return_value.run = MagicMock()
            with patch.object(sys, "argv", ["ydb-mcp"] + argv):
                main()
        return mock_cls.call_args.kwargs

    def test_disable_discovery_not_set_by_default(self):
        kwargs = self._parse(["--ydb-endpoint", "grpc://localhost:2136", "--ydb-database", "/local"])
        assert kwargs["disable_discovery"] is False

    def test_disable_discovery_flag(self):
        kwargs = self._parse(
            ["--ydb-endpoint", "grpc://localhost:2136", "--ydb-database", "/local", "--ydb-disable-discovery"]
        )
        assert kwargs["disable_discovery"] is True


# ---------------------------------------------------------------------------
# generic_tools flag
# ---------------------------------------------------------------------------


class TestGenericTools:
    def test_generic_tools_registered_by_default(self):
        s = YDBMCPServer(endpoint="grpc://localhost:2136", database="/local")
        tool_names = {t.name for t in s._tool_manager.list_tools()}
        assert tool_names == {t.value for t in YDBGenericTool}

    def test_generic_tools_disabled_in_subclass(self):
        class CustomServer(YDBMCPServer):
            generic_tools = set()

        s = CustomServer(endpoint="grpc://localhost:2136", database="/local")
        assert len(s._tool_manager.list_tools()) == 0

    def test_generic_tools_subset(self):
        class CustomServer(YDBMCPServer):
            generic_tools = {YDBGenericTool.QUERY, YDBGenericTool.STATUS}

        s = CustomServer(endpoint="grpc://localhost:2136", database="/local")
        tool_names = {t.name for t in s._tool_manager.list_tools()}
        assert tool_names == {YDBGenericTool.QUERY.value, YDBGenericTool.STATUS.value}

    def test_generic_tools_subset_with_custom(self):
        class CustomServer(YDBMCPServer):
            generic_tools = {YDBGenericTool.QUERY}

            def __init__(self, **kwargs):
                super().__init__(**kwargs)

                @self.tool()
                async def my_tool(param: str) -> str:
                    """My custom tool."""
                    return param

        s = CustomServer(endpoint="grpc://localhost:2136", database="/local")
        tool_names = {t.name for t in s._tool_manager.list_tools()}
        assert tool_names == {YDBGenericTool.QUERY.value, "my_tool"}

    def test_subclass_can_add_custom_tool(self):
        class CustomServer(YDBMCPServer):
            generic_tools = set()

            def __init__(self, **kwargs):
                super().__init__(**kwargs)

                @self.tool()
                async def my_tool(param: str) -> str:
                    """My custom tool."""
                    return param

        s = CustomServer(endpoint="grpc://localhost:2136", database="/local")
        tool_names = {t.name for t in s._tool_manager.list_tools()}
        assert tool_names == {"my_tool"}


# ---------------------------------------------------------------------------
# execute / explain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestExecute:
    async def test_execute_calls_pool(self, server, mock_pool):
        col = MagicMock()
        col.name = "id"
        rs = MagicMock()
        rs.columns = [col]
        rs.rows = []
        mock_pool.execute_with_retries.return_value = [rs]

        result = await server.execute("SELECT 1")

        mock_pool.execute_with_retries.assert_called_once_with("SELECT 1", None)
        assert result == [{"columns": ["id"], "rows": []}]

    async def test_execute_with_params(self, server, mock_pool):
        mock_pool.execute_with_retries.return_value = []
        await server.execute("SELECT $x", {"x": 42})
        call_args = mock_pool.execute_with_retries.call_args
        assert call_args[0][1] == {"$x": 42}

    async def test_explain(self, server, mock_pool):
        mock_pool.explain_with_retries.return_value = {"plan": "data"}
        result = await server.explain("SELECT 1")
        assert result == {"plan": "data"}


# ---------------------------------------------------------------------------
# list_directory / describe_path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestListDirectory:
    async def test_basic(self, server, mock_driver):
        entry = MagicMock()
        entry.name = "my_table"
        entry.type = 2
        entry.owner = "root"
        entry.permissions = []
        response = MagicMock()
        response.children = [entry]
        mock_driver.scheme_client.list_directory.return_value = response

        result = await server.list_directory("/local")

        assert result["path"] == "/local"
        assert len(result["items"]) == 1
        assert result["items"][0]["name"] == "my_table"
        assert result["items"][0]["type"] == "TABLE"

    async def test_empty_directory(self, server, mock_driver):
        response = MagicMock()
        response.children = []
        mock_driver.scheme_client.list_directory.return_value = response

        result = await server.list_directory("/local")
        assert result["items"] == []

    async def test_sorted_by_name(self, server, mock_driver):
        def make_entry(name):
            e = MagicMock()
            e.name = name
            e.type = 1
            e.owner = "root"
            e.permissions = []
            return e

        response = MagicMock()
        response.children = [make_entry("z"), make_entry("a"), make_entry("m")]
        mock_driver.scheme_client.list_directory.return_value = response

        result = await server.list_directory("/local")
        assert [i["name"] for i in result["items"]] == ["a", "m", "z"]


@pytest.mark.asyncio
class TestDescribePath:
    async def test_directory(self, server, mock_driver):
        response = MagicMock()
        response.type = "DIRECTORY"
        response.name = "mydir"
        response.owner = "root"
        response.permissions = []
        mock_driver.scheme_client.describe_path.return_value = response

        result = await server.describe_path("/local/mydir")

        assert result["path"] == "/local/mydir"
        assert result["type"] == "DIRECTORY"
        assert result["name"] == "mydir"
        assert "table" not in result

    async def test_none_response(self, server, mock_driver):
        mock_driver.scheme_client.describe_path.return_value = None
        result = await server.describe_path("/local/missing")
        assert "error" in result


# ---------------------------------------------------------------------------
# Generic MCP tool wrappers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestGenericToolHandlers:
    """Test that the generic tool closures correctly wrap execute/explain/etc."""

    async def _call_tool(self, server, name, **kwargs):
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        assert name in tools, f"Tool {name!r} not registered"
        return await tools[name].fn(**kwargs)

    async def test_ydb_query_success(self, server):
        server.execute = AsyncMock(return_value=[{"columns": ["n"], "rows": [[1]]}])
        result = await self._call_tool(server, "ydb_query", sql="SELECT 1")
        assert isinstance(result, list)
        data = json.loads(result[0].text)
        assert "result_sets" in data

    async def test_ydb_query_error(self, server):
        server.execute = AsyncMock(side_effect=RuntimeError("connection failed"))
        result = await self._call_tool(server, "ydb_query", sql="SELECT 1")
        data = json.loads(result[0].text)
        assert "error" in data

    async def test_ydb_query_with_params(self, server):
        server.execute = AsyncMock(return_value=[])
        await self._call_tool(server, "ydb_query_with_params", sql="SELECT $x", params='{"x": 1}')
        server.execute.assert_called_once_with("SELECT $x", {"$x": 1})

    async def test_ydb_query_with_params_invalid_json(self, server):
        result = await self._call_tool(server, "ydb_query_with_params", sql="SELECT $x", params="bad json")
        data = json.loads(result[0].text)
        assert "error" in data

    async def test_ydb_explain(self, server):
        server.explain = AsyncMock(return_value={"plan": {}})
        result = await self._call_tool(server, "ydb_explain_query", sql="SELECT 1")
        data = json.loads(result[0].text)
        assert "plan" in data

    async def test_ydb_status_connected(self, server, mock_driver):
        mock_driver.discovery_debug_details.return_value = "Resolved endpoints: ..."
        result = await self._call_tool(server, "ydb_status")
        data = json.loads(result[0].text)
        assert data["ydb_connection"] == "connected"

    async def test_ydb_status_error(self, server, mock_driver):
        mock_driver.discovery_debug_details.return_value = "No endpoints"
        result = await self._call_tool(server, "ydb_status")
        data = json.loads(result[0].text)
        assert data["ydb_connection"] == "error"

    async def test_ydb_list_directory(self, server):
        server.list_directory = AsyncMock(return_value={"path": "/local", "items": []})
        result = await self._call_tool(server, "ydb_list_directory", path="/local")
        data = json.loads(result[0].text)
        assert data["path"] == "/local"

    async def test_ydb_describe_path(self, server):
        server.describe_path = AsyncMock(return_value={"path": "/local/t", "type": "TABLE"})
        result = await self._call_tool(server, "ydb_describe_path", path="/local/t")
        data = json.loads(result[0].text)
        assert data["type"] == "TABLE"
