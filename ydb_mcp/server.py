"""Model Context Protocol server for YDB DBMS."""

import asyncio
import os
from typing import Any

import ydb
import ydb.aio
from mcp.server.fastmcp import FastMCP

from .params import _build_ydb_params
from .serialization import _process_result_set, _stringify_keys
from .tools import YDBGenericTool, register_generic_tools
from .version import VERSION

_AUTH_MODES = frozenset({"anonymous", "login-password", "access-token", "service-account"})

_ENTRY_TYPE_MAP = {
    1: "DIRECTORY",
    2: "TABLE",
    3: "PERS_QUEUE",
    4: "DATABASE",
    5: "RTMR_VOLUME",
    6: "BLOCK_STORE_VOLUME",
    7: "COORDINATION",
    8: "SEQUENCE",
    9: "REPLICATION",
    10: "TOPIC",
    11: "EXTERNAL_DATA_SOURCE",
    12: "EXTERNAL_TABLE",
}


class YDBMCPServer(FastMCP):
    """MCP server for YDB database.

    Control which built-in tools are registered via ``generic_tools``:

    - ``set(YDBGenericTool)`` (default) — register all built-in tools
    - ``set()`` — register none; add only your own
    - ``{YDBGenericTool.QUERY, YDBGenericTool.STATUS}`` — register only the listed tools

    Advanced subclasses can access the underlying YDB objects directly after the first
    query triggers the lazy connection:

    - ``self._driver`` — ``ydb.aio.Driver``, available for scheme operations and raw SDK calls
    - ``self._pool`` — ``ydb.aio.QuerySessionPool``, available for custom query execution

    Both are ``None`` until the first call to ``_ensure_connected()``.

    Example — expose just two built-in tools plus a custom one::

        from ydb_mcp import serialize_ydb_response

        class MyServer(YDBMCPServer):
            generic_tools = {YDBGenericTool.QUERY, YDBGenericTool.STATUS}

            def __init__(self, **kwargs):
                super().__init__(**kwargs)

                @self.tool()
                async def get_user(user_id: str) -> str:
                    '''Fetch a user by ID.'''
                    rows = await self.execute(
                        "SELECT * FROM users WHERE id = $id",
                        {"id": user_id},
                    )
                    return serialize_ydb_response(rows)

        MyServer(endpoint="grpc://localhost:2136", database="/local").run()
    """

    generic_tools: set[YDBGenericTool] = set(YDBGenericTool)

    def __init__(
        self,
        endpoint: str | None = None,
        database: str | None = None,
        auth_mode: str = "anonymous",
        login: str | None = None,
        password: str | None = None,
        access_token: str | None = None,
        sa_key_file: str | None = None,
        root_certificates: str | None = None,
        disable_discovery: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__("YDB MCP Server", **kwargs)

        if auth_mode not in _AUTH_MODES:
            raise ValueError(
                f"Unsupported auth mode: {auth_mode!r}. Supported: {', '.join(sorted(_AUTH_MODES))}"
            )
        if auth_mode == "login-password" and not (login and password):
            raise ValueError("--ydb-login and --ydb-password are required for login-password auth mode")
        if auth_mode == "access-token" and not access_token:
            raise ValueError("--ydb-access-token is required for access-token auth mode")
        if auth_mode == "service-account" and not sa_key_file:
            raise ValueError("--ydb-sa-key-file is required for service-account auth mode")

        self.endpoint = endpoint or os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136")
        self.database = database or os.environ.get("YDB_DATABASE", "/local")
        self.auth_mode = auth_mode
        self.login = login
        self.password = password
        self.access_token = access_token
        self.sa_key_file = sa_key_file
        self.root_certificates = root_certificates
        self.disable_discovery = disable_discovery

        self._driver: ydb.aio.Driver | None = None
        self._pool: ydb.aio.QuerySessionPool | None = None
        self._connect_lock = asyncio.Lock()

        register_generic_tools(self, type(self).generic_tools)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _build_credentials(self) -> ydb.Credentials:
        if self.auth_mode == "login-password":
            return ydb.credentials.StaticCredentials.from_user_password(self.login, self.password)
        if self.auth_mode == "access-token":
            return ydb.credentials.AccessTokenCredentials(self.access_token)
        if self.auth_mode == "service-account":
            return ydb.iam.ServiceAccountCredentials.from_file(self.sa_key_file)
        return ydb.credentials.AnonymousCredentials()

    async def _ensure_connected(self) -> None:
        """Lazily create the YDB driver and session pool on first use."""
        if self._driver is not None:
            return
        async with self._connect_lock:
            if self._driver is not None:
                return
            config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=self._build_credentials(),
                root_certificates=self.root_certificates,
                disable_discovery=self.disable_discovery,
                _additional_sdk_headers=(f"ydb-mcp/{VERSION}",),
            )
            driver = ydb.aio.Driver(config)
            try:
                await driver.wait(timeout=5.0)
                pool = ydb.aio.QuerySessionPool(driver)
            except Exception:
                await driver.stop()
                raise
            self._pool = pool
            self._driver = driver  # set last — signals that init is done

    # ------------------------------------------------------------------
    # Public API for subclasses
    # ------------------------------------------------------------------

    async def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        """Execute a SQL query and return result sets.

        Each result set is a ``dict`` with ``"columns"`` (list of column names)
        and ``"rows"`` (list of value lists).
        """
        await self._ensure_connected()
        assert self._pool is not None
        ydb_params = _build_ydb_params(params) if params else None
        result_sets = await self._pool.execute_with_retries(sql, ydb_params)
        return [_process_result_set(rs) for rs in result_sets]

    async def explain(self, sql: str, params: dict | None = None) -> dict:
        """Explain a SQL query and return the execution plan as a dict."""
        await self._ensure_connected()
        assert self._pool is not None
        ydb_params = _build_ydb_params(params) if params else None
        plan = await self._pool.explain_with_retries(
            query=sql,
            parameters=ydb_params,
            result_format=ydb.QueryExplainResultFormat.DICT,
        )
        return dict(_stringify_keys(plan))

    async def list_directory(self, path: str) -> dict:
        """List contents of a YDB directory.

        Returns a dict with ``"path"`` and ``"items"`` (list of entry dicts).
        """
        await self._ensure_connected()
        assert self._driver is not None
        response = await self._driver.scheme_client.list_directory(path)
        items = []
        for entry in response.children or []:
            item: dict[str, Any] = {
                "name": entry.name,
                "type": _ENTRY_TYPE_MAP.get(entry.type, str(entry.type)),
                "owner": entry.owner,
            }
            if getattr(entry, "permissions", None):
                item["permissions"] = [
                    {"subject": p.subject, "permission_names": list(p.permission_names)}
                    for p in entry.permissions
                ]
            items.append(item)
        items.sort(key=lambda x: x["name"])
        return {"path": path, "items": items}

    async def describe_path(self, path: str) -> dict:
        """Describe a YDB path (directory, table, etc.).

        Returns a metadata dict. For tables also includes column/index details.
        """
        await self._ensure_connected()
        assert self._driver is not None
        response = await self._driver.scheme_client.describe_path(path)
        if response is None:
            return {"error": f"Path '{path}' not found"}

        entry_type = _ENTRY_TYPE_MAP.get(response.type, str(response.type))
        result: dict[str, Any] = {
            "path": path,
            "type": entry_type,
            "name": response.name,
            "owner": response.owner,
        }
        if getattr(response, "permissions", None):
            result["permissions"] = [
                {"subject": p.subject, "permission_names": list(p.permission_names)}
                for p in response.permissions
            ]
        if entry_type == "TABLE":
            result["table"] = await self._describe_table(path)
        return result

    async def _describe_table(self, path: str) -> dict:
        assert self._driver is not None
        session = await self._driver.table_client.session().create()
        try:
            desc = await session.describe_table(path)
            return {
                "columns": [
                    {"name": col.name, "type": str(col.type), "family": col.family}
                    for col in desc.columns
                ],
                "primary_key": list(desc.primary_key),
                "indexes": [
                    {
                        "name": idx.name,
                        "index_columns": list(idx.index_columns),
                        "cover_columns": list(idx.cover_columns) if hasattr(idx, "cover_columns") else [],
                        "index_type": str(idx.index_type) if hasattr(idx, "index_type") else None,
                    }
                    for idx in desc.indexes
                ],
            }
        finally:
            await session.delete()

    async def aclose(self) -> None:
        """Stop the YDB session pool and driver, releasing all connections."""
        async with self._connect_lock:
            if self._pool is not None:
                await self._pool.stop()
                self._pool = None
            if self._driver is not None:
                await self._driver.stop()
                self._driver = None

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self, transport: str = "stdio") -> None:  # type: ignore[override]
        """Run the MCP server (default transport: stdio)."""
        super().run(transport=transport)  # type: ignore[arg-type]
