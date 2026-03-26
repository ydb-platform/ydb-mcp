"""Integration tests for YDB authentication modes."""

import os
import random
import string
import warnings

import pytest

from tests.integration.conftest import YDB_DATABASE, YDB_ENDPOINT, call_tool
from ydb_mcp.server import YDBMCPServer

warnings.filterwarnings("ignore", message="datetime.datetime.utcfromtimestamp.*", category=DeprecationWarning)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_anonymous_auth(server):
    """Anonymous auth (the default) can execute queries."""
    result = await call_tool(server, "ydb_query", sql="SELECT 1 AS value")
    assert "result_sets" in result
    assert result["result_sets"][0]["rows"][0][0] == 1


async def test_login_password_authentication(server):
    """Create a user, connect with login-password, verify it works; then test wrong password."""
    login = "testuser" + "".join(random.choices(string.ascii_lowercase, k=8))
    password = f"pwd_{os.urandom(4).hex()}"
    wrong_password = f"wrong_{os.urandom(4).hex()}"

    try:
        # Create the user using the anonymous session
        r = await call_tool(server, "ydb_query", sql=f"CREATE USER {login} PASSWORD '{password}';")
        assert "error" not in r, f"Could not create user: {r}"

        import asyncio
        await asyncio.sleep(1)  # wait for user creation to propagate

        # Connect with correct credentials
        auth_server = YDBMCPServer(
            endpoint=YDB_ENDPOINT,
            database=YDB_DATABASE,
            auth_mode="login-password",
            login=login,
            password=password,
        )
        try:
            await auth_server._ensure_connected()
            result = await call_tool(auth_server, "ydb_query", sql="SELECT 1+1 AS result")
            assert "result_sets" in result, f"Unexpected response: {result}"
            assert result["result_sets"][0]["rows"][0][0] == 2
        finally:
            await auth_server.aclose()

        # Connect with wrong password — query should return an error
        bad_server = YDBMCPServer(
            endpoint=YDB_ENDPOINT,
            database=YDB_DATABASE,
            auth_mode="login-password",
            login=login,
            password=wrong_password,
        )
        try:
            result = await call_tool(bad_server, "ydb_query", sql="SELECT 1 AS value")
            assert "error" in result, f"Expected an auth error, got: {result}"
        finally:
            await bad_server.aclose()

    finally:
        await call_tool(server, "ydb_query", sql=f"DROP USER {login};")
