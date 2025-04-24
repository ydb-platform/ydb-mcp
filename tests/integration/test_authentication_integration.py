import asyncio
import json
import logging
import os
import random
import string
import time
import warnings

import pytest

# Fixtures are automatically imported by pytest from conftest.py
from tests.integration.conftest import call_mcp_tool
from ydb_mcp.server import AUTH_MODE_ANONYMOUS, AUTH_MODE_LOGIN_PASSWORD

# Suppress the utcfromtimestamp deprecation warning from the YDB library
warnings.filterwarnings(
    "ignore", message="datetime.datetime.utcfromtimestamp.*", category=DeprecationWarning
)

# Table name used for tests - using timestamp to avoid conflicts
TEST_TABLE = f"mcp_integration_test_{int(time.time())}"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use loop_scope instead of scope for the asyncio marker
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_login_password_authentication(mcp_server):
    """Test authentication with login and password."""
    # Generate random login-password pair
    test_login = "test" + "".join(random.choice(string.ascii_lowercase) for _ in range(10))
    test_password = f"test_pwd_{os.urandom(4).hex()}"
    wrong_password = f"wrong_pwd_{os.urandom(4).hex()}"

    try:
        # Create test user with anonymous auth (fixture ensures we start with anonymous auth)
        logger.debug(f"Creating test user {test_login}")
        result = await call_mcp_tool(
            mcp_server, "ydb_query", sql=f"CREATE USER {test_login} PASSWORD '{test_password}';"
        )
        assert "error" not in result, f"Error creating user: {result}"

        # Test with correct credentials
        logger.debug(f"Testing with correct credentials for user {test_login}")
        mcp_server.auth_mode = AUTH_MODE_LOGIN_PASSWORD
        mcp_server.login = test_login
        mcp_server.password = test_password

        # Wait a bit for user creation to propagate
        await asyncio.sleep(1)

        await mcp_server.restart()

        # Verify we can execute a query
        result = await call_mcp_tool(mcp_server, "ydb_query", sql="SELECT 1+1 as result")
        # Parse the JSON from the 'text' field if present
        if (
            isinstance(result, list)
            and len(result) > 0
            and isinstance(result[0], dict)
            and "text" in result[0]
        ):
            parsed = json.loads(result[0]["text"])
        else:
            parsed = result
        assert "result_sets" in parsed, f"No result_sets in response: {result}"
        assert parsed["result_sets"][0]["rows"][0][0] == 2, f"Unexpected result value: {result}"

        # Test with incorrect password
        logger.debug(f"Testing with incorrect password for user {test_login}")
        mcp_server.password = wrong_password

        # Restart should succeed but queries should fail
        await mcp_server.restart()

        # Query should fail with auth error
        result = await call_mcp_tool(mcp_server, "ydb_query", sql="SELECT 1+1 as result")
        # Parse the JSON from the 'text' field if present
        if (
            isinstance(result, list)
            and len(result) > 0
            and isinstance(result[0], dict)
            and "text" in result[0]
        ):
            parsed = json.loads(result[0]["text"])
        else:
            parsed = result
        assert "error" in parsed, f"Expected error with invalid password, got: {parsed}"

        error_msg = parsed.get("error", "").lower()
        logger.debug(f"Got error message: {error_msg}")

        # Check for both connection and auth error messages since YDB might return either
        auth_keywords = [
            "auth",
            "password",
            "login",
            "credential",
            "permission",
            "unauthorized",
            "invalid",
        ]
        conn_keywords = ["connecting to ydb", "error connecting", "connection failed"]
        all_keywords = auth_keywords + conn_keywords

        if error_msg.strip() == "":
            # Allow empty error message as valid
            pass
        else:
            assert any(
                keyword in error_msg for keyword in all_keywords
            ), f"Unexpected error message: {parsed.get('error')}"

    finally:
        # Switch back to anonymous auth to clean up (fixture will handle final state reset)
        logger.debug(f"Cleaning up - dropping test user {test_login}")
        mcp_server.auth_mode = AUTH_MODE_ANONYMOUS
        mcp_server.login = None
        mcp_server.password = None

        await mcp_server.restart()

        # Drop the test user
        result = await call_mcp_tool(mcp_server, "ydb_query", sql=f"DROP USER {test_login};")
        if "error" in result:
            logger.error(f"Error dropping user: {result}")
