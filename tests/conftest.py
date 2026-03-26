"""Pytest configuration for YDB MCP tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_pool():
    pool = AsyncMock()
    pool.execute_with_retries = AsyncMock(return_value=[])
    pool.explain_with_retries = AsyncMock(return_value={})
    return pool


@pytest.fixture
def mock_driver(mock_pool):
    driver = MagicMock()
    driver.discovery_debug_details.return_value = "Resolved endpoints: grpc://localhost:2136"
    driver.scheme_client.list_directory = AsyncMock()
    driver.scheme_client.describe_path = AsyncMock()
    return driver


@pytest.fixture
def server(mock_driver, mock_pool):
    """YDBMCPServer with mocked connection (no real YDB needed)."""
    from ydb_mcp.server import YDBMCPServer

    s = YDBMCPServer(endpoint="grpc://localhost:2136", database="/local")
    s._driver = mock_driver
    s._pool = mock_pool
    return s
