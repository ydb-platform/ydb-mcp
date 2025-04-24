"""Pytest configuration for testing YDB MCP server."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_ydb_driver():
    """Mock YDB driver."""
    with patch("ydb.aio.Driver") as mock_driver_class:
        # Setup the mock driver instance
        mock_driver = AsyncMock()
        mock_driver_class.return_value = mock_driver

        # Mock the wait method
        mock_driver.wait = AsyncMock()

        yield mock_driver


@pytest.fixture
def mock_ydb_pool():
    """Mock YDB session pool."""
    with patch("ydb.aio.QuerySessionPool") as mock_pool_class:
        # Setup the mock pool instance
        mock_pool = AsyncMock()
        mock_pool_class.return_value = mock_pool

        # Mock the execute_with_retries method
        mock_pool.execute_with_retries = AsyncMock()

        yield mock_pool


@pytest.fixture
def mock_env_vars():
    """Mock environment variables."""
    env_vars = {
        "YDB_ENDPOINT": "mock-endpoint",
        "YDB_DATABASE": "mock-database",
        "YDB_ANONYMOUS_CREDENTIALS": "1",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars
