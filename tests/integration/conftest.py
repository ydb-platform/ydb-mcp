"""Common fixtures for YDB MCP integration tests.

This module provides shared fixtures for all integration tests, including
automatic Docker container management for YDB.
"""

import asyncio
import gc
import json
import logging
import os
import socket
import time
from contextlib import suppress
from urllib.parse import urlparse

import pytest
import ydb

from ydb_mcp.server import AUTH_MODE_ANONYMOUS, YDBMCPServer
from tests.docker_utils import start_ydb_container, stop_container, wait_for_port

# Configuration for the tests
YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136/local")
# Database will be extracted from the endpoint if not explicitly provided
YDB_DATABASE = os.environ.get("YDB_DATABASE")

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set default level to WARNING
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # Set test logger to WARNING

# Set specific loggers to appropriate levels
ydb_logger = logging.getLogger("ydb")
ydb_logger.setLevel(logging.ERROR)  # Raise YDB logger level to ERROR

# Keep server startup/shutdown and critical error logs at INFO/ERROR level
server_logger = logging.getLogger("ydb_mcp.server")
server_logger.setLevel(logging.ERROR)  # Raise server logger level to ERROR

# Set asyncio logger to ERROR to suppress task destruction messages
asyncio_logger = logging.getLogger("asyncio")
asyncio_logger.setLevel(logging.ERROR)


async def cleanup_pending_tasks():
    """Clean up any pending tasks in the current event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running event loop
        return

    # Get all pending tasks except the current one
    current = asyncio.current_task(loop)
    pending = [
        task for task in asyncio.all_tasks(loop) if not task.done() and task is not current
    ]

    # Explicitly suppress destroy pending warning for YDB Discovery.run tasks
    for task in pending:
        coro = getattr(task, 'get_coro', lambda: None)()
        if coro and 'Discovery.run' in repr(coro):
            task._log_destroy_pending = False

    if not pending:
        return

    logger.debug(f"Cleaning up {len(pending)} pending tasks")

    # Cancel all pending tasks
    for task in pending:
        if not task.done() and not task.cancelled():
            # Disable the destroy pending warning for this task
            task._log_destroy_pending = False
            task.cancel()

    try:
        # Wait for tasks to cancel with a timeout, using shield to prevent cancellation
        await asyncio.shield(asyncio.wait(pending, timeout=0.1))
    except Exception as e:
        logger.debug(f"Error waiting for tasks to cancel: {e}")

    # Force cancel any remaining tasks
    still_pending = [t for t in pending if not t.done()]
    if still_pending:
        logger.debug(
            f"Force cancelling {len(still_pending)} tasks that did not cancel properly"
        )
        for task in still_pending:
            # Ensure the task won't log warnings when destroyed
            task._log_destroy_pending = False
            # Force cancel and suppress any errors
            with suppress(asyncio.CancelledError, Exception):
                task.cancel()
                try:
                    await asyncio.shield(asyncio.wait_for(task, timeout=0.1))
                except asyncio.TimeoutError:
                    pass


async def cleanup_driver(driver, timeout=1.0):
    """Clean up the driver and any associated tasks."""
    if not driver:
        return

    try:
        # First handle discovery task if it exists
        if hasattr(driver, "_discovery") and driver._discovery:
            logger.debug("Handling discovery task")
            try:
                # Try to stop discovery gracefully first
                if hasattr(driver._discovery, "stop"):
                    driver._discovery.stop()

                # Then cancel the task if it exists and is still running
                if hasattr(driver._discovery, "_discovery_task"):
                    task = driver._discovery._discovery_task
                    if task and not task.done() and not task.cancelled():
                        task._log_destroy_pending = False
                        task.cancel()
                        try:
                            await asyncio.shield(asyncio.wait_for(task, timeout=0.1))
                        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                            pass
            except Exception as e:
                logger.debug(f"Error handling discovery task: {e}")

        # Stop the driver with proper error handling
        logger.debug("Stopping driver")
        try:
            # Use shield to prevent cancellation of the stop operation
            await asyncio.shield(asyncio.wait_for(driver.stop(), timeout=timeout))
        except asyncio.TimeoutError:
            logger.debug(f"Driver stop timed out after {timeout} seconds")
        except asyncio.CancelledError:
            logger.debug("Driver stop was cancelled")
        except Exception as e:
            logger.debug(f"Error stopping driver: {e}")

    finally:
        # Clean up any remaining tasks
        await cleanup_pending_tasks()


def ensure_event_loop():
    """Ensure we have a valid event loop and return it."""
    try:
        loop = asyncio.get_running_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def is_port_open(host, port):
    """Check if a port is open on the given host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            return s.connect_ex((host, port)) == 0
        except (socket.gaierror, ConnectionRefusedError, OSError):
            return False


@pytest.fixture(scope="session")
def ydb_server():
    """
    Fixture to ensure YDB server is running.
    If YDB_ENDPOINT is not available, it starts a Docker container.
    """
    # Parse the endpoint to extract host and port
    endpoint_url = urlparse(YDB_ENDPOINT)

    # Handle different endpoint formats
    if endpoint_url.scheme in ("grpc", "grpcs"):
        host_port = endpoint_url.netloc.split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 2136
    else:
        # Default to localhost:2136 if we can't parse
        host = "localhost"
        port = 2136

    # Check if YDB is already running at the specified endpoint
    if is_port_open(host, port):
        logger.info(f"YDB server is already running at {host}:{port}")
        yield None
        return

    # If YDB is not running, start via docker_utils
    logger.info(f"YDB server not running at {host}:{port}, starting Docker container")
    container = start_ydb_container()
    # Wait for YDB readiness
    wait_for_port(host, port, timeout=30)
    time.sleep(5)
    yield container
    logger.info("Stopping YDB Docker container")
    stop_container(container)


@pytest.fixture(scope="session")
async def mcp_server(ydb_server):
    """Create a YDB MCP server instance for testing."""
    # Create the server with anonymous credentials
    server = YDBMCPServer(endpoint=YDB_ENDPOINT, database=YDB_DATABASE)

    # Store the event loop
    server._loop = ensure_event_loop()

    try:
        # Initialize the server by creating the driver
        await server.create_driver()
        yield server

        # Clean up after tests
        logger.info("Cleaning up YDB server resources after tests")
        await cleanup_pending_tasks()
        if server.driver:
            await cleanup_driver(server.driver)

    except Exception as e:
        logger.error(f"Failed to initialize YDB MCP server: {e}")
        pytest.fail(f"Failed to initialize YDB MCP server: {e}")
    finally:
        # Final cleanup
        await cleanup_pending_tasks()


# Create a global variable to cache the server instance
_mcp_server_instance = None


@pytest.fixture(scope="session")
async def session_mcp_server(ydb_server):
    """Create a YDB MCP server instance once per test session and cache it."""
    global _mcp_server_instance

    if _mcp_server_instance is None:
        # Create the server with anonymous credentials
        _mcp_server_instance = YDBMCPServer(
            endpoint=YDB_ENDPOINT, database=YDB_DATABASE, auth_mode=AUTH_MODE_ANONYMOUS
        )

        try:
            # Ensure we have a valid event loop
            _mcp_server_instance._loop = ensure_event_loop()

            # Initialize the server by creating the driver
            await _mcp_server_instance.create_driver()
        except Exception as e:
            logger.error(f"Failed to initialize YDB MCP server: {e}")
            pytest.fail(f"Failed to initialize YDB MCP server: {e}")
            yield None
            return

    yield _mcp_server_instance

    # Clean up after all tests
    if _mcp_server_instance is not None:
        logger.info("Cleaning up YDB server resources after test session")
        try:
            # Clean up pending tasks first
            await cleanup_pending_tasks()

            # Clean up the driver with extended timeout
            if _mcp_server_instance.driver:
                await cleanup_driver(_mcp_server_instance.driver, timeout=10)

            # Clear the instance
            _mcp_server_instance = None

            # Force garbage collection to help clean up any remaining references
            gc.collect()
        except Exception as e:
            logger.error(f"Error during session cleanup: {e}")
        finally:
            # Final cleanup attempt for any remaining tasks
            await cleanup_pending_tasks()


@pytest.fixture(scope="function")
async def mcp_server(session_mcp_server):
    """Provide a clean MCP server connection for each test by restarting the connection."""
    if session_mcp_server is None:
        pytest.fail("Could not get a valid MCP server instance")
        return

    # Reset server state to default
    session_mcp_server.auth_mode = AUTH_MODE_ANONYMOUS
    session_mcp_server.login = None
    session_mcp_server.password = None

    try:
        # Clean up any leftover tasks before restart
        await cleanup_pending_tasks()

        # Restart the connection to ensure clean environment for the test
        if session_mcp_server.driver is not None:
            logger.info("Restarting YDB connection for clean test environment")
            await session_mcp_server.restart()

        yield session_mcp_server

    except Exception as e:
        logger.error(f"Error during test setup: {e}")
        pytest.fail(f"Failed to setup test environment: {e}")
    finally:
        # Reset server state after test
        try:
            session_mcp_server.auth_mode = AUTH_MODE_ANONYMOUS
            session_mcp_server.login = None
            session_mcp_server.password = None

            # Clean up any tasks from the test
            await cleanup_pending_tasks()

            # Restart to clean state
            await session_mcp_server.restart()
        except Exception as e:
            logger.error(f"Error during test cleanup: {e}")


async def call_mcp_tool(mcp_server, tool_name, **params):
    """Helper function to call an MCP tool and return its result in JSON format.

    Args:
        mcp_server: The MCP server instance
        tool_name: Name of the tool to call
        **params: Parameters to pass to the tool

    Returns:
        The parsed result from the tool call
    """
    # Call the tool
    result = await mcp_server.call_tool(tool_name, params)

    # If the result is a list of TextContent objects, convert them to a more usable format
    if isinstance(result, list) and len(result) > 0 and hasattr(result[0], "text"):
        try:
            # Parse the JSON text from the TextContent
            parsed_result = json.loads(result[0].text)

            # For backward compatibility with tests, if there's an error key, return it directly
            if "error" in parsed_result:
                return parsed_result

            # For query results, return the result_sets directly if present
            if "result_sets" in parsed_result:
                return parsed_result

            # For other responses (list_directory, describe_path), return the parsed JSON
            return parsed_result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return {"error": str(e)}

    return result


@pytest.fixture(autouse=True, scope="session")
async def cleanup_after_all_tests():
    """Cleanup fixture that runs after all tests to ensure proper cleanup."""
    # Setup - nothing to do
    yield

    # Cleanup after all tests
    await cleanup_pending_tasks()

    # Close any remaining event loops
    try:
        loop = asyncio.get_running_loop()
        if not loop.is_closed():
            # Cancel all tasks
            pending = [
                task
                for task in asyncio.all_tasks(loop)
                if not task.done() and task != asyncio.current_task()
            ]

            if pending:
                logger.debug(f"Cleaning up {len(pending)} pending tasks in final cleanup")
                for task in pending:
                    if not task.done() and not task.cancelled():
                        task.cancel()
                        with suppress(asyncio.CancelledError, Exception):
                            # Add a timeout to avoid hanging
                            try:
                                await asyncio.wait_for(task, timeout=1.0)
                            except asyncio.TimeoutError:
                                pass

            # Ensure all tasks are truly done
            for task in pending:
                if not task.done():
                    with suppress(asyncio.CancelledError, Exception):
                        task._log_destroy_pending = (
                            False  # Suppress the warning about task destruction
                        )

            # Close the loop
            loop.stop()
            loop.close()
    except RuntimeError:
        pass  # No running event loop
