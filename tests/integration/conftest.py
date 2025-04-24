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

from ydb_mcp.patches import suppress_task_destroyed_warning
from ydb_mcp.server import AUTH_MODE_ANONYMOUS, YDBMCPServer

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

# Use pytest-asyncio's built-in event_loop fixture
pytestmark = pytest.mark.asyncio(scope="session")


async def cleanup_pending_tasks():
    """Clean up any pending tasks in the current event loop."""
    with suppress_task_destroyed_warning():
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
    with suppress_task_destroyed_warning():
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

    # If not running, try to start a Docker container with YDB
    logger.info(f"YDB server is not running at {host}:{port}, trying to start Docker container")

    try:
        import docker

        # Try multiple Docker connection methods
        client = None
        connection_errors = []

        # Method 1: Try default Docker connection
        try:
            client = docker.from_env()
            # Test connection
            client.ping()
            logger.info("Successfully connected to Docker using default environment settings")
        except Exception as e:
            connection_errors.append(f"Default: {str(e)}")
            client = None

        # Method 2: Try DOCKER_HOST environment variable if set
        if client is None and os.environ.get("DOCKER_HOST"):
            try:
                client = docker.DockerClient(base_url=os.environ.get("DOCKER_HOST"))
                # Test connection
                client.ping()
                logger.info(
                    f"Successfully connected to Docker using DOCKER_HOST: {os.environ.get('DOCKER_HOST')}"
                )
            except Exception as e:
                connection_errors.append(f"DOCKER_HOST: {str(e)}")
                client = None

        # Method 3: Try standard Unix socket locations
        if client is None:
            socket_paths = [
                "unix:///var/run/docker.sock",  # Standard Docker socket
                "unix://" + os.path.expanduser("~/.docker/run/docker.sock"),  # macOS/Docker Desktop
                "unix://" + os.path.expanduser("~/.colima/default/docker.sock"),  # Colima
            ]

            for socket_path in socket_paths:
                try:
                    client = docker.DockerClient(base_url=socket_path)
                    # Test connection
                    client.ping()
                    logger.info(f"Successfully connected to Docker using socket: {socket_path}")
                    break
                except Exception as e:
                    connection_errors.append(f"{socket_path}: {str(e)}")
                    client = None

        if client is None:
            error_details = "\n".join(connection_errors)
            logger.error(f"Could not connect to Docker using any method. Errors:\n{error_details}")
            pytest.fail("Could not connect to Docker. Make sure Docker daemon is running.")
            return

        # Verify Docker connection
        version = client.version()
        logger.info(f"Docker connection successful. Version: {version.get('Version', 'unknown')}")

        # Start YDB container
        logger.info("Starting YDB Docker container")
        container = client.containers.run(
            image="ydbplatform/local-ydb:latest",
            detach=True,
            remove=True,
            hostname="localhost",
            platform="linux/amd64",
            ports={"2135/tcp": 2135, "2136/tcp": 2136, "8765/tcp": 8765, "9092/tcp": 9092},
            environment={
                "GRPC_TLS_PORT": "2135",
                "GRPC_PORT": "2136",
                "MON_PORT": "8765",
                "YDB_KAFKA_PROXY_PORT": "9092",
                "YDB_USE_IN_MEMORY_PDISKS": "1",
            },
        )

        # Wait for YDB to be ready (simple check: port is open)
        max_attempts = 30
        attempt = 0
        while attempt < max_attempts:
            if is_port_open(host, port):
                logger.info(f"YDB server is now running at {host}:{port}")
                break
            logger.info(f"Waiting for YDB server to start (attempt {attempt+1}/{max_attempts})...")
            time.sleep(1)
            attempt += 1

        if attempt == max_attempts:
            logger.error("Failed to start YDB server within timeout period")
            container.stop()
            pytest.fail("Could not start YDB server in Docker within timeout period")
            return

        # Give YDB a bit more time to initialize properly after port is open
        time.sleep(5)

        yield container

        # Stop the container after tests
        logger.info("Stopping YDB Docker container")
        container.stop()

    except (ImportError, ModuleNotFoundError):
        logger.warning("Docker Python library not installed. Cannot start YDB container.")
        pytest.fail("Docker Python library not installed. Install with: pip install docker")
    except docker.errors.DockerException as e:
        logger.warning(f"Docker error: {e}. Cannot start YDB container.")
        # Print more detailed error information to help diagnose the Docker connection issue
        logger.error(f"Docker connection details: Error type: {type(e)}, Error args: {e.args}")
        pytest.fail(
            f"Docker not available or not running: {e}. Make sure Docker daemon is running."
        )
    except Exception as e:
        logger.warning(f"Failed to start YDB Docker container: {e}")
        pytest.fail(f"Failed to start YDB container: {e}")


@pytest.fixture(scope="session")
async def mcp_server(ydb_server):
    """Create a YDB MCP server instance for testing."""
    # Check if the YDB server is available
    endpoint_url = urlparse(YDB_ENDPOINT)
    if endpoint_url.scheme in ("grpc", "grpcs"):
        host_port = endpoint_url.netloc.split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 2136
    else:
        host = "localhost"
        port = 2136

    if not is_port_open(host, port):
        pytest.fail(
            f"YDB server not available at {host}:{port}. Either start YDB manually or make sure Docker is running."
        )
        yield None
        return

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

    # Check if the YDB server is available
    endpoint_url = urlparse(YDB_ENDPOINT)
    if endpoint_url.scheme in ("grpc", "grpcs"):
        host_port = endpoint_url.netloc.split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 2136
    else:
        host = "localhost"
        port = 2136

    if not is_port_open(host, port):
        pytest.fail(
            f"YDB server not available at {host}:{port}. Either start YDB manually or make sure Docker is running."
        )
        yield None
        return

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
