"""Common fixtures and helpers for YDB MCP integration tests."""

import json
import logging
import os
import socket
import time
from urllib.parse import urlparse

import pytest
from mcp.types import CallToolRequest, CallToolRequestParams

from tests.docker_utils import start_ydb_container, stop_container, wait_for_port
from ydb_mcp.server import YDBMCPServer

YDB_ENDPOINT = os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136")
YDB_DATABASE = os.environ.get("YDB_DATABASE", "/local")

logging.getLogger("ydb").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("ydb_mcp").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            return s.connect_ex((host, port)) == 0
        except (socket.gaierror, ConnectionRefusedError, OSError):
            return False


@pytest.fixture(scope="session")
def ydb_server():
    """Ensure YDB is running; start a Docker container if it is not."""
    parsed = urlparse(YDB_ENDPOINT)
    host = parsed.hostname or "localhost"
    port = parsed.port or 2136

    if _is_port_open(host, port):
        yield None
        return

    logger.info("YDB not running at %s:%d — starting Docker container", host, port)
    container = start_ydb_container()
    wait_for_port(host, port, timeout=30)
    time.sleep(5)
    yield container
    stop_container(container)


@pytest.fixture
async def server(ydb_server):
    """YDBMCPServer instance per test. Connection is established lazily on first use,
    inside the test's own event loop — avoiding gRPC cross-loop issues."""
    s = YDBMCPServer(endpoint=YDB_ENDPOINT, database=YDB_DATABASE, allow_write=True)
    yield s
    await s.aclose()


@pytest.fixture
async def read_only_server(ydb_server):
    """YDBMCPServer using the default read-only query mode."""
    s = YDBMCPServer(endpoint=YDB_ENDPOINT, database=YDB_DATABASE)
    yield s
    await s.aclose()


async def call_tool(server: YDBMCPServer, tool_name: str, **params) -> dict:
    """Call a registered tool through MCP and return its parsed result."""
    handler = server._mcp_server.request_handlers[CallToolRequest]
    request = CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=tool_name, arguments=params),
    )
    result = (await handler(request)).root
    text = result.content[0].text
    if result.isError:
        return {"error": text}
    return json.loads(text)
