"""Integration tests for YDB directory listing and path description."""

import asyncio
import time
import warnings

import pytest

from tests.integration.conftest import call_tool

warnings.filterwarnings("ignore", message="datetime.datetime.utcfromtimestamp.*", category=DeprecationWarning)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_list_root_directory(server):
    result = await call_tool(server, "ydb_list_directory", path="/")
    assert "error" not in result
    assert "items" in result
    assert len(result["items"]) > 0
    for item in result["items"]:
        assert "name" in item
        assert "type" in item
        assert "owner" in item


async def test_list_directory_after_table_creation(server):
    db = server.database.rstrip("/")
    table = f"path_test_{int(time.time())}"
    try:
        r = await call_tool(
            server, "ydb_query",
            sql=f"CREATE TABLE {table} (id Uint64, name Utf8, PRIMARY KEY (id));"
        )
        assert "error" not in r, f"Error creating table: {r}"
        await asyncio.sleep(1)

        found = False
        for _ in range(5):
            listing = await call_tool(server, "ydb_list_directory", path=db)
            if any(item["name"] == table for item in listing.get("items", [])):
                found = True
                break
            await asyncio.sleep(1)

        assert found, f"Table {table!r} not visible in {db!r}"
    finally:
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {table};")


async def test_describe_each_root_item(server):
    listing = await call_tool(server, "ydb_list_directory", path="/")
    assert "items" in listing

    for item in listing["items"]:
        path = f"/{item['name']}"
        desc = await call_tool(server, "ydb_describe_path", path=path)
        assert "path" in desc, f"Missing 'path' for {path}: {desc}"
        assert desc["path"] == path
        assert "type" in desc
        assert "name" in desc
        assert "owner" in desc
        if desc["type"] == "TABLE":
            assert "table" in desc
            assert len(desc["table"]["columns"]) > 0
