"""Integration tests for YDB MCP server.

Requires a running YDB instance (started automatically via Docker if absent).
Tests call server methods directly — no HTTP transport needed.
"""

import asyncio
import datetime
import time
import warnings
from decimal import Decimal

import pytest

from tests.integration.conftest import call_tool

warnings.filterwarnings("ignore", message="datetime.datetime.utcfromtimestamp.*", category=DeprecationWarning)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Basic queries
# ---------------------------------------------------------------------------


async def test_simple_query(server):
    result = await call_tool(server, "ydb_query", sql="SELECT 1+1 AS result")
    assert "result_sets" in result
    rs = result["result_sets"][0]
    assert rs["columns"] == ["result"]
    assert rs["rows"][0][0] == 2


async def test_single_resultset_format(server):
    result = await call_tool(server, "ydb_query", sql="SELECT 42 AS answer")
    assert "result_sets" in result
    assert len(result["result_sets"]) == 1
    rs = result["result_sets"][0]
    assert rs["columns"][0] == "answer"
    assert rs["rows"][0][0] == 42


async def test_complex_query_multiple_resultsets(server):
    result = await call_tool(
        server,
        "ydb_query",
        sql="SELECT 1 AS value; SELECT 'test' AS text, 2.5 AS number;",
    )
    assert "result_sets" in result
    assert result["result_sets"][0]["rows"][0][0] == 1

    second = result["result_sets"][1]
    text_val = second["rows"][0][second["columns"].index("text")]
    num_val = second["rows"][0][second["columns"].index("number")]
    assert text_val in ("test", b"test")
    assert num_val == 2.5


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


async def test_create_table_insert_query_drop(server):
    table = f"mcp_test_{int(time.time())}"
    try:
        r = await call_tool(
            server, "ydb_query",
            sql=f"CREATE TABLE {table} (id Uint64, name Utf8, PRIMARY KEY (id));"
        )
        assert "error" not in r

        r = await call_tool(
            server, "ydb_query",
            sql=f"UPSERT INTO {table} (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');"
        )
        assert "error" not in r

        r = await call_tool(server, "ydb_query", sql=f"SELECT * FROM {table} ORDER BY id;")
        rs = r["result_sets"][0]
        assert len(rs["rows"]) == 3
        assert "id" in rs["columns"]
        assert "name" in rs["columns"]
        id_idx = rs["columns"].index("id")
        assert rs["rows"][0][id_idx] == 1
    finally:
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {table};")


async def test_multiple_resultsets_with_join(server):
    t1 = f"mcp_t1_{int(time.time())}"
    t2 = f"mcp_t2_{int(time.time())}"
    try:
        await call_tool(server, "ydb_query", sql=(
            f"CREATE TABLE {t1} (id Uint64, name Utf8, PRIMARY KEY (id));"
            f"CREATE TABLE {t2} (id Uint64, value Double, PRIMARY KEY (id));"
        ))
        await call_tool(server, "ydb_query", sql=(
            f"UPSERT INTO {t1} (id, name) VALUES (1, 'First'), (2, 'Second'), (3, 'Third');"
        ))
        await call_tool(server, "ydb_query", sql=(
            f"UPSERT INTO {t2} (id, value) VALUES (1, 10.5), (2, 20.75), (3, 30.25);"
        ))

        r = await call_tool(
            server, "ydb_query",
            sql=f"SELECT * FROM {t1} ORDER BY id; SELECT * FROM {t2} ORDER BY id;"
        )
        assert len(r["result_sets"]) == 2
        assert len(r["result_sets"][0]["rows"]) == 3
        assert len(r["result_sets"][1]["rows"]) == 3

        join = await call_tool(
            server, "ydb_query",
            sql=f"SELECT t1.id, t1.name, t2.value FROM {t1} t1 JOIN {t2} t2 ON t1.id = t2.id ORDER BY t1.id;"
        )
        rs = join["result_sets"][0]
        assert len(rs["rows"]) == 3
        assert len(rs["columns"]) == 3
    finally:
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {t1};")
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {t2};")


# ---------------------------------------------------------------------------
# Parameterized queries
# ---------------------------------------------------------------------------


async def test_parameterized_query(server):
    import json
    result = await call_tool(
        server,
        "ydb_query_with_params",
        sql="DECLARE $answer AS Int32; DECLARE $greeting AS Utf8; SELECT $answer AS answer, $greeting AS greeting",
        params=json.dumps({"answer": [-42, "Int32"], "greeting": "hello"}),
    )
    assert "result_sets" in result
    rs = result["result_sets"][0]
    answer = rs["rows"][0][rs["columns"].index("answer")]
    greeting = rs["rows"][0][rs["columns"].index("greeting")]
    assert answer == -42
    assert greeting in ("hello", b"hello")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


async def test_data_types_basic(server):
    result = await call_tool(server, "ydb_query", sql="SELECT 1 AS value, 'test' AS text")
    rs = result["result_sets"][0]
    row = rs["rows"][0]
    assert row[rs["columns"].index("value")] == 1
    text = row[rs["columns"].index("text")]
    assert text in ("test", b"test")


async def test_all_data_types(server):
    result = await call_tool(
        server,
        "ydb_query",
        sql="""
            SELECT
                true AS bool_true, false AS bool_false,
                -128 AS int8_min, 127 AS int8_max,
                -32768 AS int16_min, 32767 AS int16_max,
                -2147483648 AS int32_min, 2147483647 AS int32_max,
                -9223372036854775808 AS int64_min, 9223372036854775807 AS int64_max,
                0 AS uint8_min, 255 AS uint8_max,
                0 AS uint16_min, 65535 AS uint16_max,
                0 AS uint32_min, 4294967295 AS uint32_max,
                0 AS uint64_min, 18446744073709551615 AS uint64_max,
                3.14 AS float_value,
                2.7182818284590452 AS double_value,
                "Hello, World!" AS string_value,
                "UTF8 строка" AS utf8_value,
                Date("2023-07-15") AS date_value,
                Datetime("2023-07-15T12:30:45Z") AS datetime_value,
                Timestamp("2023-07-15T12:30:45.123456Z") AS timestamp_value,
                INTERVAL("P1DT2H3M4.567S") AS interval_value,
                CAST("123.456789" AS Decimal(22,9)) AS decimal_value,
                AsList(1, 2, 3) AS int_list
        """,
    )
    rs = result["result_sets"][0]
    row = rs["rows"][0]

    def get(col):
        return row[rs["columns"].index(col)]

    assert get("bool_true") is True
    assert get("bool_false") is False
    assert get("int8_min") == -128
    assert get("int8_max") == 127
    assert get("int32_min") == -2147483648
    assert get("int32_max") == 2147483647
    assert get("int64_min") == -9223372036854775808
    assert get("int64_max") == 9223372036854775807
    assert get("uint8_max") == 255
    assert get("uint32_max") == 4294967295
    assert get("uint64_max") == 18446744073709551615
    assert abs(get("float_value") - 3.14) < 0.0001
    assert abs(get("double_value") - 2.7182818284590452) < 1e-15
    assert get("string_value") in ("Hello, World!", b"Hello, World!")
    assert get("utf8_value") in ("UTF8 строка", "UTF8 строка".encode())

    date_val = get("date_value")
    if isinstance(date_val, str):
        assert datetime.date.fromisoformat(date_val) == datetime.date(2023, 7, 15)
    else:
        assert date_val == datetime.date(2023, 7, 15)

    dt_val = get("datetime_value")
    if isinstance(dt_val, str):
        dt_val = datetime.datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=datetime.timezone.utc)
    assert dt_val == datetime.datetime(2023, 7, 15, 12, 30, 45, tzinfo=datetime.timezone.utc)

    ts_val = get("timestamp_value")
    if isinstance(ts_val, str):
        ts_val = datetime.datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
    if ts_val.tzinfo is None:
        ts_val = ts_val.replace(tzinfo=datetime.timezone.utc)
    assert ts_val == datetime.datetime(2023, 7, 15, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)

    interval_val = get("interval_value")
    expected_interval = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=567000)
    if isinstance(interval_val, str):
        assert interval_val.endswith("s")
        assert datetime.timedelta(seconds=float(interval_val[:-1])).total_seconds() == expected_interval.total_seconds()
    else:
        assert interval_val.total_seconds() == expected_interval.total_seconds()

    decimal_val = get("decimal_value")
    if isinstance(decimal_val, str):
        decimal_val = Decimal(decimal_val)
    assert decimal_val == Decimal("123.456789")

    int_list = get("int_list")
    assert isinstance(int_list, list)
    assert int_list == [1, 2, 3]


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


async def test_ydb_status(server):
    result = await call_tool(server, "ydb_status")
    assert result["status"] == "running"
    assert result["ydb_connection"] == "connected"
    assert "ydb_endpoint" in result
    assert "ydb_database" in result
    assert "auth_mode" in result


# ---------------------------------------------------------------------------
# Directory / path operations
# ---------------------------------------------------------------------------


async def test_list_root_directory(server):
    result = await call_tool(server, "ydb_list_directory", path="/")
    assert "error" not in result
    assert result["path"] == "/"
    assert isinstance(result["items"], list)
    assert len(result["items"]) > 0
    for item in result["items"]:
        assert "name" in item
        assert "type" in item
        assert "owner" in item


async def test_list_directory_nonexistent(server):
    path = f"/nonexistent_{int(time.time())}"
    result = await call_tool(server, "ydb_list_directory", path=path)
    assert "error" in result


async def test_describe_path_table(server):
    table = f"mcp_describe_{int(time.time())}"
    try:
        await call_tool(
            server, "ydb_query",
            sql=f"CREATE TABLE {table} (id Uint64, name Utf8, value Double, PRIMARY KEY (id));"
        )
        await asyncio.sleep(1)

        db = server.database.rstrip("/")
        result = await call_tool(server, "ydb_describe_path", path=f"{db}/{table}")
        if "error" not in result:
            assert result["type"] == "TABLE"
            assert "table" in result
            assert len(result["table"]["columns"]) > 0
    finally:
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {table};")


async def test_describe_nonexistent_path(server):
    path = f"/nonexistent_{int(time.time())}"
    result = await call_tool(server, "ydb_describe_path", path=path)
    assert "error" in result


async def test_list_directory_after_table_creation(server):
    table = f"mcp_dir_test_{int(time.time())}"
    db = server.database.rstrip("/")
    try:
        await call_tool(
            server, "ydb_query",
            sql=f"CREATE TABLE {table} (id Uint64, PRIMARY KEY (id));"
        )
        await asyncio.sleep(1)

        for _ in range(5):
            listing = await call_tool(server, "ydb_list_directory", path=db)
            if any(item["name"] == table for item in listing.get("items", [])):
                break
            await asyncio.sleep(1)
        else:
            pytest.fail(f"Table {table!r} not found in directory listing")
    finally:
        await call_tool(server, "ydb_query", sql=f"DROP TABLE {table};")
