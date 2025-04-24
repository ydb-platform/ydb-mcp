"""Tests for CustomJSONEncoder in ydb_mcp server.py"""

import base64
import datetime
import decimal
import json

import pytest

from ydb_mcp.server import CustomJSONEncoder


def test_datetime_serialization():
    """Test serialization of datetime objects."""
    # Create test datetime objects
    test_datetime = datetime.datetime(2023, 7, 15, 12, 30, 45)
    test_date = datetime.date(2023, 7, 15)
    test_time = datetime.time(12, 30, 45)
    test_timedelta = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=567000)

    # Create a nested structure with datetime objects
    test_data = {
        "datetime": test_datetime,
        "date": test_date,
        "time": test_time,
        "timedelta": test_timedelta,
        "nested": {"datetime": test_datetime},
        "list_with_dates": [test_date, test_datetime],
    }

    # Serialize using CustomJSONEncoder
    serialized = json.dumps(test_data, cls=CustomJSONEncoder)

    # Deserialize back to Python objects
    deserialized = json.loads(serialized)

    # Verify datetime was serialized to ISO format
    assert deserialized["datetime"] == "2023-07-15T12:30:45"
    assert deserialized["date"] == "2023-07-15"
    assert deserialized["time"] == "12:30:45"
    assert (
        deserialized["timedelta"] == "93784.567s"
    )  # 1 day, 2 hours, 3 minutes, 4.567 seconds in seconds
    assert deserialized["nested"]["datetime"] == "2023-07-15T12:30:45"
    assert deserialized["list_with_dates"][0] == "2023-07-15"
    assert deserialized["list_with_dates"][1] == "2023-07-15T12:30:45"


def test_bytes_serialization():
    """Test serialization of bytes objects."""
    # Create test bytes objects
    test_utf8_bytes = "UTF8 строка".encode("utf-8")
    test_binary = bytes([0x00, 0x01, 0x02, 0x03, 0xFF])

    # Create a nested structure with bytes objects
    test_data = {
        "utf8_bytes": test_utf8_bytes,
        "binary": test_binary,
        "nested": {"binary": test_binary},
        "list_with_bytes": [test_utf8_bytes, test_binary],
    }

    # Serialize using CustomJSONEncoder
    serialized = json.dumps(test_data, cls=CustomJSONEncoder)

    # Deserialize back to Python objects
    deserialized = json.loads(serialized)

    # Verify UTF-8 bytes were decoded as strings
    assert deserialized["utf8_bytes"] == "UTF8 строка"

    # Verify binary data was serialized to base64
    expected_binary_base64 = base64.b64encode(test_binary).decode("ascii")
    assert deserialized["binary"] == expected_binary_base64
    assert deserialized["nested"]["binary"] == expected_binary_base64
    assert deserialized["list_with_bytes"][0] == "UTF8 строка"
    assert deserialized["list_with_bytes"][1] == expected_binary_base64

    # Test that we can decode the base64 back to bytes
    assert base64.b64decode(deserialized["binary"]) == test_binary


def test_decimal_serialization():
    """Test serialization of decimal objects."""
    # Create test decimal objects
    test_decimal = decimal.Decimal("123.456789")

    # Create a nested structure with decimal objects
    test_data = {
        "decimal": test_decimal,
        "nested": {"decimal": test_decimal},
        "list_with_decimals": [test_decimal, decimal.Decimal("0.1")],
    }

    # Serialize using CustomJSONEncoder
    serialized = json.dumps(test_data, cls=CustomJSONEncoder)

    # Deserialize back to Python objects
    deserialized = json.loads(serialized)

    # Verify decimal was serialized to string
    assert deserialized["decimal"] == "123.456789"
    assert deserialized["nested"]["decimal"] == "123.456789"
    assert deserialized["list_with_decimals"][0] == "123.456789"
    assert deserialized["list_with_decimals"][1] == "0.1"


def test_mixed_data_serialization():
    """Test serialization of mixed data types including datetime, bytes, and decimals."""
    # Create test objects
    test_datetime = datetime.datetime(2023, 7, 15, 12, 30, 45)
    test_bytes = b"Hello, World!"
    test_decimal = decimal.Decimal("123.456789")

    # Create a complex nested structure with mixed data types
    test_data = {
        "string": "Regular string",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "none": None,
        "datetime": test_datetime,
        "bytes": test_bytes,
        "decimal": test_decimal,
        "nested": {"datetime": test_datetime, "bytes": test_bytes, "decimal": test_decimal},
        "list_mixed": [test_datetime, test_bytes, test_decimal, "string", 42],
    }

    # Serialize using CustomJSONEncoder
    serialized = json.dumps(test_data, cls=CustomJSONEncoder)

    # Verify we can deserialize the JSON (no errors)
    deserialized = json.loads(serialized)

    # The fact that we can deserialize without errors is a good test,
    # but let's also check a few specific values
    assert deserialized["string"] == "Regular string"
    assert deserialized["int"] == 42
    assert deserialized["float"] == 3.14
    assert deserialized["bool"] is True
    assert deserialized["none"] is None
    assert deserialized["datetime"] == "2023-07-15T12:30:45"
    assert deserialized["bytes"] == "Hello, World!"
    assert deserialized["decimal"] == "123.456789"
