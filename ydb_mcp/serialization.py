"""JSON serialization utilities for YDB types."""

import base64
import datetime
import decimal
import json
from typing import Any


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles YDB-specific and other non-serializable types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo is not None:
                obj = obj.astimezone(datetime.timezone.utc)
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        if isinstance(obj, datetime.time):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return f"{obj.total_seconds()}s"
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, bytes):
            try:
                return obj.decode("utf-8")
            except UnicodeDecodeError:
                return base64.b64encode(obj).decode("ascii")
        return super().default(obj)


def _stringify_keys(obj: Any) -> Any:
    """Recursively convert all dict keys to strings for JSON serialization."""
    if isinstance(obj, dict):
        return {str(k): _stringify_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_stringify_keys(i) for i in obj]
    return obj


def _process_result_set(result_set: Any) -> dict:
    try:
        columns = [col.name for col in result_set.columns]
    except Exception as e:
        return {"error": str(e), "columns": [], "rows": []}
    try:
        rows = [[row[i] for i in range(len(columns))] for row in result_set.rows]
    except Exception as e:
        return {"error": str(e), "columns": columns, "rows": []}
    return {"columns": columns, "rows": rows}


def serialize_ydb_response(data: Any) -> str:
    """Serialize a YDB response to a JSON string.

    Handles YDB-specific types that the standard ``json`` module cannot serialize:
    ``Date``, ``Datetime``, ``Timestamp`` → ISO 8601 string;
    ``Interval`` → seconds string (e.g. ``"3600.0s"``);
    ``Decimal`` → decimal string;
    ``String`` (binary) → UTF-8 string, or base64 if not valid UTF-8.
    """
    return json.dumps(_stringify_keys(data), indent=2, cls=CustomJSONEncoder)
