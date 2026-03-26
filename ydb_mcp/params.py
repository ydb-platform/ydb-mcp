"""YDB query parameter building and parsing."""

import json

import ydb


def _build_ydb_params(params: dict) -> dict:
    """Normalize a params dict: add $ prefix and resolve explicit YDB types."""
    result = {}
    for key, value in params.items():
        param_key = key if key.startswith("$") else f"${key}"
        if isinstance(value, (list, tuple)) and len(value) == 2:
            param_value, type_name = value
            if isinstance(type_name, str) and hasattr(ydb.PrimitiveType, type_name):
                result[param_key] = ydb.TypedValue(param_value, getattr(ydb.PrimitiveType, type_name))
            else:
                result[param_key] = param_value
        else:
            result[param_key] = value
    return result


def _parse_params_str(params_str: str | dict) -> dict:
    """Parse a JSON params string (or dict) and normalize it for YDB."""
    if not params_str:
        return {}
    if isinstance(params_str, dict):
        return _build_ydb_params(params_str)
    if not params_str.strip():
        return {}
    return _build_ydb_params(json.loads(params_str))
