# Copyright (c) 2025, PostgreSQL Global Development Group

"""PostgreSQL type-OID registry and text-to-Python value conversion.

Maps a column's type OID to a converter so the in-process binding can return
real Python values (int, bool, datetime, list, ...) from the text-format wire
data, mirroring what psql would print. register_type_info() extends the table;
simplify_query_results() collapses 1x1 / 1xN / Nx1 shapes for conn.sql().
"""

import datetime
import decimal
import json
import uuid
from typing import Any, Callable, Dict

# Type registry - maps OID to converter function
_type_converters: Dict[int, Callable[[str], Any]] = {}
_array_to_elem_map: Dict[int, int] = {}


def register_type_info(
    name: str, oid: int, array_oid: int, converter: Callable[[str], Any]
):
    """
    Register a PostgreSQL type with its OID, array OID, and conversion function.

    Usage:
        register_type_info("bool", 16, 1000, lambda v: v == "t")
    """
    _type_converters[oid] = converter
    if array_oid is not None:
        _array_to_elem_map[array_oid] = oid


def _parse_array(value: str, elem_oid: int):
    """Parse PostgreSQL array syntax into nested Python lists."""
    stack: list[list] = []
    current_element: list[str] = []
    in_quotes = False
    was_quoted = False
    pos = 0

    while pos < len(value):
        char = value[pos]

        if in_quotes:
            if char == "\\":
                next_char = value[pos + 1]
                if next_char not in '"\\':
                    raise NotImplementedError('Only \\" and \\\\ escapes are supported')
                current_element.append(next_char)
                pos += 2
                continue
            if char == '"':
                in_quotes = False
            else:
                current_element.append(char)
        elif char == '"':
            in_quotes = True
            was_quoted = True
        elif char == "{":
            stack.append([])
        elif char in ",}":
            if current_element or was_quoted:
                elem = "".join(current_element)
                if not was_quoted and elem == "NULL":
                    stack[-1].append(None)
                else:
                    stack[-1].append(_convert_pg_value(elem, elem_oid))
                current_element = []
                was_quoted = False
            if char == "}":
                completed = stack.pop()
                if not stack:
                    return completed
                stack[-1].append(completed)
        elif char != " ":
            current_element.append(char)
        pos += 1

    raise ValueError(f"Malformed array literal: {value}")


def _convert_pg_value(value: str, type_oid: int) -> Any:
    """
    Convert PostgreSQL string value to appropriate Python type based on OID.
    Uses the registered type converters from register_type_info().
    """
    # Check if it's an array type
    if type_oid in _array_to_elem_map:
        elem_oid = _array_to_elem_map[type_oid]
        return _parse_array(value, elem_oid)

    # Use registered converter if available
    converter = _type_converters.get(type_oid)
    if converter:
        return converter(value)

    # Unknown types - return as string
    return value


def simplify_query_results(results) -> Any:
    """
    Simplify the results of a query so that the caller doesn't have to unpack
    lists and tuples of length 1.
    """
    if len(results) == 1:
        row = results[0]
        if len(row) == 1:
            # If there's only a single cell, just return the value
            return row[0]
        # If there's only a single row, just return that row
        return row

    if len(results) != 0 and len(results[0]) == 1:
        # If there's only a single column, return an array of values
        return [row[0] for row in results]

    # if there are multiple rows and columns, return the results as is
    return results


# Register standard PostgreSQL types that we'll likely encounter in tests
register_type_info("bool", 16, 1000, lambda v: v == "t")
register_type_info("int2", 21, 1005, int)
register_type_info("int4", 23, 1007, int)
register_type_info("int8", 20, 1016, int)
register_type_info("float4", 700, 1021, float)
register_type_info("float8", 701, 1022, float)
register_type_info("numeric", 1700, 1231, decimal.Decimal)
register_type_info("text", 25, 1009, str)
register_type_info("varchar", 1043, 1015, str)
register_type_info("date", 1082, 1182, datetime.date.fromisoformat)
register_type_info("time", 1083, 1183, datetime.time.fromisoformat)
register_type_info("timestamp", 1114, 1115, datetime.datetime.fromisoformat)
register_type_info("timestamptz", 1184, 1185, datetime.datetime.fromisoformat)
register_type_info("uuid", 2950, 2951, uuid.UUID)
register_type_info("json", 114, 199, json.loads)
register_type_info("jsonb", 3802, 3807, json.loads)
