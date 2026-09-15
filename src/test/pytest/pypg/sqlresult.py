# Copyright (c) 2025, PostgreSQL Global Development Group

"""The result of running a SQL query through :meth:`PostgresServer.sql`.

A single explicit type with named accessors, rather than a value that is
sometimes a scalar, sometimes a tuple, and sometimes a list (the shape the
in-process libpq layer infers). Callers say what they expect:

    node.sql("SELECT 1").scalar()              -> "1"
    node.sql("SELECT a, b FROM t").row()        -> ("a-val", "b-val")
    node.sql("SELECT x FROM t").column()        -> ["x1", "x2", ...]
    node.sql("SELECT a, b FROM t").rows         -> [("a", "b"), ...]
    node.sql("INSERT ...").rows                 -> []  (no rows)

Values are the text psql prints (unaligned, tuples-only). The result is truthy
when it has at least one row, and iterating it yields the rows.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

# psql's default unaligned field separator.
_FIELD_SEP = "|"


class SqlResult:
    """Rows returned by a SQL statement, with explicit accessors.

    Constructed from psql's unaligned tuples-only stdout. A statement that
    returns no rows (INSERT/DDL, or an empty SELECT) yields an empty result.
    """

    def __init__(self, rows: List[Tuple[str, ...]], raw: str = ""):
        self._rows = rows
        self.raw = raw

    @classmethod
    def from_psql(cls, stdout: str) -> "SqlResult":
        """Parse psql --no-align --tuples-only stdout into rows.

        Each non-final newline delimits a row; each row splits on the unaligned
        field separator. A wholly empty output is zero rows. The trailing
        newline psql emits is not a row.
        """
        text = stdout
        body = text[:-1] if text.endswith("\n") else text
        if body == "":
            return cls([], raw=stdout)
        rows = [tuple(line.split(_FIELD_SEP)) for line in body.split("\n")]
        return cls(rows, raw=stdout)

    @property
    def rows(self) -> List[Tuple[str, ...]]:
        """All rows as tuples of column-text values."""
        return self._rows

    def scalar(self) -> Optional[str]:
        """The single value of a one-row, one-column result.

        Returns None for an empty result (no rows). Raises if the result has
        more than one row or the row has more than one column, so a mis-shaped
        query is caught rather than silently truncated.
        """
        if not self._rows:
            return None
        if len(self._rows) != 1 or len(self._rows[0]) != 1:
            raise ValueError(
                "scalar() expects exactly one row and one column, got "
                f"{len(self._rows)} row(s) of "
                f"{len(self._rows[0]) if self._rows else 0} column(s)"
            )
        return self._rows[0][0]

    def row(self) -> Optional[Tuple[str, ...]]:
        """The single row of a one-row result (any number of columns).

        Returns None for an empty result; raises if there is more than one row.
        """
        if not self._rows:
            return None
        if len(self._rows) != 1:
            raise ValueError(f"row() expects exactly one row, got {len(self._rows)}")
        return self._rows[0]

    def column(self, index: int = 0) -> List[str]:
        """The values of column *index* across every row."""
        return [r[index] for r in self._rows]

    def __bool__(self) -> bool:
        return bool(self._rows)

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def __eq__(self, other) -> bool:
        if isinstance(other, SqlResult):
            return self._rows == other._rows
        return NotImplemented

    def __repr__(self) -> str:
        return f"SqlResult({self._rows!r})"
