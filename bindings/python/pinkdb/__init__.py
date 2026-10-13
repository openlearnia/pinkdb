"""pinkdb — a thin, dependency-free Python binding for pinkdb's C ABI (v1).

The binding loads ``libpinkdb_ffi`` with the standard-library :mod:`ctypes`
module (no build step, no third-party packages) and wraps the C ABI declared
in ``crates/pinkdb-ffi/include/pinkdb.h``. Build the shared library first::

    cargo build --release -p pinkdb-ffi

Usage::

    >>> import pinkdb
    >>> with pinkdb.connect_memory() as db:
    ...     db.execute("CREATE TABLE t (x INTEGER);")
    ...     db.execute("INSERT INTO t VALUES (42);")
    ...     rows = db.execute("SELECT x FROM t;")[0].rows
    >>> rows
    [[42]]

Like the C ABI underneath, :class:`Database` handles are **not thread-safe**:
use one database per thread.

The shared library is located, in order, from:

1. the ``PINKDB_FFI_LIB`` environment variable (full path to the dylib/so), or
2. the repo checkout this package lives in
   (``<repo>/target/release/libpinkdb_ffi.dylib`` / ``.so``, computed relative
   to this file).
"""

from __future__ import annotations

import ctypes
import datetime as _datetime
import os
from pathlib import Path

__all__ = [
    "Error",
    "Database",
    "Result",
    "abi_version",
    "connect",
    "connect_memory",
    "library_path",
]

__version__ = "0.1.0"

# ----- statement kinds (pinkdb_result_kind) -----

_STMT_KINDS = {
    0: "selected",
    1: "created",
    2: "inserted",
    3: "updated",
    4: "deleted",
    5: "began",
    6: "committed",
    7: "rolledback",
    8: "created_view",
    9: "dropped_table",
    10: "dropped_view",
    11: "created_database",
    12: "dropped_database",
    13: "used",
}

# ----- value / column type tags (PINKDB_TYPE_*) -----

_TYPE_NULL = 0
_TYPE_BOOLEAN = 1
_TYPE_INTEGER = 2
_TYPE_BIGINT = 3
_TYPE_DOUBLE = 4
_TYPE_VARCHAR = 5
_TYPE_DATE = 6
_TYPE_TIMESTAMP = 7

_TYPE_NAMES = {
    _TYPE_NULL: "null",
    _TYPE_BOOLEAN: "boolean",
    _TYPE_INTEGER: "integer",
    _TYPE_BIGINT: "bigint",
    _TYPE_DOUBLE: "double",
    _TYPE_VARCHAR: "varchar",
    _TYPE_DATE: "date",
    _TYPE_TIMESTAMP: "timestamp",
}

_EPOCH_DATE = _datetime.date(1970, 1, 1)
_EPOCH_DATETIME = _datetime.datetime(1970, 1, 1)


class Error(Exception):
    """An error reported by the pinkdb C ABI (see ``pinkdb_last_error``)."""


class _PinkValue(ctypes.Structure):
    """C mirror of ``pink_value`` (exact field order and widths).

    ``text_ptr`` is declared ``c_void_p`` (not ``c_char_p``) so ctypes never
    tries to NUL-terminate it: VARCHAR payloads may contain embedded NULs and
    must be sliced with ``text_len``.
    """

    _fields_ = [
        ("type_tag", ctypes.c_int32),  # int32_t type_tag
        ("is_null", ctypes.c_int32),  # int32_t is_null
        ("int64", ctypes.c_int64),  # int64_t int64
        ("double_", ctypes.c_double),  # double double_
        ("text_ptr", ctypes.c_void_p),  # const char *text_ptr
        ("text_len", ctypes.c_size_t),  # size_t text_len
    ]


def _candidate_library_paths():
    override = os.environ.get("PINKDB_FFI_LIB")
    if override:
        return [Path(override)]
    # <repo>/bindings/python/pinkdb/__init__.py -> parents[3] is <repo>.
    release_dir = Path(__file__).resolve().parents[3] / "target" / "release"
    return [release_dir / "libpinkdb_ffi.dylib", release_dir / "libpinkdb_ffi.so"]


def _find_library() -> str:
    override = os.environ.get("PINKDB_FFI_LIB")
    candidates = _candidate_library_paths()
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate.resolve())
    if override:
        raise Error(
            f"PINKDB_FFI_LIB={override!r} does not point to an existing file"
        )
    raise Error(
        "cannot find the pinkdb FFI shared library; build it with "
        "`cargo build --release -p pinkdb-ffi` from the repo root "
        f"(looked for {[str(p) for p in candidates]}), or set PINKDB_FFI_LIB "
        "to the full path of libpinkdb_ffi.dylib / libpinkdb_ffi.so"
    )


def _bind(lib: ctypes.CDLL) -> None:
    """Declare argtypes/restype for every exported function in pinkdb.h."""
    lib.pinkdb_abi_version.argtypes = []
    lib.pinkdb_abi_version.restype = ctypes.c_uint32

    lib.pinkdb_open.argtypes = [ctypes.c_char_p]
    lib.pinkdb_open.restype = ctypes.c_void_p

    lib.pinkdb_open_memory.argtypes = []
    lib.pinkdb_open_memory.restype = ctypes.c_void_p

    lib.pinkdb_close.argtypes = [ctypes.c_void_p]
    lib.pinkdb_close.restype = None

    lib.pinkdb_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    lib.pinkdb_execute.restype = ctypes.c_void_p

    lib.pinkdb_result_free.argtypes = [ctypes.c_void_p]
    lib.pinkdb_result_free.restype = None

    lib.pinkdb_result_statement_count.argtypes = [ctypes.c_void_p]
    lib.pinkdb_result_statement_count.restype = ctypes.c_size_t

    lib.pinkdb_result_kind.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.pinkdb_result_kind.restype = ctypes.c_int32

    lib.pinkdb_result_column_count.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.pinkdb_result_column_count.restype = ctypes.c_size_t

    lib.pinkdb_result_column_name.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
    ]
    lib.pinkdb_result_column_name.restype = ctypes.c_char_p

    lib.pinkdb_result_column_type.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
    ]
    lib.pinkdb_result_column_type.restype = ctypes.c_int32

    lib.pinkdb_result_row_count.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.pinkdb_result_row_count.restype = ctypes.c_size_t

    lib.pinkdb_result_value.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
    ]
    lib.pinkdb_result_value.restype = _PinkValue

    lib.pinkdb_result_affected.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.pinkdb_result_affected.restype = ctypes.c_size_t

    lib.pinkdb_result_name.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.pinkdb_result_name.restype = ctypes.c_char_p

    lib.pinkdb_last_error.argtypes = []
    lib.pinkdb_last_error.restype = ctypes.c_char_p


library_path = _find_library()

try:
    _LIB = ctypes.CDLL(library_path)
except OSError as exc:  # wrong architecture, missing symbols, ...
    raise Error(f"failed to load {library_path!r}: {exc}") from exc

_bind(_LIB)


def abi_version() -> int:
    """ABI version reported by the loaded library (currently 1)."""
    return int(_LIB.pinkdb_abi_version())


def _last_error(lib: ctypes.CDLL) -> str:
    message = lib.pinkdb_last_error()
    if message is None:
        return "unknown pinkdb error"
    return message.decode("utf-8", "replace")


def _value_to_python(v: _PinkValue):
    """Map one ``pink_value`` to a Python object (NULL -> None)."""
    if v.is_null:
        return None
    tag = v.type_tag
    if tag == _TYPE_BOOLEAN:
        return bool(v.int64)
    if tag in (_TYPE_INTEGER, _TYPE_BIGINT):
        return int(v.int64)
    if tag == _TYPE_DOUBLE:
        return float(v.double_)
    if tag == _TYPE_VARCHAR:
        if not v.text_ptr or v.text_len == 0:
            return ""
        # Trust text_len: the payload may contain embedded NULs.
        return ctypes.string_at(v.text_ptr, v.text_len).decode("utf-8")
    if tag == _TYPE_DATE:
        return _EPOCH_DATE + _datetime.timedelta(days=v.int64)
    if tag == _TYPE_TIMESTAMP:
        # Microseconds since the Unix epoch, naive UTC.
        return _EPOCH_DATETIME + _datetime.timedelta(microseconds=v.int64)
    # PINKDB_TYPE_NULL (with is_null == 0) or a future tag: value is unknown.
    return None


class Result:
    """One statement's outcome, materialized as Python values.

    Attributes
    ----------
    kind:
        One of ``selected``, ``created``, ``inserted``, ``updated``,
        ``deleted``, ``began``, ``committed``, ``rolledback``,
        ``created_view``, ``dropped_table``, ``dropped_view``,
        ``created_database``, ``dropped_database``, ``used``.
    columns / column_types:
        For ``selected``: column names and type names (``boolean``,
        ``integer``, ``bigint``, ``double``, ``varchar``, ``date``,
        ``timestamp``); empty for every other kind.
    rows:
        For ``selected``: ``list`` of rows, each a ``list`` of Python values
        (NULL -> ``None``, BOOLEAN -> ``bool``, INTEGER/BIGINT -> ``int``,
        DOUBLE -> ``float``, VARCHAR -> ``str``, DATE -> ``datetime.date``,
        TIMESTAMP -> naive UTC ``datetime.datetime``).
    affected:
        Rows affected by ``inserted``/``updated``/``deleted``; 0 otherwise.
    name:
        Object/session name for ``created_view``/``dropped_table``/
        ``dropped_view``/``created_database``/``dropped_database``/``used``;
        ``None`` otherwise.
    """

    __slots__ = ("kind", "columns", "column_types", "rows", "affected", "name")

    def __init__(self, kind, columns, column_types, rows, affected, name):
        self.kind = kind
        self.columns = columns
        self.column_types = column_types
        self.rows = rows
        self.affected = affected
        self.name = name

    def __repr__(self) -> str:
        return (
            f"Result(kind={self.kind!r}, columns={self.columns!r}, "
            f"column_types={self.column_types!r}, rows={self.rows!r}, "
            f"affected={self.affected!r}, name={self.name!r})"
        )


def _snapshot(lib: ctypes.CDLL, result_ptr) -> list:
    """Materialize every statement in a ``pinkdb_result *`` into Results."""
    results = []
    for stmt in range(lib.pinkdb_result_statement_count(result_ptr)):
        kind_tag = lib.pinkdb_result_kind(result_ptr, stmt)
        kind = _STMT_KINDS.get(kind_tag, "unknown")
        if kind_tag == 0:  # selected
            ncols = lib.pinkdb_result_column_count(result_ptr, stmt)
            columns = []
            column_types = []
            for col in range(ncols):
                name = lib.pinkdb_result_column_name(result_ptr, stmt, col)
                columns.append(name.decode("utf-8") if name is not None else "")
                column_types.append(
                    _TYPE_NAMES.get(
                        lib.pinkdb_result_column_type(result_ptr, stmt, col),
                        "unknown",
                    )
                )
            nrows = lib.pinkdb_result_row_count(result_ptr, stmt)
            rows = [
                [
                    _value_to_python(
                        lib.pinkdb_result_value(result_ptr, stmt, row, col)
                    )
                    for col in range(ncols)
                ]
                for row in range(nrows)
            ]
            results.append(Result(kind, columns, column_types, rows, 0, None))
        else:
            affected = lib.pinkdb_result_affected(result_ptr, stmt)
            name = lib.pinkdb_result_name(result_ptr, stmt)
            results.append(
                Result(
                    kind,
                    [],
                    [],
                    [],
                    int(affected),
                    name.decode("utf-8") if name is not None else None,
                )
            )
    return results


class Database:
    """A pinkdb database handle (durable file or in-memory).

    Not thread-safe, mirroring the C ABI: use one ``Database`` per thread.
    Supports use as a context manager (closes on exit).
    """

    __slots__ = ("_lib", "_handle")

    def __init__(self, lib: ctypes.CDLL, handle):
        self._lib = lib
        self._handle = handle

    def execute(self, sql: str) -> list:
        """Execute one or more ``;``-separated SQL statements.

        Returns a :class:`Result` per statement. Raises :class:`Error` with
        the message from ``pinkdb_last_error`` if any statement fails; the
        handle remains usable afterwards.
        """
        if self._handle is None:
            raise Error("database is closed")
        if not isinstance(sql, str):
            raise TypeError("sql must be a str")
        data = sql.encode("utf-8")
        result_ptr = self._lib.pinkdb_execute(self._handle, data, len(data))
        if not result_ptr:
            raise Error(_last_error(self._lib))
        try:
            return _snapshot(self._lib, result_ptr)
        finally:
            self._lib.pinkdb_result_free(result_ptr)

    def close(self) -> None:
        """Close the handle. Idempotent and safe to call more than once."""
        handle = self._handle
        if handle is not None:
            self._handle = None
            self._lib.pinkdb_close(handle)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:  # pragma: no cover - interpreter shutdown races
            pass

    def __repr__(self) -> str:
        state = "closed" if self._handle is None else "open"
        return f"<pinkdb.Database ({state})>"


def connect(path) -> Database:
    """Open a durable pinkdb database (checkpoint + WAL) at ``path``."""
    raw = os.fspath(path)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    handle = _LIB.pinkdb_open(raw)
    if not handle:
        raise Error(_last_error(_LIB))
    return Database(_LIB, handle)


def connect_memory() -> Database:
    """Open a fresh, empty in-memory pinkdb database."""
    handle = _LIB.pinkdb_open_memory()
    if not handle:  # documented never to fail; stay safe anyway
        raise Error(_last_error(_LIB))
    return Database(_LIB, handle)
