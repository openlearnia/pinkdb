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

1. the ``PINKDB_FFI_LIB`` environment variable (full path to the
   dylib/so/dll), or
2. the repo checkout this package lives in
   (``<repo>/target/release/libpinkdb_ffi.dylib`` / ``.so`` / ``.dll``,
   computed relative to this file).

All failures are reported as :class:`Error`: the loaded library must export
the pinkdb ABI (version 1), out-of-range DATE/TIMESTAMP values raise instead
of leaking :class:`OverflowError`, and bad arguments (empty or NUL-containing
paths, strings that are not valid UTF-8) are rejected before the native call.
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
    14: "loaded_extension",
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

#: ABI version this binding targets (``pinkdb_abi_version``).
_ABI_VERSION = 1


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
    return [
        release_dir / "libpinkdb_ffi.dylib",
        release_dir / "libpinkdb_ffi.so",
        release_dir / "libpinkdb_ffi.dll",
    ]


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
        "to the full path of libpinkdb_ffi.dylib / libpinkdb_ffi.so / "
        "libpinkdb_ffi.dll"
    )


def _bind(lib: ctypes.CDLL, path: str) -> None:
    """Declare argtypes/restype for every exported function in pinkdb.h.

    Raises :class:`Error` when a symbol is missing, i.e. when ``path`` points
    at a loadable library that is not pinkdb-ffi (a wrong dylib/so/dll).
    """

    def declare(name: str, argtypes, restype) -> None:
        try:
            func = getattr(lib, name)
        except AttributeError as exc:
            raise Error(
                f"{path!r} is not a pinkdb-ffi library (missing symbol {name})"
            ) from exc
        func.argtypes = argtypes
        func.restype = restype

    declare("pinkdb_abi_version", [], ctypes.c_uint32)
    declare("pinkdb_open", [ctypes.c_char_p], ctypes.c_void_p)
    declare("pinkdb_open_memory", [], ctypes.c_void_p)
    declare("pinkdb_close", [ctypes.c_void_p], None)
    declare(
        "pinkdb_execute",
        [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t],
        ctypes.c_void_p,
    )
    declare("pinkdb_result_free", [ctypes.c_void_p], None)
    declare("pinkdb_result_statement_count", [ctypes.c_void_p], ctypes.c_size_t)
    declare("pinkdb_result_kind", [ctypes.c_void_p, ctypes.c_size_t], ctypes.c_int32)
    declare(
        "pinkdb_result_column_count",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_size_t,
    )
    declare(
        "pinkdb_result_column_name",
        [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t],
        ctypes.c_char_p,
    )
    declare(
        "pinkdb_result_column_type",
        [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t],
        ctypes.c_int32,
    )
    declare(
        "pinkdb_result_row_count",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_size_t,
    )
    declare(
        "pinkdb_result_value",
        [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_size_t],
        _PinkValue,
    )
    declare(
        "pinkdb_result_affected",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_size_t,
    )
    declare("pinkdb_result_name", [ctypes.c_void_p, ctypes.c_size_t], ctypes.c_char_p)
    declare("pinkdb_last_error", [], ctypes.c_char_p)


library_path = _find_library()

try:
    _LIB = ctypes.CDLL(library_path)
except OSError as exc:  # wrong architecture, not a shared library, ...
    raise Error(f"failed to load {library_path!r}: {exc}") from exc

_bind(_LIB, library_path)

_loaded_abi_version = int(_LIB.pinkdb_abi_version())
if _loaded_abi_version != _ABI_VERSION:
    raise Error(
        f"pinkdb-ffi ABI version mismatch: {library_path!r} reports version "
        f"{_loaded_abi_version}, this binding requires version {_ABI_VERSION}"
    )


def abi_version() -> int:
    """ABI version reported by the loaded library (currently 1)."""
    return int(_LIB.pinkdb_abi_version())


def _last_error(lib: ctypes.CDLL) -> str:
    message = lib.pinkdb_last_error()
    if message is None:
        return "unknown pinkdb error"
    return message.decode("utf-8", "replace")


def _value_to_python(v: _PinkValue):
    """Map one ``pink_value`` to a Python object (NULL -> None).

    Out-of-range DATE/TIMESTAMP counts raise :class:`Error` (the ``int64``
    payload can exceed what :mod:`datetime` represents), and an unknown type
    tag from the native library is an error, never a silent ``None``.
    """
    if v.is_null:
        return None
    tag = v.type_tag
    if tag == _TYPE_NULL:
        # NULL tag with is_null == 0: still a null value.
        return None
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
        try:
            return _EPOCH_DATE + _datetime.timedelta(days=v.int64)
        except OverflowError as exc:
            raise Error(
                f"DATE value {v.int64} (days since epoch) is outside "
                "Python's datetime range"
            ) from exc
    if tag == _TYPE_TIMESTAMP:
        try:
            # Microseconds since the Unix epoch, naive UTC.
            return _EPOCH_DATETIME + _datetime.timedelta(microseconds=v.int64)
        except OverflowError as exc:
            raise Error(
                f"TIMESTAMP value {v.int64} (microseconds since epoch) is "
                "outside Python's datetime range"
            ) from exc
    raise Error(f"unknown value type tag {tag} from the native library")


class Result:
    """One statement's outcome, materialized as Python values.

    Attributes
    ----------
    kind:
        One of ``selected``, ``created``, ``inserted``, ``updated``,
        ``deleted``, ``began``, ``committed``, ``rolledback``,
        ``created_view``, ``dropped_table``, ``dropped_view``,
        ``created_database``, ``dropped_database``, ``used``,
        ``loaded_extension``.
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
        Object/session/extension name for ``created_view``/``dropped_table``/
        ``dropped_view``/``created_database``/``dropped_database``/``used``/
        ``loaded_extension``; ``None`` otherwise.
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
    Owns a native handle, so it cannot be copied: ``copy.copy`` and
    ``copy.deepcopy`` raise :class:`TypeError`.
    Supports use as a context manager (closes on exit).
    """

    __slots__ = ("_lib", "_handle")

    def __init__(self, lib: ctypes.CDLL, handle):
        self._lib = lib
        self._handle = handle

    def __copy__(self):
        raise TypeError("cannot copy a Database handle")

    def __deepcopy__(self, memo):
        raise TypeError("cannot copy a Database handle")

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
        try:
            data = sql.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise Error(f"sql is not encodable as UTF-8: {exc}") from exc
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


def _encode_path(path) -> bytes:
    """Normalize ``path`` to UTF-8 bytes, rejecting unsafe arguments.

    Raises :class:`ValueError` for an empty path or a NUL byte (the C ABI
    takes a NUL-terminated path, so a NUL would silently truncate it) and
    :class:`Error` for a string that cannot be encoded as UTF-8.
    """
    raw = os.fspath(path)
    if isinstance(raw, str):
        try:
            data = raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise Error(
                f"database path {raw!r} cannot be encoded as UTF-8: {exc}"
            ) from exc
    elif isinstance(raw, bytes):
        data = raw
    else:  # pragma: no cover - os.fspath already rejects other types
        raise TypeError(
            f"path must be str, bytes or os.PathLike, not {type(raw).__name__}"
        )
    if not data:
        raise ValueError("database path must not be empty")
    if b"\x00" in data:
        raise ValueError(
            f"database path {path!r} contains a NUL byte; the native library "
            "takes NUL-terminated C paths"
        )
    return data


def connect(path) -> Database:
    """Open a durable pinkdb database (checkpoint + WAL) at ``path``.

    ``path`` is a ``str``, ``bytes`` or ``os.PathLike``. Raises
    :class:`ValueError` for an empty path or one containing a NUL byte, and
    :class:`Error` when the native open fails (or the path is not valid
    UTF-8).
    """
    raw = _encode_path(path)
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
