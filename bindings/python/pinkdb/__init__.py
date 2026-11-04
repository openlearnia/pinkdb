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

Prepared statements (``?`` placeholders) reuse one parsed statement and bind
values per execution::

    >>> with pinkdb.connect_memory() as db:
    ...     db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
    ...     with db.prepare("INSERT INTO t VALUES (?, ?);") as stmt:
    ...         stmt.execute(1, "one")
    ...         stmt.execute(2, None)
    ...     db.execute("SELECT x, s FROM t ORDER BY x;")[0].rows
    [[1, 'one'], [2, None]]

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
import decimal as _decimal
import struct as _struct
import os
import threading
from pathlib import Path

__all__ = [
    # DB-API 2.0 (PEP 249) exceptions and type objects
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "ProgrammingError",
    "InternalError",
    "NotSupportedError",
    "DBAPITypeObject",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Connection",
    "Cursor",
    "apilevel",
    "paramstyle",
    "threadsafety",
    # native API
    "Database",
    "Result",
    "Statement",
    "abi_version",
    "abi_version_info",
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
    15: "checkpointed",
    16: "altered_table",
    # Wave 48 RBAC statements.
    17: "created_user",
    18: "altered_user",
    19: "dropped_user",
    20: "created_role",
    21: "dropped_role",
    22: "granted",
    23: "revoked",
    24: "set_variable",
    25: "created_schema",
    26: "dropped_schema",
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
_TYPE_DECIMAL = 8
_TYPE_INTERVAL = 9
_TYPE_ARRAY = 11
_TYPE_ARRAY_FLOAT = 12
_TYPE_HUGEINT = 13
_TYPE_UHUGEINT = 14
_TYPE_UUID = 15

_TYPE_NAMES = {
    _TYPE_NULL: "null",
    _TYPE_BOOLEAN: "boolean",
    _TYPE_INTEGER: "integer",
    _TYPE_BIGINT: "bigint",
    _TYPE_DOUBLE: "double",
    _TYPE_VARCHAR: "varchar",
    _TYPE_DATE: "date",
    _TYPE_TIMESTAMP: "timestamp",
    _TYPE_DECIMAL: "decimal",
    _TYPE_INTERVAL: "interval",
    _TYPE_HUGEINT: "hugeint",
    _TYPE_UHUGEINT: "uhugeint",
    _TYPE_UUID: "uuid",
}

def _type_name(tag: int) -> str:
    """The pinkdb type name for a column type tag.

    An ARRAY packs its element count above the type byte: 11 for `double`
    elements, 12 for 4-byte `float` ones.
    """
    if tag & 0xFF in (_TYPE_ARRAY, _TYPE_ARRAY_FLOAT):
        elem = "float" if tag & 0xFF == _TYPE_ARRAY_FLOAT else "double"
        return f"{elem}[{tag >> 8}]"
    return _TYPE_NAMES.get(tag, "unknown")


_EPOCH_DATE = _datetime.date(1970, 1, 1)
_EPOCH_DATETIME = _datetime.datetime(1970, 1, 1)

#: ABI version this binding targets (``pinkdb_abi_version``).
_ABI_VERSION = (1 << 16) | 4


class Warning(Exception):  # noqa: A001 - the name PEP 249 requires
    """A database warning (PEP 249). PinkDB does not issue any yet."""


class Error(Exception):
    """An error reported by the pinkdb C ABI (see ``pinkdb_last_error``).

    ``pinkdb_class`` is the stable error class from
    ``pinkdb_last_error_class``: ``parse``, ``catalog``, ``type``,
    ``parameter``, ``execution``, ``constraint``, ``io``, ``csv``,
    ``invalid_argument`` or ``internal``. Errors raised for bad Python
    arguments carry ``"invalid_argument"``.
    """

    def __init__(self, message: str, pinkdb_class: str = "invalid_argument"):
        super().__init__(message)
        self.pinkdb_class = pinkdb_class


# PEP 249 exception hierarchy. Every DB-API error is also a pinkdb.Error, so
# code written against the low-level API keeps catching everything.
class InterfaceError(Error):
    """A problem with the driver itself (closed connection, bad parameter)."""


class DatabaseError(Error):
    """A database-level failure; the parent of the specific PEP 249 errors."""


class DataError(DatabaseError):
    """A value could not be stored or decoded (CSV import failures)."""


class OperationalError(DatabaseError):
    """A runtime failure: I/O, division by zero, no transaction in progress."""


class IntegrityError(DatabaseError):
    """A constraint rejected the statement (``NOT NULL``)."""


class ProgrammingError(DatabaseError):
    """Bad SQL or a bad use of the API: parse, catalog, type, parameter."""


class InternalError(DatabaseError):
    """An engine invariant was violated; please report it as a bug."""


class NotSupportedError(DatabaseError):
    """The operation does not exist in pinkdb (stored procedures)."""


# C ABI error class -> PEP 249 exception. Keep in sync with
# crates/pinkdb-ffi/include/pinkdb.h.
_EXCEPTIONS_BY_CLASS = {
    "invalid_argument": InterfaceError,
    "parse": ProgrammingError,
    "catalog": ProgrammingError,
    "type": ProgrammingError,
    "parameter": ProgrammingError,
    "constraint": IntegrityError,
    "execution": OperationalError,
    "io": OperationalError,
    "csv": DataError,
    "internal": InternalError,
}


class _PinkString(ctypes.Structure):
    """C mirror of ``pinkdb_string_t`` (``{const char *ptr; uint64_t len}``)."""

    _fields_ = [
        ("ptr", ctypes.c_void_p),
        ("len", ctypes.c_uint64),
    ]


class _PinkU128(ctypes.Structure):
    """C mirror of ``pinkdb_hugeint`` (low word first, little-endian)."""

    _fields_ = [
        ("lower", ctypes.c_uint64),
        ("upper", ctypes.c_uint64),
    ]


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
    # A wheel bundles the library inside the package; prefer it so an installed
    # wheel never picks up a stale build from a source checkout.
    package_dir = Path(__file__).resolve().parent
    candidates = [
        package_dir / "libpinkdb_ffi.dylib",
        package_dir / "libpinkdb_ffi.so",
        package_dir / "pinkdb_ffi.dll",
    ]
    # <repo>/bindings/python/pinkdb/__init__.py -> parents[3] is <repo>.
    release_dir = Path(__file__).resolve().parents[3] / "target" / "release"
    candidates += [
        release_dir / "libpinkdb_ffi.dylib",
        release_dir / "libpinkdb_ffi.so",
        release_dir / "libpinkdb_ffi.dll",
    ]
    return candidates


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
    declare(
        "pinkdb_set_allow_sql_load_extension",
        [ctypes.c_void_p, ctypes.c_int32],
        ctypes.c_int32,
    )
    declare(
        "pinkdb_prepare",
        [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t],
        ctypes.c_void_p,
    )
    declare("pinkdb_prepared_parameter_count", [ctypes.c_void_p], ctypes.c_size_t)
    declare("pinkdb_prepared_free", [ctypes.c_void_p], None)
    declare(
        "pinkdb_execute_prepared",
        [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(_PinkValue),
            ctypes.c_size_t,
        ],
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
        "pinkdb_result_column_decimal_width",
        [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t],
        ctypes.c_int32,
    )
    declare(
        "pinkdb_result_column_decimal_scale",
        [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t],
        ctypes.c_int32,
    )
    declare(
        "pinkdb_result_fetch_chunk",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_void_p,
    )
    declare("pinkdb_destroy_chunk", [ctypes.c_void_p], None)
    declare("pinkdb_chunk_size", [ctypes.c_void_p], ctypes.c_size_t)
    declare(
        "pinkdb_chunk_column",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_void_p,
    )
    declare("pinkdb_vector_data", [ctypes.c_void_p], ctypes.c_void_p)
    declare("pinkdb_vector_validity", [ctypes.c_void_p], ctypes.c_void_p)
    declare(
        "pinkdb_validity_row_is_valid",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_int32,
    )
    declare(
        "pinkdb_result_affected",
        [ctypes.c_void_p, ctypes.c_size_t],
        ctypes.c_size_t,
    )
    declare("pinkdb_result_name", [ctypes.c_void_p, ctypes.c_size_t], ctypes.c_char_p)
    declare("pinkdb_last_error", [], ctypes.c_char_p)
    declare("pinkdb_last_error_class", [], ctypes.c_char_p)


library_path = _find_library()

try:
    _LIB = ctypes.CDLL(library_path)
except OSError as exc:  # wrong architecture, not a shared library, ...
    raise Error(f"failed to load {library_path!r}: {exc}") from exc

_bind(_LIB, library_path)

_loaded_abi_version = int(_LIB.pinkdb_abi_version())
# Same major, at least this minor: a newer minor only adds calls.
if (_loaded_abi_version >> 16 != _ABI_VERSION >> 16
        or (_loaded_abi_version & 0xFFFF) < (_ABI_VERSION & 0xFFFF)):
    raise Error(
        f"pinkdb-ffi ABI version mismatch: {library_path!r} reports version "
        f"{_loaded_abi_version >> 16}.{_loaded_abi_version & 0xFFFF}, this binding "
        f"requires {_ABI_VERSION >> 16}.{_ABI_VERSION & 0xFFFF} or a later minor"
    )


def abi_version() -> int:
    """C ABI version of the loaded library, encoded ``major << 16 | minor``
    (``65539`` is 1.3). Compare :func:`abi_version_info` instead: a newer
    minor only adds calls."""
    return int(_LIB.pinkdb_abi_version())


def abi_version_info() -> "tuple[int, int]":
    """The loaded library's C ABI version as ``(major, minor)``."""
    version = abi_version()
    return (version >> 16, version & 0xFFFF)


def _last_error(lib: ctypes.CDLL) -> str:
    message = lib.pinkdb_last_error()
    if message is None:
        return "unknown pinkdb error"
    return message.decode("utf-8", "replace")


def _last_error_class(lib: ctypes.CDLL) -> str:
    """The stable class of the error the last native call reported."""
    pinkdb_class = lib.pinkdb_last_error_class()
    if pinkdb_class is None:
        return "internal"
    return pinkdb_class.decode("utf-8", "replace")


def _native_error(lib: ctypes.CDLL) -> Error:
    """Build the PEP 249 exception matching the last native failure."""
    pinkdb_class = _last_error_class(lib)
    exception = _EXCEPTIONS_BY_CLASS.get(pinkdb_class, DatabaseError)
    return exception(_last_error(lib), pinkdb_class=pinkdb_class)


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
    # DECIMAL and INTERVAL arrive as text cells (the FFI's canonical text):
    # DECIMAL maps to an exact Decimal, INTERVAL keeps its canonical text.
    if tag & 0xFF in (_TYPE_ARRAY, _TYPE_ARRAY_FLOAT):
        # An ARRAY payload: N little-endian elements, the count in the tag's
        # upper bits. NULL elements never cross the C ABI.
        count = tag >> 8
        single = tag & 0xFF == _TYPE_ARRAY_FLOAT
        width = 4 if single else 8
        if not v.text_ptr or v.text_len != count * width:
            raise Error(
                f"{'FLOAT' if single else 'DOUBLE'}[{count}] value is "
                f"{v.text_len} bytes, not the {count * width} of its element "
                "count"
            )
        code = "f" if single else "d"
        return list(
            _struct.unpack(f"<{count}{code}", ctypes.string_at(v.text_ptr, v.text_len))
        )
    if tag in (_TYPE_DECIMAL, _TYPE_INTERVAL, _TYPE_HUGEINT, _TYPE_UHUGEINT):
        if not v.text_ptr or v.text_len == 0:
            return _decimal.Decimal(0) if tag == _TYPE_DECIMAL else ""
        text = ctypes.string_at(v.text_ptr, v.text_len).decode("utf-8")
        return _decimal.Decimal(text) if tag == _TYPE_DECIMAL else text
    raise Error(f"unknown value type tag {tag} from the native library")


def _format_uuid(bits: int) -> str:
    """The engine's canonical lowercase UUID text from its 128 bits."""
    text = f"{bits:032x}"
    return f"{text[:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:]}"


def _chunk_value(
    lib: ctypes.CDLL,
    result_ptr,
    statement: int,
    row: int,
    column: int,
    tag: int,
    data,
    validity,
    scale: int,
):
    """One row of a 1.2 vector, in the same Python type as ``_value_to_python``.

    INTERVAL and ARRAY have no Python-native form here, so they fall back to
    the result's canonical cell; VARIANT's JSON text is read from the chunk.
    """
    if not lib.pinkdb_validity_row_is_valid(validity, row):
        return None
    if not data:
        return _value_to_python(lib.pinkdb_result_value(result_ptr, statement, row, column))
    if tag == _TYPE_BOOLEAN:
        return bool(ctypes.cast(data, ctypes.POINTER(ctypes.c_uint8))[row])
    if tag == _TYPE_INTEGER:
        return int(ctypes.cast(data, ctypes.POINTER(ctypes.c_int32))[row])
    if tag == _TYPE_BIGINT:
        return int(ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[row])
    if tag == _TYPE_DOUBLE:
        return float(ctypes.cast(data, ctypes.POINTER(ctypes.c_double))[row])
    if tag in (_TYPE_VARCHAR, 10):  # 10 is VARIANT's JSON string element
        string = ctypes.cast(data, ctypes.POINTER(_PinkString))[row]
        if not string.ptr or string.len == 0:
            return ""
        return ctypes.string_at(string.ptr, string.len).decode("utf-8")
    if tag == _TYPE_DATE:
        days = int(ctypes.cast(data, ctypes.POINTER(ctypes.c_int32))[row])
        try:
            return _EPOCH_DATE + _datetime.timedelta(days=days)
        except OverflowError as exc:
            raise Error(
                f"DATE value {days} (days since epoch) is outside "
                "Python's datetime range"
            ) from exc
    if tag == _TYPE_TIMESTAMP:
        micros = int(ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[row])
        try:
            return _EPOCH_DATETIME + _datetime.timedelta(microseconds=micros)
        except OverflowError as exc:
            raise Error(
                f"TIMESTAMP value {micros} (microseconds since epoch) is "
                "outside Python's datetime range"
            ) from exc
    if tag in (_TYPE_DECIMAL, _TYPE_HUGEINT, _TYPE_UHUGEINT, _TYPE_UUID):
        words = ctypes.cast(data, ctypes.POINTER(_PinkU128))[row]
        bits = (words.upper << 64) | words.lower
        if tag == _TYPE_DECIMAL:
            signed = bits - (1 << 128) if bits >> 127 else bits
            return _decimal.Decimal(signed).scaleb(-scale)
        if tag == _TYPE_HUGEINT:
            return str(bits - (1 << 128) if bits >> 127 else bits)
        if tag == _TYPE_UHUGEINT:
            return str(bits)
        return _format_uuid(bits)
    # INTERVAL and ARRAY: keep the canonical cell until 112c gives them a
    # native Python shape.
    return _value_to_python(lib.pinkdb_result_value(result_ptr, statement, row, column))


def _chunk_rows(lib: ctypes.CDLL, result_ptr, statement: int, ncols: int):
    """Yield every row of a SELECT from the result's chunks, then free them."""
    chunk = lib.pinkdb_result_fetch_chunk(result_ptr, statement)
    try:
        while chunk:
            size = lib.pinkdb_chunk_size(chunk)
            columns = []
            for col in range(ncols):
                tag = lib.pinkdb_result_column_type(result_ptr, statement, col)
                vector = lib.pinkdb_chunk_column(chunk, col)
                columns.append(
                    (
                        tag,
                        lib.pinkdb_vector_data(vector),
                        lib.pinkdb_vector_validity(vector),
                        lib.pinkdb_result_column_decimal_scale(result_ptr, statement, col),
                    )
                )
            for row in range(size):
                yield [
                    _chunk_value(lib, result_ptr, statement, row, col, *columns[col])
                    for col in range(ncols)
                ]
            lib.pinkdb_destroy_chunk(chunk)
            chunk = lib.pinkdb_result_fetch_chunk(result_ptr, statement)
    finally:
        if chunk:
            lib.pinkdb_destroy_chunk(chunk)

#: Python types that bind by value to a ``?`` placeholder, for error messages.
_BINDABLE_TYPES = (
    "None, bool, int, float, str, datetime.date or datetime.datetime"
)


def _bind_one(index: int, value):
    """Map one Python parameter to ``(pink_value, keepalive)``.

    ``index`` is the 1-based parameter position, used in error messages.
    ``keepalive`` is the buffer backing a VARCHAR payload (or ``None``); the
    caller must keep it alive until the native call has returned.

    Type mapping: ``None`` -> NULL, ``bool`` -> BOOLEAN, ``int`` -> INTEGER or
    BIGINT (whichever the value fits), ``float`` -> DOUBLE, ``str`` ->
    VARCHAR, ``datetime.date`` -> DATE, ``datetime.datetime`` -> TIMESTAMP
    (aware datetimes convert to UTC; naive ones are taken as UTC). ``bytes``,
    ``bytearray``, ``memoryview`` and ``decimal.Decimal`` are refused: ABI v1
    has no BLOB type and cannot bind DECIMAL/INTERVAL parameters.
    """
    pink = _PinkValue()
    pink.type_tag = _TYPE_NULL
    pink.is_null = 0
    pink.int64 = 0
    pink.double_ = 0.0
    pink.text_ptr = None
    pink.text_len = 0

    if value is None:
        pink.is_null = 1
        return pink, None
    if isinstance(value, bool):  # before int: bool is an int subclass
        pink.type_tag = _TYPE_BOOLEAN
        pink.int64 = 1 if value else 0
        return pink, None
    if isinstance(value, int):
        if -(2**31) <= value < 2**31:
            pink.type_tag = _TYPE_INTEGER
        elif -(2**63) <= value < 2**63:
            pink.type_tag = _TYPE_BIGINT
        else:
            raise Error(
                f"parameter {index}: integer {value} does not fit in INTEGER "
                "or BIGINT"
            )
        pink.int64 = value
        return pink, None
    if isinstance(value, float):
        pink.type_tag = _TYPE_DOUBLE
        pink.double_ = value
        return pink, None
    if isinstance(value, str):
        try:
            data = value.encode("utf-8")
        except UnicodeEncodeError as exc:  # e.g. a lone surrogate
            raise Error(
                f"parameter {index} is not encodable as UTF-8: {exc}"
            ) from exc
        pink.type_tag = _TYPE_VARCHAR
        if data:
            # Keep text_len authoritative: the payload may contain embedded
            # NULs, so the buffer (not the C string) is the value.
            buffer = ctypes.create_string_buffer(data, len(data))
            pink.text_ptr = ctypes.addressof(buffer)
            pink.text_len = len(data)
            return pink, buffer
        return pink, None  # empty string: NULL pointer, zero length
    if isinstance(value, _datetime.datetime):  # before date: a datetime is a date
        moment = value
        if moment.tzinfo is not None:
            moment = moment.astimezone(_datetime.timezone.utc).replace(tzinfo=None)
        delta = moment - _EPOCH_DATETIME
        # timedelta normalizes negative spans as (negative days, positive
        # seconds), so this reconstructs microseconds since the epoch exactly.
        pink.type_tag = _TYPE_TIMESTAMP
        pink.int64 = (
            (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        )
        return pink, None
    if isinstance(value, _datetime.date):
        pink.type_tag = _TYPE_DATE
        pink.int64 = (value - _EPOCH_DATE).days
        return pink, None
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(
            f"parameter {index}: ABI v1 has no BLOB type and cannot bind "
            f"{type(value).__name__}; decode it to str (e.g. .decode('utf-8')) "
            "or embed it in SQL"
        )
    if isinstance(value, _decimal.Decimal):
        raise TypeError(
            f"parameter {index}: ABI v1 cannot bind DECIMAL parameters; pass "
            "the canonical text and CAST it in SQL, e.g. "
            "CAST(? AS DECIMAL(18,3))"
        )
    raise TypeError(
        f"parameter {index}: unsupported Python type {type(value).__name__!r}; "
        f"bind {_BINDABLE_TYPES}"
    )


def _bind_parameters(params) -> tuple:
    """Convert ``params`` to a ``pink_value`` array plus its keepalive buffers.

    The same native error surface as ``Database.execute`` applies: a wrong
    count is reported by the native bind step, not re-checked here.
    """
    values = []
    keepalive = []
    for index, param in enumerate(params, start=1):
        pink, buffer = _bind_one(index, param)
        values.append(pink)
        if buffer is not None:
            keepalive.append(buffer)
    return values, keepalive


class Result:
    """One statement's outcome, materialized as Python values.

    Attributes
    ----------
    kind:
        One of ``selected``, ``created``, ``inserted``, ``updated``,
        ``deleted``, ``began``, ``committed``, ``rolledback``,
        ``created_view``, ``dropped_table``, ``dropped_view``,
        ``created_database``, ``dropped_database``, ``used``,
        ``loaded_extension``, ``checkpointed``, ``altered_table``,
        ``created_user``, ``altered_user``, ``dropped_user``,
        ``created_role``, ``dropped_role``, ``granted``, ``revoked``.
    columns / column_types:
        For ``selected``: column names and type names (``boolean``,
        ``integer``, ``bigint``, ``double``, ``varchar``, ``date``,
        ``timestamp``, ``decimal``, ``interval``); empty for every other
        kind.
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
                    _type_name(
                        lib.pinkdb_result_column_type(result_ptr, stmt, col),
                    )
                )
            rows = list(_chunk_rows(lib, result_ptr, stmt, ncols))
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

    Safe to share across threads: calls are serialized by an internal lock
    (ctypes releases the GIL during native calls, and the C handle itself is
    not thread-safe). A durable file can be open in only one handle at a time,
    so share one ``Database`` rather than opening the file per thread.
    Owns a native handle, so it cannot be copied: ``copy.copy`` and
    ``copy.deepcopy`` raise :class:`TypeError`.
    Supports use as a context manager (closes on exit).
    """

    __slots__ = ("_lib", "_handle", "_lock")

    def __init__(self, lib: ctypes.CDLL, handle):
        self._lib = lib
        self._handle = handle
        self._lock = threading.Lock()

    def __copy__(self):
        raise TypeError("cannot copy a Database handle")

    def __deepcopy__(self, memo):
        raise TypeError("cannot copy a Database handle")

    def execute(self, sql: str) -> list:
        """Execute one or more ``;``-separated SQL statements.

        Returns a :class:`Result` per statement. Raises :class:`Error` with
        the message from ``pinkdb_last_error`` if any statement fails; the
        handle remains usable afterwards.

        .. warning::
           The SQL statement ``LOAD EXTENSION '<path>'`` loads and runs
           native code from a path the SQL text names. It is **disabled by
           default**: :meth:`allow_load_extension` has to enable it first.
           Do not enable it for a database whose SQL comes from untrusted
           input.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a str")
        try:
            data = sql.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise Error(f"sql is not encodable as UTF-8: {exc}") from exc
        with self._lock:
            if self._handle is None:
                raise InterfaceError("database is closed")
            result_ptr = self._lib.pinkdb_execute(self._handle, data, len(data))
            if not result_ptr:
                raise _native_error(self._lib)
            try:
                return _snapshot(self._lib, result_ptr)
            finally:
                self._lib.pinkdb_result_free(result_ptr)

    def allow_load_extension(self, allow: bool = True) -> None:
        """Allow or refuse the SQL ``LOAD EXTENSION '<path>'`` statement.

        The statement makes the host process load and run a native library
        from a path the SQL text names — arbitrary native code execution for
        anyone who can influence the SQL. It is **disabled by default**, so
        :meth:`execute` cannot load native code until this is called with
        ``True``.

        Only enable it for a database whose SQL comes from a trusted source.
        The loaded path must name a regular file inside a directory (a bare
        library name, which the dynamic loader would resolve through its
        search path, is refused), and it must not be a symlink.
        """
        if not isinstance(allow, bool):
            raise TypeError("allow must be a bool")
        with self._lock:
            if self._handle is None:
                raise InterfaceError("database is closed")
            rc = self._lib.pinkdb_set_allow_sql_load_extension(
                self._handle, 1 if allow else 0
            )
            if not rc:
                raise _native_error(self._lib)

    def prepare(self, sql: str) -> "Statement":
        """Parse one SQL statement with ``?`` placeholders into a reusable
        :class:`Statement`.

        Prepared statements take typed Python values (positional, one per
        ``?``) and can be executed repeatedly; see :class:`Statement` for the
        value mapping. Raises :class:`Error` for a multi-statement string,
        one with no statement, or any parse error (the message comes from
        ``pinkdb_last_error``), and :class:`TypeError` for non-``str`` SQL.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a str")
        try:
            data = sql.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise Error(f"sql is not encodable as UTF-8: {exc}") from exc
        with self._lock:
            if self._handle is None:
                raise InterfaceError("database is closed")
            prepared = self._lib.pinkdb_prepare(self._handle, data, len(data))
            if not prepared:
                raise _native_error(self._lib)
            try:
                parameter_count = int(
                    self._lib.pinkdb_prepared_parameter_count(prepared)
                )
            except BaseException:
                self._lib.pinkdb_prepared_free(prepared)
                raise
            return Statement(self, prepared, sql, parameter_count)

    def close(self) -> None:
        """Close the handle. Idempotent and safe to call more than once."""
        with self._lock:
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


class Statement:
    """A parsed, reusable SQL statement with ``?`` placeholders.

    Create it with :meth:`Database.prepare`; the statement is one SQL
    statement (multi-statement strings are refused) and stays valid for any
    number of :meth:`execute` calls, binding fresh values each time. Types are
    resolved by the native bind step against the current catalog, so reuse
    sees catalog changes (new columns, dropped tables).

    Parameters bind by Python type: ``None`` -> NULL, ``bool`` -> BOOLEAN,
    ``int`` -> INTEGER/BIGINT, ``float`` -> DOUBLE, ``str`` -> VARCHAR,
    ``datetime.date`` -> DATE, ``datetime.datetime`` -> TIMESTAMP (naive
    datetimes are UTC; aware ones are converted). A wrong parameter count or
    a value the statement cannot convert raises :class:`Error` with the
    native message and leaves both the statement and the database usable.

    Release the native statement with :meth:`close` (idempotent); the object
    is also a context manager. Executing after ``close`` raises
    :class:`Error`, as does executing after the owning :class:`Database` was
    closed. A ``Statement`` cannot be copied (both copies would own the same
    native pointer).
    """

    __slots__ = ("_db", "_prepared", "_sql", "_parameter_count", "_closed")

    def __init__(self, db: Database, prepared, sql: str, parameter_count: int):
        self._db = db
        self._prepared = prepared
        self._sql = sql
        self._parameter_count = parameter_count
        self._closed = False

    @property
    def sql(self) -> str:
        """The original SQL text, ``?`` placeholders included."""
        return self._sql

    @property
    def parameter_count(self) -> int:
        """Number of ``?`` placeholders: how many values ``execute`` takes."""
        return self._parameter_count

    def __copy__(self):
        raise TypeError("cannot copy a Statement handle")

    def __deepcopy__(self, memo):
        raise TypeError("cannot copy a Statement handle")

    def execute(self, *params) -> Result:
        """Bind ``params`` (one per ``?``, in textual order) and run the
        statement once, returning its single :class:`Result`.

        Raises :class:`TypeError` for an unbindable Python type and
        :class:`Error` for a wrong parameter count, a value the statement
        cannot convert, or a native execution error.
        """
        with self._db._lock:
            if self._closed:
                raise InterfaceError("statement is closed")
            if self._db._handle is None:
                raise InterfaceError("database is closed")
            values, keepalive = _bind_parameters(params)
            # ``keepalive`` holds the buffers behind VARCHAR text_ptr values;
            # both it and ``values`` live until this call returns.
            array = (_PinkValue * len(values))(*values) if values else None
            result_ptr = self._db._lib.pinkdb_execute_prepared(
                self._db._handle, self._prepared, array, len(values)
            )
            if not result_ptr:
                raise _native_error(self._db._lib)
            try:
                results = _snapshot(self._db._lib, result_ptr)
            finally:
                self._db._lib.pinkdb_result_free(result_ptr)
            if len(results) != 1:  # pragma: no cover - FFI invariant
                raise Error(
                    "internal error: prepared statement produced "
                    f"{len(results)} results"
                )
            return results[0]

    def close(self) -> None:
        """Free the native prepared statement. Idempotent; also safe after
        the owning database has been closed (it does not touch the handle)."""
        with self._db._lock:
            prepared = self._prepared
            if prepared is not None:
                self._prepared = None
                self._closed = True
                self._db._lib.pinkdb_prepared_free(prepared)

    def __enter__(self) -> "Statement":
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
        state = "closed" if self._closed else "open"
        return (
            f"<pinkdb.Statement ({state}, {self._parameter_count} parameters)>"
        )


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


def connect(path) -> "Connection":
    """Open a durable pinkdb database (checkpoint + WAL) at ``path``.

    ``path`` is a ``str``, ``bytes`` or ``os.PathLike``. The string
    ``":memory:"`` opens a fresh in-memory database instead, as it does in
    ``sqlite3`` and DuckDB (the same as :func:`connect_memory`), so it never
    creates a file of that name. Raises :class:`ValueError` for an empty path
    or one containing a NUL byte, and :class:`Error` when the native open
    fails (or the path is not valid UTF-8).
    """
    if path == ":memory:" or path == b":memory:":
        return connect_memory()
    raw = _encode_path(path)
    handle = _LIB.pinkdb_open(raw)
    if not handle:
        raise _native_error(_LIB)
    return Connection(_LIB, handle)


def connect_memory() -> "Connection":
    """Open a fresh, empty in-memory pinkdb database."""
    handle = _LIB.pinkdb_open_memory()
    if not handle:  # documented never to fail; stay safe anyway
        raise _native_error(_LIB)
    return Connection(_LIB, handle)


# ----- DB-API 2.0 (PEP 249) -----
#
# The native API above (Database, Statement, Result) materializes every row.
# The DB-API layer is a thin, lazy cursor on top of it: it keeps the native
# pinkdb_result alive and converts one row per fetch, which lets a 100k-row
# result be read without building 100k Python rows up front.
#
# Deviation from PEP 249: a connection is in autocommit mode until BEGIN, so
# execute() does not open an implicit transaction (the engine refuses some
# catalog DDL inside one) and commit()/rollback() with no open transaction are
# no-ops instead of errors.

apilevel = "2.0"
paramstyle = "qmark"
# The module and a Connection may be shared between threads (the handle is
# serialized by a lock); a Cursor must not be shared.
threadsafety = 2


class DBAPITypeObject:
    """A PEP 249 type object: equal to the C ABI type tags it groups.

    ``description`` carries the integer tag, so ``cursor.description[0][1] ==
    NUMBER`` is true for every numeric column.
    """

    __slots__ = ("name", "type_codes")

    def __init__(self, name: str, *type_codes: int):
        self.name = name
        self.type_codes = frozenset(type_codes)

    def __eq__(self, other):
        if isinstance(other, DBAPITypeObject):
            return self is other
        return other in self.type_codes

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"<pinkdb type object {self.name}>"


STRING = DBAPITypeObject("STRING", _TYPE_VARCHAR)
#: ABI v1 has no BLOB type, so nothing compares equal to BINARY yet.
BINARY = DBAPITypeObject("BINARY")
NUMBER = DBAPITypeObject("NUMBER", _TYPE_INTEGER, _TYPE_BIGINT, _TYPE_DOUBLE, _TYPE_DECIMAL)
DATETIME = DBAPITypeObject("DATETIME", _TYPE_DATE, _TYPE_TIMESTAMP)
ROWID = DBAPITypeObject("ROWID")


class Cursor:
    """A DB-API 2.0 cursor over one connection.

    ``execute`` prepares the statement (so a multi-statement string is
    refused before it runs) and keeps the native result for lazy fetching.
    Rows are tuples in ``description`` order; date/timestamp cells are
    :class:`datetime.date` / :class:`datetime.datetime` and DECIMAL cells are
    :class:`decimal.Decimal`.
    """

    def __init__(self, connection: "Connection"):
        self._connection = connection
        self._lib = connection._lib
        self._result = None
        self._columns = []
        self._type_tags = []
        self._kind = None
        self._row_count = 0
        self._row_index = 0
        self._rows = iter(())
        self._affected = 0
        self._closed = False
        #: Rows ``fetchmany()`` returns when called without a size.
        self.arraysize = 1

    @property
    def connection(self) -> "Connection":
        return self._connection

    @property
    def description(self):
        """``(name, type_code, display_size, internal_size, precision, scale,
        null_ok)`` per column, or ``None`` when the last statement produced no
        result set."""
        if self._kind != "selected":
            return None
        return [
            (name, tag, None, None, None, None, None)
            for name, tag in zip(self._columns, self._type_tags)
        ]

    @property
    def rowcount(self) -> int:
        """Rows produced by a SELECT or changed by DML; ``-1`` before any
        statement."""
        if self._kind is None:
            return -1
        return self._row_count if self._kind == "selected" else self._affected

    @property
    def lastrowid(self):
        """pinkdb has no implicit row ids, so this is always ``None``."""
        return None

    def execute(self, operation: str, parameters=None) -> "Cursor":
        """Run one statement with ``?`` placeholders and keep its rows for
        fetching. Returns the cursor."""
        if not isinstance(operation, str):
            raise TypeError("operation must be a str")
        if isinstance(parameters, dict):
            raise ProgrammingError(
                "pinkdb uses the qmark paramstyle: pass a sequence of "
                "parameters, not a mapping"
            )
        params = () if parameters is None else tuple(parameters)
        self._check_open()
        self._close_result()
        with self._connection._lock:
            handle = self._connection._handle
            if handle is None:
                raise InterfaceError("connection is closed")
            try:
                data = operation.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ProgrammingError(
                    f"statement is not encodable as UTF-8: {exc}"
                ) from exc
            prepared = self._lib.pinkdb_prepare(handle, data, len(data))
            if not prepared:
                raise _native_error(self._lib)
            try:
                try:
                    values, keepalive = _bind_parameters(params)
                except TypeError as exc:
                    raise ProgrammingError(str(exc)) from exc
                # ``keepalive`` owns the buffers behind VARCHAR text_ptr
                # values; both it and ``values`` live until the call returns.
                array = (_PinkValue * len(values))(*values) if values else None
                result = self._lib.pinkdb_execute_prepared(
                    handle, prepared, array, len(values)
                )
                if not result:
                    raise _native_error(self._lib)
            finally:
                self._lib.pinkdb_prepared_free(prepared)
            self._adopt(result)
        return self

    def executemany(self, operation: str, seq_of_parameters) -> "Cursor":
        """Run ``operation`` once per parameter sequence and set ``rowcount``
        to the total of affected (or selected) rows."""
        if not isinstance(operation, str):
            raise TypeError("operation must be a str")
        self._check_open()
        total = 0
        executed = False
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            executed = True
            total += self.rowcount
        self._close_result()
        if executed:
            # Keep the total visible through rowcount, but no rows to fetch.
            self._affected = total
            self._kind = "executemany"
        return self

    def fetchone(self):
        """The next row as a tuple, or ``None`` when the result is exhausted."""
        self._check_open()
        result = self._require_result()
        if self._row_index >= self._row_count:
            return None
        row = tuple(next(self._rows, ()))
        self._row_index += 1
        return row

    def fetchmany(self, size=None) -> list:
        """Up to ``size`` rows (``arraysize`` when omitted)."""
        if size is None:
            size = self.arraysize
        if not isinstance(size, int) or isinstance(size, bool):
            raise TypeError("size must be an int")
        if size < 0:
            raise ValueError("size must not be negative")
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self) -> list:
        """Every remaining row."""
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                return rows
            rows.append(row)

    def setinputsizes(self, sizes) -> None:
        """No-op: the ABI resolves parameter types from the bound values."""

    def setoutputsizes(self, sizes) -> None:
        """No-op: result cell types come from the statement's columns."""

    def callproc(self, procname, parameters=None):
        raise NotSupportedError("pinkdb has no stored procedures")

    def close(self) -> None:
        """Free the native result. Idempotent."""
        if not self._closed:
            self._closed = True
            self._close_result()

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
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
        state = "closed" if self._closed else "open"
        return f"<pinkdb.Cursor ({state}, {self.rowcount} rows)>"

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")

    def _require_result(self):
        if self._result is None or self._kind != "selected":
            raise InterfaceError("the last statement produced no result set")
        return self._result

    def _close_result(self) -> None:
        result = self._result
        if result is not None:
            self._result = None
            self._lib.pinkdb_result_free(result)
        self._columns = []
        self._type_tags = []
        self._kind = None
        self._row_count = 0
        self._row_index = 0
        self._rows = iter(())
        self._affected = 0

    def _adopt(self, result) -> None:
        """Take ownership of a native result and read its shape once."""
        if self._lib.pinkdb_result_statement_count(result) != 1:
            self._lib.pinkdb_result_free(result)
            raise Error(  # pragma: no cover - FFI invariant
                "internal error: a cursor statement produced more than one result"
            )
        kind_tag = self._lib.pinkdb_result_kind(result, 0)
        self._kind = _STMT_KINDS.get(kind_tag, "unknown")
        if kind_tag == 0:
            column_count = self._lib.pinkdb_result_column_count(result, 0)
            for col in range(column_count):
                name = self._lib.pinkdb_result_column_name(result, 0, col)
                self._columns.append(name.decode("utf-8") if name is not None else "")
                self._type_tags.append(
                    self._lib.pinkdb_result_column_type(result, 0, col)
                )
            self._row_count = self._lib.pinkdb_result_row_count(result, 0)
        else:
            self._affected = int(self._lib.pinkdb_result_affected(result, 0))
        self._result = result
        self._row_index = 0
        self._rows = _chunk_rows(self._lib, result, 0, len(self._columns)) if kind_tag == 0 else iter(())


class Connection(Database):
    """A DB-API 2.0 connection: a :class:`Database` plus cursors and
    transaction control.

    The connection is in autocommit mode until ``BEGIN``; ``commit()`` and
    ``rollback()`` are explicit and are no-ops when no transaction is open.
    Use this class as a context manager to close the handle on exit.
    """

    __slots__ = ()

    def cursor(self) -> Cursor:
        """Create a new cursor on this connection."""
        if self._handle is None:
            raise InterfaceError("connection is closed")
        return Cursor(self)

    def commit(self) -> None:
        """Commit the open transaction; a no-op when none is open."""
        try:
            self.execute("COMMIT")
        except OperationalError as exc:
            if "no transaction in progress" not in str(exc):
                raise

    def rollback(self) -> None:
        """Roll back the open transaction; a no-op when none is open."""
        try:
            self.execute("ROLLBACK")
        except OperationalError as exc:
            if "no transaction in progress" not in str(exc):
                raise

    def __repr__(self) -> str:
        state = "closed" if self._handle is None else "open"
        return f"<pinkdb.Connection ({state})>"
