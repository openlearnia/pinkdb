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
import os
import threading
from pathlib import Path

__all__ = [
    "Error",
    "Database",
    "Result",
    "Statement",
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
    # DECIMAL and INTERVAL arrive as text cells (the FFI's canonical text):
    # DECIMAL maps to an exact Decimal, INTERVAL keeps its canonical text.
    if tag in (_TYPE_DECIMAL, _TYPE_INTERVAL):
        if not v.text_ptr or v.text_len == 0:
            return "" if tag == _TYPE_INTERVAL else _decimal.Decimal(0)
        text = ctypes.string_at(v.text_ptr, v.text_len).decode("utf-8")
        return _decimal.Decimal(text) if tag == _TYPE_DECIMAL else text
    raise Error(f"unknown value type tag {tag} from the native library")


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
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a str")
        try:
            data = sql.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise Error(f"sql is not encodable as UTF-8: {exc}") from exc
        with self._lock:
            if self._handle is None:
                raise Error("database is closed")
            result_ptr = self._lib.pinkdb_execute(self._handle, data, len(data))
            if not result_ptr:
                raise Error(_last_error(self._lib))
            try:
                return _snapshot(self._lib, result_ptr)
            finally:
                self._lib.pinkdb_result_free(result_ptr)

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
                raise Error("database is closed")
            prepared = self._lib.pinkdb_prepare(self._handle, data, len(data))
            if not prepared:
                raise Error(_last_error(self._lib))
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
                raise Error("statement is closed")
            if self._db._handle is None:
                raise Error("database is closed")
            values, keepalive = _bind_parameters(params)
            # ``keepalive`` holds the buffers behind VARCHAR text_ptr values;
            # both it and ``values`` live until this call returns.
            array = (_PinkValue * len(values))(*values) if values else None
            result_ptr = self._db._lib.pinkdb_execute_prepared(
                self._db._handle, self._prepared, array, len(values)
            )
            if not result_ptr:
                raise Error(_last_error(self._db._lib))
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
