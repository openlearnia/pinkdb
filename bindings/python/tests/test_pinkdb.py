"""Tests for the pinkdb Python binding, using only the stdlib ``unittest``.

From the repo root (build the C ABI first):

    cargo build --release -p pinkdb-ffi
    python3 -m unittest discover -s bindings/python/tests -v

The shared library is found via ``PINKDB_FFI_LIB`` when set, else from the
repo-relative ``target/release`` directory (see ``pinkdb/__init__.py``).
"""

import copy
import datetime
import decimal
import gc
import math
import os
import pickle
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

# Make ``import pinkdb`` work regardless of the working directory.
_HERE = Path(__file__).resolve()
_BINDING_ROOT = _HERE.parents[1]  # bindings/python
if str(_BINDING_ROOT) not in sys.path:
    sys.path.insert(0, str(_BINDING_ROOT))

import pinkdb  # noqa: E402


def _non_pinkdb_library():
    """Path to a real, loadable shared library that is not pinkdb-ffi."""
    ctypes_ext = sys.modules.get("_ctypes")  # imported by ctypes via pinkdb
    candidates = ["/usr/lib/libSystem.B.dylib"]
    if ctypes_ext is not None:
        candidates.append(getattr(ctypes_ext, "__file__", None))
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise unittest.SkipTest("no suitable non-pinkdb shared library found")


def _run_child(code, **env_overrides):
    """Run ``code`` in a child interpreter with the binding on sys.path."""
    env = dict(os.environ)
    env.update(env_overrides)
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(_BINDING_ROOT), env.get("PYTHONPATH")) if part
    )
    return subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )


class AbiTests(unittest.TestCase):
    def test_abi_version_is_one(self):
        self.assertEqual(pinkdb.abi_version(), 1)

    def test_library_path_is_absolute_and_exists(self):
        self.assertTrue(os.path.isabs(pinkdb.library_path), pinkdb.library_path)
        self.assertTrue(Path(pinkdb.library_path).is_file(), pinkdb.library_path)


class MemoryValueTests(unittest.TestCase):
    def setUp(self):
        self.db = pinkdb.connect_memory()
        self.addCleanup(self.db.close)

    def test_all_types_roundtrip(self):
        results = self.db.execute(
            "CREATE TABLE types (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, "
            "s VARCHAR, dt DATE, ts TIMESTAMP);"
            "INSERT INTO types VALUES "
            "(TRUE, -2147483647 - 1, 9223372036854775807, -0.0, "
            "'héllo 🌍', DATE '2024-01-15', "
            "TIMESTAMP '2024-01-15 10:00:00.123456'), "
            "(NULL, NULL, NULL, NULL, NULL, NULL, NULL), "
            "(FALSE, 7, 8, 2.5, '', DATE '1970-01-01', "
            "TIMESTAMP '1970-01-01 00:00:00');"
            "SELECT * FROM types ORDER BY i;"
        )

        self.assertEqual([r.kind for r in results], ["created", "inserted", "selected"])
        self.assertEqual(results[1].affected, 3)
        selected = results[2]
        self.assertEqual(selected.name, None)
        self.assertEqual(
            selected.columns, ["b", "i", "bi", "d", "s", "dt", "ts"]
        )
        self.assertEqual(
            selected.column_types,
            ["boolean", "integer", "bigint", "double", "varchar", "date", "timestamp"],
        )
        self.assertEqual(len(selected.rows), 3)

        # Non-NULL row: exact Python types and values (i32::MIN via expression).
        row = selected.rows[0]
        self.assertIs(type(row[0]), bool)
        self.assertEqual(row[0], True)
        self.assertIs(type(row[1]), int)
        self.assertEqual(row[1], -2147483648)
        self.assertEqual(row[2], 9223372036854775807)
        self.assertIs(type(row[3]), float)
        self.assertEqual(row[3], 0.0)
        self.assertEqual(math.copysign(1.0, row[3]), -1.0, "-0.0 must survive")
        self.assertEqual(row[4], "héllo 🌍")
        self.assertEqual(row[5], datetime.date(2024, 1, 15))
        self.assertEqual(row[6], datetime.datetime(2024, 1, 15, 10, 0, 0, 123456))

        # NULL row (NULLs sort last): every column maps to None,
        # whatever its declared type.
        self.assertEqual(selected.rows[2], [None] * 7)

        # FALSE / empty string / epoch boundaries.
        row = selected.rows[1]
        self.assertIs(type(row[0]), bool)
        self.assertEqual(row[0], False)
        self.assertEqual(row[4], "")
        self.assertEqual(row[5], datetime.date(1970, 1, 1))
        self.assertEqual(row[6], datetime.datetime(1970, 1, 1, 0, 0, 0))

    def test_numeric_extremes_via_expressions(self):
        selected = self.db.execute(
            "SELECT -2147483647 - 1 AS i32min, 2147483647 AS i32max, "
            "-9223372036854775807 - 1 AS i64min, 9223372036854775807 AS i64max;"
        )[0]
        self.assertEqual(selected.column_types, ["integer", "integer", "bigint", "bigint"])
        self.assertEqual(
            selected.rows[0],
            [-2147483648, 2147483647, -9223372036854775808, 9223372036854775807],
        )

    def test_varchar_zero_length_and_embedded_nul(self):
        selected = self.db.execute(
            "SELECT '' AS empty, 'héllo' AS multibyte, 'a\x00b' AS embedded;"
        )[0]
        self.assertEqual(selected.columns, ["empty", "multibyte", "embedded"])
        self.assertEqual(selected.rows[0][0], "")
        self.assertEqual(selected.rows[0][1], "héllo")
        # text_len (not NUL termination) defines the payload.
        self.assertEqual(selected.rows[0][2], "a\x00b")

    def test_pre_epoch_and_epoch_boundaries(self):
        selected = self.db.execute(
            "SELECT DATE '1940-01-01', DATE '1969-12-31', DATE '1970-01-01', "
            "TIMESTAMP '1969-12-31 23:59:59.999999', "
            "TIMESTAMP '1970-01-01 00:00:00';"
        )[0]
        self.assertEqual(
            selected.column_types,
            ["date", "date", "date", "timestamp", "timestamp"],
        )
        self.assertEqual(
            selected.rows,
            [
                [
                    datetime.date(1940, 1, 1),
                    datetime.date(1969, 12, 31),
                    datetime.date(1970, 1, 1),
                    datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
                    datetime.datetime(1970, 1, 1, 0, 0, 0),
                ]
            ],
        )

    def test_double_nan_and_infinities(self):
        selected = self.db.execute(
            "SELECT 1.0 / 0.0 AS pos, -1.0 / 0.0 AS neg, 0.0 / 0.0 AS nan;"
        )[0]
        self.assertEqual(selected.column_types, ["double", "double", "double"])
        pos, neg, nan = selected.rows[0]
        self.assertTrue(math.isinf(pos))
        self.assertGreater(pos, 0.0)
        self.assertTrue(math.isinf(neg))
        self.assertLess(neg, 0.0)
        self.assertTrue(math.isnan(nan))

    def test_out_of_range_date_and_timestamp_raise_error(self):
        for sql, label in (
            ("SELECT DATE '0000-01-01';", "DATE value"),
            ("SELECT DATE '2024-01-01' + 2000000000;", "DATE value"),
            ("SELECT TIMESTAMP '0000-01-01 00:00:00';", "TIMESTAMP value"),
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(pinkdb.Error) as ctx:
                    self.db.execute(sql)
                message = str(ctx.exception)
                self.assertIn(label, message)
                self.assertIn("outside Python's datetime range", message)
        # The mapping error does not poison the handle.
        self.assertEqual(self.db.execute("SELECT 1;")[0].rows, [[1]])


class StatementKindTests(unittest.TestCase):
    def setUp(self):
        self.db = pinkdb.connect_memory()
        self.addCleanup(self.db.close)

    def test_multi_statement_kinds_and_metadata(self):
        results = self.db.execute(
            "CREATE TABLE t (a INTEGER);"
            "INSERT INTO t VALUES (1), (2);"
            "UPDATE t SET a = a + 10;"
            "DELETE FROM t WHERE a = 11;"
            "CREATE VIEW v AS SELECT a FROM t;"
            "DROP VIEW v;"
            "DROP TABLE t;"
            "CREATE DATABASE db1;"
            "USE db1;"
            "BEGIN;"
            "COMMIT;"
            "BEGIN;"
            "ROLLBACK;"
            "USE main;"
            "DROP DATABASE db1;"
            "SELECT 1;"
        )
        self.assertEqual(
            [r.kind for r in results],
            [
                "created",
                "inserted",
                "updated",
                "deleted",
                "created_view",
                "dropped_view",
                "dropped_table",
                "created_database",
                "used",
                "began",
                "committed",
                "began",
                "rolledback",
                "used",
                "dropped_database",
                "selected",
            ],
        )
        self.assertEqual(results[1].affected, 2)
        self.assertEqual(results[2].affected, 2)
        self.assertEqual(results[3].affected, 1)
        self.assertEqual(
            [results[i].name for i in (4, 5, 6, 7, 8, 13, 14)],
            ["v", "v", "t", "db1", "db1", "main", "db1"],
        )
        # Non-SELECT results expose empty column/row surface, SELECT their own.
        for result in results[:-1]:
            self.assertEqual((result.columns, result.column_types, result.rows), ([], [], []))
        self.assertEqual(results[-1].columns, ["1"])
        self.assertEqual(results[-1].rows, [[1]])
        self.assertEqual(results[-1].affected, 0)
        self.assertIsNone(results[-1].name)


class DecimalIntervalTests(unittest.TestCase):
    def test_decimal_and_interval_values(self):
        with pinkdb.connect_memory() as db:
            res = db.execute(
                "SELECT 1.5::DECIMAL(4,2) AS d, INTERVAL '1 day' AS iv;"
            )[0]
            self.assertEqual(res.column_types, ["decimal", "interval"])
            self.assertEqual(res.rows[0][0], decimal.Decimal("1.50"))
            self.assertIsInstance(res.rows[0][1], str)
            self.assertIn("day", res.rows[0][1])

    def test_altered_table_kind(self):
        with pinkdb.connect_memory() as db:
            db.execute("CREATE TABLE t (x INTEGER);")
            res = db.execute("ALTER TABLE t ADD COLUMN y INTEGER DEFAULT 0;")[0]
            self.assertEqual(res.kind, "altered_table")


class ErrorTests(unittest.TestCase):
    def test_error_message_and_handle_survives(self):
        with pinkdb.connect_memory() as db:
            with self.assertRaises(pinkdb.Error) as ctx:
                db.execute("SELECT * FROM missing;")
            self.assertIn("not found", str(ctx.exception))
            # A failed statement does not poison the handle.
            self.assertEqual(db.execute("SELECT 42;")[0].rows, [[42]])
            with self.assertRaises(pinkdb.Error):
                db.execute("THIS IS NOT SQL;")
            self.assertEqual(db.execute("SELECT 7;")[0].rows, [[7]])

    def test_open_error_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "no_such_dir" / "x.pink"
            with self.assertRaises(pinkdb.Error):
                pinkdb.connect(missing)

    def test_use_after_close_raises(self):
        db = pinkdb.connect_memory()
        db.close()
        with self.assertRaises(pinkdb.Error):
            db.execute("SELECT 1;")
        db.close()  # idempotent

    def test_error_paths_leave_handle_usable(self):
        with pinkdb.connect_memory() as db:
            with self.assertRaises(pinkdb.Error):
                db.execute("SELECT DATE '0000-01-01';")  # Python mapping error
            self.assertEqual(db.execute("SELECT 42;")[0].rows, [[42]])
            with self.assertRaises(pinkdb.Error):
                db.execute("SELECT * FROM missing;")  # native error
            self.assertEqual(db.execute("SELECT 7;")[0].rows, [[7]])

    def test_non_string_sql_is_a_type_error(self):
        with pinkdb.connect_memory() as db:
            for bad in (b"SELECT 1;", None, 42):
                with self.subTest(bad=bad):
                    with self.assertRaises(TypeError):
                        db.execute(bad)


class LoadExtensionTests(unittest.TestCase):
    """``LOAD EXTENSION`` loads and runs native code: it is off by default."""

    def test_the_statement_is_refused_by_default(self):
        with pinkdb.connect_memory() as db:
            with self.assertRaises(pinkdb.Error) as ctx:
                db.execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
            self.assertIn("LOAD EXTENSION is disabled", str(ctx.exception))

    def test_the_opt_in_flips_the_behavior(self):
        with pinkdb.connect_memory() as db:
            db.allow_load_extension(True)
            with self.assertRaises(pinkdb.Error) as ctx:
                db.execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
            # The gate is no longer the blocker: the loader reports its own
            # error for the missing file.
            self.assertNotIn("LOAD EXTENSION is disabled", str(ctx.exception))
            self.assertIn("cannot load extension", str(ctx.exception))
            # And it can be turned off again.
            db.allow_load_extension(False)
            with self.assertRaises(pinkdb.Error) as ctx:
                db.execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
            self.assertIn("LOAD EXTENSION is disabled", str(ctx.exception))

    def test_the_opt_in_validates_its_argument(self):
        with pinkdb.connect_memory() as db:
            with self.assertRaises(TypeError):
                db.allow_load_extension("yes")

    def test_the_opt_in_on_a_closed_handle_raises(self):
        db = pinkdb.connect_memory()
        db.close()
        with self.assertRaises(pinkdb.Error):
            db.allow_load_extension(True)

    def test_empty_sql_returns_no_results(self):
        with pinkdb.connect_memory() as db:
            self.assertEqual(db.execute(""), [])

    def test_lone_surrogate_sql_raises_error(self):
        with pinkdb.connect_memory() as db:
            with self.assertRaises(pinkdb.Error) as ctx:
                db.execute("SELECT '\ud800';")
            self.assertIn("not encodable as UTF-8", str(ctx.exception))
            self.assertEqual(db.execute("SELECT 1;")[0].rows, [[1]])


class PathArgumentTests(unittest.TestCase):
    def test_empty_path_is_rejected(self):
        for bad in ("", b""):
            with self.subTest(path=bad):
                with self.assertRaises(ValueError) as ctx:
                    pinkdb.connect(bad)
                self.assertIn("must not be empty", str(ctx.exception))

    def test_nul_in_path_is_rejected_before_opening(self):
        with tempfile.TemporaryDirectory() as tmp:
            for bad in (
                os.path.join(tmp, "a\x00b.pink"),
                os.path.join(tmp, "a").encode("utf-8") + b"\x00b.pink",
            ):
                with self.subTest(path=bad):
                    with self.assertRaises(ValueError) as ctx:
                        pinkdb.connect(bad)
                    self.assertIn("NUL byte", str(ctx.exception))
            # Rejected before the native call: nothing was created on disk.
            self.assertEqual(os.listdir(tmp), [])

    def test_lone_surrogate_path_raises_error(self):
        with self.assertRaises(pinkdb.Error) as ctx:
            pinkdb.connect("/tmp/\ud800.pink")
        self.assertIn("cannot be encoded as UTF-8", str(ctx.exception))

    def test_invalid_utf8_bytes_path_raises_error(self):
        with self.assertRaises(pinkdb.Error) as ctx:
            pinkdb.connect(b"/tmp/\xff.pink")
        self.assertIn("UTF-8", str(ctx.exception))


class DurableTests(unittest.TestCase):
    def test_durable_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "roundtrip.pink"

            db = pinkdb.connect(path)  # accepts PathLike
            try:
                results = db.execute(
                    "CREATE TABLE t (i INTEGER, s VARCHAR);"
                    "INSERT INTO t VALUES "
                    "(-2147483647 - 1, 'héllo'), (NULL, NULL);"
                )
                self.assertEqual(results[1].affected, 2)
            finally:
                db.close()
            db.close()  # idempotent

            reopened = pinkdb.connect(str(path))
            try:
                selected = reopened.execute("SELECT i, s FROM t ORDER BY i;")[0]
                self.assertEqual(selected.rows, [[-2147483648, "héllo"], [None, None]])
            finally:
                reopened.close()

    def test_context_manager_closes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ctx.pink"
            with pinkdb.connect(path) as db:
                self.assertEqual(db.execute("SELECT 1;")[0].rows, [[1]])
            with self.assertRaises(pinkdb.Error):
                db.execute("SELECT 1;")

    def test_del_is_safe_and_idempotent(self):
        db = pinkdb.connect_memory()
        db.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
        del db
        gc.collect()  # must not raise, must not double-free
        db2 = pinkdb.connect_memory()
        del db2
        gc.collect()


class OwnershipTests(unittest.TestCase):
    def test_200_result_loop_frees_everything(self):
        with pinkdb.connect_memory() as db:
            db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
            for i in range(200):
                results = db.execute(
                    f"INSERT INTO t VALUES ({i}, 'v{i}');"
                    f"SELECT a, s FROM t WHERE a = {i};"
                )
                self.assertEqual(results[0].kind, "inserted")
                self.assertEqual(results[1].rows, [[i, f"v{i}"]])
            count = db.execute("SELECT COUNT(*) FROM t;")[0].rows[0][0]
            self.assertEqual(count, 200)


class FreeInstrumentationTests(unittest.TestCase):
    """Shadow ``pinkdb._LIB`` symbols to prove native frees happen exactly once."""

    _NAMES = (
        "pinkdb_execute",
        "pinkdb_execute_prepared",
        "pinkdb_prepare",
        "pinkdb_result_free",
        "pinkdb_prepared_free",
        "pinkdb_close",
    )

    def setUp(self):
        self._originals = {
            name: getattr(pinkdb._LIB, name) for name in self._NAMES
        }
        self.addCleanup(self._restore_instrumentation)
        # Allocator addresses are reused after free, so "exactly once" is
        # tracked with a live set (allocated but not yet freed), not by
        # counting unique pointer values.
        self.outstanding_results = set()
        self.result_allocations = 0
        self.result_frees = 0
        self.double_frees = []
        self.outstanding_prepared = set()
        self.prepared_allocations = 0
        self.prepared_frees = 0
        self.double_prepared_frees = []
        self.closed_pointers = []

        def counting_execute(handle, sql, sql_len):
            return track_result(
                self._originals["pinkdb_execute"](handle, sql, sql_len)
            )

        def counting_prepare(handle, sql, sql_len):
            pointer = self._originals["pinkdb_prepare"](handle, sql, sql_len)
            if pointer:
                if pointer in self.outstanding_prepared:
                    raise AssertionError(
                        f"prepared pointer still live: {pointer!r}"
                    )
                self.prepared_allocations += 1
                self.outstanding_prepared.add(pointer)
            return pointer

        def track_result(pointer):
            if pointer:
                if pointer in self.outstanding_results:
                    raise AssertionError(f"result pointer still live: {pointer!r}")
                self.result_allocations += 1
                self.outstanding_results.add(pointer)
            return pointer

        def counting_execute_prepared(handle, prepared, values, count):
            return track_result(
                self._originals["pinkdb_execute_prepared"](
                    handle, prepared, values, count
                )
            )

        def counting_result_free(pointer):
            if pointer not in self.outstanding_results:
                self.double_frees.append(pointer)
                raise AssertionError(f"free of a not-outstanding result: {pointer!r}")
            self.outstanding_results.discard(pointer)
            self.result_frees += 1
            return self._originals["pinkdb_result_free"](pointer)

        def counting_prepared_free(pointer):
            if pointer not in self.outstanding_prepared:
                self.double_prepared_frees.append(pointer)
                raise AssertionError(
                    f"free of a not-outstanding prepared statement: {pointer!r}"
                )
            self.outstanding_prepared.discard(pointer)
            self.prepared_frees += 1
            return self._originals["pinkdb_prepared_free"](pointer)

        def counting_close(pointer):
            self.closed_pointers.append(pointer)
            return self._originals["pinkdb_close"](pointer)

        pinkdb._LIB.pinkdb_execute = counting_execute
        pinkdb._LIB.pinkdb_execute_prepared = counting_execute_prepared
        pinkdb._LIB.pinkdb_prepare = counting_prepare
        pinkdb._LIB.pinkdb_result_free = counting_result_free
        pinkdb._LIB.pinkdb_prepared_free = counting_prepared_free
        pinkdb._LIB.pinkdb_close = counting_close

    def _restore_instrumentation(self):
        for name, original in self._originals.items():
            setattr(pinkdb._LIB, name, original)

    def test_every_result_is_freed_exactly_once(self):
        executes = 25
        db = pinkdb.connect_memory()
        db.execute("CREATE TABLE t (x INTEGER);")
        for i in range(executes):
            db.execute(f"INSERT INTO t VALUES ({i});")
        db.close()
        self.assertEqual(self.result_allocations, executes + 1)
        self.assertEqual(self.result_frees, executes + 1)
        self.assertEqual(self.double_frees, [])
        self.assertEqual(self.outstanding_results, set())

    def test_every_prepared_statement_is_freed_exactly_once(self):
        db = pinkdb.connect_memory()
        statements = [db.prepare("SELECT ?") for _ in range(20)]
        # Executing allocates results too; close frees only the statement.
        for i, stmt in enumerate(statements):
            pointer = stmt._prepared
            self.assertIn(pointer, self.outstanding_prepared)
            stmt.execute(i)
            stmt.close()
            stmt.close()  # idempotent: must not reach the library again
            self.assertNotIn(pointer, self.outstanding_prepared)
        # The last one is released by the garbage collector instead.
        collected = db.prepare("SELECT 1")
        collected.execute()
        del collected
        gc.collect()
        db.close()

        self.assertEqual(self.prepared_allocations, 21)
        self.assertEqual(self.prepared_frees, 21)
        self.assertEqual(self.double_prepared_frees, [])
        self.assertEqual(self.outstanding_prepared, set())
        self.assertEqual(self.result_allocations, 21)
        self.assertEqual(self.result_frees, 21)
        self.assertEqual(self.outstanding_results, set())

    def test_every_handle_is_closed_exactly_once(self):
        databases = [pinkdb.connect_memory() for _ in range(10)]
        raw_handles = [db._handle for db in databases]
        self.assertTrue(all(raw_handles))
        for db in databases:
            db.close()
            db.close()  # idempotent: must not reach the library again
        for raw in raw_handles:
            self.assertEqual(self.closed_pointers.count(raw), 1)
        self.assertEqual(len(self.closed_pointers), len(databases))
        self.assertNotIn(None, self.closed_pointers)


class CopyAndPickleTests(unittest.TestCase):
    def test_copy_and_deepcopy_raise_type_error(self):
        with pinkdb.connect_memory() as db:
            for copier in (copy.copy, copy.deepcopy):
                with self.subTest(copier=copier.__name__):
                    with self.assertRaises(TypeError) as ctx:
                        copier(db)
                    self.assertIn(
                        "cannot copy a Database handle", str(ctx.exception)
                    )
            # The original still owns the only handle and stays usable.
            self.assertEqual(db.execute("SELECT 1;")[0].rows, [[1]])
        db.close()  # idempotent

    def test_pickle_fails_safely(self):
        db = pinkdb.connect_memory()
        try:
            with self.assertRaises((pickle.PicklingError, TypeError, AttributeError)):
                pickle.dumps(db)
            self.assertEqual(db.execute("SELECT 1;")[0].rows, [[1]])
        finally:
            db.close()


class MemoryGrowthTests(unittest.TestCase):
    def test_bounded_memory_over_many_executes(self):
        try:
            import resource
        except ImportError:  # pragma: no cover - non-POSIX platforms
            self.skipTest("resource module unavailable")

        def peak_rss_bytes():
            # ru_maxrss is bytes on macOS, kibibytes elsewhere.
            max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return max_rss if sys.platform == "darwin" else max_rss * 1024

        with pinkdb.connect_memory() as db:
            for i in range(50):  # warm up the allocator high-water mark
                db.execute(f"SELECT {i};")
            before = peak_rss_bytes()
            for _ in range(2000):
                self.assertEqual(db.execute("SELECT 1 + 1;")[0].rows, [[2]])
            growth = peak_rss_bytes() - before
        # Generous bound: catches egregious leaks without flakiness.
        self.assertLess(growth, 256 * 1024 * 1024, f"RSS grew by {growth} bytes")


class ThreadingTests(unittest.TestCase):
    def test_one_database_per_thread_with_error_isolation(self):
        thread_count = 8
        failures = []
        lock = threading.Lock()

        def worker(worker_id):
            try:
                with pinkdb.connect_memory() as db:
                    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
                    db.execute(
                        f"INSERT INTO t VALUES ({worker_id}, 'w{worker_id}');"
                    )
                    try:
                        db.execute("SELECT * FROM missing;")
                    except pinkdb.Error as exc:
                        if "not found" not in str(exc):
                            raise AssertionError(f"unexpected error text: {exc}")
                    else:
                        raise AssertionError("missing table must raise Error")
                    # The failed statement leaves this thread's handle usable.
                    rows = db.execute("SELECT x, s FROM t;")[0].rows
                    if rows != [[worker_id, f"w{worker_id}"]]:
                        raise AssertionError(f"rows: {rows!r}")
            except BaseException as exc:  # pragma: no cover - failure path
                with lock:
                    failures.append((worker_id, repr(exc)))

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(thread_count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(failures, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))


class SharedHandleTests(unittest.TestCase):
    def test_one_database_shared_by_many_threads(self):
        # The handle serializes calls internally; concurrent use must neither
        # crash nor lose rows.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shared.pink"
            with pinkdb.connect(path) as db:
                db.execute("CREATE TABLE t (x INTEGER);")
                failures = []

                def worker(worker_id):
                    try:
                        for i in range(50):
                            db.execute(f"INSERT INTO t VALUES ({worker_id * 1000 + i});")
                            db.execute("SELECT COUNT(*) FROM t;")
                    except BaseException as exc:  # pragma: no cover
                        failures.append(repr(exc))

                threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=60)
                self.assertEqual(failures, [])
                self.assertEqual(db.execute("SELECT COUNT(*) FROM t;")[0].rows, [[400]])

    def test_a_durable_file_allows_multiple_handles(self):
        # Multi-process writers (wave 45m): any number of handles may open a
        # durable file; writers serialize on the write lease and every handle
        # refreshes at statement boundaries.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shared.pink"
            first = pinkdb.connect(path)
            second = pinkdb.connect(path)
            try:
                first.execute("CREATE TABLE t (x INTEGER);")
                first.execute("INSERT INTO t VALUES (1);")
                second.execute("INSERT INTO t VALUES (2);")
                # The second handle refreshes and sees the first handle's row.
                self.assertEqual(
                    second.execute("SELECT COUNT(*) FROM t;")[0].rows, [[2]]
                )
            finally:
                first.close()
                second.close()


class PreparedStatementTests(unittest.TestCase):
    def setUp(self):
        self.db = pinkdb.connect_memory()
        self.addCleanup(self.db.close)

    def test_prepare_metadata_and_parse_only(self):
        stmt = self.db.prepare("INSERT INTO t (a, b) VALUES (?, ?)")
        self.addCleanup(stmt.close)
        self.assertEqual(stmt.sql, "INSERT INTO t (a, b) VALUES (?, ?)")
        self.assertEqual(stmt.parameter_count, 2)
        self.assertIn("open, 2 parameters", repr(stmt))

        zero = self.db.prepare("SELECT 1")
        self.addCleanup(zero.close)
        self.assertEqual(zero.parameter_count, 0)
        self.assertEqual(zero.execute().rows, [[1]])

        # Prepare parses only: the table may not exist yet, and the statement
        # binds against the catalog current at each execution.
        stmt2 = self.db.prepare("INSERT INTO t (a) VALUES (?)")
        self.addCleanup(stmt2.close)
        self.db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
        self.assertEqual(stmt2.execute(5).affected, 1)
        self.assertEqual(self.db.execute("SELECT a FROM t;")[0].rows, [[5]])

    def test_parameter_type_mapping(self):
        stmt = self.db.prepare("SELECT ?;".rstrip(";"))
        self.addCleanup(stmt.close)
        cases = [
            (True, "boolean", True),
            (False, "boolean", False),
            (0, "integer", 0),
            (-(2**31), "integer", -(2**31)),
            (2**31 - 1, "integer", 2**31 - 1),
            (2**31, "bigint", 2**31),
            (-(2**31) - 1, "bigint", -(2**31) - 1),
            (2**63 - 1, "bigint", 2**63 - 1),
            (1.5, "double", 1.5),
            (-0.0, "double", -0.0),
            ("héllo 🌍", "varchar", "héllo 🌍"),
            ("", "varchar", ""),
            ("a\x00b", "varchar", "a\x00b"),
            (datetime.date(2024, 1, 15), "date", datetime.date(2024, 1, 15)),
            (datetime.date.min, "date", datetime.date.min),
            (datetime.date.max, "date", datetime.date.max),
            (
                datetime.datetime(2024, 1, 15, 10, 0, 0, 123456),
                "timestamp",
                datetime.datetime(2024, 1, 15, 10, 0, 0, 123456),
            ),
            (
                datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
                "timestamp",
                datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
            ),
        ]
        for value, want_type, want in cases:
            with self.subTest(value=value):
                result = stmt.execute(value)
                self.assertEqual(result.column_types, [want_type])
                self.assertEqual(result.rows, [[want]])
                self.assertEqual(result.kind, "selected")
        # -0.0 must survive bit-for-bit.
        negative_zero = stmt.execute(-0.0).rows[0][0]
        self.assertEqual(math.copysign(1.0, negative_zero), -1.0)

    def test_datetime_with_timezone_is_converted_to_utc(self):
        aware = datetime.datetime(
            2024, 1, 15, 12, 0,
            tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
        )
        selected = self.db.prepare("SELECT ?").execute(aware)
        self.assertEqual(selected.rows, [[datetime.datetime(2024, 1, 15, 6, 30)]])

    def test_all_types_roundtrip(self):
        self.db.execute(
            "CREATE TABLE types (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, "
            "s VARCHAR, dt DATE, ts TIMESTAMP);"
        )
        insert = self.db.prepare("INSERT INTO types VALUES (?, ?, ?, ?, ?, ?, ?)")
        self.addCleanup(insert.close)
        result = insert.execute(
            True,
            -(2**31),
            2**63 - 1,
            -0.0,
            "héllo 🌍\x00",
            datetime.date(2024, 1, 15),
            datetime.datetime(2024, 1, 15, 10, 0, 0, 123456),
        )
        self.assertEqual(result.kind, "inserted")
        self.assertEqual(result.affected, 1)
        self.assertEqual(insert.execute(None, None, None, None, None, None, None).affected, 1)

        select = self.db.prepare("SELECT * FROM types WHERE i = ?")
        self.addCleanup(select.close)
        selected = select.execute(-(2**31))
        self.assertEqual(
            selected.column_types,
            ["boolean", "integer", "bigint", "double", "varchar", "date", "timestamp"],
        )
        self.assertEqual(
            selected.rows,
            [[
                True,
                -(2**31),
                2**63 - 1,
                -0.0,
                "héllo 🌍\x00",
                datetime.date(2024, 1, 15),
                datetime.datetime(2024, 1, 15, 10, 0, 0, 123456),
            ]],
        )
        # A NULL bound into the same typed slot, then a miss in the same
        # statement.
        self.assertEqual(select.execute(None).rows, [])
        nulls = self.db.execute("SELECT * FROM types WHERE i IS NULL;")[0]
        self.assertEqual(nulls.rows, [[None] * 7])

    def test_repeated_execution_rebinds(self):
        stmt = self.db.prepare("SELECT ?;".rstrip(";"))
        self.addCleanup(stmt.close)
        # The same statement binds different values and even different types
        # on each execution (auto-commit per execute, fresh bind each time).
        self.assertEqual(stmt.execute(1).rows, [[1]])
        self.assertEqual(stmt.execute("one").rows, [["one"]])
        self.assertEqual(stmt.execute(True).rows, [[True]])
        for i in range(5):
            self.assertEqual(stmt.execute(i * 10).rows, [[i * 10]])

    def test_reuse_resolves_the_current_catalog(self):
        stmt = self.db.prepare("SELECT x FROM t")
        self.addCleanup(stmt.close)
        self.db.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (7);")
        self.assertEqual(stmt.execute().rows, [[7]])
        self.db.execute(
            "DROP TABLE t; CREATE TABLE t (x VARCHAR); INSERT INTO t VALUES ('reborn');"
        )
        selected = stmt.execute()
        self.assertEqual(selected.column_types, ["varchar"])
        self.assertEqual(selected.rows, [["reborn"]])

    def test_null_handling(self):
        self.db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        insert = self.db.prepare("INSERT INTO t VALUES (?, ?)")
        self.addCleanup(insert.close)
        insert.execute(None, None)
        insert.execute(1, "x")
        selected = self.db.execute("SELECT i, s FROM t WHERE i IS NULL;")[0]
        self.assertEqual(selected.rows, [[None, None]])

        # NULL with no context and no CAST cannot be typed: the native bind
        # error names the position and the statement stays usable.
        untyped = self.db.prepare("SELECT ?")
        self.addCleanup(untyped.close)
        with self.assertRaises(pinkdb.Error) as ctx:
            untyped.execute(None)
        self.assertEqual(
            str(ctx.exception),
            "could not determine data type of parameter 1: bind a non-NULL "
            "value or add an explicit CAST(? AS <type>)",
        )
        cast = self.db.prepare("SELECT CAST(? AS INTEGER)")
        self.addCleanup(cast.close)
        self.assertEqual(cast.execute(None).rows, [[None]])
        self.assertEqual(untyped.execute(7).rows, [[7]])

    def test_wrong_arity_is_a_native_error(self):
        one = self.db.prepare("SELECT ?")
        self.addCleanup(one.close)
        with self.assertRaises(pinkdb.Error) as ctx:
            one.execute()
        self.assertEqual(
            str(ctx.exception),
            "values were not provided for prepared statement parameters: 1",
        )
        with self.assertRaises(pinkdb.Error) as ctx:
            one.execute(1, 2)
        self.assertEqual(
            str(ctx.exception),
            "parameter count mismatch: the prepared statement takes 1, "
            "but values were provided for: 2",
        )
        two = self.db.prepare("SELECT ?, ?")
        self.addCleanup(two.close)
        with self.assertRaises(pinkdb.Error) as ctx:
            two.execute(1)
        self.assertEqual(
            str(ctx.exception),
            "values were not provided for prepared statement parameters: 2",
        )
        # The failures left both statements usable.
        self.assertEqual(one.execute(9).rows, [[9]])
        self.assertEqual(two.execute(1, 2).rows, [[1, 2]])

    def test_conversion_and_range_errors(self):
        cast = self.db.prepare("SELECT CAST(? AS INTEGER)")
        self.addCleanup(cast.close)
        with self.assertRaises(pinkdb.Error) as ctx:
            cast.execute("abc")
        self.assertEqual(
            str(ctx.exception),
            'cannot bind parameter 1 as INTEGER: cannot CAST "abc" AS INTEGER',
        )
        self.assertEqual(cast.execute("42").rows, [[42]])

        stmt = self.db.prepare("SELECT ?")
        self.addCleanup(stmt.close)
        with self.assertRaises(pinkdb.Error) as ctx:
            stmt.execute(2**63)
        self.assertIn("does not fit in INTEGER or BIGINT", str(ctx.exception))
        with self.assertRaises(pinkdb.Error) as ctx:
            stmt.execute(-(2**63) - 1)
        self.assertIn("does not fit in INTEGER or BIGINT", str(ctx.exception))
        with self.assertRaises(pinkdb.Error) as ctx:
            stmt.execute("\ud800")
        self.assertIn("not encodable as UTF-8", str(ctx.exception))
        self.assertEqual(stmt.execute("ok").rows, [["ok"]])

    def test_unbindable_types_raise_type_error(self):
        stmt = self.db.prepare("SELECT ?")
        self.addCleanup(stmt.close)
        for bad in (b"bytes", bytearray(b"x"), memoryview(b"x"), decimal.Decimal("1.5"), object()):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    stmt.execute(bad)
        with self.assertRaises(TypeError) as ctx:
            stmt.execute(decimal.Decimal("1.5"))
        self.assertIn("cannot bind DECIMAL parameters", str(ctx.exception))
        with self.assertRaises(TypeError) as ctx:
            stmt.execute(b"raw")
        self.assertIn("no BLOB type", str(ctx.exception))
        self.assertEqual(stmt.execute(1).rows, [[1]])

    def test_prepare_errors(self):
        with self.assertRaises(TypeError):
            self.db.prepare(b"SELECT 1")
        with self.assertRaises(pinkdb.Error) as ctx:
            self.db.prepare("SELECT ?; SELECT ?")
        self.assertIn("exactly one statement", str(ctx.exception))
        for empty in ("", "-- comment only\n", "  ;  "):
            with self.subTest(sql=empty):
                with self.assertRaises(pinkdb.Error) as ctx:
                    self.db.prepare(empty)
                self.assertIn("exactly one statement", str(ctx.exception))
        with self.assertRaises(pinkdb.Error) as ctx:
            self.db.prepare("SELECT 1 ORDER BY ?")
        self.assertIn("not supported in ORDER BY", str(ctx.exception))
        with self.assertRaises(pinkdb.Error) as ctx:
            self.db.prepare("SELECT '\ud800';")
        self.assertIn("not encodable as UTF-8", str(ctx.exception))
        # A failed prepare leaves the database usable.
        self.assertEqual(self.db.execute("SELECT 1;")[0].rows, [[1]])

    def test_close_and_database_close_semantics(self):
        stmt = self.db.prepare("SELECT ?")
        stmt.close()
        stmt.close()  # idempotent
        with self.assertRaises(pinkdb.Error) as ctx:
            stmt.execute(1)
        self.assertEqual(str(ctx.exception), "statement is closed")
        self.assertEqual(stmt.parameter_count, 1)  # metadata stays readable

        db = pinkdb.connect_memory()
        with db.prepare("SELECT 1") as scoped:
            self.assertEqual(scoped.execute().rows, [[1]])
        with self.assertRaises(pinkdb.Error) as ctx:
            scoped.execute()
        self.assertEqual(str(ctx.exception), "statement is closed")

        orphan = db.prepare("SELECT 2")
        db.close()
        with self.assertRaises(pinkdb.Error) as ctx:
            orphan.execute()
        self.assertEqual(str(ctx.exception), "database is closed")
        orphan.close()  # freeing does not touch the closed handle
        with self.assertRaises(pinkdb.Error) as ctx:
            db.prepare("SELECT 3")
        self.assertEqual(str(ctx.exception), "database is closed")

    def test_copy_and_pickle_fail_safely(self):
        with self.db.prepare("SELECT ?") as stmt:
            for copier in (copy.copy, copy.deepcopy):
                with self.subTest(copier=copier.__name__):
                    with self.assertRaises(TypeError) as ctx:
                        copier(stmt)
                    self.assertIn("cannot copy a Statement handle", str(ctx.exception))
            with self.assertRaises((pickle.PicklingError, TypeError, AttributeError)):
                pickle.dumps(stmt)
            # The original still owns the only native statement.
            self.assertEqual(stmt.execute(1).rows, [[1]])

    def test_values_are_never_parsed_as_sql(self):
        self.db.execute("CREATE TABLE t (s VARCHAR);")
        insert = self.db.prepare("INSERT INTO t VALUES (?)")
        self.addCleanup(insert.close)
        payload = "'; DROP TABLE t; -- ?"
        insert.execute(payload)
        selected = self.db.prepare("SELECT s FROM t WHERE s = ?")
        self.addCleanup(selected.close)
        self.assertEqual(selected.execute(payload).rows, [[payload]])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM t;")[0].rows, [[1]])

    def test_transaction_rollback_and_commit(self):
        self.db.execute("CREATE TABLE t (i INTEGER);")
        insert = self.db.prepare("INSERT INTO t VALUES (?)")
        self.addCleanup(insert.close)
        self.db.execute("BEGIN;")
        insert.execute(1)
        self.db.execute("ROLLBACK;")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM t;")[0].rows, [[0]])
        self.db.execute("BEGIN;")
        insert.execute(2)
        self.db.execute("COMMIT;")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM t;")[0].rows, [[1]])

    def test_rbac_kinds_through_prepared_statements(self):
        # Wave 48 statements (kinds 17-23) surface with their names when run
        # as zero-parameter prepared statements.
        self.db.execute("CREATE TABLE t (i INTEGER);")
        cases = [
            ("CREATE USER alice PASSWORD 'secret'", "created_user", "alice"),
            ("CREATE ROLE analyst", "created_role", "analyst"),
            ("GRANT SELECT ON TABLE t TO alice", "granted", "SELECT ON TABLE main.t TO alice"),
            ("REVOKE SELECT ON TABLE t FROM alice", "revoked", "SELECT ON TABLE main.t TO alice"),
            ("ALTER USER alice PASSWORD 'rotated'", "altered_user", "alice"),
            ("DROP ROLE analyst", "dropped_role", "analyst"),
            ("DROP USER alice", "dropped_user", "alice"),
        ]
        for sql, kind, name in cases:
            with self.subTest(sql=sql):
                with self.db.prepare(sql) as stmt:
                    result = stmt.execute()
                self.assertEqual(result.kind, kind)
                self.assertEqual(result.name, name)


class PreparedDurableTests(unittest.TestCase):
    def test_prepared_writes_replay_after_reopen(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "prepared.pink"
            db = pinkdb.connect(path)
            db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
            insert = db.prepare("INSERT INTO t VALUES (?, ?)")
            insert.execute(0, "héllo")
            insert.execute(1, "wörld")
            insert.close()
            db.close()

            # The WAL logged the fully substituted SQL: replayed verbatim.
            reopened = pinkdb.connect(path)
            try:
                rows = reopened.execute("SELECT i, s FROM t ORDER BY i;")[0].rows
                self.assertEqual(rows, [[0, "héllo"], [1, "wörld"]])
            finally:
                reopened.close()

    def test_prepared_statements_span_handles(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shared.pink"
            first = pinkdb.connect(path)
            second = pinkdb.connect(path)
            try:
                first.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
                # A handle refreshes its catalog at a statement boundary; a
                # prepared bind alone does not refresh it, so touch the
                # second handle once before preparing against the new DDL.
                second.execute("SELECT 1;")
                insert = second.prepare("INSERT INTO t VALUES (?, ?)")
                insert.execute(7, "from-second")
                insert.close()
                # The first handle refreshes at its statement boundary.
                selected = first.prepare("SELECT s FROM t WHERE i = ?")
                self.assertEqual(selected.execute(7).rows, [["from-second"]])
                selected.close()
            finally:
                first.close()
                second.close()


class EnvOverrideTests(unittest.TestCase):
    def test_pinkdb_ffi_lib_override(self):
        """A child interpreter forced to load the library via PINKDB_FFI_LIB."""
        env = dict(os.environ)
        env["PINKDB_FFI_LIB"] = pinkdb.library_path
        env["PYTHONPATH"] = os.pathsep.join(
            part for part in (str(_BINDING_ROOT), env.get("PYTHONPATH")) if part
        )
        code = (
            "import sys, pinkdb;"
            "assert pinkdb.library_path == sys.argv[1], "
            "(pinkdb.library_path, sys.argv[1]);"
            "db = pinkdb.connect_memory();"
            "assert db.execute('SELECT 1 + 1;')[0].rows[0][0] == 2;"
            "db.close()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code, pinkdb.library_path],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

    def test_wrong_but_loadable_dylib_is_an_error(self):
        wrong = _non_pinkdb_library()
        proc = _run_child("import pinkdb", PINKDB_FFI_LIB=wrong)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("pinkdb.Error", proc.stderr)
        self.assertIn("is not a pinkdb-ffi library", proc.stderr)
        self.assertIn("missing symbol pinkdb_", proc.stderr)

    def test_nonexistent_ffi_lib_is_an_error(self):
        missing = str(
            Path(tempfile.gettempdir()) / "no_such_pinkdb_dir" / "libpinkdb_ffi.dylib"
        )
        proc = _run_child("import pinkdb", PINKDB_FFI_LIB=missing)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("pinkdb.Error", proc.stderr)
        self.assertIn("does not point to an existing file", proc.stderr)


if __name__ == "__main__":
    unittest.main()
