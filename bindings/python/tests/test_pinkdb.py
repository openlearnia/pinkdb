"""Tests for the pinkdb Python binding, using only the stdlib ``unittest``.

From the repo root (build the C ABI first):

    cargo build --release -p pinkdb-ffi
    python3 -m unittest discover -s bindings/python/tests -v

The shared library is found via ``PINKDB_FFI_LIB`` when set, else from the
repo-relative ``target/release`` directory (see ``pinkdb/__init__.py``).
"""

import datetime
import gc
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

# Make ``import pinkdb`` work regardless of the working directory.
_HERE = Path(__file__).resolve()
_BINDING_ROOT = _HERE.parents[1]  # bindings/python
if str(_BINDING_ROOT) not in sys.path:
    sys.path.insert(0, str(_BINDING_ROOT))

import pinkdb  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
