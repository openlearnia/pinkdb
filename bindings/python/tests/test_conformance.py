"""Run the shared conformance corpus (bindings/conformance) through the
DB-API 2.0 layer.

The corpus is language-neutral JSON; this module is the reference for its
value encoding (see bindings/conformance/README.md). Every case opens a fresh
in-memory database, applies ``setup`` and then walks ``steps``.
"""

import datetime
import decimal
import json
import os
import pathlib
import sys
import tracemalloc
import unittest

try:
    import pinkdb
except ImportError:  # a source checkout run without PYTHONPATH
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    import pinkdb

CORPUS_PATH = pathlib.Path(
    os.environ.get(
        "PINKDB_CONFORMANCE",
        pathlib.Path(__file__).resolve().parents[2] / "conformance" / "corpus.json",
    )
)

#: PEP 249 exception the C ABI error class maps to.
EXCEPTION_BY_CLASS = {
    "parse": pinkdb.ProgrammingError,
    "catalog": pinkdb.ProgrammingError,
    "type": pinkdb.ProgrammingError,
    "parameter": pinkdb.ProgrammingError,
    "constraint": pinkdb.IntegrityError,
    "execution": pinkdb.OperationalError,
    "io": pinkdb.OperationalError,
    "csv": pinkdb.DataError,
    "invalid_argument": pinkdb.InterfaceError,
    "internal": pinkdb.InternalError,
}


def _parse_double(text):
    if text == "NaN":
        return float("nan")
    if text == "Infinity":
        return float("inf")
    if text == "-Infinity":
        return float("-inf")
    return float(text)


def _decode_param(cell):
    if cell is None:
        return None
    type_name, value = cell
    if type_name in ("boolean", "integer"):
        return value
    if type_name == "bigint":
        return int(value)
    if type_name == "double":
        return _parse_double(value)
    if type_name == "date":
        return datetime.date.fromisoformat(value)
    if type_name == "timestamp":
        return datetime.datetime.fromisoformat(value)
    if type_name == "decimal":
        return decimal.Decimal(value)
    if type_name in ("varchar", "interval", "hugeint", "uhugeint"):
        return value
    raise AssertionError(f"corpus: unknown parameter type {type_name}")


def _decode_params(params):
    return None if params is None else [_decode_param(cell) for cell in params]


with CORPUS_PATH.open(encoding="utf-8") as _corpus_file:
    CORPUS = json.load(_corpus_file)


class ConformanceTest(unittest.TestCase):
    """One test per corpus case; failing steps report their position."""

    maxDiff = None

    def test_corpus_metadata(self):
        self.assertEqual(CORPUS["corpus_version"], 1)
        self.assertEqual(CORPUS["abi_version"], (1 << 16) | 3)
        self.assertEqual(
            CORPUS["abi_version"],
            pinkdb.abi_version(),
            "the corpus targets a different C ABI version than this binding",
        )
        self.assertTrue(CORPUS["cases"])

    # ----- case / step machinery -----

    def run_case(self, case):
        connection = pinkdb.connect_memory()
        try:
            for sql in case.get("setup", []):
                connection.execute(sql)
            for index, step in enumerate(case["steps"]):
                where = f"{case['name']} step {index} ({step['op']})"
                with self.subTest(step=where):
                    self.run_step(connection, step, where)
        finally:
            connection.close()

    def run_step(self, connection, step, where):
        op = step["op"]
        if op in ("query", "prepared_query"):
            cursor = connection.cursor()
            try:
                cursor.execute(step["sql"], _decode_params(step.get("params")))
                self.assert_result_set(cursor, step["expect"], where)
            finally:
                cursor.close()
        elif op == "exec":
            results = connection.execute(step["sql"])
            self.assertEqual(len(results), len(step["expect"]), f"{where}: statement count")
            for index, (result, expect) in enumerate(zip(results, step["expect"])):
                self.assert_statement(result, expect, f"{where} statement {index}")
        elif op == "exec_prepared":
            statement = connection.prepare(step["sql"])
            try:
                result = statement.execute(*_decode_params(step["params"]))
                self.assertEqual(len(step["expect"]), 1, f"{where}: one expectation")
                self.assert_statement(result, step["expect"][0], where)
            finally:
                statement.close()
        elif op == "error":
            cursor = connection.cursor()
            try:
                with self.assertRaises(pinkdb.Error) as raised:
                    cursor.execute(step["sql"], _decode_params(step.get("params")))
            finally:
                cursor.close()
            error = raised.exception
            self.assertEqual(error.pinkdb_class, step["class"], f"{where}: error class")
            self.assertIsInstance(
                error,
                EXCEPTION_BY_CLASS[step["class"]],
                f"{where}: PEP 249 exception type for class {step['class']}",
            )
            if "message_contains" in step:
                self.assertIn(step["message_contains"], str(error), where)
        elif op == "stream":
            self.assert_stream(connection, step, where)
        else:
            self.fail(f"{where}: unknown op {op}")

    def assert_result_set(self, cursor, expect, where):
        self.assertIsNotNone(cursor.description, f"{where}: no result set")
        names = [column[0] for column in cursor.description]
        types = [pinkdb._TYPE_NAMES[column[1]] for column in cursor.description]
        self.assertEqual(names, expect["columns"], f"{where}: columns")
        self.assertEqual(types, expect["types"], f"{where}: column types")
        rows = cursor.fetchall()
        self.assert_rows(rows, expect["columns"], expect["types"], expect["rows"], where)

    def assert_statement(self, result, expect, where):
        self.assertEqual(result.kind, expect["kind"], f"{where}: statement kind")
        if "name" in expect:
            self.assertEqual(result.name, expect["name"], f"{where}: name")
        if "affected" in expect:
            self.assertEqual(result.affected, expect["affected"], f"{where}: affected rows")
        if "columns" in expect:
            self.assertEqual(result.columns, expect["columns"], f"{where}: columns")
            self.assertEqual(result.column_types, expect["types"], f"{where}: column types")
            self.assert_rows(
                result.rows, expect["columns"], expect["types"], expect["rows"], where
            )

    def assert_rows(self, rows, columns, types, expected_rows, where):
        self.assertEqual(len(rows), len(expected_rows), f"{where}: row count")
        for row_index, expected_row in enumerate(expected_rows):
            self.assertEqual(
                len(rows[row_index]), len(columns), f"{where}: row {row_index} width"
            )
            for column_index, expected_cell in enumerate(expected_row):
                name = columns[column_index]
                actual = rows[row_index][column_index]
                self.assert_cell(
                    actual,
                    expected_cell,
                    types[column_index],
                    f"{where} row {row_index}, column {name}",
                )

    def assert_cell(self, actual, expected, type_name, where):
        if expected is None:
            self.assertIsNone(actual, f"{where}: expected NULL")
            return
        expected_type, expected_value = expected
        self.assertEqual(expected_type, type_name, f"{where}: corpus cell type")
        if type_name == "boolean":
            self.assertIs(actual, expected_value, where)
        elif type_name == "integer":
            self.assertEqual(actual, expected_value, where)
        elif type_name == "bigint":
            self.assertEqual(actual, int(expected_value), where)
        elif type_name == "double":
            want = _parse_double(expected_value)
            if want != want:  # NaN
                self.assertNotEqual(actual, actual, f"{where}: expected NaN")
            else:
                self.assertEqual(actual, want, where)
        elif type_name == "date":
            self.assertEqual(actual, datetime.date.fromisoformat(expected_value), where)
        elif type_name == "timestamp":
            self.assertEqual(
                actual, datetime.datetime.fromisoformat(expected_value), where
            )
        elif type_name == "decimal":
            self.assertEqual(actual, decimal.Decimal(expected_value), where)
        else:  # varchar, interval, hugeint, uhugeint
            self.assertEqual(actual, expected_value, where)

    def assert_stream(self, connection, step, where):
        expect = step["expect"]
        cursor = connection.cursor()
        try:
            cursor.execute(step["sql"])
            self.assertIsNotNone(cursor.description, f"{where}: no result set")
            names = [column[0] for column in cursor.description]
            types = [pinkdb._TYPE_NAMES[column[1]] for column in cursor.description]
            self.assertEqual(names, expect["columns"], f"{where}: columns")
            self.assertEqual(types, expect["types"], f"{where}: column types")
            self.assertEqual(cursor.rowcount, expect["row_count"], f"{where}: row count")

            # Fetch one row at a time and keep only the head/tail the corpus
            # names. The allocation high-water mark proves the rows are not
            # materialized: fetchall() would need ~10 MB for 100k rows, while
            # this loop stays under a few KB.
            head = []
            tail = []
            checksum = 0
            count = 0
            tracemalloc.start()
            try:
                while True:
                    row = cursor.fetchone()
                    if row is None:
                        break
                    value = row[0]
                    self.assertIsInstance(value, int, f"{where}: checksum column")
                    count += 1
                    checksum += value
                    if len(head) < len(expect["first_rows"]):
                        head.append(value)
                    tail.append(value)
                    if len(tail) > len(expect["last_rows"]):
                        tail.pop(0)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(count, expect["row_count"], f"{where}: streamed rows")
            self.assertEqual(checksum, expect["checksum"], f"{where}: checksum")
            self.assertEqual(
                head,
                [int(row[0][1]) for row in expect["first_rows"]],
                f"{where}: first rows",
            )
            self.assertEqual(
                tail, [int(row[0][1]) for row in expect["last_rows"]], f"{where}: last rows"
            )
            self.assertLess(peak, 1024 * 1024, f"{where}: rows are materialized")
        finally:
            cursor.close()


def _case_test(case):
    def test(self):
        self.run_case(case)

    test.__name__ = f"test_{case['name']}"
    test.__doc__ = case["description"]
    return test


for _case in CORPUS["cases"]:
    setattr(ConformanceTest, f"test_{_case['name']}", _case_test(_case))
del _case


if __name__ == "__main__":
    unittest.main()
