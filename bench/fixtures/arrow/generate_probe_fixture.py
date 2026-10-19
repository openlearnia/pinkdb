#!/usr/bin/env python3
"""Regenerate `bench/fixtures/arrow/probe_types.arrow`.

A small pyarrow-written Arrow IPC file used by the `bench/duckdiff.py` arrow
battery: pinkdb reads this *foreign* file and the harness compares the values
against DuckDB running the same data as SQL (DuckDB α ships no arrow support
at all — probed: HTTP 404 for the loadable extension — so file-level parity
is cross-checked through the values, never through DuckDB reading the file).

Keep the values here in sync with `ARROW_FIXTURE_ROWS` in
`bench/duckdiff.py`; the file is deterministic.

    python3 -m venv .venv && .venv/bin/pip install pyarrow
    .venv/bin/python bench/fixtures/arrow/generate_probe_fixture.py
"""

import datetime
import decimal
import os

import pyarrow as pa
import pyarrow.ipc as ipc

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    table = pa.table(
        {
            "i": pa.array([1, 2, 3, None, 5], type=pa.int32()),
            "b": pa.array([10, None, 30, 40, 50], type=pa.int64()),
            "d": pa.array([1.5, 2.25, None, -4.0, 5.125], type=pa.float64()),
            "s": pa.array(["alpha", None, "gämma", "", "epsilon"], type=pa.string()),
            "bo": pa.array([True, False, None, True, False], type=pa.bool_()),
            "dt": pa.array(
                [
                    datetime.date(2024, 1, 1),
                    datetime.date(2022, 12, 31),
                    None,
                    datetime.date(1970, 1, 1),
                    datetime.date(2100, 2, 28),
                ],
                type=pa.date32(),
            ),
            "ts": pa.array(
                [
                    datetime.datetime(2024, 1, 1, 12, 0, 0, 1),
                    None,
                    datetime.datetime(1999, 12, 31, 23, 59, 59, 999999),
                    datetime.datetime(1970, 1, 1, 0, 0, 0),
                    datetime.datetime(2050, 6, 15, 8, 30, 0, 123456),
                ],
                type=pa.timestamp("us"),
            ),
            "dc": pa.array(
                [
                    decimal.Decimal("1.250"),
                    decimal.Decimal("-2.500"),
                    None,
                    decimal.Decimal("0.001"),
                    decimal.Decimal("12345.678"),
                ],
                type=pa.decimal128(12, 3),
            ),
            # month_day_nano with microsecond-aligned nanoseconds, so the
            # round trip through the engine's micros is exact. The values
            # mirror the SQL literals in `bench/duckdiff.py`.
            "iv": pa.array(
                [
                    pa.MonthDayNano([1, 2, 0]),
                    None,
                    pa.MonthDayNano([1, -2, 0]),
                    pa.MonthDayNano([0, 0, 0]),
                    pa.MonthDayNano([0, 0, 18_000_000_000_000]),
                ],
                type=pa.month_day_nano_interval(),
            ),
        }
    )
    path = os.path.join(HERE, "probe_types.arrow")
    with pa.OSFile(path, "wb") as sink:
        with ipc.new_file(sink, table.schema) as w:
            w.write_table(table, max_chunksize=2)
    print(f"wrote {path} ({os.path.getsize(path)} bytes)")


if __name__ == "__main__":
    main()
