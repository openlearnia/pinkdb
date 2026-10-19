#!/usr/bin/env python3
"""Regenerate the committed Arrow IPC fixtures in this directory.

Run with a pyarrow-enabled interpreter (the wave-71 verification used a venv
with pyarrow 26.0.0):

    python3 -m venv .venv && .venv/bin/pip install pyarrow
    .venv/bin/python extensions/arrow/tests/fixtures/generate_fixtures.py

The fixtures are committed so the crate's tests need no Python at run time;
this script exists to reproduce or extend them. Everything it writes is
deterministic (fixed values, fixed batch boundaries, `ARROW1` file format).
"""

import decimal
import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

HERE = os.path.dirname(os.path.abspath(__file__))


def write(name, table, *, rows_per_batch=None, compression=None, stream=False):
    path = os.path.join(HERE, name)
    opts = ipc.IpcWriteOptions(compression=compression) if compression else None
    sink = pa.OSFile(path, "wb")
    try:
        if stream:
            with ipc.new_stream(sink, table.schema, options=opts) as w:
                w.write_table(table, max_chunksize=rows_per_batch)
        else:
            with ipc.new_file(sink, table.schema, options=opts) as w:
                w.write_table(table, max_chunksize=rows_per_batch)
    finally:
        sink.close()
    print(f"wrote {name} ({os.path.getsize(path)} bytes)")


def golden_types():
    """Every type the mapping supports, in one file, two batches."""
    batch1 = pa.record_batch(
        {
            "b": pa.array([True, None, False], type=pa.bool_()),
            "i8": pa.array([-128, 0, 127], type=pa.int8()),
            "i16": pa.array([-32768, None, 32767], type=pa.int16()),
            "i32": pa.array([1, None, -3], type=pa.int32()),
            "i64": pa.array([2**53, None, -(2**53)], type=pa.int64()),
            "u8": pa.array([0, 255, None], type=pa.uint8()),
            "u16": pa.array([65535, None, 1], type=pa.uint16()),
            "u32": pa.array([2**32 - 1, 7, None], type=pa.uint32()),
            # Float16 -> DOUBLE (widening).
            "f16": pa.array([1.5, None, -0.25], type=pa.float16()),
            "f32": pa.array([1.25, None, -2.5], type=pa.float32()),
            "f64": pa.array([1.5, None, float("inf")], type=pa.float64()),
            "s": pa.array(["héllo", None, ""], type=pa.string()),
            "ls": pa.array(["large-1", None, "large-3"], type=pa.large_string()),
            "d32": pa.array([0, None, 19723], type=pa.date32()),
            # 19723 days = 2024-01-01; Date64 is midnight-aligned ms.
            "d64": pa.array([0, None, 19723 * 86_400_000], type=pa.date64()),
            "ts_s": pa.array([0, None, 1_700_000_000], type=pa.timestamp("s")),
            "ts_ms": pa.array(
                [0, None, 1_700_000_000_123], type=pa.timestamp("ms", tz="UTC")
            ),
            "ts_us": pa.array(
                [1, None, -1_700_000_000_000_001], type=pa.timestamp("us")
            ),
            "ts_ns": pa.array([1_000, None, -1_000], type=pa.timestamp("ns")),
            "dec": pa.array(
                [decimal.Decimal("1.2345"), None, decimal.Decimal("-0.0001")],
                type=pa.decimal128(18, 4),
            ),
            "iv": pa.array(
                [
                    pa.MonthDayNano([1, 2, 3_000]),
                    None,
                    pa.MonthDayNano([-1, -2, -1_500]),
                ],
                type=pa.month_day_nano_interval(),
            ),
        }
    )
    batch2 = pa.record_batch(
        {
            "b": pa.array([None, True], type=pa.bool_()),
            "i8": pa.array([1, -1], type=pa.int8()),
            "i16": pa.array([2, -2], type=pa.int16()),
            "i32": pa.array([4, -4], type=pa.int32()),
            "i64": pa.array([8, -8], type=pa.int64()),
            "u8": pa.array([9, 10], type=pa.uint8()),
            "u16": pa.array([11, 12], type=pa.uint16()),
            "u32": pa.array([13, 14], type=pa.uint32()),
            "f16": pa.array([2.0, 3.0], type=pa.float16()),
            "f32": pa.array([5.0, 6.0], type=pa.float32()),
            "f64": pa.array([7.0, 8.0], type=pa.float64()),
            "s": pa.array(["b2a", "b2b"], type=pa.string()),
            "ls": pa.array(["b2c", "b2d"], type=pa.large_string()),
            "d32": pa.array([1, 2], type=pa.date32()),
            "d64": pa.array([86_400_000, 2 * 86_400_000], type=pa.date64()),
            "ts_s": pa.array([10, 20], type=pa.timestamp("s")),
            "ts_ms": pa.array([30, 40], type=pa.timestamp("ms", tz="UTC")),
            "ts_us": pa.array([50, 60], type=pa.timestamp("us")),
            "ts_ns": pa.array([70, 80], type=pa.timestamp("ns")),
            "dec": pa.array(
                [decimal.Decimal("2.0000"), decimal.Decimal("3.5000")],
                type=pa.decimal128(18, 4),
            ),
            "iv": pa.array(
                [
                    pa.MonthDayNano([0, 1, 2_000]),
                    pa.MonthDayNano([2, 0, 0]),
                ],
                type=pa.month_day_nano_interval(),
            ),
        }
    )
    table = pa.Table.from_batches([batch1, batch2])
    write("golden_types.arrow", table, rows_per_batch=3)


def bulk_rows():
    """2050 rows in two batches (1234 + 816): the reader re-chunks at 2048."""
    values = list(range(2050))
    table = pa.table(
        {
            "id": pa.array(values, type=pa.int32()),
            "tag": pa.array([f"t{v % 7}" for v in values], type=pa.string()),
        }
    )
    batches = [
        table.slice(0, 1234).to_batches()[0],
        table.slice(1234, 816).to_batches()[0],
    ]
    tbl = pa.Table.from_batches(batches)
    write("golden_bulk.arrow", tbl)


def compressed():
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "txt": pa.array(["lz4", "zstd", "none"], type=pa.string()),
        }
    )
    write("golden_lz4.arrow", table, compression="lz4")
    write("golden_zstd.arrow", table, compression="zstd")


def stream_file():
    table = pa.table({"id": pa.array([1, 2], type=pa.int32())})
    write("golden_stream.arrows", table, stream=True)


def unsupported_types():
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "blob": pa.array([b"\x00\x01"], type=pa.binary()),
            "t": pa.array(
                [pa.scalar(1_000, type=pa.time64("us"))], type=pa.time64("us")
            ),
        }
    )
    write("golden_unsupported.arrow", table)


def main():
    golden_types()
    bulk_rows()
    compressed()
    stream_file()
    unsupported_types()
    print("pyarrow", pa.__version__, "python", sys.version.split()[0])


if __name__ == "__main__":
    main()
