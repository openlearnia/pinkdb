#!/usr/bin/env python3
"""Generate the committed Iceberg v2 fixtures (standard library only).

pyiceberg/fastavro are not available, so this writes the Iceberg tables by
hand: the metadata JSON, the Avro manifest list and manifest files (the Avro
object-container format, encoded directly below), and the Parquet data files
(written through the DuckDB binary so they are real Parquet with real field
ids).

Usage:

    python3 gen.py [--duckdb /tmp/duckdb-2.0.0/duckdb] [--out fixtures]

Data files carry Iceberg field ids via DuckDB's `FIELD_IDS` COPY option, so
the `evolution` table's rename is resolved by field id (not by name) the way
a real Iceberg reader does it. Data-file paths inside the manifests are
**relative to the table root**, which is what makes the fixtures relocatable;
readers resolve them against the location they were given (`allow_moved_paths`
in DuckDB's spelling).

Tables:

  simple/       two snapshots (append)  -> time travel by id and timestamp
  deletes/      a positional delete file over a four-row data file
  evolution/    id 2 renamed name->full_name plus a nullable id-3 column added
  partitioned/  identity partition on `region`, one data file per partition
"""

import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
import zlib

# --------------------------------------------------------------------------
# Avro object-container writer (the subset Iceberg manifests use)
# --------------------------------------------------------------------------


def zigzag(n):
    """Zig-zag a signed integer into an unsigned varint value."""
    return (n << 1) ^ (n >> 63)


def write_varint(u):
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        if u:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def write_long(n):
    return write_varint(zigzag(n))


def _is_null(schema):
    return schema == "null"


def _matches(schema, value):
    if isinstance(schema, list):
        return any(_matches(branch, value) for branch in schema)
    if isinstance(schema, dict):
        kind = schema.get("type")
        if kind == "record":
            return isinstance(value, dict)
        if kind == "array" or kind == "map":
            return isinstance(value, list) or isinstance(value, dict)
        return _matches(kind, value)
    return {
        "null": value is None,
        "boolean": isinstance(value, bool),
        "int": isinstance(value, int) and not isinstance(value, bool),
        "long": isinstance(value, int) and not isinstance(value, bool),
        "float": isinstance(value, float),
        "double": isinstance(value, float) or isinstance(value, int),
        "string": isinstance(value, str),
        "bytes": isinstance(value, (bytes, bytearray)),
    }[schema]


def encode(schema, value):
    """Encode one value against a writer schema (records/unions/arrays/maps)."""
    if isinstance(schema, list):
        # A union: the first branch that matches, as its branch index.
        for index, branch in enumerate(schema):
            if _matches(branch, value):
                return write_long(index) + encode(branch, value)
        raise ValueError(f"no union branch for {value!r} in {schema!r}")
    if isinstance(schema, dict):
        kind = schema.get("type")
        if kind == "record":
            out = bytearray()
            for field in schema["fields"]:
                out += encode(field["type"], value[field["name"]])
            return bytes(out)
        if kind == "array":
            # An empty array is the single 0 block that terminates it; adding
            # another terminator would leave a byte readers call trailing data.
            if not value:
                return write_long(0)
            out = bytearray(write_long(len(value)))
            for item in value:
                out += encode(schema["items"], item)
            return bytes(out) + write_long(0)
        if kind == "map":
            if not value:
                return write_long(0)
            out = bytearray(write_long(len(value)))
            for key, item in value.items():
                out += encode("string", key)
                out += encode(schema["values"], item)
            return bytes(out) + write_long(0)
        return encode(kind, value)
    if schema == "null":
        return b""
    if schema == "boolean":
        return b"\x01" if value else b"\x00"
    if schema in ("int", "long"):
        return write_long(value)
    if schema == "float":
        return struct.pack("<f", value)
    if schema == "double":
        return struct.pack("<d", value)
    if schema == "string":
        raw = value.encode("utf-8")
        return write_long(len(raw)) + raw
    if schema == "bytes":
        return write_long(len(value)) + bytes(value)
    if isinstance(schema, int):
        # A fixed(n) named by size, or a structured name this writer does not
        # need (the manifests below use only the primitives above).
        return bytes(value)
    raise ValueError(f"unsupported writer schema {schema!r}")


def deflate(raw):
    compressor = zlib.compressobj(9, zlib.DEFLATED, -15)  # raw deflate
    return compressor.compress(raw) + compressor.flush()


def write_avro(path, schema, records, codec="null", sync=b"icebergfixtures!", extra=None):
    """Write one Avro object-container file holding `records`."""
    assert len(sync) == 16
    meta = {
        "avro.schema": json.dumps(schema, separators=(",", ":")).encode(),
        "avro.codec": codec.encode(),
    }
    meta.update(extra or {})
    body = bytearray()
    body += b"Obj\x01"
    body += write_long(len(meta))
    for key, value in meta.items():
        body += encode("string", key)
        body += encode("bytes", value)
    body += write_long(0)
    body += sync
    block = b"".join(encode(schema, record) for record in records)
    body += write_long(len(records))
    payload = deflate(block) if codec == "deflate" else block
    body += write_long(len(payload))
    body += payload
    body += sync
    with open(path, "wb") as f:
        f.write(body)


# --------------------------------------------------------------------------
# Iceberg manifest Avro schemas (the field ids are Iceberg's)
# --------------------------------------------------------------------------


def manifest_list_schema(partition_fields):
    partitions = {
        "type": "record",
        "name": "r508",
        "fields": [
            {"name": "contains_null", "type": "boolean", "field-id": 509},
            {"name": "contains_nan", "type": ["null", "boolean"], "default": None, "field-id": 518},
            {"name": "lower_bound", "type": ["null", "bytes"], "default": None, "field-id": 510},
            {"name": "upper_bound", "type": ["null", "bytes"], "default": None, "field-id": 511},
        ],
    }
    del partition_fields
    return {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_files_count", "type": "int", "field-id": 504},
            {"name": "existing_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
            {
                "name": "partitions",
                "type": [
                    "null",
                    {"type": "array", "items": partitions, "element-id": 508},
                ],
                "default": None,
                "field-id": 507,
            },
            {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 519},
        ],
    }


def kv_schema(name, field_id, key_id, value_id, key_type="int", value_type="long"):
    """One Iceberg map-encoded field: an array of key/value records.

    The nested key/value fields carry Iceberg's element ids (`k117_v118` for
    `column_sizes`, and so on); readers resolve them by id, not by name.
    """
    return {
        "name": name,
        "type": [
            "null",
            {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": f"k{key_id}_v{value_id}",
                    "fields": [
                        {"name": "key", "type": key_type, "field-id": key_id},
                        {"name": "value", "type": value_type, "field-id": value_id},
                    ],
                },
                "logicalType": "map",
            },
        ],
        "default": None,
        "field-id": field_id,
    }


def manifest_schema(partition_fields):
    """`partition_fields` is a list of (name, avro type, field id)."""
    part = {
        "type": "record",
        "name": "r102",
        "fields": [
            {"name": name, "type": avro, "default": None, "field-id": fid}
            for (name, avro, fid) in partition_fields
        ],
    }
    data_file = {
        "type": "record",
        "name": "r2",
        "fields": [
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {"name": "partition", "type": part, "field-id": 102},
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            kv_schema("column_sizes", 108, 117, 118),
            kv_schema("value_counts", 109, 119, 120),
            kv_schema("null_value_counts", 110, 121, 122),
            kv_schema("nan_value_counts", 137, 138, 139),
            kv_schema("lower_bounds", 125, 126, 127, value_type="bytes"),
            kv_schema("upper_bounds", 128, 129, 130, value_type="bytes"),
            {"name": "key_metadata", "type": ["null", "bytes"], "default": None, "field-id": 131},
            {
                "name": "split_offsets",
                "type": ["null", {"type": "array", "items": "long", "element-id": 133}],
                "default": None,
                "field-id": 132,
            },
            {
                "name": "equality_ids",
                "type": ["null", {"type": "array", "items": "int", "element-id": 136}],
                "default": None,
                "field-id": 135,
            },
            {"name": "sort_order_id", "type": ["null", "int"], "default": None, "field-id": 140},
            {"name": "content", "type": "int", "field-id": 134},
        ],
    }
    return {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": None, "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "default": None, "field-id": 3},
            {
                "name": "file_sequence_number",
                "type": ["null", "long"],
                "default": None,
                "field-id": 4,
            },
            {"name": "data_file", "type": data_file, "field-id": 2},
        ],
    }


def write_manifest(path, schema, records, table_schema, spec, codec="null", content="data"):
    """Write one manifest Avro file with Iceberg's required metadata."""
    extra = {
        "schema": json.dumps(table_schema, separators=(",", ":")).encode(),
        "schema-id": str(table_schema["schema-id"]).encode(),
        "partition-spec-id": str(spec["spec-id"]).encode(),
        "partition-spec": json.dumps(spec["fields"], separators=(",", ":")).encode(),
        "format-version": b"2",
        "content": content.encode(),
    }
    write_avro(path, schema, records, codec=codec, extra=extra)


def kv(pairs):
    """An Iceberg map-encoded field: a list of {key, value} records."""
    return [{"key": key, "value": value} for key, value in pairs]


# Byte-serialised bounds, the Iceberg single-value encoding: primitives are
# little-endian, strings are UTF-8.
def bound_long(n):
    return struct.pack("<q", n)


def bound_string(s):
    return s.encode("utf-8")


def bound_double(x):
    return struct.pack("<d", x)


# --------------------------------------------------------------------------
# Parquet data files, written through DuckDB with Iceberg field ids
# --------------------------------------------------------------------------


def run_duckdb(duckdb, sql):
    script = "-c"
    result = subprocess.run(
        [duckdb, script, sql], capture_output=True, text=True
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"duckdb failed: {sql}")


def copy_parquet(duckdb, path, select_sql, field_ids):
    ids = ", ".join(f"{name}: {fid}" for name, fid in field_ids.items())
    run_duckdb(
        duckdb,
        f"COPY ({select_sql}) TO '{path}' (FORMAT parquet, FIELD_IDS {{{ids}}});",
    )
    return os.path.getsize(path)


# --------------------------------------------------------------------------
# Table metadata
# --------------------------------------------------------------------------


def field(fid, name, type_, required=False):
    return {"id": fid, "name": name, "required": required, "type": type_}


def write_json(path, value):
    with open(path, "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")


def manifest_file_entry(path, length, snapshot_id, added, existing, rows_added,
                        rows_existing, sequence, partitions=None, content=0):
    return {
        "manifest_path": path,
        "manifest_length": length,
        "partition_spec_id": 0,
        "content": content,
        "sequence_number": sequence,
        "min_sequence_number": sequence,
        "added_snapshot_id": snapshot_id,
        "added_files_count": added,
        "existing_files_count": existing,
        "deleted_files_count": 0,
        "added_rows_count": rows_added,
        "existing_rows_count": rows_existing,
        "deleted_rows_count": 0,
        "partitions": partitions,
        "key_metadata": None,
    }


def data_file_entry(path, size, rows, *, status=1, content=0, partition=None,
                    sequence=1, snapshot_id=1001, counts=None, bounds=None,
                    equality=None):
    counts = counts or {}
    bounds = bounds or {}
    part = partition or {}
    return {
        "status": status,
        "snapshot_id": snapshot_id,
        "sequence_number": sequence,
        "file_sequence_number": sequence,
        "data_file": {
            "content": content,
            "file_path": path,
            "file_format": "PARQUET",
            "partition": part,
            "record_count": rows,
            "file_size_in_bytes": size,
            "column_sizes": kv((fid, size) for fid in counts),
            "value_counts": kv(counts.items()),
            "null_value_counts": kv((fid, 0) for fid in counts),
            "nan_value_counts": None,
            "lower_bounds": kv((fid, lo) for fid, (lo, _) in bounds.items()),
            "upper_bounds": kv((fid, hi) for fid, (_, hi) in bounds.items()),
            "key_metadata": None,
            "split_offsets": None,
            "equality_ids": equality,
            "sort_order_id": 0,
        },
    }


def table_metadata(location, uuid, schemas, current_schema, snapshots,
                   current_snapshot, specs, last_column_id, last_partition_id,
                   metadata_log=None):
    return {
        "format-version": 2,
        "table-uuid": uuid,
        "location": location,
        "last-sequence-number": max((s["sequence-number"] for s in snapshots), default=0),
        "last-updated-ms": max((s["timestamp-ms"] for s in snapshots), default=0),
        "last-column-id": last_column_id,
        "schemas": schemas,
        "current-schema-id": current_schema,
        "partition-specs": specs,
        "default-spec-id": 0,
        "last-partition-id": last_partition_id,
        "properties": {},
        "current-snapshot-id": current_snapshot,
        "snapshots": snapshots,
        "snapshot-log": [
            {"timestamp-ms": s["timestamp-ms"], "snapshot-id": s["snapshot-id"]}
            for s in snapshots
        ],
        "metadata-log": metadata_log or [],
    }


def snapshot(snapshot_id, sequence, timestamp_ms, manifest_list, schema_id,
             parent=None, operation="append"):
    snap = {
        "snapshot-id": snapshot_id,
        "sequence-number": sequence,
        "timestamp-ms": timestamp_ms,
        "summary": {"operation": operation},
        "manifest-list": manifest_list,
        "schema-id": schema_id,
    }
    if parent is not None:
        snap["parent-snapshot-id"] = parent
    return snap


# --------------------------------------------------------------------------
# The four tables
# --------------------------------------------------------------------------

EMPTY_SPEC = {"spec-id": 0, "fields": []}

SIMPLE_SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [field(1, "id", "long", True), field(2, "name", "string")],
}


def table_simple(duckdb, root):
    os.makedirs(f"{root}/data", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    ids = {"id": 1, "name": 2}
    s1 = copy_parquet(duckdb, f"{root}/data/data-1.parquet",
                      "SELECT * FROM (VALUES (1::BIGINT, 'ann'), (2::BIGINT, 'bob')) t(id, name)", ids)
    s2 = copy_parquet(duckdb, f"{root}/data/data-2.parquet",
                      "SELECT * FROM (VALUES (3::BIGINT, 'cy')) t(id, name)", ids)
    ms = manifest_schema([])
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/data-1.parquet", s1, 2,
                        counts={1: 2, 2: 2},
                        bounds={1: (bound_long(1), bound_long(2)),
                                2: (bound_string("ann"), bound_string("bob"))}),
    ], SIMPLE_SCHEMA, EMPTY_SPEC, codec="deflate")
    write_manifest(f"{root}/metadata/m2.avro", ms, [
        data_file_entry("data/data-2.parquet", s2, 1, sequence=2, snapshot_id=1002,
                        counts={1: 1, 2: 1},
                        bounds={1: (bound_long(3), bound_long(3)),
                                2: (bound_string("cy"), bound_string("cy"))}),
    ], SIMPLE_SCHEMA, EMPTY_SPEC)
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 1, 0, 2, 0, 1)])
    write_avro(f"{root}/metadata/snap-1002.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1002, 0, 1, 0, 2, 1),
        manifest_file_entry("metadata/m2.avro", os.path.getsize(f"{root}/metadata/m2.avro"),
                            1002, 1, 0, 1, 0, 2)])
    snaps = [
        snapshot(1001, 1, 1700000000000, "metadata/snap-1001.avro", 0),
        snapshot(1002, 2, 1700000100000, "metadata/snap-1002.avro", 0, parent=1001),
    ]
    latest = table_metadata("fixtures/simple", "11111111-1111-1111-1111-111111111111",
                            [SIMPLE_SCHEMA], 0, snaps, 1002, [
                                {"spec-id": 0, "fields": []}], 2, 999)
    first = table_metadata("fixtures/simple", "11111111-1111-1111-1111-111111111111",
                           [SIMPLE_SCHEMA], 0, snaps[:1], 1001, [
                               {"spec-id": 0, "fields": []}], 2, 999)
    write_json(f"{root}/metadata/v1.metadata.json", first)
    write_json(f"{root}/metadata/v2.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("2")


def table_deletes(duckdb, root):
    os.makedirs(f"{root}/data", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    ids = {"id": 1, "name": 2}
    data = copy_parquet(duckdb, f"{root}/data/data-1.parquet",
                        "SELECT * FROM (VALUES (1::BIGINT, 'ann'), (2::BIGINT, 'bob'),"
                        " (3::BIGINT, 'cy'), (4::BIGINT, 'dee')) t(id, name)", ids)
    # A positional delete file: file_path + pos, as Iceberg v2 defines it.
    deletes = copy_parquet(
        duckdb, f"{root}/data/pos-delete.parquet",
        "SELECT * FROM (VALUES ('data/data-1.parquet', 1::BIGINT),"
        " ('data/data-1.parquet', 3::BIGINT)) t(file_path, pos)",
        {"file_path": 2147483546, "pos": 2147483545})
    ms = manifest_schema([])
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [field(1, "id", "long", True), field(2, "name", "string")],
    }
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/data-1.parquet", data, 4,
                        counts={1: 4, 2: 4},
                        bounds={1: (bound_long(1), bound_long(4)),
                                2: (bound_string("ann"), bound_string("dee"))}),
    ], schema, EMPTY_SPEC)
    write_manifest(f"{root}/metadata/m2.avro", ms, [
        data_file_entry("data/pos-delete.parquet", deletes, 2, content=1,
                        sequence=2, snapshot_id=1002, counts={2147483546: 2, 2147483545: 2}),
    ], schema, EMPTY_SPEC, content="deletes")
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 1, 0, 4, 0, 1)])
    write_avro(f"{root}/metadata/snap-1002.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1002, 0, 1, 0, 4, 1),
        manifest_file_entry("metadata/m2.avro", os.path.getsize(f"{root}/metadata/m2.avro"),
                            1002, 1, 0, 2, 0, 2, content=1)])
    snaps = [
        snapshot(1001, 1, 1700000000000, "metadata/snap-1001.avro", 0),
        snapshot(1002, 2, 1700000100000, "metadata/snap-1002.avro", 0, parent=1001,
                 operation="delete"),
    ]
    first = table_metadata("fixtures/deletes", "22222222-2222-2222-2222-222222222222",
                           [schema], 0, snaps[:1], 1001, [
                               {"spec-id": 0, "fields": []}], 2, 999)
    latest = table_metadata("fixtures/deletes", "22222222-2222-2222-2222-222222222222",
                            [schema], 0, snaps, 1002, [
                                {"spec-id": 0, "fields": []}], 2, 999)
    write_json(f"{root}/metadata/v1.metadata.json", first)
    write_json(f"{root}/metadata/v2.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("2")


def table_evolution(duckdb, root):
    os.makedirs(f"{root}/data", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    v1 = copy_parquet(duckdb, f"{root}/data/data-1.parquet",
                      "SELECT * FROM (VALUES (1::BIGINT, 'ann'), (2::BIGINT, 'bob'),"
                      " (3::BIGINT, 'cy')) t(id, name)", {"id": 1, "name": 2})
    v2 = copy_parquet(duckdb, f"{root}/data/data-2.parquet",
                      "SELECT * FROM (VALUES (4::BIGINT, 'dee', 40::INTEGER)) t(id, full_name, age)",
                      {"id": 1, "full_name": 2, "age": 3})
    schema0 = {
        "type": "struct",
        "schema-id": 0,
        "fields": [field(1, "id", "long", True), field(2, "name", "string")],
    }
    schema1 = {
        "type": "struct",
        "schema-id": 1,
        "fields": [field(1, "id", "long", True), field(2, "full_name", "string"),
                   field(3, "age", "int")],
    }
    ms = manifest_schema([])
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/data-1.parquet", v1, 3,
                        counts={1: 3, 2: 3},
                        bounds={1: (bound_long(1), bound_long(3)),
                                2: (bound_string("ann"), bound_string("cy"))}),
    ], schema0, EMPTY_SPEC)
    write_manifest(f"{root}/metadata/m2.avro", ms, [
        data_file_entry("data/data-2.parquet", v2, 1, sequence=2, snapshot_id=1002,
                        counts={1: 1, 2: 1, 3: 1},
                        bounds={1: (bound_long(4), bound_long(4)),
                                2: (bound_string("dee"), bound_string("dee")),
                                3: (bound_long(40), bound_long(40))}),
    ], schema1, EMPTY_SPEC)
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 1, 0, 3, 0, 1)])
    write_avro(f"{root}/metadata/snap-1002.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1002, 0, 1, 0, 3, 1),
        manifest_file_entry("metadata/m2.avro", os.path.getsize(f"{root}/metadata/m2.avro"),
                            1002, 1, 0, 1, 0, 2)])
    snaps = [
        snapshot(1001, 1, 1700000000000, "metadata/snap-1001.avro", 0),
        snapshot(1002, 2, 1700000100000, "metadata/snap-1002.avro", 1, parent=1001,
                 operation="replace"),
    ]
    first = table_metadata("fixtures/evolution", "33333333-3333-3333-3333-333333333333",
                           [schema0], 0, snaps[:1], 1001, [
                               {"spec-id": 0, "fields": []}], 3, 999)
    latest = table_metadata("fixtures/evolution", "33333333-3333-3333-3333-333333333333",
                            [schema0, schema1], 1, snaps, 1002, [
                                {"spec-id": 0, "fields": []}], 3, 999)
    write_json(f"{root}/metadata/v1.metadata.json", first)
    write_json(f"{root}/metadata/v2.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("2")


def table_partitioned(duckdb, root):
    os.makedirs(f"{root}/data/region=us", exist_ok=True)
    os.makedirs(f"{root}/data/region=eu", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    ids = {"id": 1, "region": 2, "value": 3}
    us = copy_parquet(duckdb, f"{root}/data/region=us/data.parquet",
                      "SELECT * FROM (VALUES (1::BIGINT, 'us', 1.5::DOUBLE),"
                      " (2::BIGINT, 'us', 2.5::DOUBLE)) t(id, region, value)", ids)
    eu = copy_parquet(duckdb, f"{root}/data/region=eu/data.parquet",
                      "SELECT * FROM (VALUES (3::BIGINT, 'eu', 3.5::DOUBLE),"
                      " (4::BIGINT, 'eu', 4.5::DOUBLE)) t(id, region, value)", ids)
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [field(1, "id", "long", True), field(2, "region", "string"),
                   field(3, "value", "double")],
    }
    ms = manifest_schema([("region", ["null", "string"], 1000)])
    spec = {"spec-id": 0, "fields": [
        {"name": "region", "source-id": 2, "field-id": 1000, "transform": "identity"}]}
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/region=eu/data.parquet", eu, 2,
                        partition={"region": "eu"},
                        counts={1: 2, 2: 2, 3: 2},
                        bounds={1: (bound_long(3), bound_long(4)),
                                2: (bound_string("eu"), bound_string("eu")),
                                3: (bound_double(3.5), bound_double(4.5))}),
        data_file_entry("data/region=us/data.parquet", us, 2,
                        partition={"region": "us"},
                        counts={1: 2, 2: 2, 3: 2},
                        bounds={1: (bound_long(1), bound_long(2)),
                                2: (bound_string("us"), bound_string("us")),
                                3: (bound_double(1.5), bound_double(2.5))}),
    ], schema, spec)
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 2, 0, 4, 0, 1,
                            partitions=[{"contains_null": False, "contains_nan": False,
                                         "lower_bound": bound_string("eu"),
                                         "upper_bound": bound_string("us")}])])
    snaps = [snapshot(1001, 1, 1700000000000, "metadata/snap-1001.avro", 0)]
    latest = table_metadata("fixtures/partitioned", "44444444-4444-4444-4444-444444444444",
                            [schema], 0, snaps, 1001, [spec], 3, 1000)
    write_json(f"{root}/metadata/v1.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("1")


def table_seqorder(duckdb, root):
    """A delete file older than the data file it names: the rule says it does
    not apply (the file was rewritten under the same path afterwards)."""
    os.makedirs(f"{root}/data", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    ids = {"id": 1, "name": 2}
    data = copy_parquet(duckdb, f"{root}/data/data-1.parquet",
                        "SELECT * FROM (VALUES (1::BIGINT, 'ann'), (2::BIGINT, 'bob'))"
                        " t(id, name)", ids)
    deletes = copy_parquet(duckdb, f"{root}/data/pos-delete.parquet",
                           "SELECT * FROM (VALUES ('data/data-1.parquet', 0::BIGINT))"
                           " t(file_path, pos)",
                           {"file_path": 2147483546, "pos": 2147483545})
    ms = manifest_schema([])
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [field(1, "id", "long", True), field(2, "name", "string")],
    }
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/data-1.parquet", data, 2, sequence=2, snapshot_id=1001,
                        counts={1: 2, 2: 2},
                        bounds={1: (bound_long(1), bound_long(2)),
                                2: (bound_string("ann"), bound_string("bob"))}),
    ], schema, EMPTY_SPEC)
    write_manifest(f"{root}/metadata/m2.avro", ms, [
        data_file_entry("data/pos-delete.parquet", deletes, 1, content=1, sequence=1,
                        snapshot_id=1001, counts={2147483546: 1, 2147483545: 1}),
    ], schema, EMPTY_SPEC, content="deletes")
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 1, 0, 2, 0, 2),
        manifest_file_entry("metadata/m2.avro", os.path.getsize(f"{root}/metadata/m2.avro"),
                            1001, 1, 0, 1, 0, 1, content=1)])
    snaps = [snapshot(1001, 2, 1700000000000, "metadata/snap-1001.avro", 0)]
    latest = table_metadata("fixtures/seqorder", "66666666-6666-6666-6666-666666666666",
                            [schema], 0, snaps, 1001, [
                                {"spec-id": 0, "fields": []}], 2, 999)
    write_json(f"{root}/metadata/v1.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("1")


def table_equality(duckdb, root):
    """An equality delete: pinkdb refuses these, DuckDB applies them."""
    os.makedirs(f"{root}/data", exist_ok=True)
    os.makedirs(f"{root}/metadata", exist_ok=True)
    ids = {"id": 1, "name": 2}
    data = copy_parquet(duckdb, f"{root}/data/data-1.parquet",
                        "SELECT * FROM (VALUES (1::BIGINT, 'ann'), (2::BIGINT, 'bob'))"
                        " t(id, name)", ids)
    deletes = copy_parquet(duckdb, f"{root}/data/eq-delete.parquet",
                           "SELECT 'bob'::VARCHAR AS name", {"name": 2})
    ms = manifest_schema([])
    schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [field(1, "id", "long", True), field(2, "name", "string")],
    }
    write_manifest(f"{root}/metadata/m1.avro", ms, [
        data_file_entry("data/data-1.parquet", data, 2,
                        counts={1: 2, 2: 2},
                        bounds={1: (bound_long(1), bound_long(2)),
                                2: (bound_string("ann"), bound_string("bob"))}),
    ], schema, EMPTY_SPEC)
    write_manifest(f"{root}/metadata/m2.avro", ms, [
        data_file_entry("data/eq-delete.parquet", deletes, 1, content=2,
                        sequence=2, snapshot_id=1002, counts={2: 1},
                        equality=[2]),
    ], schema, EMPTY_SPEC, content="deletes")
    ml = manifest_list_schema([])
    write_avro(f"{root}/metadata/snap-1001.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1001, 1, 0, 2, 0, 1)])
    write_avro(f"{root}/metadata/snap-1002.avro", ml, [
        manifest_file_entry("metadata/m1.avro", os.path.getsize(f"{root}/metadata/m1.avro"),
                            1002, 0, 1, 0, 2, 1),
        manifest_file_entry("metadata/m2.avro", os.path.getsize(f"{root}/metadata/m2.avro"),
                            1002, 1, 0, 1, 0, 2, content=1)])
    snaps = [
        snapshot(1001, 1, 1700000000000, "metadata/snap-1001.avro", 0),
        snapshot(1002, 2, 1700000100000, "metadata/snap-1002.avro", 0, parent=1001,
                 operation="delete"),
    ]
    first = table_metadata("fixtures/equality", "55555555-5555-5555-5555-555555555555",
                           [schema], 0, snaps[:1], 1001, [
                               {"spec-id": 0, "fields": []}], 2, 999)
    latest = table_metadata("fixtures/equality", "55555555-5555-5555-5555-555555555555",
                            [schema], 0, snaps, 1002, [
                                {"spec-id": 0, "fields": []}], 2, 999)
    write_json(f"{root}/metadata/v1.metadata.json", first)
    write_json(f"{root}/metadata/v2.metadata.json", latest)
    with open(f"{root}/metadata/version-hint.text", "w") as f:
        f.write("2")


TABLES = {
    "simple": table_simple,
    "equality": table_equality,
    "seqorder": table_seqorder,
    "deletes": table_deletes,
    "evolution": table_evolution,
    "partitioned": table_partitioned,
}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default="/tmp/duckdb-2.0.0/duckdb")
    parser.add_argument("--out", default=here, help="output root (default: this dir)")
    args = parser.parse_args()
    for name, build in TABLES.items():
        root = os.path.join(args.out, name)
        if os.path.exists(root):
            shutil.rmtree(root)
        os.makedirs(root)
        build(args.duckdb, root)
        print(f"wrote {root}")


if __name__ == "__main__":
    main()
