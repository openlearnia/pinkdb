//! Arrow C Data Interface round trips: import our exported schema/array with
//! arrow-rs and compare every value with the SQL result.

use std::ffi::{CStr, CString};
use std::ptr;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, FixedSizeListArray,
    Float32Array, Float64Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use pinkdb_ffi::*;

fn exec(h: *mut PinkHandle, sql: &str) -> *mut PinkResult {
    let c = CString::new(sql).unwrap();
    let r = pinkdb_execute(h, c.as_ptr(), sql.len());
    assert!(!r.is_null(), "execute failed: {}", last_error());
    r
}

fn last_error() -> String {
    let p = pinkdb_last_error();
    if p.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

fn export_schema(result: *const PinkResult) -> FFI_ArrowSchema {
    let mut schema = ArrowSchema::empty();
    assert_eq!(pinkdb_result_arrow_schema(result, 0, &mut schema), 1);
    assert!(schema.release.is_some());
    // Move the ABI struct into arrow-rs's owner; it calls the same release.
    unsafe { ptr::read(&schema as *const ArrowSchema as *const FFI_ArrowSchema) }
}

fn fetch(result: *mut PinkResult) -> Option<ArrowArray> {
    let mut array = ArrowArray::empty();
    assert_eq!(pinkdb_result_fetch_arrow(result, 0, &mut array), 1);
    if array.release.is_none() {
        None
    } else {
        Some(array)
    }
}

fn import(array: ArrowArray, schema: &FFI_ArrowSchema) -> arrow::array::ArrayRef {
    let ffi = unsafe { ptr::read(&array as *const ArrowArray as *const FFI_ArrowArray) };
    arrow::array::make_array(unsafe { from_ffi(ffi, schema) }.unwrap())
}

fn text(value: &PinkValue) -> String {
    let bytes = unsafe { std::slice::from_raw_parts(value.text_ptr as *const u8, value.text_len) };
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn scalar_types_round_trip_with_nulls() {
    let h = pinkdb_open_memory();
    let setup = exec(
        h,
        "CREATE TABLE t (id INTEGER, b BOOLEAN, i INTEGER, l BIGINT, g DOUBLE,
                          s VARCHAR, dt DATE, ts TIMESTAMP, dec DECIMAL(18,3),
                          iv INTERVAL, hi HUGEINT, uhi UHUGEINT, u UUID, v VARIANT);
         INSERT INTO t VALUES
           (1, true, -7, 9007199254740993, 1.25, 'héllo', DATE '2020-01-02',
            TIMESTAMP '2020-01-02 03:04:05.123456', 123.456,
            INTERVAL '1 month 2 days 03:04:05.000006',
            '-99999999999999999999999999999999999999',
            '99999999999999999999999999999999999999',
            '12345678-1234-5678-9abc-def012345678',
            CAST('{\"a\":1}' AS VARIANT)),
           (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL);",
    );
    pinkdb_result_free(setup);
    let r = exec(h, "SELECT * FROM t ORDER BY id;");
    let schema = export_schema(r);
    let array = fetch(r).expect("one chunk");
    let root = import(array, &schema);
    let root = root.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(root.num_columns(), 14);
    assert_eq!(root.len(), 2);
    assert_eq!(
        root.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        2
    );

    let b = root
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!((b.value(0), b.is_null(1)), (true, true));
    let i = root
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!((i.value(0), i.is_null(1)), (-7, true));
    let l = root
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((l.value(0), l.is_null(1)), (9_007_199_254_740_993, true));
    let g = root
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((g.value(0), g.is_null(1)), (1.25, true));
    let s = root
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!((s.value(0), s.is_null(1)), ("héllo", true));
    let dt = root
        .column(6)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    let v = pinkdb_result_value(r, 0, 0, 6);
    assert_eq!((dt.value(0), dt.is_null(1)), (v.int64 as i32, true));
    let ts = root
        .column(7)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let v = pinkdb_result_value(r, 0, 0, 7);
    assert_eq!((ts.value(0), ts.is_null(1)), (v.int64, true));
    let dec = root
        .column(8)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(dec.value(0), 123_456);
    assert!(dec.is_null(1));
    let iv = root
        .column(9)
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .unwrap();
    let iv = iv.value(0);
    assert_eq!(
        (iv.months, iv.days, iv.nanoseconds),
        (1, 2, 11_045_000_006_000)
    );
    assert!(root.column(9).is_null(1));
    let hi = root
        .column(10)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let uhi = root
        .column(11)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(hi.value(0), -(10i128.pow(38) - 1));
    assert_eq!(uhi.value(0), 10i128.pow(38) - 1);
    assert!(hi.is_null(1) && uhi.is_null(1));
    let uuid = root
        .column(12)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        uuid.value(0),
        &[
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34,
            0x56, 0x78
        ]
    );
    assert!(uuid.is_null(1));
    let variant = root
        .column(13)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(variant.value(0), r#"{"a":1}"#);
    assert!(variant.is_null(1));

    // The v1 cell view still agrees for the text-shaped columns.
    assert_eq!(text(&pinkdb_result_value(r, 0, 0, 5)), "héllo");
    assert_eq!(
        text(&pinkdb_result_value(r, 0, 0, 10)),
        "-99999999999999999999999999999999999999"
    );
    assert_eq!(text(&pinkdb_result_value(r, 0, 0, 13)), r#"{"a":1}"#);

    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn multi_chunk_result_streams_arrow_arrays() {
    let h = pinkdb_open_memory();
    let r = exec(
        h,
        "SELECT i, CAST(CASE WHEN i % 7 = 0 THEN NULL ELSE i END AS VARCHAR) AS s
         FROM range(0, 5000) r(i);",
    );
    let schema = export_schema(r);
    let mut seen = 0;
    let mut chunks = 0;
    while let Some(array) = fetch(r) {
        let root = import(array, &schema);
        let root = root.as_any().downcast_ref::<StructArray>().unwrap();
        let ints = root
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let strings = root
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        chunks += 1;
        for row in 0..root.len() {
            let value = seen + row;
            assert_eq!(ints.value(row), value as i64);
            if value % 7 == 0 {
                assert!(strings.is_null(row));
            } else {
                assert_eq!(strings.value(row), value.to_string());
            }
        }
        seen += root.len();
    }
    assert_eq!(seen, 5000);
    assert_eq!(chunks, 3, "2048-row chunks: 2048 + 2048 + 904");
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn empty_result_has_a_schema_and_no_array() {
    let h = pinkdb_open_memory();
    let r = exec(h, "SELECT 1::INTEGER AS i WHERE false;");
    let schema = export_schema(r);
    assert_eq!(schema.children().count(), 1);
    assert_eq!(schema.child(0).format(), "i");
    assert!(fetch(r).is_none());
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn arrays_export_fixed_size_lists() {
    let h = pinkdb_open_memory();
    let r = exec(
        h,
        "SELECT [1.5, 2.5]::DOUBLE[2] AS d,
                [3.5, 4.5]::FLOAT[2] AS f, CAST(NULL AS DOUBLE[2]) AS n;",
    );
    let schema = export_schema(r);
    let array = fetch(r).expect("one chunk");
    let root = import(array, &schema);
    let root = root.as_any().downcast_ref::<StructArray>().unwrap();
    let d = root
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    let values = d.value(0);
    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!((values.value(0), values.value(1)), (1.5, 2.5));
    let f = root
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .value(0);
    let f = f.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!((f.value(0), f.value(1)), (3.5, 4.5));
    assert!(root.column(2).is_null(0));
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn decimals_keep_precision_and_scale() {
    let h = pinkdb_open_memory();
    let r = exec(
        h,
        "SELECT CAST('1.2' AS DECIMAL(10,1)) AS a,
                CAST('-123.456' AS DECIMAL(18,3)) AS b,
                CAST('0.00001' AS DECIMAL(5,5)) AS c,
                CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS d;",
    );
    let schema = export_schema(r);
    let array = fetch(r).expect("one chunk");
    let root = import(array, &schema);
    let root = root.as_any().downcast_ref::<StructArray>().unwrap();
    let expected = [(10, 1, 12), (18, 3, -123_456), (5, 5, 1)];
    for (column, (precision, scale, value)) in expected.into_iter().enumerate() {
        let array = root
            .column(column)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            array.data_type(),
            &arrow::datatypes::DataType::Decimal128(precision, scale)
        );
        assert_eq!(array.value(0), value);
    }
    let d = root
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        d.data_type(),
        &arrow::datatypes::DataType::Decimal128(38, 0)
    );
    assert_eq!(
        d.value(0).to_string(),
        "99999999999999999999999999999999999999"
    );
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn hugeint_exports_as_decimal128_up_to_38_digits() {
    let h = pinkdb_open_memory();
    let r = exec(
        h,
        "SELECT '99999999999999999999999999999999999999'::HUGEINT AS max,
                '-99999999999999999999999999999999999999'::HUGEINT AS min,
                '99999999999999999999999999999999999999'::UHUGEINT AS umax,
                CAST(NULL AS HUGEINT) AS n,
                sum(x) AS total FROM (VALUES (1), (2), (3)) v(x);",
    );
    let schema = export_schema(r);
    let array = fetch(r).expect("one chunk");
    let root = import(array, &schema);
    let root = root.as_any().downcast_ref::<StructArray>().unwrap();
    let col = |i: usize| {
        root.column(i)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .clone()
    };
    let limit = 10i128.pow(38) - 1;
    assert_eq!(col(0).value(0), limit);
    assert_eq!(col(1).value(0), -limit);
    assert_eq!(col(2).value(0), limit);
    assert!(root.column(3).is_null(0));
    // An integer SUM is HUGEINT: it must reach Arrow as a number.
    assert_eq!(col(4).value(0), 6);
    assert_eq!(col(4).precision(), 38);
    assert_eq!(col(4).scale(), 0);
    pinkdb_result_free(r);
    pinkdb_close(h);
}

#[test]
fn hugeint_past_38_digits_fails_the_export() {
    let h = pinkdb_open_memory();
    for sql in [
        "SELECT '170141183460469231731687303715884105727'::HUGEINT AS v;",
        "SELECT '-100000000000000000000000000000000000000'::HUGEINT AS v;",
        "SELECT '340282366920938463463374607431768211455'::UHUGEINT AS v;",
    ] {
        let r = exec(h, sql);
        let mut array = ArrowArray::empty();
        assert_eq!(pinkdb_result_fetch_arrow(r, 0, &mut array), 0, "{sql}");
        assert!(array.release.is_none());
        assert!(last_error().contains("38 digits"), "{}", last_error());
        pinkdb_result_free(r);
    }
    pinkdb_close(h);
}

#[test]
fn chunk_export_survives_chunk_and_result() {
    let h = pinkdb_open_memory();
    let r = exec(h, "SELECT 42::BIGINT AS x, 'ok' AS s;");
    let schema = export_schema(r);
    let chunk = pinkdb_result_fetch_chunk(r, 0);
    assert!(!chunk.is_null());
    let mut array = ArrowArray::empty();
    assert_eq!(pinkdb_chunk_to_arrow(chunk, &mut array), 1);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(r);
    let root = import(array, &schema);
    let root = root.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        root.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        root.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "ok"
    );
    pinkdb_close(h);
}
