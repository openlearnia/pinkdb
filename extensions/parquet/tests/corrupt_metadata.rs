//! A corrupt footer must be a read error, never an allocation abort.
//!
//! The fixtures here are hand-encoded: parquet's Thrift writer is private, so
//! [`Compact`] writes the footer fields a reader needs, which lets a test claim
//! counts no file of its size can hold. `huge_rg_num_rows.parquet` in DuckDB's
//! corpus is the same shape (a row group claiming `i64::MAX` rows); the fix is
//! DuckDB's `ValidateColumnMetadata`
//! (`extension/parquet/parquet_column_schema.cpp`).

use std::path::{Path, PathBuf};

use pinkdb::PinkDb;

/// Thrift compact type ids.
const I32: u8 = 5;
const I64: u8 = 6;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

/// Thrift compact protocol writer, as Parquet encodes its footer: field ids
/// are deltas, integers are zigzag varints, a struct ends with a zero byte.
#[derive(Default)]
struct Compact(Vec<u8>);

impl Compact {
    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.0.push(value as u8 | 0x80);
            value >>= 7;
        }
        self.0.push(value as u8);
    }

    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }

    fn field(&mut self, id: i16, kind: u8, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            self.0.push((delta as u8) << 4 | kind);
        } else {
            self.0.push(kind);
            self.zigzag(id as i64);
        }
        *last = id;
    }

    fn i32(&mut self, id: i16, value: i32, last: &mut i16) {
        self.field(id, I32, last);
        self.zigzag(value as i64);
    }

    fn i64(&mut self, id: i16, value: i64, last: &mut i16) {
        self.field(id, I64, last);
        self.zigzag(value);
    }

    fn string(&mut self, id: i16, value: &str, last: &mut i16) {
        self.field(id, BINARY, last);
        self.varint(value.len() as u64);
        self.0.extend_from_slice(value.as_bytes());
    }

    fn list(&mut self, id: i16, elem: u8, len: usize, last: &mut i16) {
        self.field(id, LIST, last);
        if len < 15 {
            self.0.push((len as u8) << 4 | elem);
        } else {
            self.0.push(0xf0 | elem);
            self.varint(len as u64);
        }
    }

    fn stop(&mut self) {
        self.0.push(0);
    }
}

/// What a fixture's footer claims, and how many data bytes the file holds.
struct Footer {
    rows: i64,
    num_values: i64,
    compressed_size: i64,
}

fn schema_element(name: &str, children: Option<i32>) -> Vec<u8> {
    let mut element = Compact::default();
    let mut last = 0i16;
    if children.is_none() {
        element.i32(1, 1, &mut last); // Type::INT32
        element.i32(3, 0, &mut last); // REQUIRED
    }
    element.string(4, name, &mut last);
    if let Some(children) = children {
        element.i32(5, children, &mut last);
    }
    element.stop();
    element.0
}

/// Write a one-column `INT32` file (`i`) whose footer says `footer`.
fn write_file(dir: &Path, name: &str, footer: Footer, data: usize) -> PathBuf {
    let path = dir.join(name);
    let data_page_offset = 4i64;
    let mut file = b"PAR1".to_vec();
    file.extend(std::iter::repeat_n(0u8, data));

    let mut meta = Compact::default();
    let mut last = 0i16;
    meta.i32(1, 1, &mut last); // Type::INT32
    meta.list(2, I32, 2, &mut last); // encodings: PLAIN, RLE
    meta.zigzag(0);
    meta.zigzag(3);
    meta.list(3, BINARY, 1, &mut last); // path_in_schema: ["i"]
    meta.varint(1);
    meta.0.extend_from_slice(b"i");
    meta.i32(4, 0, &mut last); // CompressionCodec::UNCOMPRESSED
    meta.i64(5, footer.num_values, &mut last);
    meta.i64(6, footer.compressed_size, &mut last);
    meta.i64(7, footer.compressed_size, &mut last);
    meta.i64(9, data_page_offset, &mut last);
    meta.stop();

    let mut chunk = Compact::default();
    let mut last = 0i16;
    chunk.i64(2, data_page_offset, &mut last); // file_offset
    chunk.field(3, STRUCT, &mut last); // meta_data
    chunk.0.extend_from_slice(&meta.0);
    chunk.stop();

    let mut group = Compact::default();
    let mut last = 0i16;
    group.list(1, STRUCT, 1, &mut last); // one ColumnChunk
    group.0.extend_from_slice(&chunk.0);
    group.i64(2, footer.compressed_size, &mut last);
    group.i64(3, footer.rows, &mut last);
    group.stop();

    let mut whole = Compact::default();
    let mut last = 0i16;
    whole.i32(1, 1, &mut last); // version
    whole.list(2, STRUCT, 2, &mut last); // schema
    whole
        .0
        .extend_from_slice(&schema_element("schema", Some(1)));
    whole.0.extend_from_slice(&schema_element("i", None));
    whole.i64(3, footer.rows, &mut last);
    whole.list(4, STRUCT, 1, &mut last); // row_groups
    whole.0.extend_from_slice(&group.0);
    whole.stop();

    file.extend_from_slice(&whole.0);
    file.extend_from_slice(&(whole.0.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::write(&path, file).expect("write fixture");
    path
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-parquet-corrupt-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The error a `SELECT *` over `path` reports.
fn read_error(path: &Path) -> String {
    let mut db = PinkDb::new();
    db.register_table_source(pinkdb_parquet::source());
    db.execute(&format!(
        "SELECT * FROM read_parquet('{}');",
        path.display()
    ))
    .unwrap_err()
    .to_string()
}

#[test]
fn a_column_count_that_mismatches_its_row_group_is_an_error() {
    let dir = temp_dir("mismatch");
    let path = write_file(
        &dir,
        "mismatch.parquet",
        Footer {
            rows: 10,
            num_values: 3,
            compressed_size: 16,
        },
        16,
    );
    // The footer parses — metadata functions still list it, as DuckDB does —
    // but any read refuses it before sizing a buffer from the counts.
    let meta = pinkdb_parquet::read_meta_path(path.to_str().unwrap()).unwrap();
    assert_eq!(meta.row_groups[0].rows, 10);
    assert_eq!(meta.row_groups[0].chunks[0].num_values, 3);
    let err = read_error(&path);
    assert!(
        err.contains("metadata is corrupt. Column has 3 values but row group has 10 rows"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_negative_column_count_is_an_error() {
    let dir = temp_dir("negative");
    let path = write_file(
        &dir,
        "negative.parquet",
        Footer {
            rows: 10,
            num_values: -3,
            compressed_size: 16,
        },
        16,
    );
    let err = read_error(&path);
    assert!(
        err.contains("metadata is corrupt. Column has invalid number of values (-3)"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_chunk_that_reaches_past_the_file_is_an_error() {
    let dir = temp_dir("oversized");
    let path = write_file(
        &dir,
        "oversized.parquet",
        Footer {
            rows: 3,
            num_values: 3,
            compressed_size: i64::MAX / 2,
        },
        16,
    );
    let err = read_error(&path);
    assert!(err.contains("metadata is corrupt"), "{err}");
    assert!(err.contains("spans bytes"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_huge_but_consistent_row_count_does_not_allocate_a_chunk_list() {
    // Every count agrees, so the metadata checks pass: the abort this guards
    // against was a `Vec` of one entry per 2048-row chunk of `i64::MAX` rows.
    // Decoding then meets the end of the data and errors.
    let dir = temp_dir("huge");
    let path = write_file(
        &dir,
        "huge.parquet",
        Footer {
            rows: i64::MAX,
            num_values: i64::MAX,
            compressed_size: 16,
        },
        16,
    );
    let err = read_error(&path);
    assert!(err.contains("cannot decode column"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}
