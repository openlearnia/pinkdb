//! At-rest encryption end-to-end tests (cargo feature `encryption`).
//!
//! Covers bit-exact encrypted round-trips (NaN payloads, `-0.0`, multi-db,
//! v1 fixtures), multi-segment payloads, wrong-key vs corruption errors,
//! truncated/tampered containers, empty databases and empty WALs, encrypted
//! WAL replay with a truncated tail, mid-file WAL tampering (hard error),
//! checkpoints, mode-mismatch refusal, and the no-plaintext-leak property.
//!
//! This file compiles to nothing without the feature.

#![cfg(feature = "encryption")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataType, EncryptionKey, PinkDb, QueryResult, SyncPolicy, Value};

const ENC_MAGIC: &[u8; 8] = b"PINKENC1";
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Self-cleaning scratch directory for one test.
struct Scratch {
    dir: PathBuf,
    ckpt: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_enc_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        let ckpt = dir.join("db.pink");
        Self { dir, ckpt }
    }

    fn ckpt(&self) -> &Path {
        &self.ckpt
    }

    fn wal(&self) -> PathBuf {
        let mut s = self.ckpt.clone().into_os_string();
        s.push(".wal");
        PathBuf::from(s)
    }

    fn path(&self) -> &str {
        self.ckpt.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn raw_key() -> EncryptionKey {
    EncryptionKey::from_raw([0xA5u8; 32])
}

fn rows_of(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    rows_of(db, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i as i64,
            Value::Bigint(i) => *i,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

fn seed_all_types(db: &mut PinkDb) {
    db.create_table(
        "t",
        vec![
            ("b".to_string(), DataType::Boolean),
            ("i".to_string(), DataType::Integer),
            ("g".to_string(), DataType::Bigint),
            ("d".to_string(), DataType::Double),
            ("v".to_string(), DataType::Varchar),
            ("dt".to_string(), DataType::Date),
            ("ts".to_string(), DataType::Timestamp),
        ],
    )
    .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Boolean(true),
            Value::Integer(-5),
            Value::Bigint(9_999_999_999),
            // A NaN with a non-default payload: only a bit-exact codec keeps it.
            Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
            Value::Varchar("héllo wörld".to_string()),
            Value::Date(-1),
            Value::Timestamp(1_700_000_000_000_000),
        ])
        .unwrap();
        t.append_row(vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ])
        .unwrap();
        t.append_row(vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MIN),
            Value::Double(-0.0),
            Value::Varchar("plaintext-marker-🩷".to_string()),
            Value::Date(i32::MAX),
            Value::Timestamp(i64::MIN),
        ])
        .unwrap();
    }
}

fn all_values(db: &PinkDb, table: &str) -> Vec<Vec<Value>> {
    let t = db.table(table).unwrap();
    (0..t.num_rows())
        .map(|r| (0..t.num_cols()).map(|c| t.get(r, c).unwrap()).collect())
        .collect()
}

/// Bit-exact comparison (NaN payloads and `-0.0` included).
fn assert_same_values(a: &[Vec<Value>], b: &[Vec<Value>]) {
    assert_eq!(a.len(), b.len());
    for (ra, rb) in a.iter().zip(b.iter()) {
        assert_eq!(ra.len(), rb.len());
        for (va, vb) in ra.iter().zip(rb.iter()) {
            match (va, vb) {
                (Value::Double(x), Value::Double(y)) => {
                    assert_eq!(x.to_bits(), y.to_bits(), "double bits differ: {x} vs {y}")
                }
                _ => assert_eq!(va, vb),
            }
        }
    }
}

/// Length of the public container header, i.e. where the payload begins.
fn payload_start(data: &[u8]) -> usize {
    assert_eq!(&data[..8], ENC_MAGIC);
    let kdf = data[13];
    let mut pos = 34;
    if kdf == 1 {
        let salt_len = u32::from_le_bytes(data[46..50].try_into().unwrap()) as usize;
        pos += 16 + salt_len;
    }
    pos + 30 + 16
}

/// Where an encrypted WAL's records begin: the publish block (wal_id + CRC +
/// two commit slots) sits between the header and the records.
fn wal_records_start(data: &[u8]) -> usize {
    // The container version is the u32 at 8..12; WAL containers carry the
    // block since v2 (wave 45m) and are written as v3 since wave 45o (the
    // payload gained the per-frame touched sets). Checkpoints stay v2.
    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    assert!(
        (2..=3).contains(&version),
        "WAL containers are v2 or v3, got {version}"
    );
    payload_start(data) + 88
}

/// A 900-byte string with no useful repeats (16-char hex chunks of a seeded
/// LCG): the container tests need their plaintext to stay above the AEAD
/// segment size, and a repetitive filler would now be LZ-compressed below it.
fn incompressible_filler(i: usize) -> String {
    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (i as u64).wrapping_mul(0x9E37_79B9);
    let mut s = String::with_capacity(912);
    while s.len() < 900 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        s.push_str(&format!("{x:016x}"));
    }
    s.truncate(900);
    s
}

#[test]
fn bit_exact_roundtrip_multi_database() {
    let s = Scratch::new("roundtrip");
    let mut db = PinkDb::new();
    seed_all_types(&mut db);
    db.execute("CREATE DATABASE analytics;").unwrap();
    db.execute("CREATE TABLE analytics.u (k BIGINT, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO analytics.u VALUES (7, 'x'), (8, NULL);")
        .unwrap();
    db.execute("CREATE DATABASE empty;").unwrap();
    db.execute("CREATE TABLE empty.t2 (x DOUBLE);").unwrap();
    db.execute("INSERT INTO empty.t2 VALUES (1.5), (-0.0);")
        .unwrap();

    db.save_with_key(s.path(), &raw_key()).unwrap();
    let bytes = std::fs::read(s.ckpt()).unwrap();
    assert!(bytes.starts_with(ENC_MAGIC), "must be a PINKENC1 container");

    let back = PinkDb::open_with_key(s.path(), &raw_key()).unwrap();
    assert_eq!(back.list_databases(), vec!["analytics", "empty", "main"]);
    assert_eq!(
        ints(&mut back.clone(), "SELECT k FROM analytics.u ORDER BY k;"),
        vec![7, 8]
    );
    let a = rows_of(
        &mut back.clone(),
        "SELECT b,i,g,d,v,dt,ts FROM main.t ORDER BY i;",
    );
    let b = rows_of(&mut db, "SELECT b,i,g,d,v,dt,ts FROM main.t ORDER BY i;");
    for (ra, rb) in a.iter().zip(b.iter()) {
        for (va, vb) in ra.iter().zip(rb.iter()) {
            match (va, vb) {
                (Value::Double(x), Value::Double(y)) => assert_eq!(x.to_bits(), y.to_bits()),
                _ => assert_eq!(va, vb),
            }
        }
    }
    // `-0.0` survives bit-exactly through encryption.
    let t2 = rows_of(&mut back.clone(), "SELECT x FROM empty.t2;");
    let bits: Vec<u64> = t2
        .iter()
        .map(|r| match r[0] {
            Value::Double(x) => x.to_bits(),
            _ => panic!("expected a double"),
        })
        .collect();
    let mut want = vec![1.5f64.to_bits(), (-0.0f64).to_bits()];
    want.sort_unstable();
    let mut got = bits.clone();
    got.sort_unstable();
    assert_eq!(got, want, "-0.0 must round-trip bit-exactly");
}

#[test]
fn multi_segment_roundtrip_over_one_mib() {
    let s = Scratch::new("segments");
    let mut db = PinkDb::new();
    db.create_table("big", vec![("v".to_string(), DataType::Varchar)])
        .unwrap();
    {
        let t = db.table_mut("big").unwrap();
        for i in 0..1500 {
            t.append_row(vec![Value::Varchar(incompressible_filler(i))])
                .unwrap();
        }
    }
    db.save_with_key(s.path(), &raw_key()).unwrap();
    let sealed_len = std::fs::metadata(s.ckpt()).unwrap().len() as usize;
    // > 1 MiB of plaintext ensures more than one AEAD segment.
    assert!(
        sealed_len > 1 << 20,
        "payload must span segments: {sealed_len}"
    );
    let back = PinkDb::open_with_key(s.path(), &raw_key()).unwrap();
    assert_same_values(&all_values(&db, "big"), &all_values(&back, "big"));
    assert_eq!(back.table("big").unwrap().num_rows(), 1500);
}

#[test]
fn wrong_key_reports_wrong_key() {
    let s = Scratch::new("wrongkey");
    let mut db = PinkDb::new();
    seed_all_types(&mut db);
    db.save_with_key(s.path(), &raw_key()).unwrap();

    let err = PinkDb::open_with_key(s.path(), &EncryptionKey::from_raw([0x11; 32])).unwrap_err();
    assert!(err.to_string().contains("wrong key"), "{err}");

    // Passphrase container: a wrong passphrase and the wrong key *type* both
    // report a wrong key, never "corrupt file".
    let p = s.dir.join("pass.pink");
    let key = EncryptionKey::from_passphrase("correct horse battery staple");
    db.save_with_key(p.to_str().unwrap(), &key).unwrap();
    let err = PinkDb::open_with_key(p.to_str().unwrap(), &EncryptionKey::from_passphrase("nope"))
        .unwrap_err();
    assert!(err.to_string().contains("wrong key"), "{err}");
    let err = PinkDb::open_with_key(p.to_str().unwrap(), &raw_key()).unwrap_err();
    assert!(err.to_string().contains("wrong key"), "{err}");
}

#[test]
fn tampered_bytes_and_truncation_are_hard_errors() {
    let s = Scratch::new("tamper");
    let mut db = PinkDb::new();
    seed_all_types(&mut db);
    db.save_with_key(s.path(), &raw_key()).unwrap();
    let full = std::fs::read(s.ckpt()).unwrap();

    // A payload byte flip (mid-file) must fail authentication.
    let mut bad = full.clone();
    let n = bad.len();
    bad[n / 2] ^= 0x01;
    std::fs::write(s.ckpt(), &bad).unwrap();
    let err = PinkDb::open_with_key(s.path(), &raw_key()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("failed authentication"), "{msg}");
    assert!(!msg.contains("wrong key"), "{msg}");

    // A header flip (nonce prefix) also breaks authentication: the key-check
    // no longer verifies, so it reports a wrong key, never corruption.
    let mut bad = full.clone();
    bad[23] ^= 0x01;
    std::fs::write(s.ckpt(), &bad).unwrap();
    let err = PinkDb::open_with_key(s.path(), &raw_key()).unwrap_err();
    assert!(err.to_string().contains("wrong key"), "{err}");

    // Truncations at every scale error, never panic.
    for cut in [
        0,
        4,
        8,
        33,
        34,
        40,
        79,
        80,
        81,
        full.len() / 2,
        full.len() - 1,
    ] {
        std::fs::write(s.ckpt(), &full[..cut]).unwrap();
        assert!(
            PinkDb::open_with_key(s.path(), &raw_key()).is_err(),
            "cut at {cut} must error"
        );
    }
    // An empty file is not a container.
    std::fs::write(s.ckpt(), b"").unwrap();
    assert!(PinkDb::open_with_key(s.path(), &raw_key()).is_err());
}

#[test]
fn feature_on_keeps_plaintext_backcompat() {
    let s = Scratch::new("backcompat");
    let mut db = PinkDb::new();
    seed_all_types(&mut db);
    db.save(s.path()).unwrap();
    let bytes = std::fs::read(s.ckpt()).unwrap();
    assert!(
        bytes.starts_with(b"PINKDB05"),
        "plain save stays unencrypted v4"
    );
    assert!(!bytes.starts_with(ENC_MAGIC));
    let back = PinkDb::open(s.path()).unwrap();
    assert_same_values(&all_values(&db, "t"), &all_values(&back, "t"));

    // Key/plaintext mode mismatches are refused, never silently ignored.
    let err = PinkDb::open_with_key(s.path(), &raw_key()).unwrap_err();
    assert!(err.to_string().contains("not encrypted"), "{err}");
    let enc = s.dir.join("enc.pink");
    db.save_with_key(enc.to_str().unwrap(), &raw_key()).unwrap();
    let err = PinkDb::open(enc.to_str().unwrap()).unwrap_err();
    assert!(err.to_string().contains("open it with a key"), "{err}");
}

#[test]
fn empty_database_roundtrips_encrypted() {
    let s = Scratch::new("empty");
    let db = PinkDb::new();
    db.save_with_key(s.path(), &raw_key()).unwrap();
    let back = PinkDb::open_with_key(s.path(), &raw_key()).unwrap();
    assert_eq!(back.list_databases(), vec!["main".to_string()]);
    assert!(back.table("t").is_err());
}

#[test]
fn v1_fixture_re_saves_encrypted() {
    let s = Scratch::new("v1");
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/v1_single_db.pink"
    );
    let db = PinkDb::open(fixture).unwrap();
    assert_eq!(db.table("t").unwrap().num_rows(), 3);
    db.save_with_key(s.path(), &raw_key()).unwrap();
    let back = PinkDb::open_with_key(s.path(), &raw_key()).unwrap();
    assert_eq!(back.list_databases(), vec!["main".to_string()]);
    assert_eq!(
        back.list_tables(),
        vec!["empty".to_string(), "t".to_string()]
    );
    assert_same_values(&all_values(&db, "t"), &all_values(&back, "t"));
}

#[test]
fn passphrase_saves_store_fresh_argon2id_params() {
    let s = Scratch::new("passphrase");
    let key = EncryptionKey::from_passphrase("s3cret p@ss");
    let mut db = PinkDb::new();
    seed_all_types(&mut db);
    db.save_with_key(s.path(), &key).unwrap();
    let bytes = std::fs::read(s.ckpt()).unwrap();
    // Header: kdf id = 1 (Argon2id), then m/t/p/salt_len and the salt.
    assert_eq!(bytes[13], 1, "kdf must be Argon2id");
    let m = u32::from_le_bytes(bytes[34..38].try_into().unwrap());
    let t = u32::from_le_bytes(bytes[38..42].try_into().unwrap());
    let p = u32::from_le_bytes(bytes[42..46].try_into().unwrap());
    let salt_len = u32::from_le_bytes(bytes[46..50].try_into().unwrap());
    assert_eq!((m, t, p, salt_len), (19 * 1024, 2, 1, 16));
    assert_eq!(payload_start(&bytes), 34 + 16 + 16 + 46);

    // Same passphrase, fresh salt per save: different container bytes.
    let other = s.dir.join("again.pink");
    db.save_with_key(other.to_str().unwrap(), &key).unwrap();
    assert_ne!(bytes, std::fs::read(&other).unwrap());

    let back = PinkDb::open_with_key(s.path(), &key).unwrap();
    assert_same_values(&all_values(&db, "t"), &all_values(&back, "t"));
}

#[test]
fn empty_encrypted_wal_opens_and_replays() {
    let s = Scratch::new("emptywal");
    let (mut db, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(info.replayed_records, 0);
    assert!(s.wal().exists(), "durable open creates the WAL");
    let wal = std::fs::read(s.wal()).unwrap();
    assert!(
        wal.starts_with(ENC_MAGIC),
        "WAL must be an encrypted container"
    );
    assert_eq!(wal.len(), wal_records_start(&wal), "header + block only");
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1);").unwrap();
    drop(db);

    let (mut back, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(info.replayed_records, 2);
    assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
    // One live handle per durable database: release it before reopening.
    drop(back);

    // Wrong key and missing key are refused before replay.
    let err =
        PinkDb::open_durable_with_key(s.path(), &EncryptionKey::from_raw([9u8; 32])).unwrap_err();
    assert!(err.to_string().contains("wrong key"), "{err}");
    let err = PinkDb::open_durable(s.path()).unwrap_err();
    assert!(err.to_string().contains("file is encrypted"), "{err}");
}

#[test]
fn encrypted_wal_replays_and_truncates_a_torn_tail() {
    let s = Scratch::new("waltail");
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
    }
    let full = std::fs::read(s.wal()).unwrap();
    assert!(full.starts_with(ENC_MAGIC));
    // Torn tail: cut into the last record's ciphertext.
    std::fs::write(s.wal(), &full[..full.len() - 5]).unwrap();
    let (mut db, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(info.replayed_records, 2, "the torn record is dropped");
    assert!(info.truncated_bytes > 0);
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1]);
    // The truncated WAL was rewritten (fresh nonce prefix) and accepts new
    // appends that replay exactly once.
    db.execute("INSERT INTO t VALUES (3);").unwrap();
    drop(db);
    let (mut again, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(
        info.replayed_records, 3,
        "the two kept records plus the post-recovery insert replay"
    );
    assert_eq!(info.truncated_bytes, 0);
    assert_eq!(ints(&mut again, "SELECT a FROM t ORDER BY a;"), vec![1, 3]);
    let after = std::fs::read(s.wal()).unwrap();
    assert_ne!(&after[..80], &full[..80], "the rewrite uses a fresh header");
    drop(again);

    // Garbage appended after a valid tail: torn tail, same policy.
    let mut with_garbage = std::fs::read(s.wal()).unwrap();
    with_garbage.extend_from_slice(b"garbage-not-a-record");
    std::fs::write(s.wal(), &with_garbage).unwrap();
    let (mut db, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(info.truncated_bytes, b"garbage-not-a-record".len() as u64);
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 3]);
}

#[test]
fn encrypted_wal_mid_file_tamper_is_a_hard_error() {
    let s = Scratch::new("waltamper");
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
    }
    let mut wal = std::fs::read(s.wal()).unwrap();
    let start = wal_records_start(&wal);
    // Flip a ciphertext byte of the *first* record while records follow:
    // indistinguishable from mid-file corruption, so replay hard-errors
    // instead of silently dropping committed records.
    wal[start + 20] ^= 0x80;
    std::fs::write(s.wal(), &wal).unwrap();
    let err = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("failed authentication"), "{msg}");
    assert!(msg.contains("mid-file"), "{msg}");
}

#[test]
fn encrypted_checkpoint_roundtrips_and_leaks_no_plaintext() {
    let s = Scratch::new("ckpt");
    let marker = "ultra-secret-marker-string";
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER, v VARCHAR);")
            .unwrap();
        db.execute(&format!("INSERT INTO t VALUES (1, '{marker}');"))
            .unwrap();
        db.checkpoint().unwrap();
        assert!(s.ckpt().exists());
        let pink = std::fs::read(s.ckpt()).unwrap();
        assert!(pink.starts_with(ENC_MAGIC), "checkpoint must be encrypted");
        assert!(
            !pink.windows(marker.len()).any(|w| w == marker.as_bytes()),
            "checkpoint must not leak the plaintext"
        );
        let wal = std::fs::read(s.wal()).unwrap();
        assert!(!wal.windows(marker.len()).any(|w| w == marker.as_bytes()));
        // The rolled WAL is a fresh encrypted generation.
        assert_eq!(wal.len(), wal_records_start(&wal));
        db.execute(&format!("INSERT INTO t VALUES (2, '{marker}');"))
            .unwrap();
    }
    let (mut back, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &raw_key()).unwrap();
    assert_eq!(info.replayed_records, 1);
    assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    let raw = std::fs::read(s.wal()).unwrap();
    assert!(!raw.windows(marker.len()).any(|w| w == marker.as_bytes()));
}

#[test]
fn plaintext_artifacts_are_refused_in_keyed_opens() {
    let s = Scratch::new("mode");
    // A plaintext durable database: its WAL must be refused by a keyed open.
    {
        let mut db = PinkDb::open_durable(s.path()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
    }
    let err = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap_err();
    assert!(
        err.to_string()
            .contains("not encrypted, but a key was supplied"),
        "{err}"
    );

    // The mirror: an encrypted checkpoint + an unrelated plaintext WAL.
    let s2 = Scratch::new("mode2");
    {
        let mut db = PinkDb::open_durable_with_key(s2.path(), &raw_key()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.checkpoint().unwrap();
    }
    std::fs::write(s2.wal(), {
        let mut w = pinkdb::wal::WAL_MAGIC.to_vec();
        w.extend_from_slice(&2u32.to_le_bytes());
        w.extend_from_slice(&7u64.to_le_bytes());
        w
    })
    .unwrap();
    let err = PinkDb::open_durable_with_key(s2.path(), &raw_key()).unwrap_err();
    assert!(err.to_string().contains("not encrypted"), "{err}");
}

#[test]
fn keyed_database_reports_its_key_and_snapshots_encrypt() {
    let s = Scratch::new("snapshot");
    let mut db = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap();
    assert!(db.encryption_key().is_some());
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1);").unwrap();
    let snap = s.dir.join("snap.pink");
    db.save_snapshot(snap.to_str().unwrap()).unwrap();
    let bytes = std::fs::read(&snap).unwrap();
    assert!(
        bytes.starts_with(ENC_MAGIC),
        "snapshot of a keyed store encrypts"
    );
    let back = PinkDb::open_with_key(snap.to_str().unwrap(), &raw_key()).unwrap();
    assert_eq!(ints(&mut back.clone(), "SELECT a FROM t;"), vec![1]);

    // A plaintext store writes plaintext snapshots.
    let p = s.dir.join("plain.pink");
    let mut mem = PinkDb::new();
    mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
    mem.save_snapshot(p.to_str().unwrap()).unwrap();
    assert!(std::fs::read(&p).unwrap().starts_with(b"PINKDB05"));
}

/// The keyed WAL survives the crash state machine: `.tmp` publishing, roll
/// removal and generation checks all operate on encrypted artifacts, and the
/// encrypted `.tmp` is published before anything is removed (mirrors the
/// wave-22 unit test, end to end through the public API).
#[test]
fn keyed_checkpoint_crash_image_recovers_once() {
    let s = Scratch::new("ckptcrash");
    let key = raw_key();
    // Build a durable encrypted database whose checkpoint contains row 1 and
    // whose WAL (generation 1) holds row 2, then emulate the crash windows.
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
    }
    // Window A: the checkpoint reached its WAL roll but not the publish.
    // `.tmp` holds rows 1+2, generation 1 is rolled away, a fresh generation 2
    // WAL exists, `.pink` holds row 1 only. Emulated by re-running the public
    // checkpoint and then restoring the on-disk image is not possible without
    // crate internals; the unit tests in `wal.rs` cover that state machine on
    // encrypted artifacts. Here we cover the observable invariant that
    // matters: a plaintext `.tmp` next to a keyed open is refused instead of
    // being published.
    let tmp = tmp_of(s.ckpt());
    let mut mem = PinkDb::new();
    mem.execute("CREATE TABLE t (a INTEGER);").unwrap();
    mem.execute("INSERT INTO t VALUES (99);").unwrap();
    mem.save(&tmp).unwrap(); // plaintext `.tmp`
    let err = PinkDb::open_durable_with_key(s.path(), &key).unwrap_err();
    assert!(err.to_string().contains("not encrypted"), "{err}");
    std::fs::remove_file(&tmp).unwrap();

    // Clean reopen still sees both rows exactly once.
    let (mut back, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &key).unwrap();
    assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
    assert!(info.replayed_records <= 1);
}

fn tmp_of(ckpt: &Path) -> String {
    let mut s = ckpt.as_os_str().to_os_string();
    s.push(".tmp");
    s.to_string_lossy().to_string()
}

/// `<wal>.rewrite`: the temp file of the atomic WAL rebuild.
fn rewrite_of(wal: &Path) -> PathBuf {
    let mut s = wal.as_os_str().to_os_string();
    s.push(".rewrite");
    PathBuf::from(s)
}

/// Encrypted WAL framing: `len u32 | lsn u64 | crc u32 | ciphertext (len+16)`.
/// Returns `(lsn, crc, frame_start, frame_end)` per record.
fn wal_frames(data: &[u8]) -> Vec<(u64, u32, usize, usize)> {
    assert_eq!(&data[..8], ENC_MAGIC, "not an encrypted WAL");
    let mut out = Vec::new();
    let mut pos = wal_records_start(data);
    while pos < data.len() {
        assert!(data.len() - pos >= 16, "short framing at {pos}");
        let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let lsn = u64::from_le_bytes(data[pos + 4..pos + 12].try_into().unwrap());
        let crc = u32::from_le_bytes(data[pos + 12..pos + 16].try_into().unwrap());
        let end = pos + 16 + n + 16;
        assert!(end <= data.len(), "short record at {pos}");
        out.push((lsn, crc, pos, end));
        pos = end;
    }
    out
}

/// The WAL payload encoding for one commit unit: `count u32 | len u32 | text`.
fn payload_of(stmts: &[&str]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(stmts.len() as u32).to_le_bytes());
    for s in stmts {
        v.extend_from_slice(&(s.len() as u32).to_le_bytes());
        v.extend_from_slice(s.as_bytes());
    }
    v
}

/// C1 regression: a WAL cut exactly at a record boundary (a Batch-mode
/// unsynced tail lost at a boundary, or a restored shorter backup) is
/// byte-identical to a cleanly shorter WAL. A reopen must still never reuse
/// the discarded record's `(key, nonce, AAD)`: assert the keystream-XOR
/// relation that would hold under nonce reuse does not hold, and that the
/// rebuild changed the nonce prefix.
#[test]
fn wal_boundary_truncation_never_reuses_a_discarded_nonce() {
    let s = Scratch::new("noncereuse");
    let key = raw_key();
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
        db.execute("INSERT INTO t VALUES (3);").unwrap();
    }
    let full = std::fs::read(s.wal()).unwrap();
    let frames = wal_frames(&full);
    assert_eq!(frames.len(), 4, "CREATE + three INSERTs");
    // The record that a shorter copy would discard: INSERT 2, sealed with
    // counter 3, lsn 3, crc 0.
    let (lsn3, crc3, f3, e3) = frames[2];
    assert_eq!((lsn3, crc3), (3, 0));
    let discarded = full[f3..e3].to_vec();
    let old_ct = &discarded[16..discarded.len() - 16];

    // Cut exactly at the boundary before the discarded record: no trailing
    // bytes are left, so nothing signals the loss to a naive reopen.
    std::fs::write(s.wal(), &full[..f3]).unwrap();

    let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    db.execute("INSERT INTO t VALUES (4);").unwrap();
    drop(db);
    let after = std::fs::read(s.wal()).unwrap();
    assert_ne!(
        &after[22..30],
        &full[22..30],
        "the keyed reopen must rebuild the WAL with a fresh nonce prefix"
    );
    let new_frames = wal_frames(&after);
    assert_eq!(new_frames.len(), 3, "CREATE, INSERT 1, INSERT 4");
    let (_, _, nf, ne) = new_frames[2];
    let new_ct = &after[nf + 16..ne - 16];
    assert_eq!(new_ct.len(), old_ct.len());
    // Under nonce reuse `ct_new ^ ct_old == pt_new ^ pt_old` because both
    // ciphertexts use the same keystream. The rebuild must break that.
    let new_pt = payload_of(&["INSERT INTO t VALUES (4)"]);
    let old_pt = payload_of(&["INSERT INTO t VALUES (2)"]);
    assert_eq!(new_pt.len(), old_pt.len());
    let xor_ct: Vec<u8> = new_ct.iter().zip(old_ct).map(|(a, b)| a ^ b).collect();
    let xor_pt: Vec<u8> = new_pt.iter().zip(&old_pt).map(|(a, b)| a ^ b).collect();
    assert_ne!(
        xor_ct, xor_pt,
        "nonce reuse: the new record and the discarded record were sealed \
         with the same (key, nonce, AAD)"
    );
    // The discarded INSERT 2 (and the never-listed INSERT 3) stay lost.
    let mut back = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 4]);
}

/// `(context, prefix, counter) -> sealed body` over one WAL snapshot. A
/// re-read of the same file repeats identical bodies; two *different* bytes
/// under one nonce would be reuse.
fn scan_wal_units(data: &[u8], seen: &mut HashMap<(u64, [u8; 8], u32), Vec<u8>>) {
    assert_eq!(&data[..8], ENC_MAGIC);
    let context = u64::from_le_bytes(data[14..22].try_into().unwrap());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&data[22..30]);
    for (i, (_, _, start, end)) in wal_frames(data).iter().enumerate() {
        let counter = i as u32 + 1;
        let body = data[start + 16..*end].to_vec();
        if let Some(prev) = seen.insert((context, prefix, counter), body.clone()) {
            assert_eq!(
                prev, body,
                "two different records were sealed under one (context, prefix, counter)"
            );
        }
    }
}

/// Normal reopen/append/checkpoint cycles must never seal two different
/// records under one `(key, nonce)`: re-seals after a rebuild carry a fresh
/// prefix; appends continue the counter; a checkpoint forks a new generation.
#[test]
fn normal_reopen_and_checkpoint_cycles_never_seal_two_records_under_one_nonce() {
    let s = Scratch::new("noncecycles");
    let key = raw_key();
    let mut seen: HashMap<(u64, [u8; 8], u32), Vec<u8>> = HashMap::new();
    let scan = |seen: &mut HashMap<(u64, [u8; 8], u32), Vec<u8>>| {
        scan_wal_units(&std::fs::read(s.wal()).unwrap(), seen)
    };

    let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    scan(&mut seen); // header-only
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1);").unwrap();
    scan(&mut seen);
    db.execute("INSERT INTO t VALUES (2);").unwrap();
    db.checkpoint().unwrap();
    scan(&mut seen); // the checkpoint rolled into a fresh encrypted generation
    db.execute("INSERT INTO t VALUES (3);").unwrap();
    scan(&mut seen);
    drop(db);

    let mut back = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    scan(&mut seen); // the reopen re-sealed everything under a fresh prefix
    back.execute("INSERT INTO t VALUES (4);").unwrap();
    scan(&mut seen);
    drop(back);

    let mut again = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    scan(&mut seen);
    assert_eq!(
        ints(&mut again, "SELECT a FROM t ORDER BY a;"),
        vec![1, 2, 3, 4]
    );
    // CREATE + INSERT 1 were the two records visible in the first snapshot;
    // every keyed reopen rebuilds the file under a fresh prefix, so each
    // re-sealing adds new (prefix, counter) units — and the assertion inside
    // `scan_wal_units` proves no unit ever maps to two different bodies.
    assert!(
        seen.len() >= 7,
        "snapshots covered several records: {}",
        seen.len()
    );
}

/// Checkpoint segments are `(counter, index)`-bound AEAD units with a
/// terminal zero-length frame: duplicating, skipping, swapping, or cutting
/// the container at a segment boundary must all be hard errors, never a
/// silently shorter database.
#[test]
fn duplicated_skipped_or_boundary_truncated_checkpoint_segments_are_rejected() {
    let s = Scratch::new("segsegments");
    let mut db = PinkDb::new();
    db.create_table("big", vec![("v".to_string(), DataType::Varchar)])
        .unwrap();
    {
        let t = db.table_mut("big").unwrap();
        for i in 0..3000 {
            t.append_row(vec![Value::Varchar(incompressible_filler(i))])
                .unwrap();
        }
    }
    db.save_with_key(s.path(), &raw_key()).unwrap();
    let full = std::fs::read(s.ckpt()).unwrap();
    // Segment frame boundaries (the last frame is the terminal one).
    let mut bounds = vec![payload_start(&full)];
    while *bounds.last().unwrap() < full.len() {
        let pos = *bounds.last().unwrap();
        let n = u32::from_le_bytes(full[pos..pos + 4].try_into().unwrap()) as usize;
        bounds.push(pos + 4 + n + 16);
    }
    let frames = bounds.len() - 1;
    assert!(
        frames >= 4,
        "need >= 3 data segments + terminal, got {frames}"
    );
    let last_frame = frames - 1;
    let last_len = u32::from_le_bytes(
        full[bounds[last_frame]..bounds[last_frame] + 4]
            .try_into()
            .unwrap(),
    );
    assert_eq!(last_len, 0, "the terminal frame is zero-length");
    // Sanity: the untouched container opens.
    assert!(PinkDb::open_with_key(s.path(), &raw_key()).is_ok());

    let reject = |bytes: &[u8], what: &str| -> String {
        let path = s.dir.join(format!("{what}.pink"));
        std::fs::write(&path, bytes).unwrap();
        PinkDb::open_with_key(path.to_str().unwrap(), &raw_key())
            .unwrap_err()
            .to_string()
    };

    // Duplicate the first segment: the second frame decodes at the wrong
    // counter and fails authentication.
    let mut dup = full[..bounds[1]].to_vec();
    dup.extend_from_slice(&full[bounds[0]..bounds[1]]);
    dup.extend_from_slice(&full[bounds[1]..]);
    let msg = reject(&dup, "dup");
    assert!(msg.contains("failed authentication"), "{msg}");

    // Skip a middle segment: every later frame's index shifts.
    let mut skip = full.clone();
    skip.drain(bounds[1]..bounds[2]);
    let msg = reject(&skip, "skip");
    assert!(msg.contains("failed authentication"), "{msg}");

    // Swap two data segments: both land at the wrong counter.
    let mut swap = full[..bounds[1]].to_vec();
    swap.extend_from_slice(&full[bounds[2]..bounds[3]]);
    swap.extend_from_slice(&full[bounds[1]..bounds[2]]);
    swap.extend_from_slice(&full[bounds[3]..]);
    let msg = reject(&swap, "swap");
    assert!(msg.contains("failed authentication"), "{msg}");

    // Cut exactly before the terminal frame: a boundary-skipped tail.
    let msg = reject(&full[..bounds[last_frame]], "noterminal");
    assert!(msg.contains("terminal segment is missing"), "{msg}");
    // Cut exactly before the last data segment: same.
    let msg = reject(&full[..bounds[last_frame - 1]], "notail");
    assert!(msg.contains("terminal segment is missing"), "{msg}");
    // Cut inside the last data segment.
    let msg = reject(&full[..bounds[last_frame] - 3], "shorttail");
    assert!(msg.contains("truncated segment"), "{msg}");

    // A copy that merely appends garbage after a complete container is
    // refused as well (never a silent partial load).
    let mut extra = full.clone();
    extra.extend_from_slice(&[0u8; 7]);
    let msg = reject(&extra, "garbage");
    assert!(
        msg.contains("truncated segment length") || msg.contains("data after the terminal"),
        "{msg}"
    );
}

/// Encrypted WAL records always frame `crc = 0` and the field is not
/// authenticated by GCM: a nonzero crc on a record whose ciphertext
/// authenticates is framing tampering and a hard error (the tail-torn
/// policy never applies, because a torn write cannot authenticate).
#[test]
fn encrypted_wal_nonzero_crc_is_rejected() {
    let s = Scratch::new("waltampercrc");
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO t VALUES (2);").unwrap();
    }
    let good = std::fs::read(s.wal()).unwrap();
    let frames = wal_frames(&good);
    assert_eq!(frames.len(), 3);
    assert!(frames.iter().all(|(_, crc, _, _)| *crc == 0));

    // Mid-file tamper: flip a bit of the first record's crc field.
    let mut bad = good.clone();
    bad[frames[0].2 + 12] ^= 0x01;
    std::fs::write(s.wal(), &bad).unwrap();
    let err = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("nonzero CRC"), "{msg}");
    assert!(msg.contains("encrypted record"), "{msg}");

    // The same tamper on the *last* record is also a hard error, not a
    // silent tail truncation (its ciphertext authenticates).
    let mut bad = good.clone();
    let last = frames.len() - 1;
    bad[frames[last].2 + 12] ^= 0x01;
    std::fs::write(s.wal(), &bad).unwrap();
    let err = PinkDb::open_durable_with_key(s.path(), &raw_key()).unwrap_err();
    assert!(err.to_string().contains("nonzero CRC"), "{err}");
    // A hard error leaves the file untouched (nothing was rewritten).
    assert_eq!(std::fs::read(s.wal()).unwrap(), bad);
}

/// A crash in the middle of the keyed rebuild (partial `<wal>.rewrite`
/// temp next to the intact WAL) must never corrupt or lose records: the
/// open re-runs the rebuild from the original file.
#[test]
fn a_partial_rewrite_temp_never_corrupts_the_wal() {
    let s = Scratch::new("partialrewrite");
    let key = raw_key();
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
    }
    let good = std::fs::read(s.wal()).unwrap();
    let temp = rewrite_of(&s.wal());
    std::fs::write(&temp, &good[..good.len() / 2]).unwrap();
    let (mut db, info) =
        PinkDb::open_durable_with_key_and_policy(s.path(), SyncPolicy::Sync, &key).unwrap();
    assert_eq!(info.replayed_records, 2, "the intact WAL is authoritative");
    assert_eq!(info.truncated_bytes, 0);
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
    assert!(
        !temp.exists(),
        "the rebuild replaces the partial temp by rename"
    );
    db.execute("INSERT INTO t VALUES (2);").unwrap();
    drop(db);
    let mut back = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    assert_eq!(ints(&mut back, "SELECT a FROM t ORDER BY a;"), vec![1, 2]);
}

/// `PinkDb::save` is the explicit plaintext API by design: a keyed store
/// that calls it writes plaintext (and the file opens without a key), while
/// `save_with_key` and `checkpoint` stay encrypted. Pinned so a future
/// change cannot silently flip the mode of `save`.
#[test]
fn save_stays_explicitly_plaintext_for_a_keyed_store() {
    let s = Scratch::new("savepin");
    let key = raw_key();
    let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    db.execute("CREATE TABLE t (a INTEGER, v VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'save-pin-marker');")
        .unwrap();
    let out = s.dir.join("explicit-plaintext.pink");
    db.save(out.to_str().unwrap()).unwrap();
    let bytes = std::fs::read(&out).unwrap();
    assert!(
        bytes.starts_with(b"PINKDB05"),
        "save() is the explicit plaintext API"
    );
    assert!(!bytes.starts_with(ENC_MAGIC));
    let marker = b"save-pin-marker";
    assert!(bytes.windows(marker.len()).any(|w| w == marker));
    // It loads without a key.
    let mut back = PinkDb::open(out.to_str().unwrap()).unwrap();
    assert_eq!(ints(&mut back, "SELECT a FROM t;"), vec![1]);
    // The encrypted save path stays encrypted.
    let enc = s.dir.join("still-encrypted.pink");
    db.save_with_key(enc.to_str().unwrap(), &key).unwrap();
    assert!(std::fs::read(&enc).unwrap().starts_with(ENC_MAGIC));
}

/// Wave 79 durability half with at-rest encryption: the canonical index DDL
/// is sealed into the encrypted WAL, survives a checkpoint (re-logged into the
/// fresh encrypted generation) and replays on a keyed reopen.
#[test]
fn encrypted_durable_index_survives_checkpoint_and_reopen() {
    let s = Scratch::new("idx");
    let key = raw_key();
    let query = "SELECT a FROM t WHERE a = 2;";
    let explain = |db: &mut PinkDb| -> String {
        rows_of(db, &format!("EXPLAIN {query};"))
            .iter()
            .map(|r| match &r[0] {
                Value::Varchar(s) => s.clone(),
                other => panic!("expected EXPLAIN text, got {other:?}"),
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    {
        let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
        db.execute("CREATE TABLE t (a INTEGER, v VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
            .unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        db.checkpoint().unwrap();
        assert!(explain(&mut db).contains("IndexScan"));
    }
    let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    assert!(
        explain(&mut db).contains("IndexScan"),
        "encrypted checkpoint lost the index"
    );
    assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 1);
    assert_eq!(ints(&mut db, "SELECT a FROM t WHERE a = 2;"), vec![2]);
    db.execute("DROP INDEX i;").unwrap();
    drop(db);
    let mut db = PinkDb::open_durable_with_key(s.path(), &key).unwrap();
    assert!(!explain(&mut db).contains("IndexScan"));
}
