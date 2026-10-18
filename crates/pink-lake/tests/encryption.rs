//! End-to-end tests of pink-lake at-rest encryption (`encryption` feature):
//! a keyed save/open/attach/commit/time-travel round trip, the
//! `catalog.json == snapshots/<v>.json` ciphertext invariant (including
//! through incremental commits and delete files), fail-closed behavior for a
//! missing/wrong key and for tampered or swapped containers, plaintext lakes
//! staying untouched, and the statistics fast paths on an encrypted lake
//! decoding no data file.
//!
//! Runs in every build shape that has the feature: `encryption` alone (lean
//! `.pink` data files) and `encryption,vortex-format` (Vortex data files);
//! the `s3` feature additionally runs the hermetic-mock ciphertext-CAS test.

#![cfg(feature = "encryption")]

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{
    save_store, save_store_with_key, EncryptionKey, Lake, LakeError, WritableAttachment,
};
use pinkdb::{PinkDb, QueryResult, Value};

#[cfg(feature = "s3")]
use pink_lake::{save_store_s3_with_key, S3Config};

static SEQ: AtomicU64 = AtomicU64::new(0);

const MAGIC: &[u8; 8] = b"PNKLAKE1";

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_enc_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// The test passphrase (one per test — never shared with a lake).
fn key() -> EncryptionKey {
    EncryptionKey::from_passphrase("wave-57-lake-passphrase")
}

fn wrong_key() -> EncryptionKey {
    EncryptionKey::from_passphrase("wave-57-not-it")
}

/// A store with NULLs, `-0.0`, a NaN payload, multibyte text and two tables.
fn fixture_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR, d DOUBLE);")
        .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        t.append_row(vec![
            Value::Integer(-5),
            Value::Varchar("hello".to_string()),
            Value::Double(-0.0),
        ])
        .unwrap();
        t.append_row(vec![Value::Null, Value::Null, Value::Null])
            .unwrap();
        t.append_row(vec![
            Value::Integer(9),
            Value::Varchar("héllo wörld 🌍".to_string()),
            Value::Double(f64::from_bits(0x7ff8_0000_dead_beef)),
        ])
        .unwrap();
    }
    db.execute("CREATE TABLE u (x BIGINT);").unwrap();
    db.execute("INSERT INTO u VALUES (1), (2), (3);").unwrap();
    db
}

/// Compare two stores value by value, doubles by bit pattern (so NaN
/// payloads and `-0.0` are checked exactly).
fn assert_same_store(a: &PinkDb, b: &PinkDb) {
    assert_eq!(a.list_databases(), b.list_databases(), "databases differ");
    for dbname in a.list_databases() {
        assert_eq!(
            a.list_tables_in(&dbname),
            b.list_tables_in(&dbname),
            "tables differ in {dbname}"
        );
        for table in a.list_tables_in(&dbname) {
            let ta = a.table_in(&dbname, &table).unwrap();
            let tb = b.table_in(&dbname, &table).unwrap();
            assert_eq!(ta.schema(), tb.schema(), "{dbname}.{table} schema");
            assert_eq!(ta.num_rows(), tb.num_rows(), "{dbname}.{table} rows");
            for r in 0..ta.num_rows() {
                for c in 0..ta.num_cols() {
                    let (va, vb) = (ta.get(r, c).unwrap(), tb.get(r, c).unwrap());
                    match (&va, &vb) {
                        (Value::Double(x), Value::Double(y)) => {
                            assert_eq!(
                                x.to_bits(),
                                y.to_bits(),
                                "{dbname}.{table} row {r} col {c}: {va:?} != {vb:?}"
                            );
                        }
                        _ => assert_eq!(va, vb, "{dbname}.{table} row {r} col {c}"),
                    }
                }
            }
        }
    }
}

/// Every file under `dir`, recursively, sorted.
fn all_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(all_files(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn is_container_file(path: &Path) -> bool {
    let bytes = std::fs::read(path).unwrap();
    bytes.starts_with(MAGIC)
}

/// The engine's rows for `sql`, or the error text.
fn try_rows(db: &mut PinkDb, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    match db.execute(sql) {
        Ok(mut results) => match results.pop().unwrap() {
            QueryResult::Selected { rows, .. } => Ok(rows),
            other => panic!("expected rows, got {other:?}"),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// The engine's rows for `sql` (panicking on error).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    try_rows(db, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Every object a keyed lake stores is a `PNKLAKE1` container: catalog,
/// snapshots, commit deltas, data files, delete files.
fn assert_everything_is_ciphertext(lake_dir: &Path) {
    let files = all_files(lake_dir);
    assert!(!files.is_empty(), "the lake published nothing");
    for path in &files {
        let bytes = std::fs::read(path).unwrap();
        if path.extension().is_some_and(|e| e == "tmp") {
            panic!("a leftover temp file: {}", path.display());
        }
        assert!(
            bytes.starts_with(MAGIC),
            "{} is not a PNKLAKE1 container",
            path.display()
        );
        // A container never parses as the JSON a plaintext artifact would be.
        assert!(
            serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
            "{} parses as JSON (it must be ciphertext)",
            path.display()
        );
    }
}

/// A keyed save/open round trip: everything on disk is ciphertext, the same
/// key reads the store back bit-exactly, and every keyless or wrong-key open
/// fails closed.
#[test]
fn keyed_roundtrip_is_ciphertext_and_fails_closed_without_the_key() {
    let dir = scratch("roundtrip");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    let version = save_store_with_key(&db, &lake_dir, key()).unwrap();
    assert_eq!(version, 2);
    assert_everything_is_ciphertext(&lake_dir);

    // The catalog is a container, and not readable as plaintext JSON.
    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    assert!(catalog.starts_with(MAGIC));
    assert!(String::from_utf8(catalog.clone()).is_err() || catalog[0] != b'{');

    // A keyless open never guesses.
    let err = Lake::open(&lake_dir).unwrap_err();
    assert!(matches!(err, LakeError::Encrypted { .. }), "{err:?}");
    assert!(err.to_string().contains("the lake is encrypted"), "{err}");

    // A wrong key is `wrong key`, not a corrupt file.
    let err = Lake::open_with_key(&lake_dir, wrong_key()).unwrap_err();
    assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");
    assert!(err.to_string().contains("wrong key"), "{err}");

    // The right key reads the store back bit-exactly and reports encryption.
    let mut lake = Lake::open_with_key(&lake_dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert!(lake.is_encrypted());
    assert_eq!(lake.current_version(), 2);
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);
    assert_eq!(
        lake.tables().unwrap(),
        vec![("main.t".to_string(), 3), ("main.u".to_string(), 3)]
    );
    let records = lake.snapshots().unwrap();
    assert_eq!(records.len(), 2, "the encrypted manifests parse");
    assert_eq!(records[1].tables, 2);
    let loaded = lake.load(None).unwrap();
    assert_same_store(&db, &loaded);

    // A second keyed save opens a fresh session, appends v3, stays ciphertext.
    let mut db2 = loaded;
    db2.execute("INSERT INTO u VALUES (4);").unwrap();
    let lake2 = Lake::open_with_key(&lake_dir, key()).unwrap();
    assert_eq!(lake2.tables().unwrap().len(), 2);
    assert_eq!(save_store_with_key(&db2, &lake_dir, key()).unwrap(), 3);
    assert_everything_is_ciphertext(&lake_dir);
    let loaded = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap();
    assert_same_store(&db2, &loaded);
    // Time travel through the attach layer still reads older versions.
    let report = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .attach(None)
        .unwrap();
    assert!(report.encrypted);
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        rows(&mut attached, "SELECT COUNT(*) FROM u AT (VERSION => 2);"),
        vec![vec![Value::Bigint(3)]]
    );
    assert_eq!(
        rows(&mut attached, "SELECT COUNT(*) FROM u AT (VERSION => 3);"),
        vec![vec![Value::Bigint(4)]]
    );
    cleanup(&dir);
}

/// The `catalog.json == snapshots/<v>.json` byte invariant survives
/// encryption: the same sealed bytes are published to both, on full saves and
/// on incremental commits.
#[test]
fn catalog_is_byte_identical_to_its_snapshot_under_encryption() {
    let dir = scratch("invariant");
    let lake_dir = dir.join("lake");
    let mut db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();
    save_store_with_key(&db, &lake_dir, key()).unwrap();
    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    let snapshot = std::fs::read(lake_dir.join("snapshots/3.json")).unwrap();
    assert!(catalog.starts_with(MAGIC));
    assert_eq!(
        catalog, snapshot,
        "catalog.json != snapshots/3.json (bytes)"
    );

    // The incremental commit path seals once too: the same bytes reach the
    // snapshot link and the catalog replace.
    let mut wa =
        WritableAttachment::open_with_key(lake_dir.to_str().unwrap(), None, key()).unwrap();
    db.execute("INSERT INTO t VALUES (7, 'seven', 1.5);")
        .unwrap();
    let report = wa
        .commit(
            &db,
            &pink_lake::CommitSettings {
                retry_wait_ms: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.version, 4);
    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    let snapshot = std::fs::read(lake_dir.join("snapshots/4.json")).unwrap();
    assert_eq!(
        catalog, snapshot,
        "catalog.json != snapshots/4.json (bytes)"
    );
    assert!(catalog.starts_with(MAGIC));
    assert_everything_is_ciphertext(&lake_dir);

    // The commit delta and its data file are containers, and the version
    // reads back with the appended row through the decorator.
    let loaded = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap();
    let mut loaded = loaded;
    assert_eq!(
        rows(&mut loaded, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(4)]]
    );
    cleanup(&dir);
}

/// Incremental writes on an encrypted lake: a DELETE becomes an encrypted
/// delete file, time travel reads the pre-delete version, and a keeled open
/// still refuses.
#[test]
fn commits_deletes_and_time_travel_read_through_the_decorator() {
    let dir = scratch("commit");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();

    let mut wa =
        WritableAttachment::open_with_key(lake_dir.to_str().unwrap(), None, key()).unwrap();
    // Wave 68: inlining is on by default; this test pins the file artifacts.
    wa.set_data_inlining_row_limit(0);
    let mut session = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap();
    session.execute("DELETE FROM t WHERE i = 9;").unwrap();
    session
        .execute("INSERT INTO t VALUES (11, 'eleven', 2.5);")
        .unwrap();
    let report = wa
        .commit(
            &session,
            &pink_lake::CommitSettings {
                retry_wait_ms: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(report.committed, "{report:?}");
    assert_eq!(report.data_files, 1, "{report:?}");
    assert_eq!(report.delete_files, 1, "{report:?}");
    assert_eq!(report.rows_added, 1, "{report:?}");
    assert_eq!(report.rows_deleted, 1, "{report:?}");

    // Every artifact of the commit is ciphertext, including the `.pinkdel`.
    assert_everything_is_ciphertext(&lake_dir);
    let delete_files: Vec<PathBuf> = all_files(&lake_dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "pinkdel"))
        .collect();
    assert_eq!(delete_files.len(), 1, "{delete_files:?}");
    assert!(std::fs::read(&delete_files[0]).unwrap().starts_with(MAGIC));

    // The current version has the merged content; the old version is intact.
    let mut lake = Lake::open_with_key(&lake_dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut loaded = lake.load(None).unwrap();
    assert_eq!(
        rows(&mut loaded, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(3)]]
    );
    let report = lake.attach(None).unwrap();
    assert!(report.encrypted);
    assert!(!report.remote, "a local encrypted lake is not remote");
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        rows(&mut attached, "SELECT COUNT(*) FROM t AT (VERSION => 2);"),
        vec![vec![Value::Bigint(3)]]
    );
    assert_eq!(
        try_rows(
            &mut attached,
            "SELECT i FROM t AT (VERSION => 2) ORDER BY i;"
        )
        .unwrap(),
        vec![
            vec![Value::Integer(-5)],
            vec![Value::Integer(9)],
            vec![Value::Null],
        ]
    );
    assert_eq!(
        try_rows(&mut attached, "SELECT i, s FROM t ORDER BY i NULLS LAST;").unwrap(),
        vec![
            vec![Value::Integer(-5), Value::Varchar("hello".to_string())],
            vec![Value::Integer(11), Value::Varchar("eleven".to_string())],
            vec![Value::Null, Value::Null],
        ]
    );

    // A keyless open of the same directory is still refused, and a wrong key
    // is still `wrong key`.
    assert!(matches!(
        Lake::open(&lake_dir).unwrap_err(),
        LakeError::Encrypted { .. }
    ));
    assert!(matches!(
        Lake::open_with_key(&lake_dir, wrong_key()).unwrap_err(),
        LakeError::WrongKey { .. }
    ));
    cleanup(&dir);
}

/// Tampered, truncated, domain-confused and swapped containers are
/// authenticated errors — never a guess, never a partial read. A missing key
/// or a keyed read of a plaintext lake is refused.
#[test]
fn tampered_and_swapped_containers_are_authenticated_errors() {
    let dir = scratch("tamper");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();

    // Flip a payload byte of a data file: tampering, not a wrong key.
    let data_dir = lake_dir.join("data/main.t");
    let data_file = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .expect("one data file");
    let original = std::fs::read(&data_file).unwrap();
    let mut flipped = original.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    std::fs::write(&data_file, &flipped).unwrap();
    let err = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap_err();
    assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    assert!(err.to_string().contains("corrupt or tampered"), "{err}");

    // Truncate the container (terminal frame lost): tampering.
    std::fs::write(&data_file, &original[..original.len() - 20]).unwrap();
    let err = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap_err();
    assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");

    // Restore, then swap in *another* data file's container: the path binding
    // catches it (even though both decrypt with the same key).
    std::fs::write(&data_file, &original).unwrap();
    let other = lake_dir.join("data/main.u");
    let swapped = std::fs::read(&data_file).unwrap();
    // Write the t-file's container at the u-file's path: `load` must refuse
    // it (bound to another path).
    let u_file = std::fs::read_dir(&other)
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .expect("one u data file");
    std::fs::write(&u_file, &swapped).unwrap();
    let err = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .load(None)
        .unwrap_err();
    assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    assert!(
        err.to_string().contains("bound to"),
        "the swap must name the path binding: {err}"
    );

    // A header byte flip (the nonce prefix) fails the key-check: the error
    // says wrong key *or* a tampered header — indistinguishable in principle.
    let catalog_path = lake_dir.join("catalog.json");
    let mut catalog = std::fs::read(&catalog_path).unwrap();
    catalog[19] ^= 0x01;
    std::fs::write(&catalog_path, &catalog).unwrap();
    let err = Lake::open_with_key(&lake_dir, key()).unwrap_err();
    assert!(matches!(err, LakeError::WrongKey { .. }), "{err:?}");

    // A keyed open of a plaintext lake refuses to ignore the key.
    let plain_dir = dir.join("plain");
    save_store(&db, &plain_dir).unwrap();
    let err = Lake::open_with_key(&plain_dir, key()).unwrap_err();
    assert!(matches!(err, LakeError::NotEncrypted { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("not an encrypted pink-lake container"),
        "{err}"
    );
    let err = save_store_with_key(&db, &plain_dir, key()).unwrap_err();
    assert!(matches!(err, LakeError::NotEncrypted { .. }), "{err:?}");
    // ... and the plaintext lake is untouched by the refusal.
    assert_eq!(Lake::open(&plain_dir).unwrap().current_version(), 2);
    assert!(!is_container_file(&plain_dir.join("catalog.json")));
    cleanup(&dir);
}

/// A container that cannot be opened is never silently skipped: `versions()`
/// propagates the authenticated error instead of dropping the version.
#[test]
fn an_unreadable_snapshot_is_reported_not_skipped() {
    let dir = scratch("skip");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();
    save_store_with_key(&db, &lake_dir, key()).unwrap();

    let snapshot = lake_dir.join("snapshots/2.json");
    let mut bytes = std::fs::read(&snapshot).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    std::fs::write(&snapshot, &bytes).unwrap();

    let mut lake = Lake::open_with_key(&lake_dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let err = lake.versions().unwrap_err();
    assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    let err = lake.snapshots().unwrap_err();
    assert!(matches!(err, LakeError::Tampered { .. }), "{err:?}");
    cleanup(&dir);
}

/// [`tampered_and_swapped_containers_are_authenticated_errors`]'s sibling:
/// a create that adopts a surviving encrypted `snapshots/1.json` (a lost or
/// interrupted catalog) republishes the snapshot's **stored** bytes, so the
/// byte-identity invariant holds even across the adoption path.
#[test]
fn create_adoption_republishes_the_stored_ciphertext() {
    let dir = scratch("adopt");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();

    // Simulate "catalog lost, snapshot survived": a later create adopts the
    // empty v1 snapshot instead of rewriting it.
    std::fs::remove_file(lake_dir.join("catalog.json")).unwrap();
    let adopted = Lake::create_with_key(&lake_dir, key()).unwrap();
    assert_eq!(adopted.current_version(), 1);
    assert!(adopted.is_encrypted());
    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    let snapshot = std::fs::read(lake_dir.join("snapshots/1.json")).unwrap();
    assert!(catalog.starts_with(MAGIC));
    assert_eq!(
        catalog, snapshot,
        "the adopted catalog must republish the stored snapshot bytes"
    );
    // The adopted lake is readable with the key and still refuses a keyless
    // read.
    assert_eq!(adopted.versions().unwrap(), vec![1]);
    assert!(matches!(
        Lake::open(&lake_dir).unwrap_err(),
        LakeError::Encrypted { .. }
    ));
    // A concurrent-writer adoption that finds the snapshot is *not* the empty
    // v1 manifest refuses cleanly (the guard branch).
    std::fs::remove_file(lake_dir.join("catalog.json")).unwrap();
    std::fs::copy(
        lake_dir.join("snapshots/2.json"),
        lake_dir.join("snapshots/1.json"),
    )
    .unwrap();
    let err = Lake::create_with_key(&lake_dir, key()).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    cleanup(&dir);
}

/// Plaintext lakes keep working everywhere, unchanged and unencrypted, and an
/// existing plaintext lake is never silently rewritten as encrypted.
#[test]
fn plaintext_lakes_are_unaffected() {
    let dir = scratch("plaintext");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store(&db, &lake_dir).unwrap();

    let catalog = std::fs::read(lake_dir.join("catalog.json")).unwrap();
    assert!(catalog.starts_with(b"{"), "a plaintext catalog is JSON");
    let mut lake = Lake::open(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    assert!(!lake.is_encrypted());
    assert_same_store(&db, &lake.load(None).unwrap());
    assert_eq!(lake.versions().unwrap(), vec![1, 2]);

    // A keyed open refuses; `open_or_create_with_key` refuses too (never a
    // silent rewrite), and the lake stays exactly as it was.
    assert!(matches!(
        Lake::open_with_key(&lake_dir, key()).unwrap_err(),
        LakeError::NotEncrypted { .. }
    ));
    assert!(matches!(
        Lake::open_or_create_with_key(&lake_dir, key()).unwrap_err(),
        LakeError::NotEncrypted { .. }
    ));
    assert_eq!(lake.current_version(), 2);
    assert!(!is_container_file(&lake_dir.join("catalog.json")));
    assert_same_store(&db, &Lake::open(&lake_dir).unwrap().load(None).unwrap());

    // A fresh keyed lake beside the plaintext one works.
    let enc_dir = dir.join("enc");
    save_store_with_key(&db, &enc_dir, key()).unwrap();
    assert_everything_is_ciphertext(&enc_dir);
    assert_same_store(
        &db,
        &Lake::open_with_key(&enc_dir, key())
            .unwrap()
            .load(None)
            .unwrap(),
    );
    cleanup(&dir);
}

/// Attach on an encrypted lake: the manifests' statistics (Vortex footers, or
/// the lean `.pink` stats blocks riding inside the encrypted manifest) answer
/// exact `COUNT`/`MIN`/`MAX` with **no data-file decode**; materializing a
/// column still decodes exactly once.
#[test]
fn stats_fast_paths_on_an_encrypted_lake_decode_no_data_file() {
    let dir = scratch("stats");
    let lake_dir = dir.join("lake");
    let db = fixture_store();
    save_store_with_key(&db, &lake_dir, key()).unwrap();

    let report = Lake::open_with_key(&lake_dir, key())
        .unwrap()
        .attach(None)
        .unwrap();
    assert!(report.encrypted);
    #[cfg(not(feature = "vortex-format"))]
    assert!(
        report.statistics_only,
        "a lean encrypted attach reads no data file"
    );
    let mut attached = PinkDb::new();
    attached.attach_lake(report.lake.clone()).unwrap();
    assert_eq!(
        rows(&mut attached, "SELECT COUNT(*), MIN(i), MAX(i) FROM t;"),
        vec![vec![
            Value::Bigint(3),
            Value::Integer(-5),
            Value::Integer(9)
        ]]
    );
    assert_eq!(
        report.lake.decode_stats().0,
        0,
        "statistics-only answers must not decode a data file"
    );
    // A real scan materializes (and caches): exactly one decode.
    assert_eq!(
        rows(&mut attached, "SELECT i FROM t ORDER BY i NULLS LAST;"),
        vec![
            vec![Value::Integer(-5)],
            vec![Value::Integer(9)],
            vec![Value::Null]
        ]
    );
    assert_eq!(report.lake.decode_stats().0, 1);
    cleanup(&dir);
}

/// The S3 ciphertext CAS contract: over the hermetic mock, a keyed save's
/// conditional publishes operate on the sealed bytes (catalog == snapshot,
/// every object a container), a stale writer's save still conflicts, and
/// keyless/wrong-key opens still fail closed.
#[cfg(feature = "s3")]
fn mock_s3_config(mock: &mock_s3::MockS3, bucket: &str, prefix: &str) -> S3Config {
    S3Config::new(bucket)
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret")
}

#[cfg(feature = "s3")]
#[test]
fn s3_conditional_writes_operate_on_ciphertext() {
    use mock_s3::MockS3;

    let mock = MockS3::start();
    let config = mock_s3_config(&mock, "bucket", "enc");
    let db = fixture_store();
    save_store_s3_with_key(&db, config.clone(), key()).unwrap();

    let catalog = mock.object("enc/catalog.json").expect("catalog object");
    let snapshot = mock
        .object("enc/snapshots/2.json")
        .expect("snapshot object");
    assert!(catalog.starts_with(MAGIC));
    assert_eq!(catalog, snapshot, "catalog != snapshot over S3");
    assert!(mock.keys().iter().any(|k| k.starts_with("enc/data/")));
    for k in mock.keys() {
        let bytes = mock.object(&k).unwrap();
        assert!(bytes.starts_with(MAGIC), "{k} is not ciphertext");
    }
    assert!(
        mock.conditional_attempts() > 0,
        "the conditional writes must be exercised on the sealed bytes"
    );

    // The keyed handle reads it back; keyless and wrong-key opens fail closed.
    let loaded = Lake::open_s3_with_key(config.clone(), key())
        .unwrap()
        .load(None)
        .unwrap();
    assert_same_store(&db, &loaded);
    assert!(matches!(
        Lake::open_s3(config.clone()).unwrap_err(),
        LakeError::Encrypted { .. }
    ));
    assert!(matches!(
        Lake::open_s3_with_key(config.clone(), wrong_key()).unwrap_err(),
        LakeError::WrongKey { .. }
    ));

    // A stale writer's save still loses the conditional publish: the winner
    // publishes v3 on the sealed bytes, the loser's v3 attempt conflicts.
    let mut winner = Lake::open_s3_with_key(config.clone(), key()).unwrap();
    let mut loser = Lake::open_s3_with_key(config.clone(), key()).unwrap();
    assert_eq!(winner.write_from_store(&loaded).unwrap(), 3);
    let err = loser.write_from_store(&loaded).unwrap_err();
    assert!(matches!(err, LakeError::Conflict { .. }), "{err:?}");
    let catalog = mock.object("enc/catalog.json").unwrap();
    let snapshot = mock.object("enc/snapshots/3.json").unwrap();
    assert_eq!(catalog, snapshot, "catalog != snapshot after the race");
    assert!(catalog.starts_with(MAGIC));
}

/// The degraded (check-then-put) S3 fallback repairs the snapshot pair from
/// the catalog's **stored** bytes: with a key that means the raw ciphertext,
/// not the decrypted copy (sealing the copy again would mint a different
/// container and break `catalog == snapshot`).
#[cfg(feature = "s3")]
#[test]
fn s3_degraded_fallback_reconciles_the_encrypted_pair_from_stored_bytes() {
    use mock_s3::MockS3;
    use std::time::Duration;

    let mock = MockS3::start();
    mock.reject_conditional_writes(true);
    let config = || mock_s3_config(&mock, "enc-degraded", "lake").allow_degraded(true);
    let db = fixture_store();
    save_store_s3_with_key(&db, config(), key()).unwrap();
    let v2_catalog = mock.object("lake/catalog.json").unwrap();
    assert!(v2_catalog.starts_with(MAGIC));
    assert_eq!(mock.object("lake/snapshots/2.json").unwrap(), v2_catalog);

    // Park the next save's catalog PUT, overwrite the just-published snapshot
    // with foreign bytes while it waits, then let the PUT through: the
    // post-replace verification must restore the snapshot to the stored
    // catalog container.
    let v3_snapshot_seen = {
        mock.hold_next_request("PUT", "catalog.json");
        let handle = std::thread::spawn({
            let db = fixture_store();
            let config = config();
            move || {
                let mut lake = Lake::open_s3_with_key(config, key()).unwrap();
                lake.set_data_inlining_row_limit(0);
                lake.write_from_store(&db)
            }
        });
        assert!(
            mock.wait_until_hold_reached(Duration::from_secs(10)),
            "the save's catalog PUT never arrived"
        );
        let published = mock.object("lake/snapshots/3.json").expect("v3 snapshot");
        mock.put_object("lake/snapshots/3.json", b"{\"not\":\"a container\"}\n");
        mock.release_hold();
        let result = handle.join().unwrap();
        assert!(result.is_ok(), "the degraded save must succeed: {result:?}");
        published
    };

    let catalog = mock.object("lake/catalog.json").unwrap();
    assert_eq!(catalog, v3_snapshot_seen, "the pair must be reconciled");
    assert_eq!(
        mock.object("lake/snapshots/3.json").unwrap(),
        catalog,
        "the snapshot must be the stored catalog ciphertext, not a re-sealed copy"
    );
    // The reconciled pair still opens and reads with the key.
    let reader = Lake::open_s3_with_key(config(), key()).unwrap();
    assert_eq!(reader.current_version(), 3);
    assert!(!reader.tables().unwrap().is_empty());
    let loaded = reader.load(None).unwrap();
    assert_eq!(loaded.list_databases().len(), 1);
}

// ---------------------------------------------------------------------------
// Maintenance (wave 67) on an encrypted lake.
// ---------------------------------------------------------------------------

#[test]
fn encrypted_lake_maintenance_round_trip() {
    use pink_lake::{CleanupSettings, Commit, ExpireSettings, MergeSettings};
    use std::sync::Arc;

    let dir = scratch("maint");
    let lake_dir = dir.join("lake");
    // Three commits → three encrypted data-file objects.
    let mut lake = Lake::create_with_key(&lake_dir, key()).unwrap();
    // Wave 68: inlining is on by default; this test pins file artifacts.
    lake.set_data_inlining_row_limit(0);
    let sub = |rows: &[(i32, &str)]| {
        let mut t = pinkdb::DataTable::new(
            "t".to_string(),
            vec![
                ("id".to_string(), pinkdb::DataType::Integer),
                ("s".to_string(), pinkdb::DataType::Varchar),
            ],
        )
        .unwrap();
        for (id, s) in rows {
            t.append_row(vec![Value::Integer(*id), Value::Varchar(s.to_string())])
                .unwrap();
        }
        Arc::new(t)
    };
    for chunk in [&[(1, "a"), (2, "b")][..], &[(3, "c")], &[(4, "d")]] {
        let mut commit = Commit::new();
        commit.append("main.t", sub(chunk));
        lake.commit(&commit).unwrap();
    }

    // Merge: the new data file is sealed like every other object, the
    // sources stay ciphertext-identical, and stats-fast attach still works
    // (COUNT/MIN/MAX from the sealed manifest, zero decodes).
    use std::collections::BTreeMap;
    let source_bytes: BTreeMap<PathBuf, Vec<u8>> = data_files(&lake_dir)
        .into_iter()
        .map(|p| {
            let bytes = std::fs::read(&p).unwrap();
            (p, bytes)
        })
        .collect();
    let report = lake
        .merge_adjacent_files(None, &MergeSettings::default())
        .unwrap();
    assert!(report.committed);
    assert_eq!(report.files_processed(), 3);
    for (path, bytes) in &source_bytes {
        assert_eq!(&std::fs::read(path).unwrap(), bytes, "{}", path.display());
    }
    assert_eq!(data_files(&lake_dir).len(), 4, "3 sources + 1 merged file");
    let report = Lake::open_with_key(&lake_dir, key()).unwrap().attach(None);
    let report = report.unwrap();
    assert!(report.encrypted);
    assert_eq!(report.tables, 1);
    let mut db = PinkDb::new();
    db.attach_lake(report.lake.clone()).unwrap();
    let counts = db
        .execute("SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
        .unwrap();
    match counts.first().unwrap() {
        QueryResult::Selected { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("{other:?}"),
    }

    // Expire the pre-merge snapshots (their sealed objects disappear), then
    // clean the unreferenced sources: dry run deletes nothing, confirm
    // deletes the ciphertext containers.
    let mut lake = Lake::open_with_key(&lake_dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.expire_snapshots(&ExpireSettings {
        retain_last: Some(1),
        ..ExpireSettings::default()
    })
    .unwrap();
    assert_eq!(lake.versions().unwrap().len(), 1);
    let dry = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            dry_run: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert_eq!(dry.paths.len(), 3, "{dry:?}");
    for path in &dry.paths {
        assert!(lake_dir.join(path).exists(), "dry run deleted {path}");
    }
    let real = lake
        .cleanup_old_files(&CleanupSettings {
            all: true,
            confirm: true,
            ..CleanupSettings::default()
        })
        .unwrap();
    assert!(real.deleted);
    for path in &real.paths {
        assert!(!lake_dir.join(path).exists(), "{path} survived");
    }
    // The keyed lake still opens and reads; a keyless open still refuses.
    let mut lake = Lake::open_with_key(&lake_dir, key()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let loaded = lake.load(None).unwrap();
    assert_eq!(loaded.table("t").unwrap().num_rows(), 4);
    let err = Lake::open(&lake_dir).unwrap_err();
    assert!(matches!(err, LakeError::Encrypted { .. }), "{err:?}");
    // The maintenance report itself never leaked plaintext container bytes:
    // every data object starts with the magic.
    for path in data_files(&lake_dir) {
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..8], MAGIC, "{}", path.display());
    }
    cleanup(&dir);
}

/// Every `data/` file of a lake (recursively).
fn data_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(&e.path(), out);
            } else {
                out.push(e.path());
            }
        }
    }
    let mut out = Vec::new();
    let data = dir.join("data");
    if data.exists() {
        walk(&data, &mut out);
    }
    out.sort();
    out
}
