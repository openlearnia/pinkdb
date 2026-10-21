//! M-5: durable engine artifacts are owner-only (0600) on Unix.
//!
//! The WAL stores the literal SQL text of every committed transaction, so a
//! world-readable WAL leaks the whole database (Appendix A.9). These tests
//! cover a fresh durable database, the checkpoint `.tmp` staging path, and
//! the best-effort migration that narrows an artifact a pre-fix build left at
//! 0644.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::PinkDb;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    ckpt: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_file_modes_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ckpt = dir.join("db.pink");
        Self { dir, ckpt }
    }

    fn path(&self) -> &str {
        self.ckpt.to_str().unwrap()
    }

    fn wal(&self) -> PathBuf {
        PathBuf::from(format!("{}.wal", self.ckpt.display()))
    }

    fn lock(&self) -> PathBuf {
        PathBuf::from(format!("{}.lock", self.ckpt.display()))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("metadata {}: {e}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

#[test]
fn fresh_durable_artifacts_are_owner_only() {
    let s = Scratch::new("fresh");
    {
        let mut db = PinkDb::open_durable(s.path()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (42);").unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(mode(&s.wal()), 0o600, "WAL {}", s.wal().display());
    assert_eq!(mode(&s.ckpt), 0o600, "checkpoint {}", s.ckpt.display());
    assert_eq!(mode(&s.lock()), 0o600, "lock {}", s.lock().display());
}

/// The Appendix A.9 reproduction, as a regression test: a database created
/// before the fix (durable artifacts 0644) stops leaking on the next open.
#[test]
fn existing_world_readable_artifacts_are_tightened_on_open() {
    let s = Scratch::new("tighten");
    {
        let mut db = PinkDb::open_durable(s.path()).unwrap();
        db.execute("CREATE TABLE secret (password VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO secret VALUES ('hunter2');")
            .unwrap();
        db.checkpoint().unwrap();
    }
    for path in [s.wal(), s.ckpt.clone(), s.lock()] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(mode(&path), 0o644);
    }
    {
        // Opening runs the migration before any read; the WAL/checkpoint/lock
        // must be owner-only afterwards even though the process did not
        // create them.
        let db = PinkDb::open_durable(s.path()).unwrap();
        drop(db);
    }
    assert_eq!(mode(&s.wal()), 0o600, "existing WAL was not tightened");
    assert_eq!(
        mode(&s.ckpt),
        0o600,
        "existing checkpoint was not tightened"
    );
    assert_eq!(mode(&s.lock()), 0o600, "existing lock was not tightened");
}
