//! Embedding caches: an in-process LRU bounded in bytes, and an optional
//! content-addressed directory of the same vectors.
//!
//! A key is (base URL, model, dimensions, text): the endpoint identity is
//! part of it so changing the provider or its width cannot serve a vector
//! from the wrong model, and only the text's SHA-256 reaches the cache
//! directory's file names (the text itself never does).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use sha2::{Digest, Sha256};

use crate::client::Config;

/// 64 MiB holds about 16 thousand 1024-wide vectors, more than a
/// brute-force search over a table of this size needs.
const MEMORY_BYTES: usize = 64 * 1024 * 1024;

struct Entry {
    vector: Arc<[f32]>,
    /// Last use, for evicting the least recently used entry.
    used: u64,
}

struct Lru {
    entries: HashMap<[u8; 32], Entry>,
    bytes: usize,
    clock: u64,
}

static MEMORY: OnceLock<Mutex<Lru>> = OnceLock::new();

fn memory() -> std::sync::MutexGuard<'static, Lru> {
    MEMORY
        .get_or_init(|| {
            Mutex::new(Lru {
                entries: HashMap::new(),
                bytes: 0,
                clock: 0,
            })
        })
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The cached vector for `text`, from memory or the cache directory.
pub(crate) fn get(config: &Config, text: &str) -> Option<Arc<[f32]>> {
    let key = key(config, text);
    let mut lru = memory();
    lru.clock += 1;
    let clock = lru.clock;
    if let Some(entry) = lru.entries.get_mut(&key) {
        entry.used = clock;
        return Some(entry.vector.clone());
    }
    drop(lru);
    let vector: Arc<[f32]> = read_file(config, &key)?.into();
    insert(&mut memory(), key, vector.clone());
    Some(vector)
}

/// Remember `vector` for `text`; a read-only statement never writes the
/// store, so this only touches memory and the cache directory.
pub(crate) fn put(config: &Config, text: &str, vector: Arc<[f32]>) {
    let key = key(config, text);
    insert(&mut memory(), key, vector.clone());
    write_file(config, &key, &vector);
}

fn insert(lru: &mut Lru, key: [u8; 32], vector: Arc<[f32]>) {
    lru.clock += 1;
    if let Some(old) = lru.entries.insert(
        key,
        Entry {
            vector: vector.clone(),
            used: lru.clock,
        },
    ) {
        lru.bytes -= old.vector.len() * 4;
    }
    lru.bytes += vector.len() * 4;
    while lru.bytes > MEMORY_BYTES {
        let Some((&oldest, _)) = lru.entries.iter().min_by_key(|(_, entry)| entry.used) else {
            break;
        };
        if let Some(old) = lru.entries.remove(&oldest) {
            lru.bytes -= old.vector.len() * 4;
        }
    }
}

fn key(config: &Config, text: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(config.base_url.as_bytes());
    hasher.update([0]);
    hasher.update(config.model.as_bytes());
    hasher.update([0]);
    hasher.update(config.dimensions.to_le_bytes());
    hasher.update([0]);
    hasher.update(text.as_bytes());
    hasher.finalize().into()
}

fn hex(key: &[u8; 32]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn read_file(config: &Config, key: &[u8; 32]) -> Option<Vec<f32>> {
    let bytes = std::fs::read(config.cache_dir.as_ref()?.join(hex(key))).ok()?;
    if bytes.len() != config.dimensions as usize * 4 {
        return None;
    }
    Some(
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect(),
    )
}

/// Write through a temporary file in the same directory, so a crash leaves
/// either the old file or the new one, never a torn vector.
fn write_file(config: &Config, key: &[u8; 32], vector: &[f32]) {
    let Some(directory) = &config.cache_dir else {
        return;
    };
    if std::fs::create_dir_all(directory).is_err() {
        return;
    }
    let name = hex(key);
    let temporary = directory.join(format!(".{name}.tmp-{}", std::process::id()));
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    if std::fs::write(&temporary, &bytes).is_ok()
        && std::fs::rename(&temporary, directory.join(&name)).is_err()
    {
        let _ = std::fs::remove_file(&temporary);
    }
}
