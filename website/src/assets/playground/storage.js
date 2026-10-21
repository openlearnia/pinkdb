// Storage adapters for the playground. One interface:
//
//   { name, available() -> {ok, reason}, load() -> Uint8Array|null,
//     save(bytes), clear() }
//
// All three are used from the worker. Memory keeps nothing; IndexedDB writes
// one key in one readwrite transaction (atomic); OPFS writes a temp file,
// flushes it, and swaps it into place (atomic rename when FileSystemFileHandle
// .move() exists, otherwise a versioned file plus a pointer written last).

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// ---------------------------------------------------------------- memory

export function createMemoryAdapter() {
  return {
    name: "memory",
    async available() {
      return { ok: true };
    },
    async load() {
      return null;
    },
    async save() {},
    async clear() {},
  };
}

// ---------------------------------------------------------------- IndexedDB

const DB_NAME = "pinkdb-playground";
const STORE_NAME = "snapshots";
const SNAPSHOT_KEY = "main";

let dbPromise = null;

function openDatabase() {
  if (dbPromise) {
    return dbPromise;
  }
  dbPromise = new Promise((resolve, reject) => {
    if (typeof indexedDB === "undefined") {
      reject(new Error("IndexedDB is not available in this browser"));
      return;
    }
    let request;
    try {
      request = indexedDB.open(DB_NAME, 1);
    } catch (err) {
      reject(err);
      return;
    }
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    request.onsuccess = () => {
      const db = request.result;
      db.onversionchange = () => {
        db.close();
        dbPromise = null;
      };
      db.onclose = () => {
        dbPromise = null;
      };
      resolve(db);
    };
    request.onerror = () => reject(request.error || new Error("IndexedDB could not be opened"));
    request.onblocked = () => reject(new Error("IndexedDB is blocked by another tab"));
  });
  dbPromise.catch(() => {
    dbPromise = null;
  });
  return dbPromise;
}

function transactionDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error || new Error("IndexedDB transaction failed"));
    transaction.onabort = () => reject(transaction.error || new Error("IndexedDB transaction was aborted"));
  });
}

export function createIndexedDbAdapter() {
  return {
    name: "indexeddb",

    async available() {
      if (typeof indexedDB === "undefined") {
        return { ok: false, reason: "IndexedDB is not available in this browser" };
      }
      try {
        await openDatabase();
        return { ok: true };
      } catch (err) {
        return { ok: false, reason: err.message || String(err) };
      }
    },

    async load() {
      const db = await openDatabase();
      const value = await new Promise((resolve, reject) => {
        const transaction = db.transaction(STORE_NAME, "readonly");
        const request = transaction.objectStore(STORE_NAME).get(SNAPSHOT_KEY);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error("IndexedDB read failed"));
      });
      if (value == null) {
        return null;
      }
      if (value instanceof Uint8Array) {
        return value;
      }
      if (value instanceof ArrayBuffer) {
        return new Uint8Array(value);
      }
      if (ArrayBuffer.isView(value)) {
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
      }
      return null;
    },

    async save(bytes) {
      const db = await openDatabase();
      const transaction = db.transaction(STORE_NAME, "readwrite");
      transaction.objectStore(STORE_NAME).put(bytes, SNAPSHOT_KEY);
      await transactionDone(transaction);
    },

    async clear() {
      const db = await openDatabase();
      const transaction = db.transaction(STORE_NAME, "readwrite");
      transaction.objectStore(STORE_NAME).delete(SNAPSHOT_KEY);
      await transactionDone(transaction);
    },
  };
}

// ---------------------------------------------------------------- OPFS
//
// File layout:
//   db.pink          the current snapshot when rename-by-move is available
//   db.pink.tmp      the in-progress write (flushed, then moved/renamed)
//   db-<stamp>.pink  a versioned snapshot when move() is unavailable
//   current          pointer to the live versioned file, written last
//
// load() prefers the pointer so a save that fell back to versioned files is
// never masked by an older db.pink.

const MAIN_FILE = "db.pink";
const TEMP_FILE = "db.pink.tmp";
const POINTER_FILE = "current";
const VERSION_RE = /^db-.+\.pink$/;

let versionCounter = 0;

async function rootDirectory() {
  if (typeof navigator === "undefined"
      || !navigator.storage
      || typeof navigator.storage.getDirectory !== "function") {
    throw new Error("the Origin Private File System is not available");
  }
  return navigator.storage.getDirectory();
}

function hasSyncAccessHandles() {
  const ctor = globalThis.FileSystemFileHandle;
  return !!ctor && typeof ctor.prototype.createSyncAccessHandle === "function";
}

async function removeIfPresent(directory, name) {
  try {
    await directory.removeEntry(name);
  } catch (err) {
    if (!err || err.name !== "NotFoundError") {
      throw err;
    }
  }
}

async function readFile(directory, name) {
  let handle;
  try {
    handle = await directory.getFileHandle(name);
  } catch (err) {
    if (err && err.name === "NotFoundError") {
      return null;
    }
    throw err;
  }
  const access = await handle.createSyncAccessHandle();
  try {
    const size = access.getSize();
    if (size === 0) {
      return new Uint8Array(0);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    while (offset < size) {
      const read = access.read(bytes, { at: offset });
      if (read === 0) {
        break;
      }
      offset += read;
    }
    return offset === size ? bytes : bytes.slice(0, offset);
  } finally {
    access.close();
  }
}

async function writeFile(directory, name, bytes) {
  const handle = await directory.getFileHandle(name, { create: true });
  const access = await handle.createSyncAccessHandle();
  try {
    access.truncate(0);
    let offset = 0;
    while (offset < bytes.length) {
      const written = access.write(bytes, { at: offset });
      if (!written) {
        throw new Error("the OPFS write made no progress");
      }
      offset += written;
    }
    access.flush();
  } finally {
    access.close();
  }
  return handle;
}

async function versionedFiles(directory) {
  const names = [];
  for await (const name of directory.keys()) {
    if (VERSION_RE.test(name)) {
      names.push(name);
    }
  }
  return names;
}

async function cleanupVersions(directory, keep) {
  for (const name of await versionedFiles(directory)) {
    if (name !== keep) {
      await removeIfPresent(directory, name);
    }
  }
}

async function clearFiles(directory) {
  await removeIfPresent(directory, TEMP_FILE);
  await removeIfPresent(directory, POINTER_FILE);
  await removeIfPresent(directory, MAIN_FILE);
  await cleanupVersions(directory, null);
}

export function createOpfsAdapter() {
  return {
    name: "opfs",

    async available() {
      if (typeof navigator === "undefined"
          || !navigator.storage
          || typeof navigator.storage.getDirectory !== "function") {
        return { ok: false, reason: "the Origin Private File System is not available" };
      }
      if (!hasSyncAccessHandles()) {
        return {
          ok: false,
          reason: "OPFS synchronous access handles are not available in this browser",
        };
      }
      try {
        await rootDirectory();
        return { ok: true };
      } catch (err) {
        return { ok: false, reason: err.message || String(err) };
      }
    },

    async load() {
      const directory = await rootDirectory();
      const pointer = await readFile(directory, POINTER_FILE);
      if (pointer && pointer.length > 0) {
        const name = textDecoder.decode(pointer).trim();
        if (name) {
          const bytes = await readFile(directory, name);
          if (bytes) {
            return bytes;
          }
        }
      }
      return await readFile(directory, MAIN_FILE);
    },

    async save(bytes) {
      const directory = await rootDirectory();
      await removeIfPresent(directory, TEMP_FILE);
      await writeFile(directory, TEMP_FILE, bytes);

      const temp = await directory.getFileHandle(TEMP_FILE);
      if (typeof temp.move === "function") {
        try {
          await temp.move(MAIN_FILE);
          await removeIfPresent(directory, POINTER_FILE);
          await cleanupVersions(directory, null);
          return;
        } catch (err) {
          // fall through to the versioned layout
        }
      }

      const version = `db-${Date.now().toString(36)}-${versionCounter++}.pink`;
      await writeFile(directory, version, bytes);
      // The pointer is the commit point: readers see the old file until the
      // complete new version is in place and the pointer is written.
      await writeFile(directory, POINTER_FILE, textEncoder.encode(version));
      await removeIfPresent(directory, MAIN_FILE);
      await removeIfPresent(directory, TEMP_FILE);
      await cleanupVersions(directory, version);
    },

    async clear() {
      const directory = await rootDirectory();
      await clearFiles(directory);
    },
  };
}
