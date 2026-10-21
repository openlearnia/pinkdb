// pinkdb in the browser: a hand-written ES module over the wasm C ABI.
//
// The module in ./pinkdb.wasm is a plain wasm32-unknown-unknown cdylib (no
// wasm-bindgen). Every string and every snapshot crosses as a byte range in
// linear memory, and every buffer the module returns is length-prefixed and
// must be freed by us:
//
//   [u32 LE length][payload]              -> free with pw_free(ptr, 4 + length)
//   [u32 0xFFFFFFFF][u32 len][message]    -> free with pw_free(ptr, 8 + len)
//
// The last shape is only used for a failed pw_snapshot. Memory can grow on
// any call, so memory.buffer is re-read after every wasm call; a cached
// Uint8Array would go stale (detached) after growth.

const MAX_U32 = 0xffffffff;
const JSON_EXPORTS = ["pw_alloc", "pw_free", "pw_open", "pw_close", "pw_exec", "pw_snapshot", "pw_restore", "pw_version", "memory"];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Write `bytes` into a fresh wasm allocation. Returns {ptr, len}. */
function allocBytes(exports, bytes) {
  if (bytes.length === 0) {
    return { ptr: 0, len: 0 };
  }
  const ptr = exports.pw_alloc(bytes.length);
  if (!ptr) {
    throw new Error("pinkdb wasm could not allocate memory");
  }
  new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
  return { ptr, len: bytes.length };
}

/** Read and free a `[u32 length][UTF-8 JSON]` buffer. */
function readJsonBuffer(exports, ptr) {
  if (!ptr) {
    throw new Error("pinkdb returned a null buffer");
  }
  const length = new DataView(exports.memory.buffer).getUint32(ptr, true);
  if (length === MAX_U32) {
    const errorLength = new DataView(exports.memory.buffer).getUint32(ptr + 4, true);
    const message = decoder.decode(
      new Uint8Array(exports.memory.buffer, ptr + 8, errorLength),
    );
    exports.pw_free(ptr, 8 + errorLength);
    throw new Error(message || "pinkdb could not read the snapshot");
  }
  const json = decoder.decode(
    new Uint8Array(exports.memory.buffer, ptr + 4, length),
  );
  exports.pw_free(ptr, 4 + length);
  return JSON.parse(json);
}

/** One open database handle on one wasm instance. */
export class Engine {
  constructor(exports) {
    this.exports = exports;
    this.handle = 0;
  }

  /** Open a fresh in-memory database and return its handle. */
  open() {
    if (this.handle) {
      this.close();
    }
    const handle = this.exports.pw_open();
    if (!handle) {
      throw new Error("pinkdb could not open a database");
    }
    this.handle = handle;
    return handle;
  }

  /** Close the handle; safe to call twice. */
  close() {
    if (this.handle) {
      this.exports.pw_close(this.handle);
      this.handle = 0;
    }
  }

  requireHandle() {
    if (!this.handle) {
      throw new Error("the database is not open");
    }
    return this.handle;
  }

  /**
   * Run every statement in `sql`.
   * Returns the engine's parsed JSON: {ok:true, results:[...]} or
   * {ok:false, error:"..."}.
   */
  run(sql) {
    const handle = this.requireHandle();
    const input = allocBytes(this.exports, encoder.encode(sql));
    let out = 0;
    try {
      out = this.exports.pw_exec(handle, input.ptr, input.len);
    } finally {
      if (input.ptr) {
        this.exports.pw_free(input.ptr, input.len);
      }
    }
    return readJsonBuffer(this.exports, out);
  }

  /**
   * Serialize the database to `.pink` bytes.
   * Throws an Error carrying the engine's message when it refuses.
   */
  snapshot() {
    const handle = this.requireHandle();
    const out = this.exports.pw_snapshot(handle);
    if (!out) {
      throw new Error("pinkdb returned a null snapshot");
    }
    const length = new DataView(this.exports.memory.buffer).getUint32(out, true);
    if (length === MAX_U32) {
      const errorLength = new DataView(this.exports.memory.buffer).getUint32(out + 4, true);
      const message = decoder.decode(
        new Uint8Array(this.exports.memory.buffer, out + 8, errorLength),
      );
      this.exports.pw_free(out, 8 + errorLength);
      throw new Error(message || "pinkdb could not write a snapshot");
    }
    const bytes = new Uint8Array(
      this.exports.memory.buffer,
      out + 4,
      length,
    ).slice();
    this.exports.pw_free(out, 4 + length);
    return bytes;
  }

  /**
   * Replace this handle's database with snapshot `bytes`.
   * Returns the engine's parsed JSON: {ok:true} or {ok:false, error:"..."}.
   * The current database is kept when the snapshot is refused.
   */
  restore(bytes) {
    const handle = this.requireHandle();
    const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const input = allocBytes(this.exports, data);
    let out = 0;
    try {
      out = this.exports.pw_restore(handle, input.ptr, input.len);
    } finally {
      if (input.ptr) {
        this.exports.pw_free(input.ptr, input.len);
      }
    }
    return readJsonBuffer(this.exports, out);
  }

  /** The engine version string. */
  version() {
    const parsed = readJsonBuffer(this.exports, this.exports.pw_version());
    if (!parsed || typeof parsed.version !== "string") {
      throw new Error("pinkdb returned no version");
    }
    return parsed.version;
  }
}

/** Fetch and instantiate the wasm module, then return an Engine over it. */
export async function load(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`could not fetch pinkdb.wasm (HTTP ${response.status})`);
  }
  const bytes = await response.arrayBuffer();
  let instance;
  try {
    ({ instance } = await WebAssembly.instantiate(bytes, {}));
  } catch (err) {
    throw new Error(`could not instantiate pinkdb.wasm: ${err.message || err}`);
  }
  const exports = instance.exports;
  for (const name of JSON_EXPORTS) {
    if (!(name in exports)) {
      throw new Error(`pinkdb.wasm is missing the ${name} export`);
    }
  }
  return new Engine(exports);
}
