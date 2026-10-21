#!/usr/bin/env node
// Dependency-free smoke test for the docs playground's wasm module.
//
// Loads website/src/assets/playground/pinkdb.wasm with WebAssembly and
// exercises every export of the ABI: version, open, exec (DDL/DML/SELECT with
// the JSON value mapping), an engine error, snapshot -> restore into a second
// handle, a refused corrupt snapshot, and close. Exits non-zero if any check
// fails.

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const WASM_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../src/assets/playground/pinkdb.wasm",
);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function assertEqual(actual, expected, what) {
  const a = JSON.stringify(actual);
  const b = JSON.stringify(expected);
  if (a !== b) {
    throw new Error(`${what}: expected ${b}, got ${a}`);
  }
}

function loadModule() {
  const bytes = readFileSync(WASM_PATH);
  const module = new WebAssembly.Module(bytes);
  const imports = {};

  // This crate is plain Rust (`std` on wasm32-unknown-unknown, no
  // wasm-bindgen), so the module should import nothing. If a build ever grows
  // an import (an env hook, say), provide the minimal stubs below so the
  // script still loads; the behavioural checks would expose a broken build.
  const required = WebAssembly.Module.imports(module);
  if (required.length > 0) {
    console.warn(
      `note: module imports ${required.length} host item(s); using minimal stubs: ` +
        required.map((i) => `${i.module}.${i.name} (${i.kind})`).join(", "),
    );
  }
  for (const imp of required) {
    imports[imp.module] ??= {};
    if (imp.kind === "function") {
      imports[imp.module][imp.name] = () => 0;
    } else if (imp.kind === "memory") {
      imports[imp.module][imp.name] = new WebAssembly.Memory({ initial: 256 });
    } else if (imp.kind === "global") {
      imports[imp.module][imp.name] = new WebAssembly.Global(
        { value: "i32", mutable: true },
        0,
      );
    } else {
      throw new Error(`unsupported wasm import ${imp.module}.${imp.name} (${imp.kind})`);
    }
  }

  return new WebAssembly.Instance(module, imports);
}

function main() {
  const exports = loadModule().exports;
  for (const name of [
    "memory",
    "pw_alloc",
    "pw_free",
    "pw_open",
    "pw_close",
    "pw_exec",
    "pw_snapshot",
    "pw_restore",
    "pw_version",
  ]) {
    assert(name in exports, `missing wasm export ${name}`);
  }
  const e = exports;

  function writeBytes(bytes) {
    const ptr = e.pw_alloc(bytes.length);
    assert(ptr !== 0, "pw_alloc returned NULL");
    new Uint8Array(e.memory.buffer, ptr, bytes.length).set(bytes);
    return { ptr, len: bytes.length };
  }

  // `[u32 LE length][UTF-8 JSON]`; always free with 4 + length.
  function readJson(ptr) {
    assert(ptr !== 0, "expected a JSON buffer, got NULL");
    const length = new DataView(e.memory.buffer).getUint32(ptr, true);
    assert(length !== 0xffffffff, "expected a JSON buffer, got an error buffer");
    const json = textDecoder.decode(new Uint8Array(e.memory.buffer, ptr + 4, length));
    e.pw_free(ptr, 4 + length);
    return JSON.parse(json);
  }

  function exec(handle, sql) {
    const input = writeBytes(textEncoder.encode(sql));
    const out = e.pw_exec(handle, input.ptr, input.len);
    e.pw_free(input.ptr, input.len);
    return readJson(out);
  }

  function restore(handle, bytes) {
    const input = writeBytes(bytes);
    const out = e.pw_restore(handle, input.ptr, input.len);
    e.pw_free(input.ptr, input.len);
    return readJson(out);
  }

  // `[u32 LE length][.pink bytes]` or `[u32 0xFFFFFFFF][u32 len][message]`.
  function snapshot(handle) {
    const out = e.pw_snapshot(handle);
    assert(out !== 0, "pw_snapshot returned NULL");
    const view = new DataView(e.memory.buffer);
    const first = view.getUint32(out, true);
    if (first === 0xffffffff) {
      const length = view.getUint32(out + 4, true);
      const message = textDecoder.decode(new Uint8Array(e.memory.buffer, out + 8, length));
      e.pw_free(out, 8 + length);
      return { ok: false, error: message };
    }
    const bytes = new Uint8Array(e.memory.buffer, out + 4, first).slice();
    e.pw_free(out, 4 + first);
    return { ok: true, bytes };
  }

  const checks = [];
  function check(name, fn) {
    try {
      fn();
      checks.push(true);
      console.log(`ok   ${name}`);
    } catch (err) {
      checks.push(false);
      console.error(`FAIL ${name}\n     ${err.message}`);
    }
  }

  let handle = 0;
  let restoredHandle = 0;
  let selectedJson = "";
  let snapshotBytes = null;

  check("pw_version reports a version", () => {
    const version = readJson(e.pw_version());
    assert(typeof version.version === "string" && version.version.length > 0,
      `bad version JSON: ${JSON.stringify(version)}`);
  });

  check("pw_open returns a handle > 0", () => {
    handle = e.pw_open();
    assert(handle > 0, `handle = ${handle}`);
  });

  check("CREATE TABLE + INSERT + SELECT returns the right rows and types", () => {
    const out = exec(
      handle,
      `CREATE TABLE t (id INTEGER, big BIGINT, d DOUBLE, s VARCHAR, dt DATE);
       INSERT INTO t VALUES (1, 9007199254740993, 2.5, 'quote '' backslash \\ tab\tend', DATE '2024-01-15');
       INSERT INTO t VALUES (2, -9007199254740993, NULL, NULL, NULL);
       SELECT * FROM t ORDER BY id;`,
    );
    assert(out.ok === true, `exec failed: ${JSON.stringify(out)}`);
    assertEqual(out.results.map((r) => r.kind), ["Created", "Inserted", "Inserted", "Selected"],
      "statement kinds");
    assertEqual(out.results[1], { kind: "Inserted", count: 1 }, "first INSERT result");

    const selected = out.results[3];
    assertEqual(selected.names, ["id", "big", "d", "s", "dt"], "column names");
    assertEqual(selected.types, ["INTEGER", "BIGINT", "DOUBLE", "VARCHAR", "DATE"], "column types");
    assertEqual(selected.rows, [
      [1, "9007199254740993", 2.5, "quote ' backslash \\ tab\tend", "2024-01-15"],
      [2, "-9007199254740993", null, null, null],
    ], "selected rows");
    selectedJson = JSON.stringify(selected);
  });

  check("BIGINT 9007199254740993 crosses as a string", () => {
    const row = JSON.parse(selectedJson).rows[0];
    assert(typeof row[1] === "string" && row[1] === "9007199254740993",
      `BIGINT value was ${JSON.stringify(row[1])}`);
  });

  check("non-finite doubles and exact types use the text mapping", () => {
    const out = exec(
      handle,
      `SELECT CAST('NaN' AS DOUBLE) AS nan, CAST('Infinity' AS DOUBLE) AS inf,
              CAST('-Infinity' AS DOUBLE) AS ninf,
              CAST('1.25' AS DECIMAL(5,2)) AS dec, INTERVAL '1 day' AS iv;`,
    );
    assert(out.ok === true, `exec failed: ${JSON.stringify(out)}`);
    assertEqual(out.results[0].types,
      ["DOUBLE", "DOUBLE", "DOUBLE", "DECIMAL(5,2)", "INTERVAL"], "special types");
    assertEqual(out.results[0].rows,
      [["NaN", "Infinity", "-Infinity", "1.25", "1 day"]], "special values");
  });

  check("a failing statement returns ok:false with the engine error", () => {
    const out = exec(handle, "SELECT * FROM no_such_table;");
    assert(out.ok === false, `expected failure, got ${JSON.stringify(out)}`);
    assert(typeof out.error === "string" && out.error.length > 0, "missing error text");
    assert(out.error.includes("not found"), `unexpected error: ${out.error}`);
  });

  check("pw_snapshot produces a .pink image", () => {
    const snap = snapshot(handle);
    assert(snap.ok === true, `snapshot failed: ${JSON.stringify(snap)}`);
    snapshotBytes = snap.bytes;
    assert(snapshotBytes.length > 8, "snapshot is empty");
    const magic = textDecoder.decode(snapshotBytes.slice(0, 8));
    assert(magic === "PINKDB05", `bad magic ${JSON.stringify(magic)}`);
  });

  check("pw_restore into a second handle yields the same SELECT", () => {
    assert(snapshotBytes !== null, "no snapshot from the previous check");
    restoredHandle = e.pw_open();
    assert(restoredHandle > 0 && restoredHandle !== handle, "second handle not distinct");
    const out = restore(restoredHandle, snapshotBytes);
    assertEqual(out, { ok: true }, "restore result");
    const again = exec(restoredHandle, "SELECT * FROM t ORDER BY id;");
    assert(again.ok === true, `select failed: ${JSON.stringify(again)}`);
    assertEqual(JSON.stringify(again.results[0]), selectedJson, "restored rows differ");
  });

  check("restoring garbage bytes returns ok:false and keeps the database", () => {
    const garbage = textEncoder.encode("this is not a .pink snapshot");
    const out = restore(restoredHandle, garbage);
    assert(out.ok === false, `expected refusal, got ${JSON.stringify(out)}`);
    assert(typeof out.error === "string" && out.error.includes("bad magic"),
      `unexpected error: ${out.error}`);
    const stillThere = exec(restoredHandle, "SELECT COUNT(*) AS n FROM t;");
    assert(stillThere.ok === true, `database was lost: ${JSON.stringify(stillThere)}`);
    assertEqual(stillThere.results[0].rows, [[2]], "rows after refused restore");
  });

  check("pw_close releases handles and further use fails cleanly", () => {
    e.pw_close(handle);
    e.pw_close(restoredHandle);
    const out = exec(handle, "SELECT 1;");
    assert(out.ok === false && out.error.includes("unknown handle"),
      `expected unknown handle, got ${JSON.stringify(out)}`);
  });

  const passed = checks.filter(Boolean).length;
  console.log(`\n${passed}/${checks.length} checks passed`);
  if (passed !== checks.length) {
    process.exit(1);
  }
  console.log("wasm smoke test passed");
}

try {
  main();
} catch (err) {
  console.error(`wasm smoke test failed to run: ${err.stack ?? err}`);
  process.exit(1);
}
