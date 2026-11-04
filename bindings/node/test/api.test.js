'use strict';

// Binding-level tests for the Node API itself: file databases, transaction
// helpers, error classes and the lifetime rules around close(). The corpus in
// bindings/conformance covers SQL behaviour; this file covers the wrapper.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');

const { ABI_VERSION, PinkdbError, abiVersion, abiVersionInfo, open, openMemory } = require('..');

function tempDatabase(name = 'test.pinkdb') {
  const directory = mkdtempSync(path.join(tmpdir(), 'pinkdb-node-'));
  return {
    path: path.join(directory, name),
    cleanup: () => rmSync(directory, { recursive: true, force: true }),
  };
}

test('abiVersion matches the package constant', () => {
  const { major, minor } = abiVersionInfo();
  assert.equal(major, ABI_VERSION >>> 16);
  assert.ok(minor >= (ABI_VERSION & 0xffff), `minor ${minor}`);
  assert.equal(abiVersion(), (major << 16) | minor);
});

test('a file database survives close and reopen', () => {
  const file = tempDatabase();
  try {
    const db = open(file.path);
    db.run('CREATE TABLE t (a INTEGER, s VARCHAR)');
    db.run("INSERT INTO t VALUES (1, 'one'), (2, 'two')");
    db.close();

    const reopened = open(file.path);
    try {
      assert.deepEqual(reopened.query('SELECT a, s FROM t ORDER BY a'), [
        { a: 1, s: 'one' },
        { a: 2, s: 'two' },
      ]);
    } finally {
      reopened.close();
    }
  } finally {
    file.cleanup();
  }
});

test('run reports a result per statement and query returns only rows', () => {
  const db = openMemory();
  try {
    const results = db.run('CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2)');
    assert.equal(results.length, 2);
    assert.equal(results[0].kind, 'created');
    assert.equal(results[1].kind, 'inserted');
    assert.equal(results[1].affected, 2);
    assert.equal(results[1].rows, undefined);

    const selected = db.run('SELECT a FROM t ORDER BY a')[0];
    assert.equal(selected.kind, 'selected');
    assert.equal(selected.rowCount, 2);
    assert.deepEqual(selected.columns, ['a']);
    assert.deepEqual(selected.columnTypes, ['integer']);
    assert.deepEqual(selected.rows, [{ a: 1 }, { a: 2 }]);

    assert.deepEqual(db.query('SELECT a FROM t ORDER BY a'), [{ a: 1 }, { a: 2 }]);
  } finally {
    db.close();
  }
});

test('a FLOAT array arrives as a Float32Array', () => {
  const db = openMemory();
  try {
    const result = db.run(
      "SELECT CAST('[1.5, -0.25, 3]' AS FLOAT[3]) AS v, " +
        "CAST(NULL AS FLOAT[2]) AS n, len(CAST('[1, 2]' AS FLOAT[2])) AS l",
    )[0];
    assert.deepEqual(result.columnTypes, ['float[3]', 'float[2]', 'integer']);
    const value = result.rows[0].v;
    assert.ok(value instanceof Float32Array, `expected a Float32Array, got ${value}`);
    assert.deepEqual(Array.from(value), [1.5, -0.25, 3]);
    assert.equal(result.rows[0].n, null);
    assert.equal(result.rows[0].l, 2);
  } finally {
    db.close();
  }
});

test('a DOUBLE array arrives as a Float64Array', () => {
  const db = openMemory();
  try {
    const result = db.run(
      "SELECT CAST('[1.5, -0.25, 3]' AS DOUBLE[3]) AS v, " +
        "CAST(NULL AS DOUBLE[2]) AS n, len(CAST('[1, 2]' AS DOUBLE[2])) AS l",
    )[0];
    assert.deepEqual(result.columnTypes, ['double[3]', 'double[2]', 'integer']);
    const value = result.rows[0].v;
    assert.ok(value instanceof Float64Array, `expected a Float64Array, got ${value}`);
    assert.deepEqual(Array.from(value), [1.5, -0.25, 3]);
    assert.equal(result.rows[0].n, null);
    assert.equal(result.rows[0].l, 2);
  } finally {
    db.close();
  }
});

test('prepared statements bind parameters and expose their count', () => {
  const db = openMemory();
  try {
    db.run('CREATE TABLE t (a INTEGER, b BIGINT, s VARCHAR)');
    const insert = db.prepare('INSERT INTO t VALUES (?, ?, ?)');
    assert.equal(insert.parameterCount, 3);
    assert.equal(insert.run([1, 2n, 'héllo']).affected, 1);
    assert.equal(insert.run([3, 4n, 'wörld']).affected, 1);
    insert.close();

    const select = db.prepare('SELECT a, b, s FROM t WHERE a >= ? ORDER BY a');
    try {
      assert.deepEqual(select.all([0]), [
        { a: 1, b: 2n, s: 'héllo' },
        { a: 3, b: 4n, s: 'wörld' },
      ]);
      assert.deepEqual(select.all([3]), [{ a: 3, b: 4n, s: 'wörld' }]);
    } finally {
      select.close();
    }
  } finally {
    db.close();
  }
});

test('begin, commit and rollback control the transaction', () => {
  const db = openMemory();
  try {
    db.run('CREATE TABLE t (a INTEGER)');
    db.begin();
    db.run('INSERT INTO t VALUES (1)');
    db.rollback();
    assert.deepEqual(db.query('SELECT a FROM t'), []);

    db.begin();
    db.run('INSERT INTO t VALUES (2)');
    db.commit();
    assert.deepEqual(db.query('SELECT a FROM t'), [{ a: 2 }]);
  } finally {
    db.close();
  }
});

test('errors carry the C ABI class', () => {
  const db = openMemory();
  try {
    for (const [sql, expected] of [
      ['SELEC 1', 'parse'],
      ['SELECT * FROM no_such_table', 'catalog'],
      ['SELECT 1 / 0', 'execution'],
    ]) {
      assert.throws(
        () => db.run(sql),
        (error) => {
          assert.ok(error instanceof PinkdbError);
          assert.equal(error.name, 'PinkdbError');
          assert.equal(error.class, expected);
          return true;
        },
        sql,
      );
    }
    assert.throws(
      () => db.query('SELECT ?', [{ not: 'a parameter' }]),
      (error) => {
        assert.equal(error.class, 'invalid_argument');
        assert.match(error.message, /unsupported parameter type/);
        return true;
      },
    );
  } finally {
    db.close();
  }
});

test('a row stream stops early and a closed database invalidates statements', () => {
  const db = openMemory();
  db.run('CREATE TABLE t (a INTEGER)');
  db.run('INSERT INTO t VALUES (1), (2), (3)');
  const prepared = db.prepare('SELECT a FROM t ORDER BY a');
  const stream = prepared.iterate();
  assert.deepEqual(stream.next(), { done: false, value: { a: 1 } });
  stream.close();
  assert.deepEqual(stream.next(), { done: true, value: undefined });
  prepared.close();

  const other = db.prepare('SELECT 1');
  db.close();
  assert.throws(
    () => other.all(),
    (error) => {
      assert.equal(error.class, 'invalid_argument');
      assert.match(error.message, /database is closed/);
      return true;
    },
  );
  other.close();
});
