'use strict';

// Runs the language-neutral corpus in bindings/conformance against the Node
// binding. Every expectation comes from the corpus; this file only interprets
// the ops and compares typed cells.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');

const { ABI_VERSION, PinkdbError, openMemory } = require('../index.js');

const corpusPath =
  process.env.PINKDB_CONFORMANCE ??
  path.join(__dirname, '..', '..', 'conformance', 'corpus.json');
const corpus = JSON.parse(readFileSync(corpusPath, 'utf8'));

function parseDouble(text) {
  if (text === 'NaN') return NaN;
  if (text === 'Infinity') return Infinity;
  if (text === '-Infinity') return -Infinity;
  return Number(text);
}

function decodeParam(cell) {
  if (cell === null) return null;
  const [type, value] = cell;
  switch (type) {
    case 'boolean':
    case 'integer':
      return value;
    case 'bigint':
      return BigInt(value);
    case 'double':
      return parseDouble(value);
    case 'varchar':
    case 'date':
    case 'timestamp':
    case 'decimal':
    case 'interval':
      return value;
    default:
      throw new Error(`corpus: unknown parameter type ${type}`);
  }
}

function decodeParams(params) {
  return params === undefined ? undefined : params.map(decodeParam);
}

function assertCell(actual, expected, type, where) {
  if (expected === null) {
    assert.equal(actual, null, `${where}: expected NULL`);
    return;
  }
  const [expectedType, expectedValue] = expected;
  assert.equal(expectedType, type, `${where}: corpus type ${expectedType} but column is ${type}`);
  switch (type) {
    case 'boolean':
      assert.equal(actual, expectedValue, where);
      break;
    case 'integer':
      assert.equal(actual, expectedValue, where);
      break;
    case 'bigint':
      assert.equal(typeof actual, 'bigint', `${where}: expected a BigInt`);
      assert.equal(actual, BigInt(expectedValue), where);
      break;
    case 'double': {
      const want = parseDouble(expectedValue);
      const ok = Number.isNaN(want) ? Number.isNaN(actual) : actual === want;
      assert.ok(ok, `${where}: expected ${expectedValue}, got ${actual}`);
      break;
    }
    case 'string':
      assert.equal(actual, expectedValue, where);
      break;
    default:
      assert.equal(actual, expectedValue, where);
  }
}

function assertRows(rows, columns, types, expectedRows, where) {
  assert.equal(rows.length, expectedRows.length, `${where}: row count`);
  expectedRows.forEach((expectedRow, rowIndex) => {
    const row = rows[rowIndex];
    assert.equal(
      Object.keys(row).length,
      columns.length,
      `${where}: row ${rowIndex} has the wrong number of columns`,
    );
    expectedRow.forEach((cell, columnIndex) => {
      const name = columns[columnIndex];
      assertCell(
        row[name],
        cell,
        types[columnIndex],
        `${where} row ${rowIndex}, column ${name}`,
      );
    });
  });
}

function assertStatement(result, expect, where) {
  assert.equal(result.kind, expect.kind, `${where}: statement kind`);
  if (expect.affected !== undefined) {
    assert.equal(result.affected, expect.affected, `${where}: affected rows`);
  }
  if (expect.columns) {
    assert.deepEqual(result.columns, expect.columns, `${where}: columns`);
    assert.deepEqual(result.columnTypes, expect.types, `${where}: column types`);
    assertRows(result.rows ?? [], expect.columns, expect.types, expect.rows, where);
  }
}

function runCase(kase) {
  const db = openMemory();
  try {
    for (const sql of kase.setup ?? []) {
      db.run(sql);
    }
    kase.steps.forEach((step, index) => {
      const where = `${kase.name} step ${index} (${step.op})`;
      switch (step.op) {
        case 'query': {
          const params = decodeParams(step.params);
          const results = db.run(step.sql, params);
          assert.equal(results.length, 1, `${where}: expected one statement result`);
          assert.equal(results[0].kind, 'selected', `${where}: expected a SELECT`);
          assert.deepEqual(results[0].columns, step.expect.columns, `${where}: columns`);
          assert.equal(results[0].columnTypes.length, step.expect.types.length, `${where}: types`);
          assert.deepEqual(results[0].columnTypes, step.expect.types, `${where}: types`);
          assertRows(
            results[0].rows,
            step.expect.columns,
            step.expect.types,
            step.expect.rows,
            where,
          );
          break;
        }
        case 'prepared_query': {
          const prepared = db.prepare(step.sql);
          try {
            const params = decodeParams(step.params);
            const result = prepared.run(params);
            assert.equal(result.kind, 'selected', `${where}: expected a SELECT`);
            assert.deepEqual(result.columns, step.expect.columns, `${where}: columns`);
            assert.deepEqual(result.columnTypes, step.expect.types, `${where}: types`);
            assertRows(
              prepared.all(params),
              step.expect.columns,
              step.expect.types,
              step.expect.rows,
              where,
            );
          } finally {
            prepared.close();
          }
          break;
        }
        case 'exec': {
          const results = db.run(step.sql);
          assert.equal(results.length, step.expect.length, `${where}: statement count`);
          step.expect.forEach((expect, statementIndex) => {
            assertStatement(results[statementIndex], expect, `${where} statement ${statementIndex}`);
          });
          break;
        }
        case 'exec_prepared': {
          const prepared = db.prepare(step.sql);
          try {
            const result = prepared.run(decodeParams(step.params));
            assert.equal(step.expect.length, 1, `${where}: expected one statement expectation`);
            assertStatement(result, step.expect[0], where);
          } finally {
            prepared.close();
          }
          break;
        }
        case 'error': {
          const params = decodeParams(step.params);
          const run = () => {
            if ('params' in step) {
              db.prepare(step.sql).run(params);
            } else {
              db.run(step.sql);
            }
          };
          assert.throws(run, (error) => {
            assert.ok(error instanceof PinkdbError, `${where}: expected a PinkdbError`);
            assert.equal(error.class, step.class, `${where}: error class`);
            if (step.message_contains) {
              assert.ok(
                error.message.includes(step.message_contains),
                `${where}: message "${error.message}" does not contain "${step.message_contains}"`,
              );
            }
            return true;
          }, where);
          break;
        }
        case 'stream': {
          const prepared = db.prepare(step.sql);
          try {
            const meta = prepared.run();
            assert.deepEqual(meta.columns, step.expect.columns, `${where}: columns`);
            assert.deepEqual(meta.columnTypes, step.expect.types, `${where}: types`);
            const head = [];
            const tail = [];
            let count = 0;
            let checksum = 0n;
            for (const row of prepared.iterate()) {
              const first = row[step.expect.columns[0]];
              assert.equal(typeof first, 'bigint', `${where}: checksum column must be BIGINT`);
              count += 1;
              checksum += first;
              if (head.length < step.expect.first_rows.length) head.push(first);
              tail.push(first);
              if (tail.length > step.expect.last_rows.length) tail.shift();
            }
            assert.equal(count, step.expect.row_count, `${where}: row count`);
            assert.deepEqual(
              head,
              step.expect.first_rows.map((row) => BigInt(row[0][1])),
              `${where}: first rows`,
            );
            assert.deepEqual(
              tail,
              step.expect.last_rows.map((row) => BigInt(row[0][1])),
              `${where}: last rows`,
            );
            assert.equal(checksum, BigInt(step.expect.checksum), `${where}: checksum`);
            break;
          } finally {
            prepared.close();
          }
        }
        default:
          throw new Error(`${where}: unknown op ${step.op}`);
      }
    });
  } finally {
    db.close();
  }
}

test('corpus ABI version matches the binding', () => {
  assert.equal(corpus.corpus_version, 1, 'corpus version');
  assert.equal(corpus.abi_version, ABI_VERSION, 'C ABI version');
});

for (const kase of corpus.cases) {
  test(`conformance: ${kase.name}`, () => {
    runCase(kase);
  });
}
