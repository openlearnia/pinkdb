'use strict';

// Public Node.js API for pinkdb. The native addon in `build/pinkdb.node`
// marshals values; this file owns the error class, the ABI gate and the row
// iterator protocol so the surface is idiomatic JavaScript.

const native = require('./build/pinkdb.node');

const ABI_VERSION = 1;

/**
 * Error raised by pinkdb. `class` is the stable C ABI error class:
 * `parse`, `catalog`, `type`, `parameter`, `execution`, `constraint`, `io`,
 * `csv`, `invalid_argument` or `internal`.
 */
class PinkdbError extends Error {
  constructor(pinkdbClass, message) {
    super(message);
    this.name = 'PinkdbError';
    this.class = pinkdbClass;
  }
}

// The addon prefixes native failures with `<class>\u0001`; split it here so the
// message a caller sees is exactly the ABI's message.
const CLASS_SEPARATOR = '\u0001';

function guard(fn) {
  try {
    return fn();
  } catch (error) {
    if (error instanceof Error && typeof error.message === 'string') {
      const at = error.message.indexOf(CLASS_SEPARATOR);
      if (at > 0) {
        throw new PinkdbError(
          error.message.slice(0, at),
          error.message.slice(at + CLASS_SEPARATOR.length),
        );
      }
    }
    throw error;
  }
}

class RowStream {
  #stream;

  constructor(stream) {
    this.#stream = stream;
  }

  next() {
    const row = guard(() => this.#stream.next());
    if (row === null || row === undefined) {
      return { done: true, value: undefined };
    }
    return { done: false, value: row };
  }

  [Symbol.iterator]() {
    return this;
  }

  return() {
    this.close();
    return { done: true, value: undefined };
  }

  close() {
    this.#stream.close();
  }
}

class Prepared {
  #prepared;

  constructor(prepared) {
    this.#prepared = prepared;
  }

  /** Number of `?` placeholders in the statement. */
  get parameterCount() {
    return this.#prepared.parameterCount();
  }

  /** Execute for its effect: `{kind, name, affected, columns, columnTypes, rowCount}`. */
  run(params) {
    return guard(() => this.#prepared.run(params));
  }

  /** Execute and return every row as an object keyed by column name. */
  all(params) {
    return guard(() => this.#prepared.all(params));
  }

  /** Execute and return a lazy synchronous iterator over the rows. */
  iterate(params) {
    return new RowStream(guard(() => this.#prepared.iterate(params)));
  }

  close() {
    this.#prepared.close();
  }
}

class Database {
  #database;

  constructor(database) {
    this.#database = database;
  }

  /**
   * Run one or more `;`-separated statements. With `params` the SQL must be a
   * single statement with `?` placeholders. Returns one result per statement:
   * `{kind, name, affected, columns, columnTypes, rowCount, rows?}`, where
   * `rows` is present only for SELECT results.
   */
  run(sql, params) {
    return guard(() => this.#database.execute(sql, params));
  }

  /** Prepare, run and return the rows of a single statement. */
  query(sql, params) {
    return guard(() => this.#database.query(sql, params));
  }

  prepare(sql) {
    return new Prepared(guard(() => this.#database.prepare(sql)));
  }

  begin() {
    this.run('BEGIN');
  }

  commit() {
    this.run('COMMIT');
  }

  rollback() {
    this.run('ROLLBACK');
  }

  close() {
    this.#database.close();
  }
}

/** Open a database file, or an in-memory database when `path` is omitted. */
function open(path) {
  return new Database(guard(() => native.open(path ?? null)));
}

/** Open an in-memory database. */
function openMemory() {
  return open();
}

function abiVersion() {
  return native.abiVersion();
}

const actualAbi = guard(() => native.abiVersion());
if (actualAbi !== ABI_VERSION) {
  throw new Error(
    `pinkdb: the native addon speaks C ABI v${actualAbi}, this package expects v${ABI_VERSION}; ` +
      'rebuild with `npm run build` against a matching libpinkdb_ffi',
  );
}

module.exports = {
  ABI_VERSION,
  PinkdbError,
  Database,
  Prepared,
  RowStream,
  abiVersion,
  open,
  openMemory,
};
