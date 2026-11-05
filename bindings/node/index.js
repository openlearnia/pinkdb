'use strict';

// Public Node.js API for pinkdb. The native addon in `build/pinkdb.node`
// marshals values; this file owns the error class, the ABI gate and the row
// iterator protocol so the surface is idiomatic JavaScript.

const native = require('./build/pinkdb.node');

// 1.4: (major << 16) | minor. The addon still exposes the v1 cell API.
const ABI_VERSION = (1 << 16) | 5;

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
  #database;
  #closed = false;

  constructor(stream, database) {
    this.#stream = stream;
    this.#database = database;
    database._enterStream(this);
  }

  next() {
    let row;
    try {
      row = guard(() => this.#stream.next());
    } catch (error) {
      this.close();
      throw error;
    }
    if (row === null || row === undefined) {
      this.close();
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
    if (this.#closed) return;
    this.#closed = true;
    try {
      guard(() => this.#stream.close());
    } finally {
      this.#database._leaveStream(this);
    }
  }
}

class Prepared {
  #prepared;
  #database;

  constructor(prepared, database) {
    this.#prepared = prepared;
    this.#database = database;
  }

  /** Number of `?` placeholders in the statement. */
  get parameterCount() {
    return this.#prepared.parameterCount();
  }

  /** Execute for its effect: `{kind, name, affected, columns, columnTypes, rowCount}`. */
  run(params) {
    this.#database._assertIdle();
    return guard(() => this.#prepared.run(params));
  }

  /** Execute and return every row as an object keyed by column name. */
  all(params) {
    this.#database._assertIdle();
    return guard(() => this.#prepared.all(params));
  }

  /** Execute and return a lazy synchronous iterator over the rows. */
  iterate(params) {
    this.#database._assertIdle();
    return new RowStream(guard(() => this.#prepared.iterate(params)), this.#database);
  }

  close() {
    this.#prepared.close();
  }
}

class Database {
  #database;
  #busy = null;

  constructor(database) {
    this.#database = database;
  }

  _assertIdle() {
    if (this.#busy !== null) {
      throw new PinkdbError(
        'invalid_argument',
        'database is busy with an open row stream; close it first',
      );
    }
  }

  _enterStream(stream) {
    this._assertIdle();
    this.#busy = stream;
  }

  _leaveStream(stream) {
    if (this.#busy === stream) this.#busy = null;
  }

  /**
   * Run one or more `;`-separated statements. With `params` the SQL must be a
   * single statement with `?` placeholders. Returns one result per statement:
   * `{kind, name, affected, columns, columnTypes, rowCount, rows?}`, where
   * `rows` is present only for SELECT results.
   */
  run(sql, params) {
    this._assertIdle();
    return guard(() => this.#database.execute(sql, params));
  }

  /** Prepare, run and return the rows of a single statement. */
  query(sql, params) {
    this._assertIdle();
    return guard(() => this.#database.query(sql, params));
  }

  prepare(sql) {
    this._assertIdle();
    return new Prepared(guard(() => this.#database.prepare(sql)), this);
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
    this._assertIdle();
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

/** The native C ABI version, encoded `major << 16 | minor` (65539 is 1.3). */
function abiVersion() {
  return native.abiVersion();
}

/** The native C ABI version as `{ major, minor }`; a newer minor only adds calls. */
function abiVersionInfo() {
  const version = abiVersion();
  return { major: version >>> 16, minor: version & 0xffff };
}

const actualAbi = guard(() => native.abiVersion());
// Same major, at least this minor: a newer minor only adds calls.
if (actualAbi >>> 16 !== ABI_VERSION >>> 16 || (actualAbi & 0xffff) < (ABI_VERSION & 0xffff)) {
  throw new Error(
    `pinkdb: the native addon speaks C ABI v${actualAbi >>> 16}.${actualAbi & 0xffff}, ` +
      `this package expects v${ABI_VERSION >>> 16}.${ABI_VERSION & 0xffff} or a later minor; ` +
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
  abiVersionInfo,
  open,
  openMemory,
};
