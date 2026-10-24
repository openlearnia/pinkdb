'use strict';

// Build the N-API addon with cargo and copy it to build/pinkdb.node. The
// addon links libpinkdb_ffi statically, so this is the only build step a user
// needs (`npm run build`).

const { spawnSync } = require('node:child_process');
const { copyFileSync, existsSync, mkdirSync } = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const release = process.env.PINKDB_NODE_PROFILE !== 'debug';
const manifest = path.join(root, 'native', 'Cargo.toml');
const args = ['build', '--manifest-path', manifest];
if (release) args.push('--release');

const result = spawnSync('cargo', args, { stdio: 'inherit' });
if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

const targetDir = process.env.CARGO_TARGET_DIR
  ? path.resolve(process.env.CARGO_TARGET_DIR)
  : path.join(root, 'native', 'target');
const profileDir = path.join(targetDir, release ? 'release' : 'debug');
const libraries = [
  'libpinkdb_node.dylib',
  'libpinkdb_node.so',
  'pinkdb_node.dll',
];
const library = libraries.map((name) => path.join(profileDir, name)).find(existsSync);
if (!library) {
  console.error(`pinkdb: cargo produced no addon library in ${profileDir}`);
  process.exit(1);
}

mkdirSync(path.join(root, 'build'), { recursive: true });
copyFileSync(library, path.join(root, 'build', 'pinkdb.node'));
console.log(`pinkdb: built ${path.relative(root, library)} -> build/pinkdb.node`);
