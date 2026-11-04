'use strict';

// One timed row-iteration pass over `lineitem` through `Prepared.iterate()`.
// Used by the 112b A/B: pass the package directory of each build.
//
//   node bench/iterate_bench.js ../../.. /tmp/store.pink

const { open } = require(process.argv[2]);
const store = process.argv[3];

const db = open(store);
const started = process.hrtime.bigint();
let rows = 0;
let checksum = 0;
for (const row of db.prepare('SELECT * FROM lineitem').iterate()) {
    rows++;
    for (const key in row) {
        const value = row[key];
        if (typeof value === 'number') checksum += value;
        else if (typeof value === 'bigint') checksum += Number(value & 0xffffn);
        else if (typeof value === 'string') checksum += value.length;
    }
}
const ms = Number(process.hrtime.bigint() - started) / 1e6;
db.close();
console.log(JSON.stringify({ ms, rows, checksum }));
