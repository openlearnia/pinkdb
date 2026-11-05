//! CRC32C throughput of the `.pink` format's checksum
//! (`pinkdb::persist::crc32c`), the one every checkpoint write and every
//! first read of a chunk pays.
//!
//! Usage: crc_bench [rounds]
//!
//! Prints the best GB/s over `rounds` passes per buffer size. Compare two
//! builds by running their binaries alternately (interleaved A/B).

use std::time::Instant;

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .map_or(15, |r| r.parse().expect("rounds"));
    // A fixed pseudo-random fill: the checksum's speed does not depend on the
    // bytes, but a constant buffer would invite the optimizer.
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let buf: Vec<u8> = (0..64 << 20)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let mut sink = 0u32;
    for size in [64usize, 4 << 10, 256 << 10, 64 << 20] {
        // Each pass covers 256 MiB, in `size`-byte calls (a chunk's column is
        // one call; small sizes show the per-call overhead).
        let calls = ((256usize << 20) / size).max(1);
        let mut best = f64::MAX;
        for _ in 0..rounds {
            let t = Instant::now();
            for c in 0..calls {
                let at = (c * size) % (buf.len() - size + 1);
                sink ^= pinkdb::persist::crc32c(&buf[at..at + size]);
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        let gbs = (calls * size) as f64 / best / 1e9;
        println!(
            "size={size:>9} B  best={:8.2} ms  {gbs:6.2} GB/s",
            best * 1e3
        );
    }
    println!("checksum sink {sink:08x}");
}
