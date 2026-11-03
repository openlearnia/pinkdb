//! Build and run the interleaved C benchmark in `tests/c/chunk_bench.c`.
//!
//! ```sh
//! PINKDB_BENCH_STORE=/tmp/store.pink \
//! cargo test -q -p pinkdb-ffi --test chunk_bench -- --ignored --nocapture
//! ```

use std::env;
use std::path::PathBuf;
use std::process::Command;

#[test]
#[ignore = "reads a TPC-H store; run by hand"]
fn chunk_bench() {
    let store = env::var("PINKDB_BENCH_STORE").expect("set PINKDB_BENCH_STORE to a .pink store");
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let deps = env::current_exe()
        .expect("test executable path")
        .parent()
        .expect("deps directory")
        .to_path_buf();
    let binary = env::temp_dir().join("pinkdb-chunk-bench");
    let cc = env::var_os("CC").unwrap_or_else(|| "cc".into());
    let status = Command::new(cc)
        .arg("-std=c11")
        .arg("-O2")
        .arg("-g")
        .arg("-pthread")
        .arg(format!("-I{}", manifest.join("include").display()))
        .arg(manifest.join("tests/c/chunk_bench.c"))
        .arg(format!("-L{}", deps.display()))
        .arg("-lpinkdb_ffi")
        .arg(format!("-Wl,-rpath,{}", deps.display()))
        .arg("-o")
        .arg(&binary)
        .status()
        .expect("run the C compiler");
    assert!(status.success(), "benchmark did not compile");
    let status = Command::new(&binary)
        .arg(&store)
        .status()
        .expect("run the benchmark");
    assert!(status.success(), "benchmark failed");
}
