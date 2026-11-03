# sqllogictest baseline

Pass rate of pinkdb on DuckDB's own sqllogictest corpus, measured with
`tools/sqllogictest` (`pinkdb-slt`). The corpus is DuckDB's MIT-licensed
`test/sql` tree; the format semantics are ported from DuckDB's
`test/sqlite/sqllogic_{parser,command,test_runner}.cpp`,
`result_helper.cpp` and `test/helpers/test_config.cpp`.

- Corpus: `~/Desktop/projects/duckdb-v2.0.0/test/sql`, the DuckDB v2.0.0
  release (`d54cbbe3df`): **4,755** `.test` files.
- Run: release build, `--jobs 8`, `.test` only (`.test_slow` is behind
  `--slow`), one child process per file (the runner's default isolation).
- **4,755 files: 447 pass**, 3,933 fail, 370 skip, 1 crash, 4 timeout.
- File pass rate **9.4%** (10.2% of files that ran); record pass rate
  **83.6%** of non-skipped records. Wall time: **67.9 s**.
- This replaces the 451/4,233 figure measured on the alpha fork's DuckDB
  checkout (`ducklake-vortex`), which is a different engine revision with
  different expectations. The two numbers are not comparable test by test;
  this one is the reference.
- Five files do not report pass or fail. `copy/parquet/parquet_row_group_row_count.test`
  aborts (SIGABRT, a 3.6e16-byte allocation) and is recorded as `crash`; the
  child process contains it, and the other 4,750 files still report. Four hit
  a deadline: `catalog/table/create_table_as_abort.test` on the 60 s file
  deadline, where the child is killed, and
  `aggregate/grouping_sets/grouping_sets_many_sets.test`,
  `delete/test_parallel_delete_transaction_local.test` and
  `optimizer/plan/test_filter_pushdown_large.test` on the 10 s statement
  deadline. Five more files sit within seconds of that deadline and flip
  between `timeout` and their own failure depending on machine load:
  `cte/recursive_cte_limit.test`, `cte/test_recursive_cte_union_all.test`,
  `sql_export/plan_reconstruction_coverage.test`,
  `sql_export/recursive_state.test`, `sql_export/single_child_union.test`.
  A run with a busy machine reported nine timeouts instead of four.
- Isolation is why the corpus completes at all: a panic is caught in the
  worker thread, but a stack overflow or an abort ends its process. Two runs
  of the same corpus at `--jobs 8`, isolated and `--in-process`, take the same
  wall time (69.3 s and 69.5 s on a copy without the aborting file); a child
  costs about 6 ms of startup, which the eight workers overlap. `--in-process`
  remains for debugging. Files that set the process-wide `memory_limit` still
  run one at a time after the parallel phase, as `--in-process` needs it;
  with one process per file it is no longer required.

## Format coverage

Supported: `statement ok|error|maybe` (with expected error substring,
`<REGEX>:`/`<!REGEX>:`), `query` with `T`/`I`/`R` types and
`nosort`/`rowsort`/`valuesort`, labels compared across queries,
`<n> values hashing to <md5>`, `<FILE>:` result blocks (pipe CSV with a
header), `require` (json, parquet, datetime, math, string; build properties
such as `skip_reload`, `64bit`, `vector_size`), `require-env`,
`mode skip|unskip`, `halt`, `skipif`/`onlyif` (pinkdb matches both `duckdb`
and `pinkdb`), `loop`/`foreach`/`endloop` with `{var}`/`${var}`,
`concurrentloop`/`concurrentforeach` (executed serially, noted per file),
`load`/`restart` (file-backed store in a temp dir), `sleep`,
`set variable`, `reset label`, the full DuckDB placeholder set
(`{TEST_DIR}`, `{TEMP_DIR}`, `{TEMP_DIR_ROOT}`, `{DATA_DIR}`, `{WORKING_DIR}`,
`{BUILD_DIR}`, `{CATALOG_DIR}`, `{TEST_NAME}`, `{TEST_ID}`, `{RUN_ID}`,
`{UUID}`, legacy spellings, ...), environment substitution, and `data/...`
paths resolved against the DuckDB root.

Numeric comparison follows DuckDB's `TestResultHelper::CompareValues`: exact
text first, then a cast of both sides to the result column's type for numeric
columns (`65.000000` equals a BIGINT `65`), doubles with the `|a-b| <=
|b|*0.01 + 1e-8` tolerance, and booleans as `true`/`1`, `false`/`0`.

A file using any other directive (for example `include`, `unzip`,
`set seed`, `waitfor`, `mode output_*`) is skipped with the directive as the
reason. Files requiring a DuckDB extension or build feature pinkdb does not
have (`autocomplete`, `icu`, `httpfs`, `tpch`, `block_size`, ...) are
skipped for the same reason.

## Pass rate per area

| area | files | pass | fail | skip | crash | timeout | files pass | run files pass | records pass | records fail | record pass |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| aggregate | 184 | 17 | 161 | 5 | 0 | 1 | 9.2% | 9.5% | 494 | 162 | 75.3% |
| alter | 130 | 21 | 109 | 0 | 0 | 0 | 16.2% | 16.2% | 460 | 109 | 80.8% |
| attach | 99 | 3 | 86 | 10 | 0 | 0 | 3.0% | 3.4% | 246 | 86 | 74.1% |
| binder | 44 | 3 | 39 | 2 | 0 | 0 | 6.8% | 7.1% | 78 | 39 | 66.7% |
| cast | 43 | 3 | 40 | 0 | 0 | 0 | 7.0% | 7.0% | 50 | 40 | 55.6% |
| catalog | 160 | 12 | 146 | 1 | 0 | 1 | 7.5% | 7.5% | 453 | 146 | 75.6% |
| collate | 34 | 2 | 25 | 7 | 0 | 0 | 5.9% | 7.4% | 59 | 25 | 70.2% |
| conjunction | 2 | 2 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 25 | 0 | 100.0% |
| connect | 5 | 0 | 2 | 3 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| constraints | 53 | 7 | 46 | 0 | 0 | 0 | 13.2% | 13.2% | 185 | 46 | 80.1% |
| copy | 584 | 35 | 523 | 25 | 1 | 0 | 6.0% | 6.3% | 729 | 523 | 58.2% |
| copy_database | 11 | 0 | 11 | 0 | 0 | 0 | 0.0% | 0.0% | 29 | 11 | 72.5% |
| create | 15 | 0 | 14 | 1 | 0 | 0 | 0.0% | 0.0% | 25 | 14 | 64.1% |
| cte | 79 | 6 | 70 | 3 | 0 | 0 | 7.6% | 7.9% | 139 | 70 | 66.5% |
| delete | 21 | 6 | 13 | 1 | 0 | 1 | 28.6% | 30.0% | 252 | 14 | 94.7% |
| detailed_profiler | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| error | 17 | 1 | 16 | 0 | 0 | 0 | 5.9% | 5.9% | 11 | 16 | 40.7% |
| execution | 5 | 0 | 5 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 5 | 66.7% |
| explain | 9 | 0 | 9 | 0 | 0 | 0 | 0.0% | 0.0% | 24 | 9 | 72.7% |
| export | 25 | 0 | 24 | 1 | 0 | 0 | 0.0% | 0.0% | 33 | 24 | 57.9% |
| extensions | 15 | 0 | 8 | 7 | 0 | 0 | 0.0% | 0.0% | 7 | 8 | 46.7% |
| filter | 21 | 3 | 17 | 1 | 0 | 0 | 14.3% | 15.0% | 126 | 17 | 88.1% |
| function | 429 | 20 | 356 | 53 | 0 | 0 | 4.7% | 5.3% | 2196 | 356 | 86.1% |
| generated_columns | 39 | 1 | 38 | 0 | 0 | 0 | 2.6% | 2.6% | 32 | 38 | 45.7% |
| index | 125 | 16 | 108 | 1 | 0 | 0 | 12.8% | 12.9% | 663 | 108 | 86.0% |
| insert | 12 | 7 | 5 | 0 | 0 | 0 | 58.3% | 58.3% | 97 | 5 | 95.1% |
| join | 148 | 27 | 119 | 2 | 0 | 0 | 18.2% | 18.5% | 837 | 118 | 87.6% |
| json | 122 | 3 | 118 | 1 | 0 | 0 | 2.5% | 2.5% | 173 | 117 | 59.7% |
| keywords | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 2 | 66.7% |
| limit | 6 | 2 | 3 | 1 | 0 | 0 | 33.3% | 40.0% | 21 | 3 | 87.5% |
| logging | 21 | 0 | 17 | 4 | 0 | 0 | 0.0% | 0.0% | 10 | 17 | 37.0% |
| merge | 27 | 2 | 25 | 0 | 0 | 0 | 7.4% | 7.4% | 119 | 25 | 82.6% |
| multi_file | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 4 | 20.0% |
| optimizer | 134 | 12 | 113 | 8 | 0 | 1 | 9.0% | 9.5% | 403 | 114 | 77.9% |
| order | 29 | 5 | 24 | 0 | 0 | 0 | 17.2% | 17.2% | 602 | 24 | 96.2% |
| ordinality | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 2 | 50.0% |
| overflow | 7 | 0 | 6 | 1 | 0 | 0 | 0.0% | 0.0% | 8 | 6 | 57.1% |
| parallelism | 17 | 3 | 13 | 1 | 0 | 0 | 17.6% | 18.8% | 572 | 13 | 97.8% |
| parser | 62 | 2 | 60 | 0 | 0 | 0 | 3.2% | 3.2% | 73 | 59 | 55.3% |
| peg_parser | 88 | 1 | 31 | 56 | 0 | 0 | 1.1% | 3.1% | 30 | 31 | 49.2% |
| pg_catalog | 17 | 0 | 17 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 17 | 15.0% |
| pivot | 37 | 1 | 36 | 0 | 0 | 0 | 2.7% | 2.7% | 90 | 36 | 71.4% |
| pragma | 51 | 0 | 50 | 1 | 0 | 0 | 0.0% | 0.0% | 112 | 50 | 69.1% |
| prepared | 39 | 0 | 39 | 0 | 0 | 0 | 0.0% | 0.0% | 15 | 39 | 27.8% |
| progress | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| projection | 16 | 2 | 14 | 0 | 0 | 0 | 12.5% | 12.5% | 90 | 14 | 86.5% |
| returning | 7 | 2 | 5 | 0 | 0 | 0 | 28.6% | 28.6% | 57 | 5 | 91.9% |
| sample | 15 | 5 | 8 | 2 | 0 | 0 | 33.3% | 38.5% | 15 | 8 | 65.2% |
| secrets | 14 | 0 | 13 | 1 | 0 | 0 | 0.0% | 0.0% | 7 | 13 | 35.0% |
| select | 9 | 3 | 6 | 0 | 0 | 0 | 33.3% | 33.3% | 31 | 6 | 83.8% |
| serialization | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| setops | 24 | 10 | 14 | 0 | 0 | 0 | 41.7% | 41.7% | 137 | 14 | 90.7% |
| settings | 50 | 3 | 44 | 3 | 0 | 0 | 6.0% | 6.4% | 1512 | 44 | 97.2% |
| show_select | 12 | 1 | 10 | 1 | 0 | 0 | 8.3% | 9.1% | 31 | 10 | 75.6% |
| sql_export | 96 | 5 | 90 | 1 | 0 | 0 | 5.2% | 5.3% | 270 | 90 | 75.0% |
| storage | 546 | 90 | 335 | 121 | 0 | 0 | 16.5% | 21.2% | 2420 | 333 | 87.9% |
| storage_version | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| subquery | 96 | 24 | 71 | 1 | 0 | 0 | 25.0% | 25.3% | 458 | 71 | 86.6% |
| table_function | 57 | 5 | 49 | 3 | 0 | 0 | 8.8% | 9.3% | 77 | 49 | 61.1% |
| timezone | 21 | 0 | 1 | 20 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| topn | 18 | 5 | 13 | 0 | 0 | 0 | 27.8% | 27.8% | 110 | 13 | 89.4% |
| tpcds | 3 | 0 | 0 | 3 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpch | 3 | 0 | 0 | 3 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| transactions | 80 | 17 | 63 | 0 | 0 | 0 | 21.2% | 21.2% | 606 | 63 | 90.6% |
| trigger | 29 | 0 | 29 | 0 | 0 | 0 | 0.0% | 0.0% | 60 | 29 | 67.4% |
| types | 507 | 20 | 475 | 12 | 0 | 0 | 3.9% | 4.0% | 749 | 475 | 61.2% |
| unnest | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| update | 38 | 16 | 22 | 0 | 0 | 0 | 42.1% | 42.1% | 3146 | 22 | 99.3% |
| upsert | 46 | 9 | 37 | 0 | 0 | 0 | 19.6% | 19.6% | 140 | 33 | 80.9% |
| vacuum | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 2 | 33.3% |
| variables | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 2 | 84.6% |
| variant | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 2 | 50.0% |
| vector_types | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| window | 80 | 7 | 70 | 3 | 0 | 0 | 8.8% | 9.1% | 589 | 70 | 89.4% |

### Second-level areas

| area | files | pass | fail | skip | crash | timeout | files pass | run files pass | records pass | records fail | record pass |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| aggregate/aggregate_state_export | 34 | 0 | 33 | 1 | 0 | 0 | 0.0% | 0.0% | 29 | 33 | 46.8% |
| aggregate/aggregates | 91 | 5 | 83 | 3 | 0 | 0 | 5.5% | 5.7% | 210 | 83 | 71.7% |
| aggregate/count_execution_paths.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| aggregate/distinct | 20 | 4 | 16 | 0 | 0 | 0 | 20.0% | 20.0% | 92 | 16 | 85.2% |
| aggregate/first_last_nested_vectors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| aggregate/group | 16 | 4 | 12 | 0 | 0 | 0 | 25.0% | 25.0% | 58 | 12 | 82.9% |
| aggregate/grouping_sets | 9 | 2 | 6 | 0 | 0 | 1 | 22.2% | 22.2% | 33 | 7 | 82.5% |
| aggregate/having | 4 | 1 | 3 | 0 | 0 | 0 | 25.0% | 25.0% | 16 | 3 | 84.2% |
| aggregate/qualify | 4 | 1 | 3 | 0 | 0 | 0 | 25.0% | 25.0% | 48 | 3 | 94.1% |
| aggregate/quantile_fun.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| aggregate/sparse_clustered_aggregation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| aggregate/variant | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 2 | 60.0% |
| alter/add_col | 17 | 7 | 10 | 0 | 0 | 0 | 41.2% | 41.2% | 75 | 10 | 88.2% |
| alter/add_pk | 19 | 0 | 19 | 0 | 0 | 0 | 0.0% | 0.0% | 36 | 19 | 65.5% |
| alter/add_unique | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 2 | 77.8% |
| alter/alter_col | 7 | 0 | 7 | 0 | 0 | 0 | 0.0% | 0.0% | 27 | 7 | 79.4% |
| alter/alter_schema_set_schema_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| alter/alter_table_nullability_requires_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| alter/alter_table_set_partitioned_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| alter/alter_table_set_sorted_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| alter/alter_table_set_table_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| alter/alter_type | 19 | 1 | 18 | 0 | 0 | 0 | 5.3% | 5.3% | 49 | 18 | 73.1% |
| alter/default | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 2 | 60.0% |
| alter/drop_col | 14 | 5 | 9 | 0 | 0 | 0 | 35.7% | 35.7% | 52 | 9 | 85.2% |
| alter/drop_constraint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| alter/list | 3 | 0 | 3 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 3 | 0.0% |
| alter/map | 3 | 0 | 3 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 3 | 0.0% |
| alter/rename_col | 8 | 2 | 6 | 0 | 0 | 0 | 25.0% | 25.0% | 17 | 6 | 73.9% |
| alter/rename_schema | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| alter/rename_table | 14 | 5 | 9 | 0 | 0 | 0 | 35.7% | 35.7% | 162 | 9 | 94.7% |
| alter/rename_view | 5 | 1 | 4 | 0 | 0 | 0 | 20.0% | 20.0% | 22 | 4 | 84.6% |
| alter/struct | 8 | 0 | 8 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 8 | 0.0% |
| alter/test_alter_database_rename.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| alter/test_alter_if_exists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_all_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_catalog_error_early_out.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_checkpoint_on_detach.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_checkpoint_vacuum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_conflicting_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_copy.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_create_index.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| attach/attach_cross_catalog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_custom_block_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_database_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_database_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_dbname_quotes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| attach/attach_default_database_case.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_default_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_defaults.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_dependencies.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| attach/attach_detach_reattach_transaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_did_you_mean.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| attach/attach_different_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_duckdb_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_enable_external_access.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_encrypted_db_key_test.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_encryption_block_header.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_encryption_downgrade_prevention.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_encryption_fallback_readonly.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_enums.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_export_import.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| attach/attach_expr.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_external_access.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_filepath_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 101 | 1 | 99.0% |
| attach/attach_foreign_key.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_foreign_key_external_catalog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_fsspec.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_hidden.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_home_directory.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_huggingface_index.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_icu_collation.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_if_not_exists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_if_not_exists_detach.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_issue16122.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_issue7567.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| attach/attach_issue7711.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/attach_issue_21491.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_issue_7660.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| attach/attach_lambda_view.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_macros.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| attach/attach_modify_multiple_databases.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_multi_identifiers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| attach/attach_nested_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_new_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_no_wal_writes_mode.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_or_replace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_persistent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_pragma_storage_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_prefetch_header.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_read_only.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_read_only_transaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| attach/attach_remote_http_logging.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_replay_with_no_wal_writes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| attach/attach_reserved.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_row_group_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_row_group_size_decreasing.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_same_db.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_sequence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_serialize_dependency.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_show_all_tables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| attach/attach_show_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| attach/attach_storage_version.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_table_constraints.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_table_ddl.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| attach/attach_table_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| attach/attach_to_external_resource.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/attach_transactionality.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_use_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/attach_use_string_literal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| attach/attach_view_search_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_views.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| attach/attach_volatile_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_wal_alter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/attach_wal_alter_sequence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/detach_keyword.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| attach/detach_manager_default_database.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| attach/external_resource_attach_failure.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/external_resource_discovery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/external_resource_lifecycle.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/external_resource_logging.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/external_resource_null_handle.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/external_resource_option_collision.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/external_resource_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/in_memory_attach.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| attach/reattach_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/remote_file_concurrently.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| attach/searchpath_fallback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| attach/show_databases.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| attach/show_schemas.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| attach/system_catalog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/alias_error_10057.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_group_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_having.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_order_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_qualify.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_select_projection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_qualification_where.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/alias_where_side_effects.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/column_ref_as_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| binder/column_value_alias_group.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| binder/default_expression_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/duplicate_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/empty_column_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/expression_binder_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/function_chaining_19035.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/group_by_incremental_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/group_by_qualification.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| binder/integer_literal_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/legacy_null_type.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| binder/missing_column_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/named_parameter_cast.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| binder/not_similar_to.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/old_implicit_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/old_implicit_cast_template.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/order_by_view.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/qualified_alias_method_call.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/separate_schema_tables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| binder/similar_to.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/string_literal_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/table_alias_single_quotes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| binder/table_view_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| binder/test_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| binder/test_alias_map_in_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/test_case_insensitive_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| binder/test_duplicate_in_set_operation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/test_function_chaining_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| binder/test_having_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/test_implicit_struct_pack.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| binder/test_in_with_collate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| binder/test_null_type_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| binder/test_postitional_ref_order_binding.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| binder/test_select_clause_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| binder/test_string_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cast/array_to_varchar_quoting.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/boolean_autocast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/cast_error_location.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/cast_error_logical_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/cast_sign_only_whitespace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/common_operator_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cast/decimal_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| cast/decimal_constexpr_limits.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/decimal_float_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/decimal_integer_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/dot_function_missing_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/double_float_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/float_decimal_cast.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| cast/float_integer_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/signed_cast_repro.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| cast/string_to_integer_decimal_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_integer_exponent_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_integer_many_decimals.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_list_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_list_escapes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_list_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_map_escapes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_map_invalid_entries.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_map_null_keys.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_struct_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_struct_escapes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_struct_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_struct_variant_child.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/string_to_unnamed_struct.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/struct_to_map.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/test_bit_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/test_boolean_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| cast/test_copy_constant_vector_offset.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/test_exponent_in_cast.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| cast/test_string_binary_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| cast/test_string_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cast/test_try_cast.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| cast/timestamp_casts.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/timestamp_casts_icu.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/timestamp_date_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cast/try_cast_blob_to_bit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/union_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cast/union_member_variant_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/case_insensitive_alter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| catalog/case_insensitive_binder.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 14 | 0 | 100.0% |
| catalog/case_insensitive_caps.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| catalog/case_insensitive_cte.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| catalog/case_insensitive_operations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 19 | 1 | 95.0% |
| catalog/case_insensitive_using.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/comment_on.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/comment_on_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/comment_on_column_remote_catalog.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| catalog/comment_on_dependencies.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| catalog/comment_on_extended.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/comment_on_pg_description.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| catalog/comment_on_wal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/dependencies | 25 | 4 | 21 | 0 | 0 | 0 | 16.0% | 16.0% | 63 | 21 | 75.0% |
| catalog/dependency | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| catalog/did_you_mean.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| catalog/drop_create_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| catalog/function | 36 | 1 | 35 | 0 | 0 | 0 | 2.8% | 2.8% | 118 | 35 | 77.1% |
| catalog/identifier_embedded_nul.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/information_schema_read_only.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/issue_9459.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| catalog/nested_schema | 20 | 0 | 20 | 0 | 0 | 0 | 0.0% | 0.0% | 31 | 20 | 60.8% |
| catalog/quoted_search_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| catalog/sequence | 15 | 0 | 15 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 15 | 40.0% |
| catalog/table | 8 | 2 | 5 | 0 | 0 | 1 | 25.0% | 25.0% | 32 | 5 | 86.5% |
| catalog/test_alter_oid_stability.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| catalog/test_catalog_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/test_create_from_select.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| catalog/test_extension_suggestion.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/test_if_not_exists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| catalog/test_incorrect_table_creation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/test_querying_from_detached_catalog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| catalog/test_quoted_column_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| catalog/test_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/test_schema_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| catalog/test_set_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| catalog/test_set_search_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 16 | 1 | 94.1% |
| catalog/test_standard_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/test_temporary.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/test_temporary_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| catalog/test_temporary_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/test_unicode_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| catalog/view | 19 | 1 | 18 | 0 | 0 | 0 | 5.3% | 5.3% | 67 | 18 | 78.8% |
| collate/collate_distinct_aggregate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| collate/collate_distinct_aggregate_icu.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/collate_filter_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| collate/collate_in_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/collate_like.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/collate_nested.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/collate_non_varchar.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| collate/collate_order_by_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| collate/collate_ordered_aggregate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| collate/collate_string_col.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/collate_struct_minmax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/collate_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| collate/collation_invalid_utf8.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/collation_sort_keys.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/count_distinct_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| collate/icu_collation_propagation.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 2 | 0 | 100.0% |
| collate/nop_collation_join_filter_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| collate/test_collate_accent_insensitive.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_collate_and_grouping_sets.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/test_collate_between.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| collate/test_collate_case_insensitive.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_collate_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| collate/test_collate_list.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_collate_list_contains.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_collate_null_constant.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| collate/test_collate_orderby_column_number.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_collate_pivot.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 20 | 0 | 100.0% |
| collate/test_collation_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| collate/test_combined_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_default_collations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| collate/test_icu_collate.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/test_pragma_collations.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| collate/test_strpos_collate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| collate/test_unsupported_collations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| conjunction/or_between.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 12 | 0 | 100.0% |
| conjunction/or_comparison.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 13 | 0 | 100.0% |
| connect/concurrent_connect_detach.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| connect/connect_passthrough_quack.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| connect/connect_quack.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| connect/connect_string_ephemeral.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| connect/connect_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| constraints/check | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 4 | 0.0% |
| constraints/foreignkey | 28 | 0 | 28 | 0 | 0 | 0 | 0.0% | 0.0% | 89 | 28 | 76.1% |
| constraints/primarykey | 12 | 6 | 6 | 0 | 0 | 0 | 50.0% | 50.0% | 64 | 6 | 91.4% |
| constraints/test_constraint_with_updates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| constraints/test_named_column_constraints.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| constraints/test_not_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| constraints/unique | 6 | 1 | 5 | 0 | 0 | 0 | 16.7% | 16.7% | 28 | 5 | 84.8% |
| copy/async_file_character_device.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/async_file_lifecycle.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| copy/copy_blob.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/copy_blob_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/copy_column_identifier_conversion.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| copy/copy_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy/copy_legacy_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| copy/copy_order_by_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/copy_order_by_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/copy_to_parameter_filename.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/csv | 281 | 12 | 262 | 7 | 0 | 0 | 4.3% | 4.4% | 404 | 262 | 60.7% |
| copy/encryption | 9 | 0 | 0 | 9 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| copy/file_size_bytes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy/file_size_bytes_csv_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| copy/file_size_bytes_existing_target.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/file_size_bytes_tiny_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| copy/format_uuid.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| copy/hive_partition_null_overwrite.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/parquet | 241 | 22 | 211 | 7 | 1 | 0 | 9.1% | 9.4% | 259 | 211 | 55.1% |
| copy/partitioned | 28 | 1 | 26 | 1 | 0 | 0 | 3.6% | 3.7% | 33 | 26 | 55.9% |
| copy/per_thread_output.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| copy/return_files.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy/return_stats.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| copy/return_stats_extra_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| copy/return_stats_nested.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/return_stats_truncate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/return_stats_variant.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy/row_groups_per_file.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy/tmp_file.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_database_different_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_database_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy_database/copy_database_fk.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| copy_database/copy_database_fk_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 15 | 1 | 93.8% |
| copy_database/copy_database_gen_col.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_database_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_database_multiple.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy_database/copy_database_simple.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_database_with_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| copy_database/copy_database_with_unique_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| copy_database/copy_table_with_sequence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_as.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| create/create_as_issue_11968.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_as_partition_sorted_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_database.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_index_on_issue_13643.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| create/create_objects_readonly.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| create/create_or_replace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| create/create_schema_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_table_as_duplicate_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| create/create_table_as_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_table_compression.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| create/create_table_empty_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_table_extra_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_table_with_arraybounds.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| create/create_using_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/cte_bc.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| cte/cte_colname_issue_10074.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/cte_describe.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/cte_filter_pushdown_predicates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| cte/cte_inlining_empty_result.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| cte/cte_issue_17311.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/cte_null_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/cte_on_conflict_issue.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| cte/cte_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| cte/cte_with_replacement_scan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/dml_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/incorrect_recursive_cte.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| cte/insert_cte_bug_3417.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/lazy_cte_bind.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/lazy_cte_bind_correlated.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 1 | 0 | 100.0% |
| cte/materialized | 27 | 0 | 26 | 1 | 0 | 0 | 0.0% | 0.0% | 29 | 26 | 52.7% |
| cte/recursive_array_slice.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/recursive_cte_batch_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_column_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_complex_pipelines.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| cte/recursive_cte_correlated_bindings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/recursive_cte_correlated_complex.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| cte/recursive_cte_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_invariant_build.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_invariant_build_logging.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_key_aggregation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/recursive_cte_key_final_drain.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_key_hll_aggregation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_key_probe.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| cte/recursive_cte_key_snapshot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_key_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_key_union_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| cte/recursive_cte_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_logging.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_ordered_aggregate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_ordered_member.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_parallel_output_publication.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/recursive_cte_parallel_union_all_sink.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_cte_parallel_union_distinct_sink.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| cte/recursive_hang_2745.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/test_bug_922.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 1 | 0 | 100.0% |
| cte/test_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| cte/test_cte_in_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| cte/test_cte_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/test_issue_5673.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| cte/test_moderate_union_all.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| cte/test_nested_recursive_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/test_outer_joins_recursive_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/test_recursive_cte_recurring.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| cte/test_recursive_cte_tutorial.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| cte/test_recursive_cte_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| cte/test_recursive_cte_union_all.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| cte/using_key_aggregate_qualification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| delete/bulk_delete_version_info_memory.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| delete/cleanup_delete_on_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| delete/delete_compression_after_restart.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| delete/delete_compression_blocked_by_old_snapshot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| delete/delete_query_node.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| delete/full_vector_delete_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| delete/full_vector_delete_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| delete/large_deletes_transactions.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| delete/list_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| delete/masked_vector_further_delete.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| delete/masked_vector_rollback.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 11 | 0 | 100.0% |
| delete/partial_delete_version_info_memory.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| delete/test_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| delete/test_delete_indexed.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 78 | 1 | 98.7% |
| delete/test_large_delete.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| delete/test_large_delete_parallel.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| delete/test_parallel_delete_transaction_local.test | 1 | 0 | 0 | 0 | 0 | 1 | 0.0% | 0.0% | 40 | 1 | 97.6% |
| delete/test_segment_deletes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 22 | 1 | 95.7% |
| delete/test_truncate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| delete/test_using_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| delete/test_using_delete_duplicates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| detailed_profiler/test_detailed_profiler.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| error/aggregate_order_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/correlated_at_clause.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/error_location_multicaret.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/error_location_not_duplicated.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/error_position.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/error_position_json_execute.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/escape_percent_sign.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/extension_function_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/incorrect_sql.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/lineitem_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/missing_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/mix_aggregate_and_non_aggregate.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| error/qualified_column_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/subquery_single_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/table_function_as_scalar_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| error/test_try_nested_conditional.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| error/unknown_window_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| execution/column_binding_resolver.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| execution/function_null_verification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| execution/limit_percent_runtime.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| execution/physical_verify_vector.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| execution/positional_scan_nested.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| explain/explain_all_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| explain/explain_execute.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| explain/explain_formats.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| explain/order_by_topn_explain_fqn.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| explain/test_explain.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| explain/test_explain_analyze.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| explain/test_explain_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| explain/test_explain_table_scan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| explain/tree_renderer_branches.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| export/empty_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| export/export_compression_level.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| export/export_csv_empty_strings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| export/export_database.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| export/export_external_access.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_functions.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| export/export_generated_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| export/export_hive_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| export/export_indexes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_macros.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| export/export_quoted_enum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_quoted_structs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_quoted_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_temp_tables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_typed_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/export_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| export/parquet | 8 | 0 | 8 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 8 | 50.0% |
| export/parquet_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| extensions/aliasing.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/allowed_directories_install.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| extensions/attach_type_alias_conflict.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/checked_load.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| extensions/concurrent_failed_aliased_load.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| extensions/extension_repositories.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| extensions/extension_repositories_lock.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| extensions/extension_repository_not_autoloaded.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| extensions/load_arbitrary_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| extensions/load_use_extension_one_query.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/permissions_duckdb_extension.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| extensions/quack_peeling.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/require_direct_load.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/sqlite_alter_table_add_pk.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| extensions/version_is_valid_sqlite.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| filter/filter_cache.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| filter/filter_cache_dictionary.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| filter/null_selection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| filter/test_alias_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| filter/test_columns_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| filter/test_constant_comparisons.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 20 | 1 | 95.2% |
| filter/test_dynamic_filter_nullable.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| filter/test_expression_executor_select.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| filter/test_illegal_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| filter/test_last_option_corruption.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| filter/test_like_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| filter/test_obsolete_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| filter/test_or_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| filter/test_struct_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| filter/test_throwing_filter_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| filter/test_transitive_filters.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 49 | 0 | 100.0% |
| filter/test_try_filter_doesnt_mutate_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| filter/test_variant_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| filter/test_zonemap_disjunctions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| filter/test_zonemap_segment.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 22 | 0 | 100.0% |
| filter/transitive_filter_bounds.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| function/array | 11 | 2 | 9 | 0 | 0 | 0 | 18.2% | 18.2% | 28 | 9 | 75.7% |
| function/autocomplete | 26 | 1 | 0 | 25 | 0 | 0 | 3.8% | 100.0% | 0 | 0 | 0.0% |
| function/blob | 6 | 0 | 6 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 6 | 25.0% |
| function/comparison | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| function/date | 18 | 3 | 15 | 0 | 0 | 0 | 16.7% | 16.7% | 124 | 15 | 89.2% |
| function/enum | 5 | 0 | 5 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 5 | 0.0% |
| function/generic | 36 | 3 | 32 | 1 | 0 | 0 | 8.3% | 8.6% | 127 | 32 | 79.9% |
| function/index | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| function/interval | 5 | 0 | 5 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 5 | 61.5% |
| function/list | 112 | 0 | 110 | 2 | 0 | 0 | 0.0% | 0.0% | 1266 | 110 | 92.0% |
| function/nested | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 4 | 0.0% |
| function/numeric | 29 | 1 | 27 | 1 | 0 | 0 | 3.4% | 3.6% | 49 | 27 | 64.5% |
| function/operator | 10 | 0 | 10 | 0 | 0 | 0 | 0.0% | 0.0% | 37 | 10 | 78.7% |
| function/string | 91 | 9 | 80 | 2 | 0 | 0 | 9.9% | 10.1% | 485 | 80 | 85.8% |
| function/table | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 4 | 0.0% |
| function/test_scalar_executor.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| function/time | 5 | 0 | 5 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 5 | 0.0% |
| function/timestamp | 35 | 1 | 16 | 18 | 0 | 0 | 2.9% | 5.9% | 33 | 16 | 67.3% |
| function/timetz | 4 | 0 | 2 | 2 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| function/tokenize | 2 | 0 | 0 | 2 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| function/uuid | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 2 | 83.3% |
| function/variant | 21 | 0 | 21 | 0 | 0 | 0 | 0.0% | 0.0% | 25 | 21 | 54.3% |
| generated_columns/generated_column_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| generated_columns/stored | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| generated_columns/virtual | 37 | 1 | 36 | 0 | 0 | 0 | 2.7% | 2.7% | 32 | 36 | 47.1% |
| index/art | 124 | 16 | 107 | 1 | 0 | 0 | 12.9% | 13.0% | 662 | 107 | 86.1% |
| index/create_index_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| insert/insert_by_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| insert/insert_from_many_grouping_sets.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| insert/insert_query_node.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| insert/insert_rollback.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| insert/null_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 15 | 1 | 93.8% |
| insert/test_big_insert.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 12 | 0 | 100.0% |
| insert/test_insert.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| insert/test_insert_ignored_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| insert/test_insert_invalid.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| insert/test_insert_query.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| insert/test_insert_type.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| insert/unaligned_interleaved_appends.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 19 | 0 | 100.0% |
| join/asof | 20 | 0 | 20 | 0 | 0 | 0 | 0.0% | 0.0% | 37 | 20 | 64.9% |
| join/comparison_operand_extraction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| join/cross_product | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| join/delim_join_reuse.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| join/empty_joins.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| join/external | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 36 | 2 | 94.7% |
| join/full_outer | 8 | 4 | 4 | 0 | 0 | 0 | 50.0% | 50.0% | 54 | 4 | 93.1% |
| join/hash_join | 6 | 0 | 5 | 1 | 0 | 0 | 0.0% | 0.0% | 94 | 5 | 94.9% |
| join/iejoin | 15 | 0 | 14 | 1 | 0 | 0 | 0.0% | 0.0% | 60 | 14 | 81.1% |
| join/inner | 25 | 8 | 17 | 0 | 0 | 0 | 32.0% | 32.0% | 169 | 17 | 90.9% |
| join/left_outer | 11 | 7 | 4 | 0 | 0 | 0 | 63.6% | 63.6% | 101 | 4 | 96.2% |
| join/mark | 13 | 0 | 13 | 0 | 0 | 0 | 0.0% | 0.0% | 40 | 13 | 75.5% |
| join/natural | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| join/nearest | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 4 | 77.8% |
| join/non_inner_decorrelation_regressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| join/non_inner_join_subquery.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 17 | 0 | 100.0% |
| join/non_inner_join_subquery_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| join/pair_dependent_full_outer_join_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 18 | 1 | 94.7% |
| join/pair_dependent_full_outer_join_subquery_bindings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| join/pair_dependent_full_outer_join_subquery_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| join/pair_dependent_join_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| join/pair_dependent_outer_reference_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| join/pair_dependent_right_semi_anti.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| join/pair_dependent_volatile_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| join/positional | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 2 | 81.8% |
| join/pushdown | 3 | 1 | 2 | 0 | 0 | 0 | 33.3% | 33.3% | 12 | 2 | 85.7% |
| join/right_outer | 3 | 2 | 1 | 0 | 0 | 0 | 66.7% | 66.7% | 36 | 1 | 97.3% |
| join/semianti | 10 | 0 | 10 | 0 | 0 | 0 | 0.0% | 0.0% | 38 | 10 | 79.2% |
| join/set_operators | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| join/test_between_null_bound_join.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| join/test_complex_join_expr.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| join/test_complex_range_join.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| join/test_join_always_true_conditions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| join/test_join_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| join/test_join_on_aggregates.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| join/test_merge_join_predicate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| join/test_nested_inequality.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| join/test_predicate_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| join/test_string_payload.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| json/copy_geojson.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/copy_json_null_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/copy_json_partition_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/issues | 42 | 1 | 41 | 0 | 0 | 0 | 2.4% | 2.4% | 10 | 41 | 19.6% |
| json/scalar | 34 | 1 | 32 | 1 | 0 | 0 | 2.9% | 3.0% | 151 | 31 | 83.0% |
| json/table | 23 | 0 | 23 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 23 | 8.0% |
| json/test_json_bool.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| json/test_json_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_copy_dateformat_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_copy_serialize_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_deserialize_grouping_sets.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_deserialize_invalid_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_deserialize_invalid_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_deserialize_invalid_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_deserialize_missing_refs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_empty_object.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_execute_serialized_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| json/test_json_geometry_format.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_group_window_macro.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| json/test_json_macros.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| json/test_json_path_long_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_persistence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_serialize_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| json/test_json_serialize_sql.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| json/test_json_sqlite.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| keywords/escaped_quotes_expressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| keywords/keywords_in_expressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| limit/limit_exceeds_child_cardinality.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| limit/streaming_limit_pipeline_flush.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| limit/streaming_limit_pipeline_flush2.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| limit/test_batch_limit_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| limit/test_limit0.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| limit/test_preserve_insertion_order.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| logging/enable_logging_file_no_path.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/file_system_logging_attach.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| logging/file_system_logging_attach_deadlock.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| logging/http_log_timing.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| logging/http_request_log_redaction.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| logging/http_response_log_redaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| logging/http_url_log_redaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| logging/logging.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_branch_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_buffer_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_call_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_context_ids.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_csv.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_file_bind_replace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_file_persistence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_file_truncate_rewrite.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/logging_memory_after_truncate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| logging/logging_non_queryable_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| logging/logging_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/test_enable_logging_unknown_type_file_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| logging/test_logging_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| merge/merge_into.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_bind_matching_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_by_source.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| merge/merge_into_constraint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| merge/merge_into_default.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| merge/merge_into_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| merge/merge_into_explain_analyze.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| merge/merge_into_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| merge/merge_into_insert_star.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| merge/merge_into_invalid_action.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| merge/merge_into_invalid_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| merge/merge_into_invalid_column_reference.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| merge/merge_into_join_as_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| merge/merge_into_multiple_updates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_parenthesis_bug.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| merge/merge_into_pipelines.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_returning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_returning_explain_no_optimizer.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| merge/merge_into_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| merge/merge_into_subquery_action.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| merge/merge_into_subquery_condition.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| merge/merge_into_subquery_condition_delete.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 23 | 0 | 100.0% |
| merge/merge_into_too_few_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| merge/merge_into_update_star.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| merge/merge_multi_match_error.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 36 | 0 | 100.0% |
| merge/merge_not_matched_by_source_join_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| merge/trigger_merge_into.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| multi_file/multi_file_file_row_number.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| multi_file/multi_file_open_options.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| multi_file/multi_file_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| multi_file/multi_file_schema_filter_casts.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/aggregate_stats_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/anti_join_empty_rhs_projection_map.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/any_extremum_rewrite.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 17 | 1 | 94.4% |
| optimizer/array_extract_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/barrier_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/bool_ref_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/boolean_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/boolean_wrapper_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/case_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/coalesce_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/constant_function_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/contains_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/cross_column_expression_stats_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/date_part_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/distinct_sibling_column_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| optimizer/divide_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/empty_string_concat_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/expression | 25 | 3 | 21 | 1 | 0 | 0 | 12.0% | 12.5% | 91 | 21 | 81.2% |
| optimizer/false_or_null_segment_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| optimizer/in_list_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/instr_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/interval_column_stats_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/interval_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/joins | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| optimizer/late_materialization_pushdown_extract.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/late_materialization_table_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/left_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/list_contains_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/map_extract_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/monotone_function_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/monotone_preimage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/monotone_preimage_tinyint_boundaries.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/monotone_preimage_tz.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| optimizer/multiple_column_stats_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/narrowing_cast_not_invertible.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/nested_null_segment_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/not_conjunction_simplification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/null_check_statistics_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/nullable_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/or_null_simplification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/plan | 15 | 3 | 10 | 1 | 0 | 1 | 20.0% | 21.4% | 52 | 11 | 82.5% |
| optimizer/power_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/predicate_factoring.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| optimizer/prefix_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/projection_placement | 2 | 0 | 0 | 2 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| optimizer/pushdown_extract_multiple_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/pushdown_extract_reapplied.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/regex_range_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| optimizer/rewrite_nan_null.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 38 | 0 | 100.0% |
| optimizer/secure_view_explain.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/secure_view_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/secure_view_pushdown_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/secure_view_statistics.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/sign_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/string_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/string_stats_merge.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/string_stats_merge_truncated_prefix.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| optimizer/struct_segment_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/substring_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/sum_aggregate_stats_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/table_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 18 | 1 | 94.7% |
| optimizer/temporal_cast_dst.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| optimizer/temporal_cast_zonemap.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/test_common_subplan_cte_binding_order.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| optimizer/test_common_subplan_nested_bindings.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| optimizer/test_common_subplan_projection_maps.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/test_common_subplan_table_filters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| optimizer/test_duplicate_groups_optimizer.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| optimizer/test_fd_derived_groups.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| optimizer/test_glob_row_group_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/test_in_rewrite_rule.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/test_negated_filter_cardinality.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| optimizer/test_no_pushdown_cast_into_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/test_not_comparison_simplification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/test_rollup_column_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/test_rowid_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/test_rowid_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/test_rowid_pushdown_deletes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 31 | 1 | 96.9% |
| optimizer/test_rowid_pushdown_plan.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| optimizer/test_window_self_join.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| optimizer/tests_no_pushdown_under_samples.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/timestamp_tz_cast_zonemap_pruning.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| optimizer/timestamp_unit_cast_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/topn_window_elimination_argument_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/topn_window_elimination_late_materialization.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| optimizer/topn_window_elimination_max_n.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| optimizer/topn_window_elimination_multiplicity.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| optimizer/topn_window_elimination_nullable_order.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 11 | 0 | 100.0% |
| optimizer/topn_window_set_elimination.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/try_struct_extract_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/unsigned_divide_statistics.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/upper_lower_zonemap_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/variant_json_cast_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| optimizer/window_self_join_optimizer_csv.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| optimizer/yearweek_negative_iso_year.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/hugeint_order_by_extremes.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| order/issue_11936.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/limit_full_outer_join.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| order/limit_invalid_index_value.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/limit_parameter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| order/limit_percent_view.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| order/limit_union.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| order/negative_offset.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/order_by_all.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| order/order_by_internal_5293.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/order_by_negative_zero.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| order/order_merge_buffer_fallback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| order/order_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/test_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| order/test_limit_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/test_limit_nested_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/test_limit_parameter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/test_limit_percent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| order/test_nulls_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 20 | 1 | 95.2% |
| order/test_order_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 22 | 1 | 95.7% |
| order/test_order_by_exceptions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| order/test_order_by_non_ordinal_literal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| order/test_order_large.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| order/test_order_pragma.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| order/test_order_range_mapping.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/test_order_unnest.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| order/top_n_issue_21623.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| order/top_n_limit_zero.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 509 | 0 | 100.0% |
| order/top_n_nulls.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| ordinality/ordinality_constant.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| ordinality/ordinality_inout.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| overflow/bigint_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| overflow/double_float_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| overflow/expression_tree_depth.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| overflow/integer_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| overflow/table_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| overflow/test_numeric_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| overflow/test_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parallelism/async_task_schedule_pool.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| parallelism/interquery | 7 | 1 | 6 | 0 | 0 | 0 | 14.3% | 14.3% | 498 | 6 | 98.8% |
| parallelism/intraquery | 8 | 1 | 6 | 1 | 0 | 0 | 12.5% | 14.3% | 27 | 6 | 81.8% |
| parallelism/modify_thread_settings.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 44 | 0 | 100.0% |
| parser/adjacent_string_literal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/backtick_identifiers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/bare_column_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| parser/bare_column_alias_keywords.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/columns_aliases.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/columns_issue9867.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/create_macro_tostring.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/cte_printing.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| parser/custom_prefix_operator.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/deeply_nested_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/digit_separators.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/division_operator_precedence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/dollar_quotes_internal_issue2224.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/drop_qualified_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| parser/empty_quoted_identifier.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/expression_depth_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/external_resource_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/fetch_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| parser/from_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/function_chaining.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/hex_string_literal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/indirection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/invalid_utf8_parser_location.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| parser/invisible_spaces.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/is_not_null_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/join_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/literal_round_trip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/nested_join_backtracking.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/not_comparison_precedence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/numeric_literal_underscore_trailing_dot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/parsed_data_statement_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/parser_rewrites_method_chain.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/position_prepare.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/qualified_underscore_digit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| parser/reject_accidental_expression_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/reject_accidental_statement_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/reject_accidental_type_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/reject_unknown_option_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/single_quoted_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/star_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/statement_tostring_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/switch_case.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/switch_null_arguments.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/table_at_clause.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/test_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/test_columns_lists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/test_columns_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/test_columns_prepared.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/test_columns_unpacked.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/test_columns_where.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/test_deep_expression_chains.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/test_deep_expression_nesting.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/test_long_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/test_many_or.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/test_operator_precedence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| parser/test_value_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| parser/trailing_commas.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/unquoted_underscore_digit_identifiers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| parser/unreserved_keyword_casing.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| parser/values_list_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/values_list_without_brackets.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| parser/wide_type_definitions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| peg_parser/binary_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| peg_parser/deep_nesting.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| peg_parser/enable_peg_parser.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| peg_parser/escape_string.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| peg_parser/fuzzer | 16 | 1 | 15 | 0 | 0 | 0 | 6.2% | 6.2% | 5 | 15 | 25.0% |
| peg_parser/identifier.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| peg_parser/parser | 47 | 0 | 1 | 46 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| peg_parser/peg_syntax_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| peg_parser/qualified_operator.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| peg_parser/transformer | 17 | 0 | 7 | 10 | 0 | 0 | 0.0% | 0.0% | 13 | 7 | 65.0% |
| peg_parser/unterminated_literals.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_attribute.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_constraint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_database.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_depend.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_enum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pg_catalog/pg_namespace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_prepared_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_privilege.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_sequence.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_sequences.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pg_catalog/pg_tablespace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/pg_views.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pg_catalog/sqlalchemy.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pg_catalog/system_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/optional_pivots.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| pivot/pivot_15141.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_6390.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/pivot_bigquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/pivot_case_insensitive.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_databricks.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/pivot_empty.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_empty_string_value.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/pivot_enum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/pivot_expressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| pivot/pivot_generated.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/pivot_identical_aggregates_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/pivot_in_boolean.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 12 | 0 | 100.0% |
| pivot/pivot_in_map.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/pivot_in_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| pivot/pivot_operator_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_prepare.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_query_text.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_star.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/pivot_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_struct_aggregate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/pivot_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/pivot_value_collisions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/test_multi_pivot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/test_pivot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pivot/test_pivot_duplicate_aggregates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/test_unpivot.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/test_unpivot_stmt.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pivot/top_level_pivot_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pivot/unpivot_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/unpivot_internal_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pivot/unpivot_no_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pivot/unpivot_non_aligned_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pivot/unpivot_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/unpivot_unnamed_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pivot/unpivot_view.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pragma/pragma_database_size_readonly.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pragma/profiling | 29 | 0 | 29 | 0 | 0 | 0 | 0.0% | 0.0% | 62 | 29 | 68.1% |
| pragma/storage_info_empty_enum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_db_invalidation_after_load.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_disabled_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| pragma/test_enable_profile.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_force_compression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| pragma/test_memory_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pragma/test_metadata_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pragma/test_pragma_database_list.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pragma/test_pragma_database_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_pragma_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_pragma_parsing.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_pragma_sanitized_inputs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| pragma/test_pragma_version.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| pragma/test_query_log.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| pragma/test_show_tables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| pragma/test_show_tables_from.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_show_tables_temp_views.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| pragma/test_storage_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| pragma/test_table_info.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pragma/test_table_info_quoted_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| pragma/test_various_pragmas.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| prepared/invalid_prepare.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| prepared/parameter_order_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/parameter_variants.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_catalog_table_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/prepare_copy.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_default_varchar.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_from_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_in.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/prepare_lambda.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_list_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_maintain_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_mixed_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_offset_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepare_summarize.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/prepare_window_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepared_named_param.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepared_null_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepared_select.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/prepared_stale_statistics.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| prepared/prepared_statistics_optimization.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/prepared_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| prepared/test_basic_prepare.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_issue_2079.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_issue_21077.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_issue_4010.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/test_issue_4042.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/test_issue_6276.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_ambiguous_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_delete_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_drop.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_issue_5132.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| prepared/test_prepare_issue_8500.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_select.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| prepared/test_prepare_unused_cte.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| progress/test_verify_progress.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| projection/coalesce_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| projection/coalesce_rowid.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| projection/reference_only_output.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| projection/select_star_exclude.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| projection/select_star_like.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| projection/select_star_rename.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| projection/select_star_replace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| projection/select_struct_star.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| projection/test_columns_replace_using.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| projection/test_complex_expressions.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 37 | 0 | 100.0% |
| projection/test_row_id.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| projection/test_row_id_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| projection/test_scalar_projection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| projection/test_simple_projection.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 13 | 0 | 100.0% |
| projection/test_table_star.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| projection/test_value_list.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| returning/no_crash_when_no_returning_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| returning/returning_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| returning/returning_delete_list.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| returning/returning_delete_same_transaction.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 31 | 0 | 100.0% |
| returning/returning_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| returning/returning_large.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| returning/returning_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| sample/can_sample_from_ingested_files.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sample/helper_operator_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sample/reservoir_sample.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sample/reservoir_sample_memory_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sample/same_seed_same_sample.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sample/same_seed_same_sample_vec_size_2.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| sample/system_sample_index_scan.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| sample/table_samples | 5 | 4 | 1 | 0 | 0 | 0 | 80.0% | 80.0% | 2 | 1 | 66.7% |
| sample/test_sample_too_big.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| sample/test_system_rows.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sample/test_system_rows_pushdown.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| secrets/connection_secret_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| secrets/create_secret_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/create_secret_filesystem_logging.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| secrets/create_secret_hffs_autoload.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/create_secret_persistence_no_client_context.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/create_transaction_secret.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| secrets/drop_persistent_secret_if_exists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/drop_temporary_secret_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| secrets/http_env_scope_lookup.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/http_secret_redaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/persistent_secret_map_value.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| secrets/secret_autoloading_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| secrets/secret_compatibility_http.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| secrets/test_secret_type_parameters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| select/test_multi_column_reference.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| select/test_positional_reference.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| select/test_projection_names.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 14 | 0 | 100.0% |
| select/test_schema_reference.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| select/test_select_alias_prefix_colon.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| select/test_select_empty_table.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| select/test_select_into.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| select/test_select_locking.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| select/test_select_qualified_view.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| serialization/logical_operator_serialization.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| setops/ambiguous_order_by.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| setops/except_intersect_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| setops/setops_pushdown.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| setops/test_except.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| setops/test_full_outer_join_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| setops/test_nested_except.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| setops/test_pg_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| setops/test_setops.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| setops/test_union_all_by_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| setops/test_union_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| setops/test_union_by_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| setops/test_union_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| setops/test_union_except_empty.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| setops/test_union_order.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| setops/test_union_subquery.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| setops/test_union_type_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| setops/union_aggregate.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| setops/union_all_projection_pushdown.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 16 | 0 | 100.0% |
| setops/union_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| setops/union_order_by_expression.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| setops/union_pipeline.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| setops/union_shared_scan.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| setops/union_single_dedup.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| setops/value_union.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| settings/access_mode.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/allowed_configs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/allowed_configs_extensions.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| settings/allowed_directories.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/allowed_directories_home_directory.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/allowed_paths.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/block_allocator_memory.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/concurrent_set_disabled_optimizers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1429 | 1 | 99.9% |
| settings/connection_local_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/debug_deprecated_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/default_null_order_extended.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| settings/deprecated_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| settings/dialect_compatibility_mode.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/drop_set_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/errors_as_json.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/http_client_pool_capacity.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| settings/integer_division_setting.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/lock_configuration_pragmas.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/lock_configuration_schema.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| settings/max_execution_time.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| settings/max_execution_time_nested_expansion.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/max_memory_percentage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/max_streaming_buffer_size.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/memory_limit_settings_unlimited.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/operator_memory_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| settings/pragma_assignment_deprecated.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| settings/reset | 2 | 2 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| settings/set_null_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/set_schema_temp_main.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/setting_collection_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_disabled_optimizer.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| settings/setting_exhaustive.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_null_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/setting_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| settings/setting_preserve_identifier_case.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_profiling_mode.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| settings/setting_threads.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/setting_validation_and_reset.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| settings/settings_icu.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| settings/test_disabled_file_systems.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/test_disabled_local_filesystem_metadata.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| settings/test_disabled_local_filesystem_secrets.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| settings/test_external_access_metadata.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| settings/test_external_access_secrets.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| settings/test_lock_configuration.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/test_progress_bar_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| settings/user_agent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| show_select/describe_rowid.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| show_select/describe_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| show_select/show_quote_identifier.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| show_select/show_select_constraints.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| show_select/show_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| show_select/show_settings_extension.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| show_select/summarize_subquery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| show_select/test_describe_all.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| show_select/test_describe_quoted.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 17 | 0 | 100.0% |
| show_select/test_show_select.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| show_select/test_summarize.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| show_select/test_summarize_quoted.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/aggregate_literals.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/aggregate_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| sql_export/aggregate_topn_rewrites.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/alias.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/basic_plan_shapes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| sql_export/call_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| sql_export/catalog_invocations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/ci_reconstruction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/collated_aggregate_state.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/collation_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/composition_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/composition_q1.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 17 | 0 | 100.0% |
| sql_export/composition_q1_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/composition_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| sql_export/compressed_materialization_sql_export.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| sql_export/compression_fallbacks.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/compression_fallbacks_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/constant_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/correlated_metadata.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/correlated_sql_export.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 11 | 0 | 100.0% |
| sql_export/cte_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/cte_result_matrices.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/dml_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/empty_aggregate_state.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/empty_enum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/execution_once.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/explain_sql.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/explain_sql_formatting.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/explain_sql_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| sql_export/explain_sql_scopes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/expression_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/function_result_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/geometry_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/grouped_mark_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/icu_datepart_roundtrip.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| sql_export/join_multiplicities.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/json_ordering.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/legacy_source_serialization.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/mark_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/mark_condition_shapes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/mark_export_effects.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| sql_export/mark_nested_comparison_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/mark_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/mark_strategies.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/named_scalar_arguments.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/nested_collated_nulls.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/nested_collations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/nested_result_collation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/nested_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/null_safe_mark_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/numbered_scan_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/pivot_list_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/pivot_sql_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/plain_scope_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/plan_reconstruction_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/pruned_offset.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| sql_export/pure_values_input.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/pushed_column_paths.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/pushed_projection_rejection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/range_folded_offset.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/range_frames.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/range_origin_inputs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/recursive_payload_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/recursive_payload_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/recursive_state.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/regex_range_filter_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/result_modifiers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/rewrite_qualification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| sql_export/rewritten_functions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/sampling_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/scalar_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/scope_reuse.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/scope_reuse_plan.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/secure_view_predicates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/secure_view_sql_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/single_child_union.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| sql_export/slices_and_limits.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/source_arguments.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/source_invocation_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 18 | 1 | 94.7% |
| sql_export/source_names.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| sql_export/source_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/sql_export_aggregate_order_by_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/sql_export_join_scopes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/sql_export_quantiles.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/statement_verification.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/structural_expressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 20 | 1 | 95.2% |
| sql_export/table_row_number_sql_export.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/tuple_type_constants.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/union_type_witness.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| sql_export/union_value.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| sql_export/values_consumers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/values_effect_rejection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| sql_export/variant_key_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/variant_key_rejection.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| sql_export/window_unnest_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| sql_export/zero_column_relations.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/adaptive_segment_allocation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/adaptive_segment_lifecycle.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/adaptive_segment_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| storage/alter | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 4 | 66.7% |
| storage/append_strings_to_persistent.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| storage/attach | 7 | 0 | 7 | 0 | 0 | 0 | 0.0% | 0.0% | 23 | 7 | 76.7% |
| storage/attach_mmap.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/bc | 5 | 0 | 2 | 3 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| storage/buffer_manager_temp_dir.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/catalog | 26 | 6 | 20 | 0 | 0 | 0 | 23.1% | 23.1% | 129 | 20 | 86.6% |
| storage/checkpoint | 8 | 0 | 8 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 8 | 60.0% |
| storage/checkpointed_self_append.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| storage/checkpointed_self_append_tinyint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/commit_abort.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 16 | 0 | 100.0% |
| storage/commit_abort_medium.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 15 | 0 | 100.0% |
| storage/commit_index_deletes.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| storage/compact_block_size | 9 | 0 | 6 | 3 | 0 | 0 | 0.0% | 0.0% | 5 | 6 | 45.5% |
| storage/compression | 190 | 17 | 93 | 80 | 0 | 0 | 8.9% | 15.5% | 417 | 91 | 82.1% |
| storage/concurrent_attach_if_not_exists_create.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/constraints | 2 | 1 | 1 | 0 | 0 | 0 | 50.0% | 50.0% | 4 | 1 | 80.0% |
| storage/corrupt | 6 | 0 | 0 | 6 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/delete | 5 | 4 | 1 | 0 | 0 | 0 | 80.0% | 80.0% | 81 | 1 | 98.8% |
| storage/distinct_statistics_sample.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| storage/distinct_statistics_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| storage/encryption | 7 | 0 | 0 | 7 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/evict_unpinned_without_temp_directory.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/extended_string_stats.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/extensions | 2 | 1 | 1 | 0 | 0 | 0 | 50.0% | 50.0% | 4 | 1 | 80.0% |
| storage/external_file_cache | 4 | 0 | 2 | 2 | 0 | 0 | 0.0% | 0.0% | 4 | 2 | 66.7% |
| storage/filter_pushdown_struct.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/full_vector_delete_checkpoint.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| storage/icu_collation.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/issue3789_node_segment_tree.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| storage/issue7582_list_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| storage/lazy_load | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/many_checkpoints.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 16 | 0 | 100.0% |
| storage/many_self_append.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/memory | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/metadata | 9 | 1 | 8 | 0 | 0 | 0 | 11.1% | 11.1% | 26 | 8 | 76.5% |
| storage/mix | 3 | 2 | 1 | 0 | 0 | 0 | 66.7% | 66.7% | 43 | 1 | 97.7% |
| storage/multiple_clients_checkpoint_pending_updates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| storage/nan_min_statistics.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/nested | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/null_byte_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| storage/optimistic_write | 11 | 4 | 7 | 0 | 0 | 0 | 36.4% | 36.4% | 84 | 7 | 92.3% |
| storage/overflow_strings | 2 | 0 | 1 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/partial_blocks | 5 | 0 | 5 | 0 | 0 | 0 | 0.0% | 0.0% | 417 | 5 | 98.8% |
| storage/partial_delete_checkpoint_mask.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/partial_delete_pending_compress.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/piecemeal_delete_checkpoint_compress.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 15 | 0 | 100.0% |
| storage/pragma_storage_info_scan_type.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/prefetch | 6 | 0 | 4 | 2 | 0 | 0 | 0.0% | 0.0% | 99 | 4 | 96.1% |
| storage/read_duckdb | 9 | 0 | 9 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 9 | 0.0% |
| storage/reclaim_space | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 2 | 81.8% |
| storage/row_group_fetch_bulk.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 31 | 0 | 100.0% |
| storage/rowid_gaps | 19 | 0 | 18 | 1 | 0 | 0 | 0.0% | 0.0% | 3 | 18 | 14.3% |
| storage/shutdown_create_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/shutdown_running_transaction.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| storage/shutdown_running_transaction_updates.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| storage/shutdown_unique_index.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 13 | 0 | 100.0% |
| storage/storage_enable_external_access.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| storage/storage_exceeds_block_large_string.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/storage_exceeds_single_block.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/storage_exceeds_single_block_strings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/storage_exceeds_single_block_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/storage_option_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/storage_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/storage_version_65.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/storage_versions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/store_group_order_all.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| storage/temp_directory | 12 | 0 | 8 | 4 | 0 | 0 | 0.0% | 0.0% | 15 | 8 | 65.2% |
| storage/test_empty_shutdown.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 0 | 0 | 0.0% |
| storage/test_empty_table.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| storage/test_eviction_queue_dead_nodes_underflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| storage/test_ignore_duplicate_deletes_unique_index.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| storage/test_index_checkpoint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/test_large_commits.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| storage/test_read_only_wal_replay.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/test_restart_extension.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| storage/test_show_tables_persistent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| storage/test_storage_scan.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| storage/test_store_integers.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 47 | 0 | 100.0% |
| storage/test_store_nulls_strings.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| storage/test_truncate_persistent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/test_unaligned_scan.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| storage/test_unique_index_checkpoint.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/total_string_length_nested_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage/total_string_length_repeated_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| storage/types | 57 | 5 | 47 | 5 | 0 | 0 | 8.8% | 9.6% | 98 | 47 | 67.6% |
| storage/unicode_filename.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 46 | 0 | 100.0% |
| storage/unzip.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| storage/update | 5 | 3 | 2 | 0 | 0 | 0 | 60.0% | 60.0% | 28 | 2 | 93.3% |
| storage/vacuum | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| storage/wal | 63 | 22 | 41 | 0 | 0 | 0 | 34.9% | 34.9% | 534 | 41 | 92.9% |
| storage_version/storage_hyperloglog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| storage_version/storage_version_bump.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| subquery/any_all | 15 | 7 | 8 | 0 | 0 | 0 | 46.7% | 46.7% | 142 | 8 | 94.7% |
| subquery/any_all_variant_extremum.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| subquery/complex | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 4 | 55.6% |
| subquery/exists | 9 | 4 | 5 | 0 | 0 | 0 | 44.4% | 44.4% | 57 | 5 | 91.9% |
| subquery/lateral | 21 | 0 | 21 | 0 | 0 | 0 | 0.0% | 0.0% | 32 | 21 | 60.4% |
| subquery/scalar | 40 | 9 | 30 | 1 | 0 | 0 | 22.5% | 23.1% | 185 | 30 | 86.0% |
| subquery/table | 4 | 3 | 1 | 0 | 0 | 0 | 75.0% | 75.0% | 17 | 1 | 94.4% |
| subquery/test_neumann.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 11 | 0 | 100.0% |
| subquery/test_offset.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/create_external_resource.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/database_oid.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| table_function/duckdb_available_metrics.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/duckdb_constraints.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_constraints_fk.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/duckdb_constraints_issue11284.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_constraints_issue12863.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/duckdb_databases.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_dependencies.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_dialects.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_eviction_queues.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| table_function/duckdb_eviction_queues_transactions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| table_function/duckdb_extensions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_functions_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_indexes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_keywords.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_memory_convert_to_persistent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_optimizers.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_prepared_statements.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_schemas.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_sequences.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_settings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_settings_extension.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/duckdb_tables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/duckdb_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/duckdb_views.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/external_resource_discovery.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/external_resource_discovery_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/external_resource_logging.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/external_resource_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/icu_range_timestamptz.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| table_function/information_schema.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| table_function/information_schema_fkey_constraint_names.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/information_schema_issue12867.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/information_schema_types.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 1 | 0 | 100.0% |
| table_function/issue_25882.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| table_function/lateral_table_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/postgres_setof_compatibility.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/range_cardinality_extremes.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| table_function/range_function_different_iterators.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| table_function/range_function_lateral.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/range_non_foldable.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| table_function/range_timestamp.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| table_function/range_timestamp_lateral_empty.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/read_text_and_blob.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/register_external_resource.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/sqlite_master.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/sqlite_master_connections.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| table_function/sqlite_master_quotes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/table_function_positional_by_name.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/test_information_schema_columns.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/test_range_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| table_function/test_repeat_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/test_repeat_row_function.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| table_function/test_table_function.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| table_function/warn_deprecated_identifier_conversion.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| timezone/disable_timestamptz_casts.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| timezone/icu_calendar_operations.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_chinese.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_coptic.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_hebrew.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_islamic.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_islamic_observed.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_japanese.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_persian.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_range_error.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_calendar_systems.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_casts.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_datetime_overflow.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_datetime_sweep.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_parse_time_zone_per_row.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_timezone.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_timezone_edge_cases.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_timezone_far_future.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/test_icu_timezone_properties.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| timezone/timestamptz_ns_to_date.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| topn/test_top_n.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| topn/test_top_n_distinct_row_group_pruner.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| topn/test_top_n_lists.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 20 | 1 | 95.2% |
| topn/test_top_n_medium.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| topn/test_top_n_nested_struct.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| topn/test_top_n_nulls_first.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| topn/test_top_n_nulls_first_row_group_reorderer_edge_cases.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| topn/test_top_n_nulls_row_group_pruner.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| topn/test_top_n_nulls_row_group_pruner_extensive.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| topn/test_top_n_nulls_small.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 46 | 0 | 100.0% |
| topn/test_top_n_offset_no_limit_pruner.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| topn/test_top_n_structs.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| topn/top_n_materialization.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| topn/top_n_pruning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| topn/topn_dynamic_filter_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| topn/topn_nulls_first.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| topn/tpcds_q14_topn.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| topn/tpcds_q59_topn.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| tpcds/dsdgen_readonly.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpcds/dsdgen_sync_scan.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpcds/tpcds_sf0.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpch/dbgen_readonly.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpch/dbgen_reference_values.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| tpch/tpch_sf0.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| transactions/aborted_transaction_commit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| transactions/commit_revert_failure.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| transactions/concurrent_drop_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| transactions/concurrent_drop_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| transactions/conflict_drop_then_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| transactions/conflict_rename_append.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| transactions/count_star_transactions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/delete_and_drop_in_same_transaction.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| transactions/delete_index_lookup_transactions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| transactions/min_max_deletes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/min_max_updates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/revert_commit_index_corruption_drop_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| transactions/rollback_drop.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| transactions/rollback_drop_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| transactions/rollback_drop_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| transactions/rollback_drop_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| transactions/rollback_drop_table_with_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| transactions/rollback_drop_table_with_delete_concurrent.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| transactions/rollback_drop_table_with_pk.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| transactions/statement-preprocessor | 9 | 0 | 9 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 9 | 10.0% |
| transactions/test_alter_preserves_delete_indexes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| transactions/test_alter_sequence_ownership_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_alter_sequence_ownership_conflict2.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_alter_sequence_ownership_conflict3.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_basic_transactions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_cardinality_estimation_uncommitted.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| transactions/test_commit_cleanup_oom.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| transactions/test_commit_cleanup_oom_index_retry.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| transactions/test_create_drop_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_create_drop_conflict_variation1.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_create_drop_conflict_variation2.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/test_ddl_after_dml.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| transactions/test_from_update_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_index_abort.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 10 | 1 | 90.9% |
| transactions/test_index_delete_cleanup_non_main_storage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| transactions/test_index_huge_aborted_append.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 11 | 0 | 100.0% |
| transactions/test_index_large_aborted_append.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 126 | 0 | 100.0% |
| transactions/test_index_local_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| transactions/test_index_pending_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_index_pending_delete_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_index_pending_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| transactions/test_index_pending_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| transactions/test_index_rollback_flushed_data.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| transactions/test_index_transaction_local.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 30 | 0 | 100.0% |
| transactions/test_index_versioned_deletes.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 28 | 0 | 100.0% |
| transactions/test_index_versioned_updates.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| transactions/test_interleaved_versions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 12 | 1 | 92.3% |
| transactions/test_multi_transaction_append.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 19 | 1 | 95.0% |
| transactions/test_multi_version.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 19 | 0 | 100.0% |
| transactions/test_multi_version_large.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| transactions/test_null_version.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| transactions/test_parser_error_transaction_invalidation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_read_only_transaction_temp_writes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| transactions/test_read_only_transactions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| transactions/test_stacked_schema_change.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 17 | 0 | 100.0% |
| transactions/test_transaction_abort.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| transactions/test_transaction_functionality.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| transactions/test_transaction_local_add.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| transactions/test_transaction_local_alter_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| transactions/test_transaction_local_data.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 12 | 0 | 100.0% |
| transactions/test_transaction_local_drop.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| transactions/test_transaction_local_unique.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| transactions/test_transaction_local_unique_ops.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 23 | 0 | 100.0% |
| transactions/test_transactional_sequences.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| transactions/test_validity_append_rollback.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| transactions/transaction_errors.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| transactions/transaction_insert_delete_chunks.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/transaction_insert_mixed_deletes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| transactions/txid_current.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| transactions/types | 3 | 2 | 1 | 0 | 0 | 0 | 66.7% | 66.7% | 32 | 1 | 97.0% |
| trigger/create_trigger_catalog.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/create_trigger_parse.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| trigger/create_trigger_wal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/drop_trigger.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/drop_trigger_parse.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| trigger/drop_trigger_wal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/temporary_trigger.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| trigger/trigger_after_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_after_delete_referencing.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_after_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_after_insert_referencing.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_after_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_after_update_old_new.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_after_update_old_new_wal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_after_update_referencing.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_alter_table.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_before_after_mixed.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_before_delete.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_before_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_before_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_concurrent_rename.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_dependency.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_dependency_restart.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| trigger/trigger_for_each_row.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_multiple_per_event.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| trigger/trigger_referencing_validation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| trigger/trigger_returning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| trigger/trigger_returning_binder_parity.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| trigger/trigger_update_from_multi_match.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| types/alias | 9 | 0 | 9 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 9 | 10.0% |
| types/bignum | 22 | 0 | 22 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 22 | 4.3% |
| types/bit | 9 | 0 | 9 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 9 | 18.2% |
| types/blob | 7 | 0 | 7 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 7 | 12.5% |
| types/cast_expression_storage_version.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/date | 8 | 1 | 7 | 0 | 0 | 0 | 12.5% | 12.5% | 51 | 7 | 87.9% |
| types/decimal | 26 | 2 | 24 | 0 | 0 | 0 | 7.7% | 7.7% | 92 | 24 | 79.3% |
| types/enum | 23 | 1 | 21 | 1 | 0 | 0 | 4.3% | 4.5% | 4 | 21 | 16.0% |
| types/float | 13 | 5 | 8 | 0 | 0 | 0 | 38.5% | 38.5% | 173 | 8 | 95.6% |
| types/geo | 38 | 0 | 38 | 0 | 0 | 0 | 0.0% | 0.0% | 21 | 38 | 35.6% |
| types/hugeint | 12 | 3 | 9 | 0 | 0 | 0 | 25.0% | 25.0% | 110 | 9 | 92.4% |
| types/interval | 13 | 0 | 13 | 0 | 0 | 0 | 0.0% | 0.0% | 59 | 13 | 81.9% |
| types/list | 39 | 1 | 38 | 0 | 0 | 0 | 2.6% | 2.6% | 36 | 38 | 48.6% |
| types/map | 6 | 0 | 6 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 6 | 40.0% |
| types/nested | 88 | 2 | 85 | 1 | 0 | 0 | 2.3% | 2.3% | 77 | 85 | 47.5% |
| types/null | 5 | 1 | 4 | 0 | 0 | 0 | 20.0% | 20.0% | 19 | 4 | 82.6% |
| types/numeric | 18 | 0 | 18 | 0 | 0 | 0 | 0.0% | 0.0% | 25 | 18 | 58.1% |
| types/string | 2 | 1 | 1 | 0 | 0 | 0 | 50.0% | 50.0% | 10 | 1 | 90.9% |
| types/struct | 52 | 0 | 51 | 1 | 0 | 0 | 0.0% | 0.0% | 8 | 51 | 13.6% |
| types/test_date_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| types/test_interval_negation_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/test_null_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/test_qualified_type_cast.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| types/test_quotes.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/test_typeof.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 1 | 0 | 100.0% |
| types/test_user_type_errors_18452.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/time | 11 | 0 | 10 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 10 | 0.0% |
| types/timestamp | 23 | 1 | 18 | 4 | 0 | 0 | 4.3% | 5.3% | 22 | 18 | 55.0% |
| types/type | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| types/type_constructor_error_location.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/type_constructors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| types/type_expression_roundtrip.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/type_literal.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| types/uhugeint | 11 | 0 | 11 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 11 | 31.2% |
| types/union | 20 | 0 | 20 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 20 | 23.1% |
| types/unsigned | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 4 | 0.0% |
| types/uuid | 2 | 0 | 2 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 2 | 0.0% |
| types/variant | 32 | 1 | 27 | 4 | 0 | 0 | 3.1% | 3.6% | 17 | 27 | 38.6% |
| unnest/list_constant_reference.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| update/compact_update_allocation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| update/force_update_to_del_and_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| update/issue_22392.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| update/null_update_merge.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 23 | 0 | 100.0% |
| update/null_update_merge_transaction.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 22 | 0 | 100.0% |
| update/string_update_null_heap.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| update/string_update_transaction_local_7348.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| update/test_big_string_update.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 20 | 0 | 100.0% |
| update/test_cascading_updates.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| update/test_multiple_assignment.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| update/test_null_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| update/test_repeated_string_update.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 12 | 0 | 100.0% |
| update/test_stress_update_issue_19688.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2731 | 1 | 100.0% |
| update/test_string_update.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 13 | 0 | 100.0% |
| update/test_string_update_many_strings.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| update/test_string_update_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 6 | 1 | 85.7% |
| update/test_string_update_rollback.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 24 | 0 | 100.0% |
| update/test_string_update_rollback_null.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| update/test_update.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 21 | 0 | 100.0% |
| update/test_update_delete_same_tuple.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| update/test_update_from.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| update/test_update_issue_3170.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 20 | 0 | 100.0% |
| update/test_update_many_updaters.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 14 | 1 | 93.3% |
| update/test_update_many_updaters_nulls.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 15 | 1 | 93.8% |
| update/test_update_mix.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 17 | 0 | 100.0% |
| update/test_update_same_string_value.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| update/test_update_same_value.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 19 | 1 | 95.0% |
| update/test_update_with_non_regular_type.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| update/update_after_commit.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| update/update_default.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 3 | 0 | 100.0% |
| update/update_delete_wal.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| update/update_error_suggestions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| update/update_from_multi_match.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 55 | 1 | 98.2% |
| update/update_join_nulls.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| update/update_null_integers.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| update/update_query_node.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 11 | 1 | 91.7% |
| update/update_string_null_merge_dict_fsst.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| update/update_tuple.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/concurrent_insert_or_replace.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/insert_or_replace | 10 | 2 | 8 | 0 | 0 | 0 | 20.0% | 20.0% | 36 | 8 | 81.8% |
| upsert/minimal_reproducable_example.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| upsert/no_complex_default.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/postgres | 4 | 0 | 4 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| upsert/test_big_insert.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| upsert/test_big_insert_no_vector_verification.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| upsert/test_generated_column.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| upsert/test_problematic_conditional_do_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_aliased.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_basic.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| upsert/upsert_conflict_target.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/upsert_conflict_target_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_coverage.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| upsert/upsert_default.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| upsert/upsert_default_expressions.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| upsert/upsert_default_value_causes_conflict.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| upsert/upsert_default_values.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_distinct_bug.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 7 | 0 | 100.0% |
| upsert/upsert_duplicates_issue.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/upsert_excluded_references.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| upsert/upsert_explicit_index.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| upsert/upsert_global_mixed_conflicts.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_inner_conflict_do_nothing.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 4 | 0 | 100.0% |
| upsert/upsert_lambda.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| upsert/upsert_local_no_tuples.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| upsert/upsert_multiple_constraints.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/upsert_order_coverage.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 9 | 0 | 100.0% |
| upsert/upsert_partial_update.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| upsert/upsert_returning.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/upsert_set_expressions.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 8 | 0 | 100.0% |
| upsert/upsert_shorthand.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| upsert/upsert_transaction.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| upsert/upsert_unique_null.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 6 | 0 | 100.0% |
| vacuum/test_analyze.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| vacuum/vacuum_nested_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| variables/test_variable_syntax.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 9 | 1 | 90.0% |
| variables/test_variables.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| variant/variant_comparator.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| variant/variant_nested_arrays.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| vector_types/list_vector_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| vector_types/vector_types_errors.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/parallel_collection_combine.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| window/segment_tree_leaf_batches.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_basic_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_boundary_expr.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_constant_orderby.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| window/test_cume_dist_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_dense_rank.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_duplicate_partition.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| window/test_empty_frames.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 5 | 1 | 83.3% |
| window/test_evil_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| window/test_fill_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_fill_physical_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_fill_prefix_order.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_ignore_nulls.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_invalid_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| window/test_last_ignore_nulls_large_partition.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_lead_lag.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_lead_lag_offset_overflow.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_leadlag_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_list_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| window/test_mad_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_mode_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_mode_window_varlen.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_naive_aggregation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| window/test_negative_range.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_no_default_window_spec.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_nthvalue.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| window/test_ntile.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 13 | 1 | 92.9% |
| window/test_order_by_all.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_parallel_group_cleanup.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_partition_constant_arguments.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_quantile_skip_list.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_quantile_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_range_optimisation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_rank.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_rank_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_rownumber_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_scalar_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_single_following_frame.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_split_partition_heap.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 2 | 0 | 100.0% |
| window/test_streaming_lead_lag.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_streaming_nthvalue.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_streaming_window.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 4 | 1 | 80.0% |
| window/test_streaming_window_distinct.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_thread_count.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_tpcc_results.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_tpcds_q49.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| window/test_value_exclude.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_value_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_volatile_independence.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| window/test_wide_orderby.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_agg_stats_propagation.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_aggregate_macro.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_binding.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 7 | 1 | 87.5% |
| window/test_window_binding_ctes.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 10 | 0 | 100.0% |
| window/test_window_bool.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_clause.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_clause_scope.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 8 | 1 | 88.9% |
| window/test_window_constant_aggregate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_constant_allocator.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_cse.test | 1 | 0 | 0 | 1 | 0 | 0 | 0.0% | 0.0% | 0 | 0 | 0.0% |
| window/test_window_dbplyr.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 5 | 0 | 100.0% |
| window/test_window_distinct.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_exclude_state_destructor.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_filter.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 3 | 1 | 75.0% |
| window/test_window_finalize_race.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 403 | 0 | 100.0% |
| window/test_window_fusion.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_interval.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_order_collate.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_rows.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_window_segment_tree_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_window_string_agg.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_tpcds.test | 1 | 1 | 0 | 0 | 0 | 0 | 100.0% | 100.0% | 16 | 0 | 100.0% |
| window/test_window_types.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 0 | 1 | 0.0% |
| window/test_window_unnest_error.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/test_window_value_edges.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/test_window_wisconsin.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 1 | 1 | 50.0% |
| window/window_distinct_memory_limit.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/window_expression_comparison.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 2 | 1 | 66.7% |
| window/window_mtcars.test | 1 | 0 | 1 | 0 | 0 | 0 | 0.0% | 0.0% | 33 | 1 | 97.1% |

## Top failure reasons

| files | reason |
|---:|---|
| 179 | statement failed: parse error: error at N:N: expected one of "\"", ['a'..='z' \| 'A'..='Z' \| '_'], alter, analyze, attach, call, describe, detach, do, explain, f… |
| 155 | statement failed: parse error: error at N:N: expected one of keyword, macro, materialized, or, persistent, procedure, role, schema, secret, temp, temporary, tri… |
| 108 | statement failed: parse error: error at N:N: expected one of "(", "[", ['a'..='z' \| 'A'..='Z' \| 'N'..='N' \| '_'], keyword, known column type |
| 101 | statement failed: parse error: error at N:N: expected one of automatic_migration, catalog, create_if_not_exists, data, data_inlining_row_limit, data_path, encry… |
| 91 | statement failed: parse error: error at N:N: expected one of alter, analyze, attach, call, copy, describe, detach, do, explain, force, grant, install, keyword, … |
| 88 | statement failed: parse error: error at N:N: expected T[] is a variable-length LIST; pinkdb has fixed-size ARRAY only (wave NaN) |
| 74 | statement failed: parse error: error at N:N: expected ['N'..='N'] |
| 70 | statement failed: parse error: error at N:N: expected keyword |
| 68 | query failed: parse error: error at N:N: expected one of alter, analyze, attach, call, copy, describe, detach, do, explain, force, grant, install, keyword, merg… |
| 66 | expected N rows, got N |
| 62 | query failed: parse error: error at N:N: expected one of "(", "[", ['a'..='z' \| 'A'..='Z' \| 'N'..='N' \| '_'], keyword, known column type |
| 62 | statement failed: parse error: error at N:N: expected ['a'..='z' \| 'A'..='Z' \| '_'] |
| 55 | query failed: parse error: error at N:N: expected one of "$", "$$", "'", "(", "*", "+", "-", ".", "?", "[", "\"", ['N'..='N'], ['a'..='z' \| 'A'..='Z' \| '_'] |
| 53 | statement failed: parse error: error at N:N: expected one of "$", "$$", "'", "(", "*", "+", "-", ".", "?", "[", "\"", ['N'..='N'], ['a'..='z' \| 'A'..='Z' \| '_'] |
| 40 | query failed: Pragma Function with name storage_info does not exist! |
| 40 | query failed: parse error: error at N:N: expected one of at, ilike, keyword, similar |
| 34 | statement failed: parse error: error at N:N: expected one of ['a'..='z' \| 'A'..='Z' \| 'N'..='N' \| '_'], identifier, keyword |
| 33 | statement failed: parse error: error at N:N: expected one of constraint, default, keyword, primary, references, unique |
| 32 | statement failed: parse error: error at N:N: expected one of add, keyword, rename |
| 31 | statement failed: parse error: error at N:N: expected one of "(", "[", keyword, known column type |
| 30 | query failed: parse error: error at N:N: expected "(" |
| 29 | statement failed: parse error: error at N:N: expected one of identifier, keyword |
| 27 | statement failed: parse error: error at N:N: expected one of "$", "$$", "'", "(", "*", "+", "-", ".", "?", "[", "\"", ['N'..='N'], ['a'..='z' \| 'A'..='Z' \| '_']… |
| 25 | query failed: parse error: error at N:N: expected one of "(", "[", keyword, known column type |
| 25 | statement failed: parse error: error at N:N: expected one of ")", ['a'..='z' \| 'A'..='Z' \| 'N'..='N' \| '_'] |
| 24 | expected an error containing "" |
| 24 | query failed: parse error: error at N:N: expected one of at, fetch, for, keyword, qualify, tablesample, using, window |
| 23 | query failed: parse error: error at N:N: expected one of "$", "$$", "'", "(", ")", "*", "+", "-", ".", "?", "[", "\"", ['N'..='N'], ['a'..='z' \| 'A'..='Z' \| '_'… |
| 23 | query failed: parse error: error at N:N: expected to |
| 22 | statement failed: ARRAY elements must be INTEGER, BIGINT or DOUBLE, not VARCHAR |

## Top skip reasons

| files | reason |
|---:|---|
| 81 | unsupported directive: unzip |
| 77 | require autocomplete |
| 73 | require icu |
| 36 | require block_size |
| 26 | require httpfs |
| 15 | unsupported directive: include |
| 14 | require tpch |
| 9 | unsupported directive: set seed |
| 7 | require quack |
| 7 | require tpcds |
| 3 | require allow_unsigned_extensions |
| 3 | require windows |
| 2 | require debug_fs |
| 2 | require exact_vector_size |
| 2 | require inet |
| 2 | require ram |
| 2 | require-env LOCAL_EXTENSION_REPO |
| 1 | require encodings |
| 1 | require linux |
| 1 | require longdouble |

## Failure audit

After the comparison and placeholder fixes, 25 `value N differs` /
no-result failures were run on both DuckDB (`duckdb -no-agent`, v2.0
alpha) and pinkdb. The no-result group (26 files) was DML under `query`:
DuckDB returns the affected-row count and pinkdb already produced that count
in `QueryResult::Inserted/Updated/Deleted`, so the runner now compares it;
all 26 pass. The table is the sampled `value N differs` group, first cells
only; the full statement setup was replayed on DuckDB in a temp database.

| # | file | expected | DuckDB | pinkdb | verdict |
|---:|---|---|---|---|---|
| 1 | `aggregate/aggregates/test_aggr_string.test` | `NULL NULL NULL NULL 1` | same | non-NULL strings, `2` | engine: NULL ordering |
| 2 | `aggregate/group/test_group_by_alias.test` | `NULL NULL NULL` | same | `1 1 1` | engine: NULL ordering |
| 3 | `aggregate/group/test_group_null.test` | `NULL 4.000000` | `NULL 4` | `2 4` | engine: NULL ordering; numeric compare fixed |
| 4 | `aggregate/grouping_sets/cube.test` | `NULL 7` | same | `CS 5` | engine: NULL ordering |
| 5 | `aggregate/grouping_sets/grouping_sets_filter.test` | `NULL 7` | same | `CS 5` | engine: NULL ordering |
| 6 | `aggregate/grouping_sets/rollup.test` | `NULL 7` | same | `CS 5` | engine: NULL ordering |
| 7 | `binder/test_alias.test` | `NULL NULL` | same | `0 2` | engine: NULL ordering |
| 8 | `catalog/materialized_view/native_materialized_view.test` | `MATERIALIZED VIEW` | parser error | `BASE TABLE` | engine/CLI-version gap |
| 9 | `catalog/test_alter_oid_stability.test` | `true` | `true` | `0` | engine: OID/alter stability |
| 10 | `constraints/primarykey/test_pk_update_delete.test` | `11 1` | same | `4 1` | engine: update/delete count |
| 11 | `constraints/unique/test_unique.test` | `NULL 6` | same | `2 5` | engine: NULL ordering and unique scan |
| 12 | `copy/parquet/parquet_arrow_timestamp.test` | `2020-10-05 17:21:49` | same | `1970-01-19 ...` | engine: timestamp-ms parquet decode; text formatting fixed |
| 13 | `filter/test_obsolete_filters.test` | `NULL NULL` | same | `1 10` | engine: NULL ordering |
| 14 | `function/date/test_strftime_exhaustive.test` | `NULL` | `NULL` | `Wed` | engine: NULL ordering |
| 15 | `function/generic/test_between.test` | `NULL` | `NULL` | `1` | engine: NULL ordering |
| 16 | `function/generic/test_in.test` | `NULL NULL` | same | `1 1` | engine: NULL ordering |
| 17 | `function/numeric/test_round.test` | `42.123400 42.12345` | `42.1235 42.1235` | `42.1235 inf` | engine: `round(...,1000)` overflow |
| 18 | `function/operator/test_arithmetic.test` | `NULL NULL` | same | `0 0` | engine: NULL ordering |
| 19 | `function/string/test_concat.test` | `NULL` | `NULL` | `hello hello` | engine: NULL ordering |
| 20 | `function/string/test_concat_binding.test` | `[1, 2]` | `[1, 2]` | `[1][2]` | engine: `||` on arrays |
| 21 | `function/string/test_length.test` | `NULL` | `NULL` | `5` | engine: NULL ordering |
| 22 | `function/timestamp/test_now.test` | `1` | `1` | `2` | engine: `now()` distinct values |
| 23 | `insert/test_insert_type.test` | `NULL` | `NULL` | `4` | engine: NULL ordering |
| 24 | `join/full_outer/full_outer_join_union.test` | `NULL NULL 2 20` | `1 1 1 10` | `1 1 1 10` | both engines agree; expected from another version |
| 25 | `join/full_outer/test_full_outer_join.test` | `NULL NULL 2 20` | same | `1 1 1 10` | engine: NULL ordering |

The remaining `value N differs` files in this baseline are the same
engine-side causes (mostly `SET default_null_order` being accepted but not
honoured by pinkdb, plus cast/format text from the engine). The runner-side
fixes in this revision were: the full placeholder set, numeric text compare,
`<FILE>:` CSV results, DML affected-row counts, and DuckDB-style
DOUBLE/TIMESTAMP/array text.

## Refreshing this baseline

Rebuild the runner and rerun the whole corpus, writing both the full report
and the minimal baseline:

```sh
cargo build --release -p pinkdb-sqllogictest
CORPUS=~/Desktop/projects/duckdb-v2.0.0/test/sql
./target/release/pinkdb-slt --corpus "$CORPUS" --jobs 8 \
  --out "$TMPDIR/slt.json" \
  --baseline-out bench/sqllogictest/baseline.json
./target/release/pinkdb-slt summary "$TMPDIR/slt.json" > /tmp/slt-summary.md
```

Splice `/tmp/slt-summary.md` in from "Pass rate per area" to the end,
update the counts and wall time in the header, and commit the new
`bench/sqllogictest/baseline.json` with the doc. Add `--slow` to include
`.test_slow`, or `--filter '<glob>'` to measure one area. Check a run
against the committed baseline with:

```sh
./target/release/pinkdb-slt check "$TMPDIR/slt.json" \
  --baseline bench/sqllogictest/baseline.json
```

`check` exits non-zero for a file that passed in the baseline and no longer
passes, and prints files that newly pass.

