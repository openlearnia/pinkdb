/* Interleaved A/B: the v1 cell API vs 1.2 chunk fetch over every column of a
 * store's `lineitem` table. Build and run through tests/chunk_bench.rs:
 *
 *   PINKDB_BENCH_STORE=/tmp/store.pink \
 *   cargo test -q -p pinkdb-ffi --test chunk_bench -- --ignored --nocapture
 */
#include "pinkdb.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static volatile uint64_t sink;

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static uint64_t scan_v1(pinkdb_handle *db) {
    static const char sql[] = "SELECT * FROM lineitem";
    pinkdb_result *result = pinkdb_execute(db, sql, sizeof(sql) - 1);
    if (!result) {
        fprintf(stderr, "v1 query failed: %s\n", pinkdb_last_error());
        exit(1);
    }
    size_t rows = pinkdb_result_row_count(result, 0);
    size_t columns = pinkdb_result_column_count(result, 0);
    uint64_t sum = 0;
    for (size_t row = 0; row < rows; row++) {
        for (size_t column = 0; column < columns; column++) {
            pink_value value = pinkdb_result_value(result, 0, row, column);
            if (value.is_null) continue;
            sum += (uint64_t)value.text_len + (uint64_t)value.int64;
            if (value.text_ptr) sum += (uintptr_t)value.text_ptr;
        }
    }
    pinkdb_result_free(result);
    return sum;
}

static uint64_t scan_chunks(pinkdb_handle *db) {
    static const char sql[] = "SELECT * FROM lineitem";
    pinkdb_result *result = pinkdb_execute(db, sql, sizeof(sql) - 1);
    if (!result) {
        fprintf(stderr, "chunk query failed: %s\n", pinkdb_last_error());
        exit(1);
    }
    size_t columns = pinkdb_result_column_count(result, 0);
    uint64_t sum = 0;
    pinkdb_data_chunk *chunk;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        size_t rows = pinkdb_chunk_size(chunk);
        for (size_t row = 0; row < rows; row++) {
            for (size_t column = 0; column < columns; column++) {
                const pinkdb_vector *vector = pinkdb_chunk_column(chunk, column);
                if (!pinkdb_validity_row_is_valid(pinkdb_vector_validity(vector), row))
                    continue;
                const void *data = pinkdb_vector_data(vector);
                if (!data) continue;
                switch (pinkdb_result_column_type(result, 0, column) & 0xff) {
                case PINKDB_TYPE_BOOLEAN:
                    sum += ((const uint8_t *)data)[row];
                    break;
                case PINKDB_TYPE_INTEGER:
                case PINKDB_TYPE_DATE:
                    sum += (uint64_t)((const int32_t *)data)[row];
                    break;
                case PINKDB_TYPE_BIGINT:
                case PINKDB_TYPE_TIMESTAMP:
                    sum += (uint64_t)((const int64_t *)data)[row];
                    break;
                case PINKDB_TYPE_DOUBLE: {
                    uint64_t bits;
                    memcpy(&bits, (const double *)data + row, sizeof(bits));
                    sum += bits;
                    break;
                }
                case PINKDB_TYPE_VARCHAR: {
                    const pinkdb_string_t *text = (const pinkdb_string_t *)data;
                    sum += text[row].len + (uintptr_t)text[row].ptr;
                    break;
                }
                case PINKDB_TYPE_INTERVAL: {
                    const pinkdb_interval *interval = (const pinkdb_interval *)data;
                    sum += (uint64_t)interval[row].months + (uint64_t)interval[row].days +
                           (uint64_t)interval[row].micros;
                    break;
                }
                default: {
                    const pinkdb_hugeint *wide = (const pinkdb_hugeint *)data;
                    sum += wide[row].lower + (uint64_t)wide[row].upper;
                    break;
                }
                }
            }
        }
        pinkdb_destroy_chunk(chunk);
    }
    pinkdb_result_free(result);
    return sum;
}

static int compare_double(const void *a, const void *b) {
    double x = *(const double *)a;
    double y = *(const double *)b;
    return (x > y) - (x < y);
}

static void report(const char *api, double *samples, int n) {
    double sorted[16];
    memcpy(sorted, samples, (size_t)n * sizeof(double));
    qsort(sorted, (size_t)n, sizeof(double), compare_double);
    printf("%s median_ms=%.1f runs=", api, sorted[n / 2]);
    for (int i = 0; i < n; i++) printf("%s%.1f", i ? "," : "", samples[i]);
    printf("\n");
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <store.pink>\n", argv[0]);
        return 2;
    }
    pinkdb_handle *db = pinkdb_open(argv[1]);
    if (!db) {
        fprintf(stderr, "open failed: %s\n", pinkdb_last_error());
        return 1;
    }
    printf("abi=%u\n", pinkdb_abi_version());
    /* Warm both paths once, so page cache and the executor's caches are hot. */
    sink += scan_v1(db) + scan_chunks(db);

    double v1[5], chunks[5];
    for (int i = 0; i < 5; i++) {
        if (i % 2 == 0) {
            double t = now_ms();
            sink += scan_v1(db);
            v1[i] = now_ms() - t;
            t = now_ms();
            sink += scan_chunks(db);
            chunks[i] = now_ms() - t;
        } else {
            double t = now_ms();
            sink += scan_chunks(db);
            chunks[i] = now_ms() - t;
            t = now_ms();
            sink += scan_v1(db);
            v1[i] = now_ms() - t;
        }
        printf("run=%d v1_ms=%.1f chunk_ms=%.1f\n", i, v1[i], chunks[i]);
        fflush(stdout);
    }
    report("v1", v1, 5);
    report("chunk", chunks, 5);
    printf("checksum=%" PRIu64 "\n", sink);
    pinkdb_close(db);
    return 0;
}
