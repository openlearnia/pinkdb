/* Interleaved A/B/C: the v1 cell API, 1.2 chunk fetch, and 1.3 Arrow export
 * over every column of a store's `lineitem` table. Build and run through
 * tests/chunk_bench.rs:
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

/* Sum one Arrow column (including nested fixed-size lists) and check its
 * validity bitmap, so the compiler cannot elide the buffer walk. */
static uint64_t scan_column(const struct ArrowArray *array,
                            const struct ArrowSchema *schema) {
    size_t rows = (size_t)array->length;
    const uint8_t *valid =
        array->n_buffers > 0 ? (const uint8_t *)array->buffers[0] : NULL;
    uint64_t sum = 0;
    const char *format = schema->format;
    if (format[0] == '+') {
        for (size_t row = 0; row < rows; row++) {
            size_t index = (size_t)array->offset + row;
            if (valid && !(valid[index / 8] & (1u << (index % 8)))) continue;
            sum += 1;
        }
        if (array->n_children > 0) {
            uint64_t child = scan_column(array->children[0], schema->children[0]);
            sum += child;
        }
        return sum;
    }
    for (size_t row = 0; row < rows; row++) {
        size_t index = (size_t)array->offset + row;
        if (valid && !(valid[index / 8] & (1u << (index % 8)))) continue;
        switch (format[0]) {
        case 'b':
            sum += (((const uint8_t *)array->buffers[1])[index / 8] >> (index % 8)) & 1u;
            break;
        case 'i':
        case 'f':
            sum += (uint64_t)((const int32_t *)array->buffers[1])[index];
            break;
        case 'l':
        case 'g':
            sum += (uint64_t)((const int64_t *)array->buffers[1])[index];
            break;
        case 't':
            /* DATE (tdD) is i32, TIMESTAMP (tsu:) is i64, INTERVAL (tin) is 16 bytes. */
            if (strcmp(format, "tdD") == 0) {
                sum += (uint64_t)((const int32_t *)array->buffers[1])[index];
            } else if (strcmp(format, "tin") == 0) {
                const int32_t *interval = (const int32_t *)array->buffers[1] + index * 4;
                sum += (uint64_t)interval[0] + (uint64_t)interval[1];
                sum += *(const int64_t *)(interval + 2);
            } else {
                sum += (uint64_t)((const int64_t *)array->buffers[1])[index];
            }
            break;
        case 'u': {
            const int32_t *offsets = (const int32_t *)array->buffers[1];
            sum += (uint64_t)(offsets[index + 1] - offsets[index]);
            sum += (uintptr_t)((const uint8_t *)array->buffers[2] + offsets[index]);
            break;
        }
        case 'd': {
            const uint64_t *wide = (const uint64_t *)array->buffers[1] + index * 2;
            sum += wide[0] + wide[1];
            break;
        }
        case 'w': {
            const uint8_t *bytes = (const uint8_t *)array->buffers[1] + index * 16;
            for (int byte = 0; byte < 16; byte++) sum += bytes[byte];
            break;
        }
        default:
            break;
        }
    }
    return sum;
}

static uint64_t scan_arrow(pinkdb_handle *db) {
    static const char sql[] = "SELECT * FROM lineitem";
    pinkdb_result *result = pinkdb_execute(db, sql, sizeof(sql) - 1);
    if (!result) {
        fprintf(stderr, "arrow query failed: %s\n", pinkdb_last_error());
        exit(1);
    }
    struct ArrowSchema schema;
    if (!pinkdb_result_arrow_schema(result, 0, &schema)) {
        fprintf(stderr, "arrow schema failed: %s\n", pinkdb_last_error());
        exit(1);
    }
    uint64_t sum = 0;
    for (;;) {
        struct ArrowArray array;
        if (!pinkdb_result_fetch_arrow(result, 0, &array)) {
            fprintf(stderr, "arrow fetch failed: %s\n", pinkdb_last_error());
            exit(1);
        }
        if (!array.release) break;
        sum += (uint64_t)array.length;
        for (int64_t column = 0; column < array.n_children; column++)
            sum += scan_column(array.children[column], schema.children[column]);
        array.release(&array);
    }
    schema.release(&schema);
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
    /* Warm every path once, so page cache and the executor's caches are hot. */
    sink += scan_v1(db) + scan_chunks(db) + scan_arrow(db);

    double v1[5], chunks[5], arrows[5];
    for (int i = 0; i < 5; i++) {
        for (int turn = 0; turn < 3; turn++) {
            int which = (i + turn) % 3;
            double start = now_ms();
            if (which == 0) {
                sink += scan_v1(db);
                v1[i] = now_ms() - start;
            } else if (which == 1) {
                sink += scan_chunks(db);
                chunks[i] = now_ms() - start;
            } else {
                sink += scan_arrow(db);
                arrows[i] = now_ms() - start;
            }
        }
        printf("run=%d v1_ms=%.1f chunk_ms=%.1f arrow_ms=%.1f\n", i, v1[i],
               chunks[i], arrows[i]);
        fflush(stdout);
    }
    report("v1", v1, 5);
    report("chunk", chunks, 5);
    report("arrow", arrows, 5);
    printf("checksum=%" PRIu64 "\n", sink);
    pinkdb_close(db);
    return 0;
}
