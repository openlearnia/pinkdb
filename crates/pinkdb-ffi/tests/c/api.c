/* C-level acceptance suite for ABI 1.5: database/connection split, config,
 * per-result errors, interrupt, chunk fetch, typed vectors, Arrow export,
 * appender and typed parameter binding.
 * Built and run by tests/c_api.rs. Keep it dependency-free (libc + pthreads). */

#include "pinkdb.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures;

#define CHECK(cond)                                                           \
    do {                                                                      \
        if (!(cond)) {                                                        \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
            failures++;                                                       \
        }                                                                     \
    } while (0)

static const char *result_error(pinkdb_result *result) {
    const char *error = pinkdb_result_error(result);
    return error ? error : "";
}

static const char *result_class(pinkdb_result *result) {
    const char *class = pinkdb_result_error_class(result);
    return class ? class : "";
}

static int64_t scalar(pinkdb_result *result) {
    pink_value value = pinkdb_result_value(result, 0, 0, 0);
    return value.is_null ? -1 : value.int64;
}

static void test_open_with_config(void) {
    pinkdb_config *config = pinkdb_create_config();
    CHECK(config != NULL);
    CHECK(pinkdb_config_set(config, "threads", "2") == 1);
    CHECK(pinkdb_config_set(config, "memory_limit", "256MB") == 1);
    CHECK(pinkdb_config_set(config, "wal_sync_method", "fdatasync") == 1);
    CHECK(pinkdb_config_set(config, "allow_sql_load_extension", "false") == 1);
    CHECK(pinkdb_config_set(config, "no_such_option", "1") == 0);
    CHECK(pinkdb_config_error(config) != NULL);
    CHECK(strstr(pinkdb_config_error(config), "no_such_option") != NULL);
    {
        unsigned char short_key[31] = {0};
        CHECK(pinkdb_config_set_encryption_key(config, short_key,
                                               sizeof(short_key)) == 0);
        CHECK(strstr(pinkdb_config_error(config), "32 bytes") != NULL);
    }
    CHECK(pinkdb_config_set(config, "read_only", "false") == 1);
    CHECK(pinkdb_config_error(config) == NULL);

    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, config, &db, &errmsg) == 1);
    CHECK(db != NULL);
    CHECK(errmsg == NULL);
    pinkdb_destroy_config(config);

    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);
    CHECK(conn != NULL);

    pinkdb_result *result = NULL;
    CHECK(pinkdb_query(conn, "SELECT 40 + 2", 13, &result) == 1);
    CHECK(scalar(result) == 42);
    pinkdb_result_free(result);

    pinkdb_disconnect(conn);
    pinkdb_close_database(db);

    CHECK(pinkdb_open_ext(NULL, NULL, &db, &errmsg) == 1);
    pinkdb_close_database(db);
}

static void test_read_only(void) {
    pinkdb_config *config = pinkdb_create_config();
    CHECK(pinkdb_config_set(config, "access_mode", "read_only") == 1);
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(":memory:", config, &db, &errmsg) == 1);
    pinkdb_destroy_config(config);

    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);
    pinkdb_result *result = NULL;
    CHECK(pinkdb_query(conn, "CREATE TABLE t(a INT)", 21, &result) == 0);
    CHECK(strcmp(result_class(result), "permission") == 0);
    CHECK(strstr(result_error(result), "read-only") != NULL);
    pinkdb_result_free(result);

    CHECK(pinkdb_query(conn, "SELECT 7", 8, &result) == 1);
    CHECK(strcmp(result_error(result), "") == 0);
    CHECK(strcmp(result_class(result), "") == 0);
    pinkdb_result_free(result);

    pinkdb_disconnect(conn);
    pinkdb_close_database(db);
}

static void test_per_result_errors(void) {
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, NULL, &db, &errmsg) == 1);
    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);

    pinkdb_result *first = NULL;
    pinkdb_result *second = NULL;
    CHECK(pinkdb_query(conn, "SELECT * FROM missing_table", strlen("SELECT * FROM missing_table"), &first) == 0);
    CHECK(pinkdb_query(conn, "SELEC", 5, &second) == 0);
    CHECK(strcmp(result_class(first), "catalog") == 0);
    CHECK(strstr(result_error(first), "missing_table") != NULL);
    CHECK(strcmp(result_class(second), "parse") == 0);
    /* Both errors are still available: no thread-local overwrite. */
    CHECK(strstr(result_error(first), "missing_table") != NULL);
    pinkdb_result_free(first);
    pinkdb_result_free(second);

    pinkdb_prepared *prepared = NULL;
    CHECK(pinkdb_prepare_ext(conn, "SELEC", 5, &prepared, &errmsg) == 0);
    CHECK(prepared == NULL);
    CHECK(errmsg != NULL);
    CHECK(strstr(errmsg, "parse") != NULL);
    pinkdb_free(errmsg);
    errmsg = NULL;
    CHECK(pinkdb_prepare_ext(conn, "SELECT ?", 8, &prepared, &errmsg) == 1);
    CHECK(prepared != NULL);
    CHECK(errmsg == NULL);
    pink_value value;
    memset(&value, 0, sizeof(value));
    value.type_tag = PINKDB_TYPE_INTEGER;
    value.int64 = 5;
    CHECK(pinkdb_query_prepared(conn, prepared, &value, 2, &first) == 0);
    CHECK(strcmp(result_class(first), "parameter") == 0);
    pinkdb_result_free(first);
    CHECK(pinkdb_query_prepared(conn, prepared, &value, 1, &first) == 1);
    CHECK(scalar(first) == 5);
    pinkdb_result_free(first);
    pinkdb_prepared_free(prepared);

    pinkdb_disconnect(conn);
    pinkdb_close_database(db);
}

/* One connection per thread, all over the same store. */
typedef struct {
    pinkdb_database *db;
    int id;
    int failed;
} worker_arg;

static void *worker(void *raw) {
    worker_arg *arg = raw;
    pinkdb_connection *conn = NULL;
    if (pinkdb_connect(arg->db, &conn) != 1) {
        arg->failed = 1;
        return NULL;
    }
    char sql[256];
    pinkdb_result *result = NULL;
    snprintf(sql, sizeof(sql), "CREATE TEMP TABLE mine_%d(a INT)", arg->id);
    if (pinkdb_query(conn, sql, strlen(sql), &result) != 1) {
        arg->failed = 1;
    }
    pinkdb_result_free(result);

    snprintf(sql, sizeof(sql), "INSERT INTO shared VALUES (%d)", arg->id);
    if (pinkdb_query(conn, sql, strlen(sql), &result) != 1) {
        arg->failed = 1;
    }
    pinkdb_result_free(result);

    snprintf(sql, sizeof(sql), "SELECT count(*) FROM mine_%d", arg->id);
    if (pinkdb_query(conn, sql, strlen(sql), &result) != 1 ||
        scalar(result) != 0) {
        arg->failed = 1;
    }
    pinkdb_result_free(result);
    pinkdb_disconnect(conn);
    return NULL;
}

static void test_connections_across_threads(void) {
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, NULL, &db, &errmsg) == 1);
    pinkdb_connection *setup = NULL;
    CHECK(pinkdb_connect(db, &setup) == 1);
    pinkdb_result *result = NULL;
    CHECK(pinkdb_query(setup, "CREATE TABLE shared(a INT)", strlen("CREATE TABLE shared(a INT)"), &result) == 1);
    pinkdb_result_free(result);
    pinkdb_disconnect(setup);

    enum { THREADS = 4 };
    pthread_t threads[THREADS];
    worker_arg args[THREADS];
    for (int i = 0; i < THREADS; i++) {
        args[i].db = db;
        args[i].id = i;
        args[i].failed = 0;
        CHECK(pthread_create(&threads[i], NULL, worker, &args[i]) == 0);
    }
    for (int i = 0; i < THREADS; i++) {
        pthread_join(threads[i], NULL);
        CHECK(args[i].failed == 0);
    }

    pinkdb_connection *check = NULL;
    CHECK(pinkdb_connect(db, &check) == 1);
    CHECK(pinkdb_query(check, "SELECT count(*) FROM shared", strlen("SELECT count(*) FROM shared"), &result) == 1);
    CHECK(scalar(result) == THREADS);
    pinkdb_result_free(result);
    pinkdb_disconnect(check);
    pinkdb_close_database(db);
}

typedef struct {
    pinkdb_connection *conn;
    long long interrupted_ns;
} interrupt_arg;

static void *interrupter(void *raw) {
    interrupt_arg *arg = raw;
    /* Give the query time to start, then ask it to stop. */
    struct timespec pause = {0, 100 * 1000 * 1000};
    nanosleep(&pause, NULL);
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    arg->interrupted_ns = (long long)now.tv_sec * 1000000000LL + now.tv_nsec;
    pinkdb_interrupt(arg->conn);
    return NULL;
}

static void test_interrupt(void) {
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, NULL, &db, &errmsg) == 1);
    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);

    /* 8^10 = 1,073,741,824 rows: long enough that the interrupt lands while
     * the statement is still running on any machine in CI. */
    char sql[2048];
    strcpy(sql, "SELECT count(*) FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8)) t0");
    for (int i = 1; i < 10; i++) {
        char part[64];
        snprintf(part, sizeof(part), ", (VALUES (1),(2),(3),(4),(5),(6),(7),(8)) t%d", i);
        strcat(sql, part);
    }
    interrupt_arg arg = {conn, 0};
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, interrupter, &arg) == 0);
    pinkdb_result *result = NULL;
    int ok = pinkdb_query(conn, sql, strlen(sql), &result);
    struct timespec done;
    clock_gettime(CLOCK_MONOTONIC, &done);
    long long done_ns = (long long)done.tv_sec * 1000000000LL + done.tv_nsec;
    pthread_join(thread, NULL);
    CHECK(ok == 0);
    CHECK(strcmp(result_class(result), "interrupted") == 0);
    if (arg.interrupted_ns != 0 && done_ns >= arg.interrupted_ns) {
        fprintf(stderr, "interrupt latency: %.3f ms\n",
                (double)(done_ns - arg.interrupted_ns) / 1000000.0);
    }
    pinkdb_result_free(result);

    /* The flag does not poison the next statement. */
    CHECK(pinkdb_query(conn, "SELECT 1", 8, &result) == 1);
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);

    pinkdb_disconnect(conn);
    pinkdb_close_database(db);
}


static pinkdb_database *test_db;

static pinkdb_connection *open_conn(void) {
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, NULL, &test_db, &errmsg) == 1);
    CHECK(errmsg == NULL);
    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(test_db, &conn) == 1);
    return conn;
}

static void close_conn(pinkdb_connection *conn) {
    pinkdb_disconnect(conn);
    pinkdb_close_database(test_db);
    test_db = NULL;
}

static pinkdb_result *query_ok(pinkdb_connection *conn, const char *sql) {
    pinkdb_result *result = NULL;
    if (pinkdb_query(conn, sql, strlen(sql), &result) != 1) {
        fprintf(stderr, "query failed: %s\n  error: %s\n", sql,
                result_error(result));
    }
    CHECK(result != NULL);
    CHECK(strcmp(result_error(result), "") == 0);
    return result;
}

/* Fetch the first chunk of a SELECT and return it. */
static pinkdb_data_chunk *one_chunk(pinkdb_result *result) {
    pinkdb_data_chunk *chunk = pinkdb_result_fetch_chunk(result, 0);
    CHECK(chunk != NULL);
    return chunk;
}

static void test_chunk_scalars(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE types ("
                   "b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR,"
                   " dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT,"
                   " uh UHUGEINT, dec DECIMAL(18,3), u UUID)");
    query_ok(conn,
             "INSERT INTO types VALUES ("
             "true, 1, 9007199254740993, 1.5, 'h\xc3\xa9',"
             " DATE '2020-01-02', TIMESTAMP '2020-01-02 03:04:05.123456',"
             " INTERVAL '1 month 2 days 03:04:05.000006',"
             " CAST('170141183460469231731687303715884105727' AS HUGEINT),"
             " CAST('340282366920938463463374607431768211455' AS UHUGEINT),"
             " CAST('123.456' AS DECIMAL(18,3)),"
             " '12345678-1234-5678-9abc-def012345678'::UUID),"
             "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
             " NULL, NULL),"
             "(false, -7, -5, -2.25, '', DATE '1969-12-31',"
             " TIMESTAMP '1969-12-31 23:59:59', INTERVAL '-1 day',"
             " CAST('-1' AS HUGEINT), CAST('0' AS UHUGEINT),"
             " CAST('-0.001' AS DECIMAL(18,3)),"
             " '00000000-0000-0000-0000-000000000000'::UUID),"
             "(true, 42, 0, 0.0, 'tail', DATE '1970-01-01',"
             " TIMESTAMP '1970-01-01 00:00:00', INTERVAL '0 days',"
             " CAST('0' AS HUGEINT), CAST('1' AS UHUGEINT),"
             " CAST('0' AS DECIMAL(18,3)),"
             " 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID)");
    pinkdb_result *result = query_ok(
        conn,
        "SELECT b, i, bi, d, s, dt, ts, iv, h, uh, dec, u FROM types");

    CHECK(pinkdb_result_column_count(result, 0) == 12);
    CHECK(pinkdb_result_column_type(result, 0, 0) == PINKDB_TYPE_BOOLEAN);
    CHECK(pinkdb_result_column_type(result, 0, 1) == PINKDB_TYPE_INTEGER);
    CHECK(pinkdb_result_column_type(result, 0, 2) == PINKDB_TYPE_BIGINT);
    CHECK(pinkdb_result_column_type(result, 0, 3) == PINKDB_TYPE_DOUBLE);
    CHECK(pinkdb_result_column_type(result, 0, 4) == PINKDB_TYPE_VARCHAR);
    CHECK(pinkdb_result_column_type(result, 0, 5) == PINKDB_TYPE_DATE);
    CHECK(pinkdb_result_column_type(result, 0, 6) == PINKDB_TYPE_TIMESTAMP);
    CHECK(pinkdb_result_column_type(result, 0, 7) == PINKDB_TYPE_INTERVAL);
    CHECK(pinkdb_result_column_type(result, 0, 8) == PINKDB_TYPE_HUGEINT);
    CHECK(pinkdb_result_column_type(result, 0, 9) == PINKDB_TYPE_UHUGEINT);
    CHECK(pinkdb_result_column_type(result, 0, 10) == PINKDB_TYPE_DECIMAL);
    CHECK(pinkdb_result_column_type(result, 0, 11) == PINKDB_TYPE_UUID);
    CHECK(pinkdb_result_column_decimal_width(result, 0, 10) == 18);
    CHECK(pinkdb_result_column_decimal_scale(result, 0, 10) == 3);
    CHECK(pinkdb_result_column_decimal_width(result, 0, 1) == -1);
    CHECK(pinkdb_result_column_array_length(result, 0, 1) == 0);
    CHECK(pinkdb_result_column_array_element_type(result, 0, 1) == -1);

    pinkdb_data_chunk *chunk = one_chunk(result);
    CHECK(pinkdb_chunk_size(chunk) == 4);
    CHECK(pinkdb_chunk_column(chunk, 12) == NULL);

    const uint64_t *valid = pinkdb_vector_validity(pinkdb_chunk_column(chunk, 0));
    CHECK(valid != NULL);
    CHECK(pinkdb_validity_row_is_valid(valid, 0) == 1);
    CHECK(pinkdb_validity_row_is_valid(valid, 1) == 0);
    CHECK(pinkdb_validity_row_is_valid(valid, 2) == 1);
    CHECK(pinkdb_validity_row_is_valid(NULL, 999) == 1);

    /* Row 0 has the largest values, row 1 is all NULL, row 2 the negatives. */
    const uint8_t *b = pinkdb_vector_data(pinkdb_chunk_column(chunk, 0));
    CHECK(b[0] == 1 && b[2] == 0 && b[3] == 1);
    const int32_t *i = pinkdb_vector_data(pinkdb_chunk_column(chunk, 1));
    CHECK(i[0] == 1 && i[2] == -7 && i[3] == 42);
    const int64_t *bi = pinkdb_vector_data(pinkdb_chunk_column(chunk, 2));
    CHECK(bi[0] == 9007199254740993LL && bi[2] == -5);
    const double *d = pinkdb_vector_data(pinkdb_chunk_column(chunk, 3));
    CHECK(d[0] == 1.5 && d[2] == -2.25 && d[3] == 0.0);
    const pinkdb_string_t *s = pinkdb_vector_data(pinkdb_chunk_column(chunk, 4));
    CHECK(s[0].len == 3 && memcmp(s[0].ptr, "h\xc3\xa9", 3) == 0);
    CHECK(s[1].ptr == NULL && s[1].len == 0);
    CHECK(s[2].len == 0);
    CHECK(s[3].len == 4 && memcmp(s[3].ptr, "tail", 4) == 0);
    const int32_t *dt = pinkdb_vector_data(pinkdb_chunk_column(chunk, 5));
    CHECK(dt[0] == 18263 && dt[2] == -1 && dt[3] == 0);
    const int64_t *ts = pinkdb_vector_data(pinkdb_chunk_column(chunk, 6));
    CHECK(ts[0] == 1577934245123456LL && ts[2] == -1000000LL && ts[3] == 0);
    const pinkdb_interval *iv = pinkdb_vector_data(pinkdb_chunk_column(chunk, 7));
    CHECK(iv[0].months == 1 && iv[0].days == 2 && iv[0].micros == 11045000006LL);
    CHECK(iv[2].days == -1);
    const pinkdb_hugeint *h = pinkdb_vector_data(pinkdb_chunk_column(chunk, 8));
    CHECK(h[0].lower == 0xffffffffffffffffULL && h[0].upper == INT64_MAX);
    CHECK(h[2].lower == 0xffffffffffffffffULL && h[2].upper == -1);
    const pinkdb_uhugeint *uh = pinkdb_vector_data(pinkdb_chunk_column(chunk, 9));
    CHECK(uh[0].lower == 0xffffffffffffffffULL && uh[0].upper == 0xffffffffffffffffULL);
    CHECK(uh[2].lower == 0 && uh[2].upper == 0);
    const pinkdb_hugeint *dec = pinkdb_vector_data(pinkdb_chunk_column(chunk, 10));
    CHECK(dec[0].upper == 0 && dec[0].lower == 123456);
    CHECK(dec[2].upper == -1 && dec[2].lower == (uint64_t)-1);
    const pinkdb_uhugeint *u = pinkdb_vector_data(pinkdb_chunk_column(chunk, 11));
    CHECK(u[0].lower == 0x9abcdef012345678ULL && u[0].upper == 0x1234567812345678ULL);
    CHECK(u[2].lower == 0 && u[2].upper == 0);
    CHECK(u[3].lower == 0xffffffffffffffffULL && u[3].upper == 0xffffffffffffffffULL);

    /* Fetch order is statement-local, and past the end stays NULL. */
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);

    /* A chunk outlives its result: destroy the result first, read, then free. */
    result = query_ok(conn, "SELECT 7");
    chunk = one_chunk(result);
    pinkdb_result_free(result);
    i = pinkdb_vector_data(pinkdb_chunk_column(chunk, 0));
    CHECK(i[0] == 7);
    pinkdb_destroy_chunk(chunk);

    close_conn(conn);
}

static void test_chunk_many_rows(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE big (i INTEGER, s VARCHAR, d DOUBLE, dt DATE)");
    query_ok(conn,
             "INSERT INTO big SELECT i::INTEGER,"
             " CASE WHEN i % 2 = 0 THEN NULL ELSE 'row' || i END,"
             " i / 10.0, DATE '2020-01-01' + i::INTEGER"
             " FROM range(0, 5000) t(i)");
    pinkdb_result *result =
        query_ok(conn, "SELECT i, s, d, dt FROM big ORDER BY i");
    CHECK(pinkdb_result_row_count(result, 0) == 5000);

    size_t seen = 0;
    size_t chunks = 0;
    pinkdb_data_chunk *chunk;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        size_t n = pinkdb_chunk_size(chunk);
        CHECK(n > 0 && n <= 2048);
        const int32_t *i = pinkdb_vector_data(pinkdb_chunk_column(chunk, 0));
        const pinkdb_string_t *s = pinkdb_vector_data(pinkdb_chunk_column(chunk, 1));
        const uint64_t *valid = pinkdb_vector_validity(pinkdb_chunk_column(chunk, 1));
        CHECK(valid != NULL);
        for (size_t r = 0; r < n; r++) {
            CHECK(i[r] == (int32_t)(seen + r));
            if ((seen + r) % 2 == 0) {
                CHECK(pinkdb_validity_row_is_valid(valid, r) == 0);
            } else {
                CHECK(pinkdb_validity_row_is_valid(valid, r) == 1);
                char expect[32];
                int len = snprintf(expect, sizeof(expect), "row%zu", seen + r);
                CHECK(s[r].len == (uint64_t)len);
                CHECK(memcmp(s[r].ptr, expect, (size_t)len) == 0);
            }
        }
        seen += n;
        chunks++;
        pinkdb_destroy_chunk(chunk);
    }
    CHECK(chunks == 3);
    CHECK(seen == 5000);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_chunk_empty_and_multi_statement(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = query_ok(conn, "SELECT 1 WHERE false");
    CHECK(pinkdb_result_column_count(result, 0) == 1);
    CHECK(pinkdb_result_row_count(result, 0) == 0);
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_result_free(result);

    result = query_ok(conn, "SELECT 1; SELECT 2");
    CHECK(pinkdb_result_statement_count(result) == 2);
    pinkdb_data_chunk *first = one_chunk(result);
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_data_chunk *second = pinkdb_result_fetch_chunk(result, 1);
    CHECK(second != NULL);
    const int32_t *v = pinkdb_vector_data(pinkdb_chunk_column(second, 0));
    CHECK(v[0] == 2);
    CHECK(pinkdb_result_fetch_chunk(result, 1) == NULL);
    CHECK(pinkdb_result_fetch_chunk(result, 2) == NULL);
    pinkdb_destroy_chunk(first);
    pinkdb_destroy_chunk(second);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_chunk_errors_and_arrays(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = NULL;
    CHECK(pinkdb_query(conn, "SELECT * FROM missing_table",
                       strlen("SELECT * FROM missing_table"), &result) == 0);
    CHECK(strcmp(result_class(result), "catalog") == 0);
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_result_free(result);

    result = query_ok(conn, "CREATE TABLE t (a INTEGER)");
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_result_free(result);
    result = query_ok(conn, "INSERT INTO t VALUES (1)");
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_result_free(result);

    /* ARRAY columns have no 1.2 data pointer but still expose a valid
     * schema; the v1 cell API remains the way to read their values. */
    result = query_ok(conn,
                      "SELECT [1.5, 2.5]::DOUBLE[2] AS d,"
                      " [3.5, 4.5]::FLOAT[2] AS f,"
                      " CAST('12.34' AS DECIMAL(10,2)) AS dec");
    CHECK(pinkdb_result_column_type(result, 0, 0) == PINKDB_ARRAY_TAG(2));
    CHECK(pinkdb_result_column_array_length(result, 0, 0) == 2);
    CHECK(pinkdb_result_column_array_element_type(result, 0, 0) == PINKDB_TYPE_DOUBLE);
    CHECK(pinkdb_result_column_array_element_type(result, 0, 1) == PINKDB_TYPE_FLOAT);
    pinkdb_data_chunk *chunk = one_chunk(result);
    CHECK(pinkdb_vector_data(pinkdb_chunk_column(chunk, 0)) == NULL);
    CHECK(pinkdb_vector_data(pinkdb_chunk_column(chunk, 1)) == NULL);
    CHECK(pinkdb_result_column_decimal_width(result, 0, 2) == 10);
    CHECK(pinkdb_result_column_decimal_scale(result, 0, 2) == 2);
    pink_value array_cell = pinkdb_result_value(result, 0, 0, 0);
    CHECK(array_cell.type_tag == PINKDB_ARRAY_TAG(2));
    CHECK(array_cell.int64 == 2);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);
    close_conn(conn);
}

static int metadata_has(const char *metadata, const char *key,
                        const char *value) {
    if (!metadata) return 0;
    int32_t count;
    memcpy(&count, metadata, sizeof(count));
    metadata += sizeof(count);
    size_t key_len = strlen(key);
    size_t value_len = strlen(value);
    for (int32_t i = 0; i < count; i++) {
        int32_t len;
        memcpy(&len, metadata, sizeof(len));
        metadata += sizeof(len);
        int key_match = len >= 0 && (size_t)len == key_len &&
                        memcmp(metadata, key, key_len) == 0;
        metadata += len;
        memcpy(&len, metadata, sizeof(len));
        metadata += sizeof(len);
        int value_match = len >= 0 && (size_t)len == value_len &&
                          memcmp(metadata, value, value_len) == 0;
        metadata += len;
        if (key_match && value_match) return 1;
    }
    return 0;
}

static void test_arrow_schema_and_array(void) {
    pinkdb_connection *conn = open_conn();
    static const char sql[] =
        "SELECT true AS b, 1::INTEGER AS i, 9007199254740993::BIGINT AS l,"
        " 1.5::DOUBLE AS g, 'hé' AS s, DATE '2020-01-02' AS dt,"
        " TIMESTAMP '2020-01-02 03:04:05.123456' AS ts,"
        " INTERVAL '1 month 2 days 03:04:05.000006' AS iv,"
        " '-99999999999999999999999999999999999999'::HUGEINT AS hi,"
        " '99999999999999999999999999999999999999'::UHUGEINT AS uhi,"
        " CAST('123.456' AS DECIMAL(18,3)) AS dec,"
        " '12345678-1234-5678-9abc-def012345678'::UUID AS u,"
        " [1.5, 2.5]::DOUBLE[2] AS arr, [3.5, 4.5]::FLOAT[2] AS af,"
        " CAST('{\"a\":1}' AS VARIANT) AS v";
    pinkdb_result *result = query_ok(conn, sql);

    struct ArrowSchema schema;
    CHECK(pinkdb_result_arrow_schema(result, 0, &schema) == 1);
    CHECK(schema.release != NULL);
    CHECK(strcmp(schema.format, "+s") == 0);
    CHECK(schema.n_children == 15);
    CHECK(schema.children != NULL);
    static const char *formats[15] = {
        "b", "i", "l", "g", "u", "tdD", "tsu:", "tin", "d:38,0",
        "d:38,0", "d:18,3", "w:16", "+w:2", "+w:2", "u",
    };
    for (int i = 0; i < 15; i++) {
        CHECK(strcmp(schema.children[i]->format, formats[i]) == 0);
        CHECK(schema.children[i]->flags == ARROW_FLAG_NULLABLE);
    }
    CHECK(metadata_has(schema.children[11]->metadata,
                       "ARROW:extension:name", "arrow.uuid"));
    CHECK(metadata_has(schema.children[14]->metadata,
                       "ARROW:extension:name", "pinkdb.variant"));
    CHECK(schema.children[12]->n_children == 1);
    CHECK(strcmp(schema.children[12]->children[0]->format, "g") == 0);
    CHECK(strcmp(schema.children[13]->children[0]->format, "f") == 0);
    schema.release(&schema);
    CHECK(schema.release == NULL);

    struct ArrowArray array;
    CHECK(pinkdb_result_fetch_arrow(result, 0, &array) == 1);
    CHECK(array.release != NULL);
    CHECK(array.length == 1);
    CHECK(array.offset == 0);
    CHECK(array.n_buffers == 1);
    CHECK(array.n_children == 15);
    struct ArrowArray **kids = array.children;
    CHECK(kids[0]->buffers[0] == NULL);
    CHECK((((const uint8_t *)kids[0]->buffers[1])[0] & 1) == 1);
    CHECK(((const int32_t *)kids[1]->buffers[1])[0] == 1);
    CHECK(((const int64_t *)kids[2]->buffers[1])[0] == 9007199254740993LL);
    CHECK(((const double *)kids[3]->buffers[1])[0] == 1.5);
    const int32_t *offsets = kids[4]->buffers[1];
    CHECK(offsets[0] == 0 && offsets[1] == 3);
    CHECK(memcmp(kids[4]->buffers[2], "h\xc3\xa9", 3) == 0);
    const int32_t *interval = kids[7]->buffers[1];
    CHECK(interval[0] == 1 && interval[1] == 2);
    CHECK(*(const int64_t *)(interval + 2) == 11045000006000LL);
    /* 10^38 - 1 as little-endian {lower, upper} words, and its negation. */
    const uint64_t *hi = kids[8]->buffers[1];
    const uint64_t *uhi = kids[9]->buffers[1];
    CHECK(uhi[0] == 0x098a223fffffffffULL && uhi[1] == 0x4b3b4ca85a86c47aULL);
    CHECK(hi[0] == ~uhi[0] + 1 && hi[1] == ~uhi[1]);
    const uint64_t *decimal = kids[10]->buffers[1];
    CHECK(decimal[0] == 123456 && decimal[1] == 0);
    static const uint8_t uuid[16] = {
        0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78,
        0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78,
    };
    CHECK(memcmp(kids[11]->buffers[1], uuid, sizeof(uuid)) == 0);

    CHECK(kids[12]->n_buffers == 1 && kids[12]->n_children == 1);
    CHECK(kids[12]->children[0]->length == 2);
    const double *arr = kids[12]->children[0]->buffers[1];
    CHECK(arr[0] == 1.5 && arr[1] == 2.5);
    const float *af = kids[13]->children[0]->buffers[1];
    CHECK(af[0] == 3.5f && af[1] == 4.5f);
    const int32_t *variant_offsets = kids[14]->buffers[1];
    CHECK(variant_offsets[1] > variant_offsets[0]);

    struct ArrowArray trailing;
    CHECK(pinkdb_result_fetch_arrow(result, 0, &trailing) == 1);
    CHECK(trailing.release == NULL);
    array.release(&array);
    CHECK(array.release == NULL);
    pinkdb_result_free(result);

    /* A schema and an array keep their own data after the result/chunk dies. */
    result = query_ok(conn, "SELECT 42::BIGINT AS x");
    struct ArrowSchema kept_schema;
    CHECK(pinkdb_result_arrow_schema(result, 0, &kept_schema) == 1);
    pinkdb_result_free(result);
    CHECK(strcmp(kept_schema.format, "+s") == 0);
    kept_schema.release(&kept_schema);

    result = query_ok(conn, "SELECT 42::BIGINT AS x");
    pinkdb_data_chunk *chunk = one_chunk(result);
    struct ArrowArray kept_array;
    CHECK(pinkdb_chunk_to_arrow(chunk, &kept_array) == 1);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);
    CHECK(kept_array.children[0]->length == 1);
    CHECK(((const int64_t *)kept_array.children[0]->buffers[1])[0] == 42);
    kept_array.release(&kept_array);
    CHECK(kept_array.release == NULL);

    result = query_ok(conn, "SELECT 1 WHERE false");
    struct ArrowArray empty;
    CHECK(pinkdb_result_fetch_arrow(result, 0, &empty) == 1);
    CHECK(empty.release == NULL);
    pinkdb_result_free(result);

    close_conn(conn);
}


static void test_streaming_large_result(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = query_ok(conn, "SELECT i FROM range(0, 100000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    CHECK(pinkdb_result_column_count(result, 0) == 1);
    CHECK(pinkdb_result_rows_fetched(result, 0) == 0);

    uint64_t seen = 0;
    pinkdb_data_chunk *chunk;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        seen += pinkdb_chunk_size(chunk);
        pinkdb_destroy_chunk(chunk);
    }
    CHECK(seen == 100000);
    CHECK(pinkdb_result_rows_fetched(result, 0) == 100000);
    CHECK(strcmp(result_error(result), "") == 0);
    CHECK(pinkdb_result_row_count(result, 0) == 100000);
    pinkdb_result_free(result);

    /* Row count drains a live cursor instead of waiting for the caller. */
    result = query_ok(conn, "SELECT i FROM range(0, 100000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    chunk = pinkdb_result_fetch_chunk(result, 0);
    CHECK(chunk != NULL);
    pinkdb_destroy_chunk(chunk);
    CHECK(pinkdb_result_row_count(result, 0) == 100000);
    CHECK(pinkdb_result_is_streaming(result, 0) == 0);
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        CHECK(pinkdb_chunk_size(chunk) > 0);
        pinkdb_destroy_chunk(chunk);
    }
    pinkdb_result_free(result);

    result = query_ok(conn, "SELECT 1; SELECT 2");
    CHECK(pinkdb_result_is_streaming(result, 0) == 0);
    CHECK(pinkdb_result_is_streaming(result, 1) == 0);
    pinkdb_result_free(result);
    close_conn(conn);
}

/* Past 16 MiB a chunk fetch keeps nothing for the v1 cell API, so a v1 read
 * afterwards reports an error instead of answering with rows missing. */
static void test_streaming_v1_after_a_large_fetch(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = query_ok(conn, "SELECT i FROM range(0, 5000000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    pinkdb_data_chunk *chunk;
    uint64_t seen = 0;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        seen += pinkdb_chunk_size(chunk);
        pinkdb_destroy_chunk(chunk);
    }
    CHECK(seen == 5000000);
    CHECK(strcmp(result_error(result), "") == 0);
    pink_value v = pinkdb_result_value(result, 0, 0, 0);
    (void)v;
    CHECK(strstr(result_error(result), "16 MiB") != NULL);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_streaming_early_free(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result =
        query_ok(conn, "SELECT i FROM range(0, 100000000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    pinkdb_data_chunk *chunk = pinkdb_result_fetch_chunk(result, 0);
    CHECK(chunk != NULL);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);

    /* Dropping the cursor stops its producer; the connection is usable. */
    result = query_ok(conn, "SELECT count(*) FROM range(0, 10)");
    CHECK(scalar(result) == 10);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_streaming_mid_error(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = query_ok(
        conn,
        "SELECT CASE WHEN i = 1000000 THEN i / 0 ELSE i END,"
        " 'x' || i::VARCHAR || 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"
        " FROM range(0, 2000000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    pinkdb_data_chunk *chunk;
    int saw_rows = 0;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        saw_rows = 1;
        pinkdb_destroy_chunk(chunk);
    }
    CHECK(saw_rows == 1);
    CHECK(strcmp(result_error(result), "") != 0);
    CHECK(strcmp(result_class(result), "execution") == 0);
    CHECK(pinkdb_result_fetch_chunk(result, 0) == NULL);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_streaming_interrupt(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result =
        query_ok(conn, "SELECT i FROM range(0, 100000000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    pinkdb_data_chunk *chunk = pinkdb_result_fetch_chunk(result, 0);
    CHECK(chunk != NULL);
    pinkdb_destroy_chunk(chunk);

    /* Interrupt between fetches, a few chunks in: the whole range streams in
       well under 100 ms, so a timed interrupter thread can arrive after the
       last chunk. The fetch after the interrupt ends the stream early. */
    int fetched = 1;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        pinkdb_destroy_chunk(chunk);
        if (++fetched == 4) {
            pinkdb_interrupt(conn);
        }
    }
    CHECK(fetched == 4);
    CHECK(strcmp(result_class(result), "interrupted") == 0);
    pinkdb_result_free(result);

    result = query_ok(conn, "SELECT 1");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_streaming_arrow(void) {
    pinkdb_connection *conn = open_conn();
    pinkdb_result *result = query_ok(conn, "SELECT i FROM range(0, 10000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 1);
    uint64_t rows = 0;
    struct ArrowArray array;
    while (pinkdb_result_fetch_arrow(result, 0, &array) == 1) {
        if (array.release == NULL) {
            break;
        }
        rows += (uint64_t)array.length;
        array.release(&array);
    }
    CHECK(rows == 10000);
    CHECK(pinkdb_result_rows_fetched(result, 0) == 10000);
    CHECK(strcmp(result_error(result), "") == 0);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_streaming_materialize_config(void) {
    pinkdb_config *config = pinkdb_create_config();
    CHECK(config != NULL);
    CHECK(pinkdb_config_set(config, "materialize_results", "true") == 1);
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, config, &db, &errmsg) == 1);
    pinkdb_destroy_config(config);
    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);
    pinkdb_result *result = query_ok(conn, "SELECT i FROM range(0, 5000) t(i)");
    CHECK(pinkdb_result_is_streaming(result, 0) == 0);
    CHECK(pinkdb_result_row_count(result, 0) == 5000);
    uint64_t seen = 0;
    pinkdb_data_chunk *chunk;
    while ((chunk = pinkdb_result_fetch_chunk(result, 0)) != NULL) {
        seen += pinkdb_chunk_size(chunk);
        pinkdb_destroy_chunk(chunk);
    }
    CHECK(seen == 5000);
    pinkdb_result_free(result);
    pinkdb_disconnect(conn);
    pinkdb_close_database(db);
}

static void test_appender_all_types(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE app_all ("
                   "b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR,"
                   " dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT,"
                   " uh UHUGEINT, dec DECIMAL(18,3), u UUID)");
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "app_all", 7, &app,
                                 &errmsg) == 1);
    CHECK(app != NULL && errmsg == NULL);

    CHECK(pinkdb_append_bool(app, true) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int64(app, 9007199254740993LL) == 1);
    CHECK(pinkdb_append_double(app, 1.5) == 1);
    CHECK(pinkdb_append_varchar(app, "h\xc3\xa9", 3) == 1);
    CHECK(pinkdb_append_date(app, 18263) == 1);
    CHECK(pinkdb_append_timestamp(app, 1577934245123456LL) == 1);
    pinkdb_interval iv = {1, 2, 11045000006LL};
    CHECK(pinkdb_append_interval(app, iv) == 1);
    pinkdb_hugeint h = {0xffffffffffffffffULL, INT64_MAX};
    CHECK(pinkdb_append_hugeint(app, h) == 1);
    pinkdb_uhugeint uh = {0xffffffffffffffffULL, 0xffffffffffffffffULL};
    CHECK(pinkdb_append_uhugeint(app, uh) == 1);
    pinkdb_hugeint dec = {123456, 0};
    CHECK(pinkdb_append_decimal(app, dec, 18, 3) == 1);
    pinkdb_uhugeint u = {0x9abcdef012345678ULL, 0x1234567812345678ULL};
    CHECK(pinkdb_append_uuid(app, u) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);

    for (int i = 0; i < 12; i++) {
        CHECK(pinkdb_append_null(app) == 1);
    }
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_error(app) == NULL);
    CHECK(pinkdb_appender_destroy(app) == 1);

    /* BLOB exists in the ABI for symmetry but not in the engine. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "app_all", 7, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_blob(app, "x", 1) == 0);
    CHECK(strstr(pinkdb_last_error(), "BLOB") != NULL);
    CHECK(pinkdb_appender_destroy(app) == 1);

    pinkdb_result *result = query_ok(
        conn,
        "SELECT b = true, i = 1, bi = 9007199254740993, d = 1.5,"
        " s = 'h\xc3\xa9', dt = DATE '2020-01-02',"
        " ts = TIMESTAMP '2020-01-02 03:04:05.123456',"
        " iv = INTERVAL '1 month 2 days 03:04:05.000006',"
        " h = CAST('170141183460469231731687303715884105727' AS HUGEINT),"
        " uh = CAST('340282366920938463463374607431768211455' AS UHUGEINT),"
        " dec = CAST('123.456' AS DECIMAL(18,3)),"
        " u = '12345678-1234-5678-9abc-def012345678'::UUID"
        " FROM app_all WHERE i = 1");
    for (int col = 0; col < 12; col++) {
        pink_value value = pinkdb_result_value(result, 0, 0, (size_t)col);
        if (value.is_null != 0 || value.int64 != 1) {
            fprintf(stderr, "app_all col %d: tag=%d null=%d int=%lld\n", col,
                    value.type_tag, value.is_null, (long long)value.int64);
        }
        CHECK(value.is_null == 0 && value.int64 == 1);
    }
    pinkdb_result_free(result);

    result = query_ok(conn, "SELECT count(*) FROM app_all WHERE i IS NULL");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);
    result = query_ok(conn,
                      "SELECT b IS NULL, i IS NULL, bi IS NULL, d IS NULL,"
                      " s IS NULL, dt IS NULL, ts IS NULL, iv IS NULL,"
                      " h IS NULL, uh IS NULL, dec IS NULL, u IS NULL"
                      " FROM app_all WHERE i IS NULL");
    for (int col = 0; col < 12; col++) {
        pink_value value = pinkdb_result_value(result, 0, 0, (size_t)col);
        CHECK(value.is_null == 0 && value.int64 == 1);
    }
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_binding_all_types(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE bind_all ("
                   "b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR,"
                   " dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT,"
                   " uh UHUGEINT, dec DECIMAL(18,3), u UUID)");
    pinkdb_prepared *prepared = NULL;
    char *errmsg = NULL;
    const char *sql = "INSERT INTO bind_all VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    CHECK(pinkdb_prepare_ext(conn, sql, strlen(sql), &prepared, &errmsg) == 1);
    CHECK(prepared != NULL && errmsg == NULL);
    CHECK(pinkdb_prepared_parameter_count(prepared) == 12);

    CHECK(pinkdb_bind_bool(prepared, 1, true) == 1);
    CHECK(pinkdb_bind_int32(prepared, 2, 1) == 1);
    CHECK(pinkdb_bind_int64(prepared, 3, 9007199254740993LL) == 1);
    CHECK(pinkdb_bind_double(prepared, 4, 1.5) == 1);
    CHECK(pinkdb_bind_varchar(prepared, 5, "h\xc3\xa9", 3) == 1);
    CHECK(pinkdb_bind_date(prepared, 6, 18263) == 1);
    CHECK(pinkdb_bind_timestamp(prepared, 7, 1577934245123456LL) == 1);
    pinkdb_interval iv = {1, 2, 11045000006LL};
    CHECK(pinkdb_bind_interval(prepared, 8, iv) == 1);
    pinkdb_hugeint h = {0xffffffffffffffffULL, INT64_MAX};
    CHECK(pinkdb_bind_hugeint(prepared, 9, h) == 1);
    pinkdb_uhugeint uh = {0xffffffffffffffffULL, 0xffffffffffffffffULL};
    CHECK(pinkdb_bind_uhugeint(prepared, 10, uh) == 1);
    pinkdb_hugeint dec = {123456, 0};
    CHECK(pinkdb_bind_decimal(prepared, 11, dec, 18, 3) == 1);
    pinkdb_uhugeint u = {0x9abcdef012345678ULL, 0x1234567812345678ULL};
    CHECK(pinkdb_bind_uuid(prepared, 12, u) == 1);

    pinkdb_result *result = NULL;
    CHECK(pinkdb_execute_bound(conn, prepared, &result) == 1);
    CHECK(strcmp(result_error(result), "") == 0);
    pinkdb_result_free(result);

    /* All NULLs, then execute again without rebinding only the nulls. */
    for (size_t index = 1; index <= 12; index++) {
        CHECK(pinkdb_bind_null(prepared, index) == 1);
    }
    CHECK(pinkdb_execute_bound(conn, prepared, &result) == 1);
    pinkdb_result_free(result);

    result = query_ok(
        conn,
        "SELECT b = true, i = 1, bi = 9007199254740993, d = 1.5,"
        " s = 'h\xc3\xa9', dt = DATE '2020-01-02',"
        " ts = TIMESTAMP '2020-01-02 03:04:05.123456',"
        " iv = INTERVAL '1 month 2 days 03:04:05.000006',"
        " h = CAST('170141183460469231731687303715884105727' AS HUGEINT),"
        " uh = CAST('340282366920938463463374607431768211455' AS UHUGEINT),"
        " dec = CAST('123.456' AS DECIMAL(18,3)),"
        " u = '12345678-1234-5678-9abc-def012345678'::UUID"
        " FROM bind_all WHERE i = 1");
    for (int col = 0; col < 12; col++) {
        pink_value value = pinkdb_result_value(result, 0, 0, (size_t)col);
        if (value.is_null != 0 || value.int64 != 1) {
            fprintf(stderr, "bind_all col %d: tag=%d null=%d int=%lld\n", col,
                    value.type_tag, value.is_null, (long long)value.int64);
        }
        CHECK(value.is_null == 0 && value.int64 == 1);
    }
    pinkdb_result_free(result);
    result = query_ok(conn, "SELECT count(*) FROM bind_all WHERE i IS NULL");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);

    CHECK(pinkdb_bind_blob(prepared, 1, "x", 1) == 0);
    CHECK(strstr(pinkdb_last_error(), "BLOB") != NULL);
    CHECK(pinkdb_bind_decimal(prepared, 1, dec, 0, 0) == 0);
    CHECK(strstr(pinkdb_last_error(), "DECIMAL width") != NULL);

    CHECK(pinkdb_clear_bindings(prepared) == 1);
    CHECK(pinkdb_execute_bound(conn, prepared, &result) == 0);
    CHECK(strcmp(result_class(result), "parameter") == 0);
    CHECK(strstr(result_error(result), "binding 1") != NULL);
    pinkdb_result_free(result);

    CHECK(pinkdb_bind_int32(prepared, 0, 1) == 0);
    CHECK(strstr(pinkdb_last_error(), "out of range") != NULL);
    CHECK(pinkdb_bind_int32(prepared, 13, 1) == 0);
    CHECK(strstr(pinkdb_last_error(), "out of range") != NULL);
    CHECK(pinkdb_bind_int32(prepared, 1, 7) == 1);
    CHECK(pinkdb_bind_int32(prepared, 2, 8) == 1);
    CHECK(pinkdb_bind_null(prepared, 3) == 1);
    CHECK(pinkdb_execute_bound(conn, prepared, &result) == 0);
    CHECK(strcmp(result_class(result), "parameter") == 0);
    CHECK(strstr(result_error(result), "binding 4") != NULL);
    pinkdb_result_free(result);
    pinkdb_prepared_free(prepared);
    close_conn(conn);
}

static void test_appender_constraints_and_transactions(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn,
             "CREATE TABLE constrained (id INTEGER PRIMARY KEY,"
             " u INTEGER UNIQUE, v INTEGER)");
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "constrained", 11, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int32(app, 10) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    /* Same primary key twice inside one flush: the whole flush is rejected. */
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int32(app, 11) == 1);
    CHECK(pinkdb_append_int32(app, 2) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 0);
    CHECK(strcmp(pinkdb_last_error_class(), "constraint") == 0 ||
          strcmp(pinkdb_last_error_class(), "execution") == 0);
    CHECK(strstr(pinkdb_appender_error(app), "Duplicate") != NULL ||
          strstr(pinkdb_appender_error(app), "duplicate") != NULL);
    CHECK(pinkdb_appender_destroy(app) == 0);
    pinkdb_result *result = query_ok(conn, "SELECT count(*) FROM constrained");
    CHECK(scalar(result) == 0);
    pinkdb_result_free(result);

    /* Duplicate against an existing row. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "constrained", 11, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int32(app, 10) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    CHECK(pinkdb_appender_create(conn, NULL, 0, "constrained", 11, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int32(app, 20) == 1);
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 0);
    pinkdb_appender_destroy(app);
    result = query_ok(conn, "SELECT count(*) FROM constrained");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);

    /* UNIQUE duplicate inside one flush is rejected as a whole. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "constrained", 11, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 2) == 1);
    CHECK(pinkdb_append_int32(app, 10) == 1);
    CHECK(pinkdb_append_int32(app, 4) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_append_int32(app, 10) == 1);
    CHECK(pinkdb_append_int32(app, 5) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 0);
    pinkdb_appender_destroy(app);
    result = query_ok(conn, "SELECT count(*) FROM constrained");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);

    /* NULL in the PRIMARY KEY is a NOT NULL violation. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "constrained", 11, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_append_null(app) == 1);
    CHECK(pinkdb_append_int32(app, 30) == 1);
    CHECK(pinkdb_append_int32(app, 6) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 0);
    CHECK(strcmp(pinkdb_last_error_class(), "constraint") == 0);
    CHECK(strstr(pinkdb_appender_error(app), "NOT NULL") != NULL);
    pinkdb_appender_destroy(app);
    result = query_ok(conn, "SELECT count(*) FROM constrained");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);

    /* CHECK constraints are not implemented by the SQL parser/AST yet; this
     * pins the clear refusal instead of pretending the appender enforces it. */
    result = NULL;
    CHECK(pinkdb_query(conn, "CREATE TABLE checked (a INT CHECK (a > 0))",
                       strlen("CREATE TABLE checked (a INT CHECK (a > 0))"),
                       &result) == 0);
    CHECK(strcmp(result_class(result), "parse") == 0);
    pinkdb_result_free(result);

    /* Inside one transaction the flush is visible there and vanishes on
     * ROLLBACK. */
    query_ok(conn, "CREATE TABLE tx (id INTEGER)");
    query_ok(conn, "BEGIN");
    CHECK(pinkdb_appender_create(conn, NULL, 0, "tx", 2, &app, &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_append_int32(app, 2) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM tx");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);
    query_ok(conn, "ROLLBACK");
    result = query_ok(conn, "SELECT count(*) FROM tx");
    CHECK(scalar(result) == 0);
    pinkdb_result_free(result);

    close_conn(conn);
}

static void test_appender_auto_flush(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE auto (id INTEGER PRIMARY KEY, v INTEGER)");
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "auto", 4, &app, &errmsg) == 1);
    pinkdb_appender_set_auto_flush(app, 2);
    for (int id = 1; id <= 2; id++) {
        CHECK(pinkdb_append_int32(app, id) == 1);
        CHECK(pinkdb_append_int32(app, id * 10) == 1);
        CHECK(pinkdb_appender_end_row(app) == 1);
    }
    /* Rows 1 and 2 are committed at the second row. Row 4 duplicates row 1
     * and fails the auto-flush of rows 3 and 4; the first boundary stays. */
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_append_int32(app, 30) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_append_int32(app, 10) == 1);
    CHECK(pinkdb_appender_end_row(app) == 0);
    CHECK(pinkdb_appender_flush(app) == 0);
    CHECK(pinkdb_appender_destroy(app) == 0);
    pinkdb_result *result = query_ok(conn, "SELECT count(*) FROM auto");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);

    /* destroy flushes the remaining rows of a healthy appender. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "auto", 4, &app, &errmsg) == 1);
    pinkdb_appender_set_auto_flush(app, 2);
    for (int id = 10; id < 15; id++) {
        CHECK(pinkdb_append_int32(app, id) == 1);
        CHECK(pinkdb_append_int32(app, id * 10) == 1);
        CHECK(pinkdb_appender_end_row(app) == 1);
    }
    CHECK(pinkdb_appender_destroy(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM auto");
    CHECK(scalar(result) == 7);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_appender_chunks_and_widths(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE chunked (id INTEGER, s VARCHAR)");
    pinkdb_result *result = query_ok(
        conn, "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (id, s)");
    pinkdb_data_chunk *chunk = one_chunk(result);
    CHECK(chunk != NULL);
    CHECK(pinkdb_chunk_size(chunk) == 2);
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "chunked", 7, &app, &errmsg) == 1);
    CHECK(pinkdb_appender_append_chunk(app, chunk) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);
    pinkdb_result *counted = query_ok(conn, "SELECT count(*), sum(id) FROM chunked");
    CHECK(scalar(counted) == 2);
    pinkdb_result_free(counted);

    result = query_ok(conn, "SELECT 7");
    chunk = one_chunk(result);
    CHECK(pinkdb_appender_create(conn, NULL, 0, "chunked", 7, &app, &errmsg) == 1);
    CHECK(pinkdb_appender_append_chunk(app, chunk) == 0);
    CHECK(strstr(pinkdb_appender_error(app), "columns") != NULL);
    CHECK(pinkdb_appender_destroy(app) == 0);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);

    CHECK(pinkdb_appender_create(conn, NULL, 0, "chunked", 7, &app, &errmsg) == 1);
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_appender_end_row(app) == 0);
    CHECK(strstr(pinkdb_appender_error(app), "row 1") != NULL);
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_append_varchar(app, "c", 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM chunked");
    CHECK(scalar(result) == 3);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_appender_lifecycle_and_durable(void) {
    pinkdb_database *db = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_open_ext(NULL, NULL, &db, &errmsg) == 1);
    pinkdb_connection *conn = NULL;
    CHECK(pinkdb_connect(db, &conn) == 1);
    query_ok(conn, "CREATE TABLE life (id INTEGER)");
    pinkdb_appender *app = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "no_such_table", 13, &app,
                                 &errmsg) == 0);
    CHECK(app == NULL && errmsg != NULL);
    CHECK(strstr(errmsg, "no_such_table") != NULL);
    pinkdb_free(errmsg);

    /* zero is clamped to one row, so the first append is already committed. */
    CHECK(pinkdb_appender_create(conn, NULL, 0, "life", 4, &app, &errmsg) == 1);
    pinkdb_appender_set_auto_flush(app, 0);
    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_append_int32(app, 2) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    pinkdb_result *result = query_ok(conn, "SELECT count(*) FROM life");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);
    pinkdb_disconnect(conn);
    pinkdb_close_database(db);

    char path[256];
    snprintf(path, sizeof(path), "/tmp/pinkdb-c-%ld.pink", (long)getpid());
    CHECK(pinkdb_open_ext(path, NULL, &db, &errmsg) == 1);
    CHECK(pinkdb_connect(db, &conn) == 1);
    query_ok(conn, "CREATE TABLE durable (id INTEGER, s VARCHAR)");
    CHECK(pinkdb_appender_create(conn, NULL, 0, "durable", 7, &app, &errmsg) == 1);
    for (int id = 1; id <= 3; id++) {
        CHECK(pinkdb_append_int32(app, id) == 1);
        CHECK(pinkdb_append_varchar(app, "x", 1) == 1);
        CHECK(pinkdb_appender_end_row(app) == 1);
    }
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    pinkdb_disconnect(conn);
    pinkdb_close_database(db);

    CHECK(pinkdb_open_ext(path, NULL, &db, &errmsg) == 1);
    CHECK(pinkdb_connect(db, &conn) == 1);
    result = query_ok(conn, "SELECT count(*), sum(id) FROM durable");
    CHECK(scalar(result) == 3);
    pinkdb_result_free(result);
    result = query_ok(conn, "SELECT count(*) FROM durable WHERE id IN (1,2,3)");
    CHECK(scalar(result) == 3);
    pinkdb_result_free(result);
    pinkdb_disconnect(conn);
    pinkdb_close_database(db);
    unlink(path);
}

static void test_appender_query_between_appends(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE between_rows (id INTEGER)");
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "between_rows", 12, &app,
                                 &errmsg) == 1);

    CHECK(pinkdb_append_int32(app, 1) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    /* Unflushed rows are invisible to a query on the same connection. */
    pinkdb_result *result = query_ok(conn, "SELECT count(*) FROM between_rows");
    CHECK(scalar(result) == 0);
    pinkdb_result_free(result);

    CHECK(pinkdb_append_int32(app, 2) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM between_rows");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);

    /* The connection stays usable for appends after the query. */
    CHECK(pinkdb_append_int32(app, 3) == 1);
    CHECK(pinkdb_appender_end_row(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM between_rows");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);
    CHECK(pinkdb_appender_destroy(app) == 1);
    result = query_ok(conn, "SELECT count(*) FROM between_rows");
    CHECK(scalar(result) == 3);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_appender_disconnect_with_live_appender(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE detached (id INTEGER)");
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "detached", 8, &app,
                                 &errmsg) == 1);
    for (int id = 1; id <= 2; id++) {
        CHECK(pinkdb_append_int32(app, id) == 1);
        CHECK(pinkdb_appender_end_row(app) == 1);
    }
    /* Disconnect flushes and detaches: no use-after-free, and the appender
     * reports a closed connection on every later call. */
    pinkdb_disconnect(conn);
    CHECK(pinkdb_appender_flush(app) == 0);
    CHECK(strstr(pinkdb_appender_error(app), "connection is closed") != NULL);
    CHECK(pinkdb_append_int32(app, 3) == 0);
    CHECK(pinkdb_appender_destroy(app) == 0);

    pinkdb_connection *again = NULL;
    CHECK(pinkdb_connect(test_db, &again) == 1);
    pinkdb_result *result = query_ok(again, "SELECT count(*) FROM detached");
    CHECK(scalar(result) == 2);
    pinkdb_result_free(result);
    pinkdb_disconnect(again);
    pinkdb_close_database(test_db);
    test_db = NULL;
}

static void test_appender_chunk_every_type(void) {
    pinkdb_connection *conn = open_conn();
    query_ok(conn, "CREATE TABLE chunk_all ("
                   "b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR,"
                   " dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT,"
                   " uh UHUGEINT, dec DECIMAL(18,3), u UUID)");
    query_ok(conn, "CREATE TABLE chunk_src ("
                   "b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR,"
                   " dt DATE, ts TIMESTAMP, iv INTERVAL, h HUGEINT,"
                   " uh UHUGEINT, dec DECIMAL(18,3), u UUID)");
    query_ok(conn, "INSERT INTO chunk_src VALUES ("
                   " true, 1, 9007199254740993, 1.5, 'h\xc3\xa9',"
                   " DATE '2020-01-02', TIMESTAMP '2020-01-02 03:04:05.123456',"
                   " INTERVAL '1 month 2 days 03:04:05.000006',"
                   " CAST('170141183460469231731687303715884105727' AS HUGEINT),"
                   " CAST('340282366920938463463374607431768211455' AS UHUGEINT),"
                   " CAST('123.456' AS DECIMAL(18,3)),"
                   " '12345678-1234-5678-9abc-def012345678'::UUID),"
                   " (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
                   " NULL, NULL, NULL)");
    pinkdb_result *result = query_ok(conn, "SELECT * FROM chunk_src");
    pinkdb_data_chunk *chunk = one_chunk(result);
    CHECK(pinkdb_chunk_size(chunk) == 2);
    pinkdb_appender *app = NULL;
    char *errmsg = NULL;
    CHECK(pinkdb_appender_create(conn, NULL, 0, "chunk_all", 9, &app,
                                 &errmsg) == 1);
    CHECK(pinkdb_appender_append_chunk(app, chunk) == 1);
    CHECK(pinkdb_appender_flush(app) == 1);
    CHECK(pinkdb_appender_destroy(app) == 1);
    pinkdb_destroy_chunk(chunk);
    pinkdb_result_free(result);

    result = query_ok(
        conn,
        "SELECT b = true, i = 1, bi = 9007199254740993, d = 1.5,"
        " s = 'h\xc3\xa9', dt = DATE '2020-01-02',"
        " ts = TIMESTAMP '2020-01-02 03:04:05.123456',"
        " iv = INTERVAL '1 month 2 days 03:04:05.000006',"
        " h = CAST('170141183460469231731687303715884105727' AS HUGEINT),"
        " uh = CAST('340282366920938463463374607431768211455' AS UHUGEINT),"
        " dec = CAST('123.456' AS DECIMAL(18,3)),"
        " u = '12345678-1234-5678-9abc-def012345678'::UUID"
        " FROM chunk_all WHERE i = 1");
    for (int col = 0; col < 12; col++) {
        pink_value value = pinkdb_result_value(result, 0, 0, (size_t)col);
        CHECK(value.is_null == 0 && value.int64 == 1);
    }
    pinkdb_result_free(result);
    result = query_ok(conn, "SELECT count(*) FROM chunk_all WHERE i IS NULL");
    CHECK(scalar(result) == 1);
    pinkdb_result_free(result);
    close_conn(conn);
}

static void test_version(void) {
    CHECK(PINKDB_ABI_VERSION_MAJOR == 1);
    CHECK(PINKDB_ABI_VERSION_MINOR == 5);
    CHECK(PINKDB_ABI_VERSION == ((1u << 16) | 5u));
    CHECK(pinkdb_abi_version() == PINKDB_ABI_VERSION);
    CHECK((pinkdb_abi_version() >> 16) == PINKDB_ABI_VERSION_MAJOR);
}

int main(void) {
    test_open_with_config();
    test_read_only();
    test_per_result_errors();
    test_chunk_scalars();
    test_chunk_many_rows();
    test_chunk_empty_and_multi_statement();
    test_chunk_errors_and_arrays();
    test_arrow_schema_and_array();
    test_connections_across_threads();
    test_interrupt();
    test_streaming_large_result();
    test_streaming_early_free();
    test_streaming_mid_error();
    test_streaming_interrupt();
    test_streaming_arrow();
    test_streaming_materialize_config();
    test_streaming_v1_after_a_large_fetch();
    test_appender_all_types();
    test_binding_all_types();
    test_appender_constraints_and_transactions();
    test_appender_auto_flush();
    test_appender_chunks_and_widths();
    test_appender_query_between_appends();
    test_appender_disconnect_with_live_appender();
    test_appender_chunk_every_type();
    test_appender_lifecycle_and_durable();
    test_version();
    if (failures != 0) {
        fprintf(stderr, "%d C API check(s) failed\n", failures);
        return 1;
    }
    printf("c api: all checks passed\n");
    return 0;
}
