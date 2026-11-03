/* C-level acceptance suite for ABI 1.2: database/connection split, config,
 * per-result errors, interrupt, chunk fetch and typed vectors. Built and run
 * by tests/c_api.rs. Keep it dependency-free (libc + pthreads). */

#include "pinkdb.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
    CHECK(pinkdb_query(conn, sql, strlen(sql), &result) == 1);
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

static void test_version(void) {
    CHECK(PINKDB_ABI_VERSION_MAJOR == 1);
    CHECK(PINKDB_ABI_VERSION_MINOR == 2);
    CHECK(PINKDB_ABI_VERSION == ((1u << 16) | 2u));
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
    test_connections_across_threads();
    test_interrupt();
    test_version();
    if (failures != 0) {
        fprintf(stderr, "%d C API check(s) failed\n", failures);
        return 1;
    }
    printf("c api: all checks passed\n");
    return 0;
}
