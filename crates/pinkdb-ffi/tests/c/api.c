/* C-level acceptance suite for ABI 1.1: database/connection split, config,
 * per-result errors, interrupt and version macros. Built and run by
 * tests/c_api.rs. Keep it dependency-free (libc + pthreads). */

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

static void test_version(void) {
    CHECK(PINKDB_ABI_VERSION_MAJOR == 1);
    CHECK(PINKDB_ABI_VERSION_MINOR == 1);
    CHECK(PINKDB_ABI_VERSION == ((1u << 16) | 1u));
    CHECK(pinkdb_abi_version() == PINKDB_ABI_VERSION);
    CHECK((pinkdb_abi_version() >> 16) == PINKDB_ABI_VERSION_MAJOR);
}

int main(void) {
    test_open_with_config();
    test_read_only();
    test_per_result_errors();
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
