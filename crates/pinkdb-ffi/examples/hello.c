/* Hello-world for the pinkdb C ABI.
 *
 * Build (from crates/pinkdb-ffi):
 *   make hello          # or: see the Makefile for the exact cc flags
 * Run:
 *   ./hello
 */
#include <stdio.h>
#include <string.h>

#include "pinkdb.h"

int main(void) {
    pinkdb_handle *db = pinkdb_open_memory();
    if (db == NULL) {
        fprintf(stderr, "open failed: %s\n", pinkdb_last_error());
        return 1;
    }

    const char *sql =
        "CREATE TABLE sales (region VARCHAR, total DOUBLE);"
        "INSERT INTO sales VALUES ('west', 10.5), ('east', 7.0), ('north', NULL);"
        "SELECT region, total FROM sales ORDER BY total DESC;";

    pinkdb_result *result = pinkdb_execute(db, sql, strlen(sql));
    if (result == NULL) {
        fprintf(stderr, "execute failed: %s\n", pinkdb_last_error());
        pinkdb_close(db);
        return 1;
    }

    size_t statements = pinkdb_result_statement_count(result);
    for (size_t s = 0; s < statements; s++) {
        if (pinkdb_result_kind(result, s) != PINKDB_STMT_SELECTED) {
            continue;
        }
        size_t columns = pinkdb_result_column_count(result, s);
        size_t rows = pinkdb_result_row_count(result, s);
        for (size_t c = 0; c < columns; c++) {
            printf("%s%s", c ? "\t" : "", pinkdb_result_column_name(result, s, c));
        }
        printf("\n");
        for (size_t row = 0; row < rows; row++) {
            for (size_t c = 0; c < columns; c++) {
                pink_value v = pinkdb_result_value(result, s, row, c);
                if (c) {
                    printf("\t");
                }
                if (v.is_null) {
                    printf("NULL");
                } else if (v.type_tag == PINKDB_TYPE_VARCHAR) {
                    printf("%.*s", (int)v.text_len, v.text_ptr);
                } else if (v.type_tag == PINKDB_TYPE_DOUBLE) {
                    printf("%g", v.double_);
                } else {
                    printf("%lld", (long long)v.int64);
                }
            }
            printf("\n");
        }
    }

    pinkdb_result_free(result);
    pinkdb_close(db);
    return 0;
}
