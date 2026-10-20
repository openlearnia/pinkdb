/*
 * pinkdb extension ABI v1 — test fixture for the WAL-replay security test.
 *
 * The library has a constructor: it runs when the library is *mapped*
 * (dlopen) and appends a line to the marker file. A WAL replay that loads
 * a path recorded inside the database file would rewrite the marker — that
 * is the C-1 reproduction ("opening a database file executes native code
 * recorded in its WAL").
 *
 * The marker path is compiled in (the test passes -DPINKDB_TEST_MARKER_PATH=…),
 * so no environment variable is involved and the parallel tests of the
 * binary cannot race each other.
 */
#include "pinkdb_extension.h"

#include <stdio.h>

#ifndef PINKDB_TEST_MARKER_PATH
#define PINKDB_TEST_MARKER_PATH "/tmp/pinkdb_marker_ext_not_built"
#endif

/* ext_marker_identity(a INTEGER) -> INTEGER: a constant, so the test can
 * check that the replayed row really came from the extension. */
static int32_t ext_marker_identity(void *user_data,
                                    const pinkdb_ext_value *args,
                                    size_t n_args,
                                    pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    out->type_tag = PINKDB_EXT_TYPE_INTEGER;
    out->is_null = 0;
    out->int64 = 7;
    return 0;
}

/* The constructor: runs exactly when the library is mapped. */
__attribute__((constructor)) static void marker_constructor(void) {
    FILE *f = fopen(PINKDB_TEST_MARKER_PATH, "a");
    if (f == NULL) {
        return;
    }
    fputs("EXECUTED\n", f);
    fclose(f);
}

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "marker_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }
    const int32_t t_int[1] = {PINKDB_EXT_TYPE_INTEGER};
    if (api->register_scalar(host_ctx, "ext_marker_identity", 1, t_int,
                             PINKDB_EXT_TYPE_INTEGER, ext_marker_identity,
                             NULL) != 0) {
        return 10;
    }
    return 0;
}
