/*
 * pinkdb extension example — math_ext (ABI v1, plain C99).
 *
 * Registers two numeric scalar functions:
 *
 *     ext_clamp(v BIGINT, lo BIGINT, hi BIGINT) -> BIGINT
 *         NULL in -> NULL out; errors (code 1) when lo > hi.
 *     ext_deg2rad(deg DOUBLE) -> DOUBLE
 *         Degrees to radians. NULL in -> NULL out.
 *
 * Neither function needs the host allocator (no VARCHAR results), so the
 * `PinkdbHostApi` struct is used only during init. See ../README.md for the
 * full contract and `../include/pinkdb_extension.h` for the reference.
 */

#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------------ */
/* Callbacks                                                                 */
/* ------------------------------------------------------------------------ */

/* ext_clamp(v, lo, hi): clamp v into [lo, hi]. */
static int32_t ext_clamp(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;

    /* NULL arguments are handed through (is_null set): decide what NULL
     * means. Here: any NULL argument makes the result NULL. */
    if (args[0].is_null || args[1].is_null || args[2].is_null) {
        out->is_null = 1;
        return 0;
    }

    int64_t v = args[0].int64, lo = args[1].int64, hi = args[2].int64;
    if (lo > hi) {
        /* Ordinary SQL errors are non-zero return codes (never NULL). The
         * host aborts the statement and reports the code. */
        return 1;
    }
    if (v < lo) {
        v = lo;
    } else if (v > hi) {
        v = hi;
    }

    /* Declare the result. `out` arrives pre-filled with NULL; every
     * non-NULL result must set its tag, clear `is_null`, and fill the
     * matching payload field. */
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = v;
    return 0;
}

/* ext_deg2rad(deg): degrees to radians. */
static int32_t ext_deg2rad(void *user_data, const pinkdb_ext_value *args,
                           size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;

    if (args[0].is_null) {
        out->is_null = 1;
        return 0;
    }

    /* Spelled out instead of M_PI: M_PI is not guaranteed by strict C99. */
    static const double pi = 3.14159265358979323846;
    out->type_tag = PINKDB_EXT_TYPE_DOUBLE;
    out->is_null = 0;
    out->double_ = args[0].double_ * pi / 180.0;
    return 0;
}

/* ------------------------------------------------------------------------ */
/* ABI entry points                                                          */
/* ------------------------------------------------------------------------ */

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "math_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }

    static const int32_t t_clamp[3] = {
        PINKDB_EXT_TYPE_BIGINT, PINKDB_EXT_TYPE_BIGINT, PINKDB_EXT_TYPE_BIGINT
    };
    static const int32_t t_double[1] = {PINKDB_EXT_TYPE_DOUBLE};

    if (api->register_scalar(host_ctx, "ext_clamp", 3, t_clamp,
                             PINKDB_EXT_TYPE_BIGINT, ext_clamp, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "ext_deg2rad", 1, t_double,
                             PINKDB_EXT_TYPE_DOUBLE, ext_deg2rad, NULL) != 0) {
        return 11;
    }
    return 0;
}
