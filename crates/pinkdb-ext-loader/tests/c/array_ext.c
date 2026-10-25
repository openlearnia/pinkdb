/*
 * pinkdb extension ABI v1 — ARRAY (minor 5) test extension.
 *
 * Compiled at test time with the system `cc` against
 * `crates/pinkdb-ext-loader/include/pinkdb_extension.h`. Every function takes
 * or returns a fixed-size `DOUBLE[3]`, whose type tag carries its length
 * (`PINKDB_EXT_ARRAY_TAG(3)`), and reads the payload as a plain
 * `const double *` — the engine's own element buffer, no per-element boxing.
 * The `arrf_*` functions do the same with the 4-byte kind, `FLOAT[3]`
 * (`PINKDB_EXT_ARRAY_FLOAT_TAG(3)`), which is the shape 107b's distance
 * kernels and 107c's `embed()` consume.
 */
#include "pinkdb_extension.h"

#include <stdint.h>
#include <string.h>

#define LEN 3
#define ARR PINKDB_EXT_ARRAY_TAG(LEN)

static PinkdbHostApi g_api;

/* Non-NULL argument bytes are the host's view of one row's elements. */
static const double *elements(const pinkdb_ext_value *value) {
    return (const double *)value->text_ptr;
}

static int32_t arr_twice(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    if (n_args != 1) {
        return 4;
    }
    /* A NULL array arrives with the declared type and no payload — check
     * `is_null` before the payload, never the other way round. */
    if (args[0].is_null) {
        return 0; /* out stays NULL */
    }
    if (args[0].type_tag != ARR || args[0].text_len != LEN * sizeof(double) ||
        args[0].int64 != LEN) {
        return 5;
    }
    double *buffer = (double *)g_api.alloc(LEN * sizeof(double));
    if (buffer == NULL) {
        return 3;
    }
    const double *in = elements(&args[0]);
    for (int i = 0; i < LEN; i++) {
        buffer[i] = in[i] * 2.0;
    }
    out->type_tag = ARR;
    out->is_null = 0;
    out->int64 = LEN;
    out->text_ptr = (const char *)buffer;
    out->text_len = LEN * sizeof(double);
    return 0;
}

/* A DOUBLE result next to an ARRAY argument, to prove both channels coexist. */
static int32_t arr_sum(void *user_data, const pinkdb_ext_value *args,
                       size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    if (n_args != 1 || args[0].is_null) {
        return 0;
    }
    const double *in = elements(&args[0]);
    double total = 0.0;
    for (int i = 0; i < LEN; i++) {
        total += in[i];
    }
    out->type_tag = PINKDB_EXT_TYPE_DOUBLE;
    out->is_null = 0;
    out->double_ = total;
    return 0;
}

/* A constant ARRAY result: the declared length and the payload must agree. */
static int32_t arr_origin(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)args;
    (void)n_args;
    double *buffer = (double *)g_api.alloc(LEN * sizeof(double));
    if (buffer == NULL) {
        return 3;
    }
    buffer[0] = 1.0;
    buffer[1] = 0.5;
    buffer[2] = -0.0;
    out->type_tag = ARR;
    out->is_null = 0;
    out->int64 = LEN;
    out->text_ptr = (const char *)buffer;
    out->text_len = LEN * sizeof(double);
    return 0;
}

#define FARR PINKDB_EXT_ARRAY_FLOAT_TAG(LEN)

/* The 4-byte counterpart: the payload is N little-endian f32 values. */
static int32_t arrf_twice(void *user_data, const pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    if (n_args != 1) {
        return 4;
    }
    if (args[0].is_null) {
        return 0;
    }
    if (args[0].type_tag != FARR || args[0].text_len != LEN * sizeof(float) ||
        args[0].int64 != LEN) {
        return 5;
    }
    const float *in = (const float *)args[0].text_ptr;
    float *buffer = (float *)g_api.alloc(LEN * sizeof(float));
    if (buffer == NULL) {
        return 3;
    }
    for (int i = 0; i < LEN; i++) {
        buffer[i] = in[i] * 2.0f;
    }
    out->type_tag = FARR;
    out->is_null = 0;
    out->int64 = LEN;
    out->text_ptr = (const char *)buffer;
    out->text_len = LEN * sizeof(float);
    return 0;
}

/* Returns DOUBLE, so one row mixes an f32 view and a scalar result. */
static int32_t arrf_sum(void *user_data, const pinkdb_ext_value *args,
                        size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    if (n_args != 1 || args[0].is_null) {
        return 0;
    }
    if (args[0].type_tag != FARR || args[0].text_len != LEN * sizeof(float)) {
        return 5;
    }
    const float *in = (const float *)args[0].text_ptr;
    double total = 0.0;
    for (int i = 0; i < LEN; i++) {
        total += (double)in[i];
    }
    out->type_tag = PINKDB_EXT_TYPE_DOUBLE;
    out->is_null = 0;
    out->double_ = total;
    return 0;
}

static const char EXT_NAME[] = "array_ext";

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return EXT_NAME;
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }
    g_api = *api;

    const int32_t t_arr[1] = {ARR};
    const int32_t t_pair[2] = {ARR, ARR};
    if (api->register_scalar(host_ctx, "arr_twice", 1, t_arr, ARR, arr_twice, NULL) != 0) {
        return 10;
    }
    if (api->register_scalar(host_ctx, "arr_sum", 1, t_arr, PINKDB_EXT_TYPE_DOUBLE, arr_sum,
                             NULL) != 0) {
        return 11;
    }
    if (api->register_scalar(host_ctx, "arr_origin", 0, NULL, ARR, arr_origin, NULL) != 0) {
        return 12;
    }
    /* Registered, never called successfully: the host refuses a row holding a
     * NULL element before it reaches the callback. */
    if (api->register_scalar(host_ctx, "arr_pair", 2, t_pair, PINKDB_EXT_TYPE_DOUBLE, arr_sum,
                             NULL) != 0) {
        return 13;
    }
    const int32_t t_farr[1] = {FARR};
    if (api->register_scalar(host_ctx, "arrf_twice", 1, t_farr, FARR, arrf_twice, NULL) != 0) {
        return 14;
    }
    if (api->register_scalar(host_ctx, "arrf_sum", 1, t_farr, PINKDB_EXT_TYPE_DOUBLE, arrf_sum,
                             NULL) != 0) {
        return 15;
    }
    return 0;
}
