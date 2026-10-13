/*
 * pinkdb extension example — cpp_ext (ABI v1, C++).
 *
 * Proves the ABI header compiles as C++ and that the required exports work
 * with C linkage:
 *
 *     ext_add_checked(a BIGINT, b BIGINT) -> BIGINT
 *         NULL in -> NULL out; returns error code 1 on i64 overflow.
 *
 * The header wraps its declarations in `extern "C"` guards, so including it
 * from C++ is safe. The three symbols the loader looks up must have C linkage
 * too — that is what the `extern "C"` block around their definitions below
 * guarantees (without it they would be name-mangled and the load would fail
 * with a missing-symbol error).
 *
 * Build:  c++ -std=c++17 -O2 -fPIC -Wall -Wextra -I../include -shared \
 *             -o cpp_ext.dylib cpp_ext.cpp
 */

#include "pinkdb_extension.h"

#include <cstddef>
#include <cstdint>
#include <type_traits>

/*
 * Compile-time proof that the C++ view of the ABI matches the C header's
 * layout: the value struct is standard-layout and its field offsets are the
 * ones the host marshals. A layout change would fail this build rather than
 * silently reinterpret every value.
 */
static_assert(std::is_standard_layout<pinkdb_ext_value>::value,
              "pinkdb_ext_value must be standard-layout");
static_assert(offsetof(pinkdb_ext_value, type_tag) == 0, "type_tag offset");
static_assert(offsetof(pinkdb_ext_value, is_null) == 4, "is_null offset");
static_assert(offsetof(pinkdb_ext_value, int64) == 8, "int64 offset");
static_assert(offsetof(pinkdb_ext_value, double_) == 16, "double_ offset");

namespace {

/* ext_add_checked(a, b): a + b, refusing to wrap. */
int32_t ext_add_checked(void *user_data, const pinkdb_ext_value *args,
                        size_t n_args, pinkdb_ext_value *out) {
    (void)user_data;
    (void)n_args;

    if (args[0].is_null || args[1].is_null) {
        out->is_null = 1;
        return 0;
    }

    const int64_t a = args[0].int64;
    const int64_t b = args[1].int64;
    /* Portable overflow check (no compiler builtins): the sum of two i64s
     * overflows iff b pushes a past the representable range. */
    if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b)) {
        return 1; /* statement error, reported with this code */
    }

    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = a + b;
    return 0;
}

} // namespace

/* ------------------------------------------------------------------------ */
/* ABI entry points — C linkage is mandatory.                                */
/* ------------------------------------------------------------------------ */

extern "C" uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

extern "C" const char *pinkdb_extension_name(void) {
    return "cpp_ext";
}

extern "C" int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    if (api == nullptr || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    if (api->struct_size < sizeof(PinkdbHostApi)) {
        return 2;
    }

    static const int32_t arg_types[2] = {
        PINKDB_EXT_TYPE_BIGINT, PINKDB_EXT_TYPE_BIGINT
    };
    if (api->register_scalar(host_ctx, "ext_add_checked", 2, arg_types,
                             PINKDB_EXT_TYPE_BIGINT, ext_add_checked,
                             nullptr) != 0) {
        return 10;
    }
    return 0;
}
