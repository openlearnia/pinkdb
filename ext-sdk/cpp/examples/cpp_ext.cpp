/*
 * pinkdb extension example — cpp_ext (ABI v1, C++17, built on pinkdb_ext.hpp).
 *
 * The same library the C SDK's examples/cpp_ext.cpp builds, now written against
 * the header-only wrapper, plus the two paths the C example cannot show:
 *
 *     ext_add_checked(a BIGINT, b BIGINT) -> BIGINT
 *         a + b refusing to wrap: NULL in -> NULL out, error code 1 on i64
 *         overflow. (Loaded by ext-sdk/c/smoke.sh, which asserts on it.)
 *     ext_reverse(s VARCHAR) -> VARCHAR
 *         the input's code points in reverse order, so multibyte text survives
 *         and the empty string maps to an empty (zero-length) result.
 *     ext_now() -> TIMESTAMP
 *         microseconds since the epoch, registered VOLATILE: it reads the clock,
 *         so the engine refuses to call it in a statement it WAL-logs.
 *
 * The three exports the loader looks up are emitted by PINKDB_EXT_EXPORTS and a
 * hand-written pinkdb_extension_init; the wrapper's `guard` keeps every
 * exception inside the extern "C" boundary.
 *
 * Build: make -C ext-sdk/cpp          (or: c++ -std=c++17 -O2 -fPIC -Wall
 *        -Wextra -Werror -Iinclude -dynamiclib -o cpp_ext.dylib cpp_ext.cpp)
 */

#include "pinkdb_ext.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace ext = pinkdb::ext;

namespace {

// The host api copy taken in init. Read-only once init returns; callbacks run
// concurrently on host threads, so it must not be mutated afterwards.
ext::Host g_host;

/* ext_add_checked(a, b): a + b, refusing to wrap. */
int32_t add_checked(void *user_data, const ext::Args &args, ext::Writer &out) {
    (void)user_data;

    if (args[0].is_null() || args[1].is_null()) {
        out.set_null();
        return 0;
    }

    const int64_t a = args[0].bigint();
    const int64_t b = args[1].bigint();
    /* Portable overflow check (no compiler builtins): the sum of two i64s
     * overflows iff b pushes a past the representable range. */
    if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b)) {
        throw ext::Error(ext::kError, "ext_add_checked: BIGINT overflow");
    }

    out.set_bigint(a + b);
    return 0;
}

/* ext_reverse(s): reverse the UTF-8 code points (not the bytes), so multibyte
 * text stays readable; an empty input is an empty, non-NULL result. */
int32_t reverse(void *user_data, const ext::Args &args, ext::Writer &out) {
    (void)user_data;

    if (args[0].is_null()) {
        out.set_null();
        return 0;
    }

    const std::string_view input = args[0].varchar();
    std::string reversed;
    reversed.reserve(input.size());
    size_t end = input.size();
    while (end > 0) {
        size_t start = end - 1;
        // Walk back over continuation bytes (10xxxxxx) to the lead byte.
        while (start > 0 && (static_cast<unsigned char>(input[start]) & 0xC0u) == 0x80u) {
            --start;
        }
        reversed.append(input.substr(start, end - start));
        end = start;
    }

    out.set_text(reversed); /* host-allocated; the host frees it after the call */
    return 0;
}

/* ext_now(): the current time in microseconds since 1970-01-01 UTC. Reads the
 * clock, therefore VOLATILE — never registered as deterministic. */
int32_t now(void *user_data, const ext::Args &args, ext::Writer &out) {
    (void)user_data;
    (void)args;

    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    out.set_timestamp(static_cast<int64_t>(micros));
    return 0;
}

} // namespace

/* ------------------------------------------------------------------------ */
/* ABI entry points — C linkage is mandatory.                                */
/* ------------------------------------------------------------------------ */

PINKDB_EXT_EXPORTS("cpp_ext")

extern "C" int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    return ext::guard("pinkdb_extension_init", [&]() -> int32_t {
        g_host = ext::Host::adopt(api, host_ctx);

        if (g_host.register_function("ext_add_checked", {ext::kBigint, ext::kBigint},
                                     ext::kBigint, &add_checked) != 0) {
            return 10;
        }
        if (g_host.register_function("ext_reverse", {ext::kVarchar}, ext::kVarchar, &reverse) !=
            0) {
            return 11;
        }
        // A host too old to declare the flag fails the load here rather than
        // silently registering a clock reader as deterministic.
        if (g_host.register_volatile("ext_now", {}, ext::kTimestamp, &now) != 0) {
            return 12;
        }
        return 0;
    });
}
