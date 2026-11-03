/*
 * The pinkdb ABI conformance fixture, C++17 SDK build.
 *
 * `ext-sdk/conformance/README.md` is the contract: the extension is named
 * `conformance_ext` and registers exactly the `cf_*` functions below, which
 * every SDK implements so the loader tests can measure the four SDKs against
 * one contract instead of against their own examples.
 *
 * Two of the required behaviours do not fit a safe surface, so the fixture
 * advertises what it really does in `pinkdb_conformance_caps`:
 *
 *   bit 0  cf_fail allocates its 4 bytes through the host's `alloc` and then
 *          returns code 1, so the host's error-path free is observable;
 *   bit 1  cf_unwind really throws inside the callback (the wrapper's `guard`
 *          contains it and reports a clean error code).
 *
 * Both bits are set here: caps = 3.
 *
 * Build: make -C ext-sdk/cpp        (loader tests build it themselves with
 *        c++ -std=c++17 -shared -fPIC -Iext-sdk/cpp/include)
 */

#include "pinkdb_ext.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace ext = pinkdb::ext;

namespace {

constexpr int64_t kCaps = 3; /* bit 0: cf_fail allocates; bit 1: cf_unwind throws */

ext::Host g_host;

std::atomic<int64_t> g_calls{0};
std::atomic<int64_t> g_foreign_threads{0};
// Hash of the OS thread that ran init; every callback on another thread proves
// the ABI's "no thread affinity" rule.
std::atomic<uint64_t> g_init_thread{0};

uint64_t current_thread() noexcept {
    return static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
}

/// Every callback's first statement: count the call, and count it as foreign
/// when it is not on init's thread.
void enter() noexcept {
    g_calls.fetch_add(1, std::memory_order_relaxed);
    if (current_thread() != g_init_thread.load(std::memory_order_relaxed)) {
        g_foreign_threads.fetch_add(1, std::memory_order_relaxed);
    }
}

// ---------------------------------------------------------------------------
// Value passthrough: every tag, both directions, NULL in -> NULL out
// ---------------------------------------------------------------------------

int32_t cf_echo(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_text(args[0].varchar()); // embedded NULs, multibyte, empty allowed
    return 0;
}

int32_t cf_bool(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_bool(args[0].boolean());
    return 0;
}

int32_t cf_int(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_int(args[0].integer());
    return 0;
}

int32_t cf_bigint(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_bigint(args[0].bigint());
    return 0;
}

int32_t cf_sum_bigint(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_bigint(args[0].bigint());
    return 0;
}

int32_t cf_double(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    // A copy of the bits, never a re-formatted decimal: NaN payloads and ±inf
    // arrive unchanged.
    out.set_double(args[0].double_());
    return 0;
}

int32_t cf_date(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_date(args[0].date());
    return 0;
}

int32_t cf_timestamp(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_timestamp(args[0].timestamp());
    return 0;
}

int32_t cf_hugeint(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_hugeint(args[0].hugeint());
    return 0;
}

int32_t cf_uhugeint(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    out.set_uhugeint(args[0].uhugeint());
    return 0;
}

int32_t cf_interval(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    // months/days ride in a bit pattern, micros is a number: the accessor pair
    // is what keeps the two from being confused.
    out.set_interval(args[0].interval());
    return 0;
}

/// Appends `prefix` and the integer, or `null` for a NULL argument.
void append_int(std::string &text, const char *prefix, const ext::Value &value, int64_t number) {
    text += prefix;
    if (value.is_null()) {
        text += "null";
    } else {
        text += std::to_string(number);
    }
}

int32_t cf_variant(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    // Opaque pinkdb-encoded bytes on the text channel; the writer allocates
    // them through the host, exactly like a VARCHAR result.
    out.set_variant(args[0].variant());
    return 0;
}

int32_t cf_all(void *, const ext::Args &args, ext::Writer &out) {
    enter();

    std::string summary;
    summary += "b=";
    if (args[0].is_null()) {
        summary += "null";
    } else {
        summary += args[0].boolean() ? "1" : "0";
    }
    append_int(summary, ",i=", args[1], args[1].integer());
    append_int(summary, ",big=", args[2], args[2].bigint());

    summary += ",d=";
    if (args[3].is_null()) {
        summary += "null";
    } else {
        const double value = args[3].double_();
        uint64_t bits = 0;
        std::memcpy(&bits, &value, sizeof bits);
        char hex[24];
        std::snprintf(hex, sizeof hex, "0x%016llx", static_cast<unsigned long long>(bits));
        summary += hex;
    }

    summary += ",v=";
    if (args[4].is_null()) {
        summary += "null";
    } else {
        summary.append(args[4].varchar());
    }

    append_int(summary, ",date=", args[5], args[5].date());
    append_int(summary, ",ts=", args[6], args[6].timestamp());

    summary += ",iv=";
    if (args[7].is_null()) {
        summary += "null";
    } else {
        const ext::Interval interval = args[7].interval();
        summary += std::to_string(interval.months);
        summary += "/";
        summary += std::to_string(interval.days);
        summary += "/";
        summary += std::to_string(interval.micros);
    }

    out.set_text(summary);
    return 0;
}

/// A zero-length, non-NULL VARCHAR: no allocation, `text_ptr` NULL, `text_len`
/// 0, which the host reads as the empty string.
int32_t cf_empty(void *, const ext::Args &, ext::Writer &out) {
    enter();
    out.set_text(std::string_view());
    return 0;
}

/// `n` bytes of 'x' through the host's alloc; a negative `n` is a NULL result,
/// not an error.
int32_t cf_fill(void *, const ext::Args &args, ext::Writer &out) {
    enter();
    if (args[0].is_null()) {
        return 0;
    }
    const int64_t n = args[0].bigint();
    if (n < 0) {
        return 0;
    }
    if (static_cast<uint64_t>(n) > (64ull << 20)) {
        // The ABI rejects a VARCHAR result over 64 MiB without reading or
        // freeing it; refuse here so the buffer is never built.
        throw ext::Error(ext::kError, "cf_fill: refusing a VARCHAR result over 64 MiB");
    }
    out.set_text(std::string(static_cast<size_t>(n), 'x'));
    return 0;
}

/// Publishes host-allocated text and *then* fails with code 1: the host must
/// free the buffer on its error path (caps bit 0 says the allocation is real).
int32_t cf_fail(void *, const ext::Args &, ext::Writer &out) {
    enter();
    out.set_text(out.make_text("oops"));
    return 1;
}

/// The worst case for any SDK: an exception inside the callback. `guard`
/// contains it, prints one diagnostic and reports a clean non-zero code.
int32_t cf_unwind(void *, const ext::Args &, ext::Writer &) {
    enter();
    throw std::runtime_error("conformance_ext: cf_unwind throws on purpose");
}

/// Registered with PINKDB_EXT_FLAG_VOLATILE: not a pure function of its
/// arguments, declared honestly.
int32_t cf_volatile(void *, const ext::Args &, ext::Writer &out) {
    enter();
    out.set_bigint(42);
    return 0;
}

/// A plain callback; the counters `enter()` keeps are what the test reads.
int32_t cf_thread(void *, const ext::Args &, ext::Writer &out) {
    enter();
    out.set_bigint(1);
    return 0;
}

} // namespace

/* ------------------------------------------------------------------------ */
/* ABI entry points                                                          */
/* ------------------------------------------------------------------------ */

PINKDB_EXT_EXPORTS("conformance_ext")

extern "C" int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    return ext::guard("pinkdb_extension_init", [&]() -> int32_t {
        // Record the init thread before registering: every callback that lands
        // on another thread is what the foreign-thread counter reports.
        g_init_thread.store(current_thread(), std::memory_order_relaxed);
        g_host = ext::Host::adopt(api, host_ctx);

        const auto reg = [](const char *name, std::initializer_list<int32_t> arg_types,
                            int32_t ret_type, ext::ScalarFn fn,
                            bool is_volatile) -> int32_t {
            return is_volatile
                       ? g_host.register_volatile(name, arg_types, ret_type, fn, nullptr)
                       : g_host.register_function(name, arg_types, ret_type, fn, nullptr);
        };

        if (reg("cf_echo", {ext::kVarchar}, ext::kVarchar, &cf_echo, false) != 0) {
            return 10;
        }
        if (reg("cf_bool", {ext::kBoolean}, ext::kBoolean, &cf_bool, false) != 0) {
            return 11;
        }
        if (reg("cf_int", {ext::kInteger}, ext::kInteger, &cf_int, false) != 0) {
            return 12;
        }
        if (reg("cf_bigint", {ext::kBigint}, ext::kBigint, &cf_bigint, false) != 0) {
            return 13;
        }
        if (reg("cf_hugeint", {ext::kHugeint}, ext::kHugeint, &cf_hugeint, false) != 0) {
            return 25;
        }
        if (reg("cf_uhugeint", {ext::kUhugeint}, ext::kUhugeint, &cf_uhugeint, false) != 0) {
            return 26;
        }
        if (reg("cf_sum_bigint", {ext::kBigint}, ext::kBigint, &cf_sum_bigint, false) != 0) {
            return 27;
        }
        if (reg("cf_double", {ext::kDouble}, ext::kDouble, &cf_double, false) != 0) {
            return 14;
        }
        if (reg("cf_date", {ext::kDate}, ext::kDate, &cf_date, false) != 0) {
            return 15;
        }
        if (reg("cf_timestamp", {ext::kTimestamp}, ext::kTimestamp, &cf_timestamp, false) != 0) {
            return 16;
        }
        if (reg("cf_interval", {ext::kInterval}, ext::kInterval, &cf_interval, false) != 0) {
            return 17;
        }
        if (reg("cf_variant", {ext::kVariant}, ext::kVariant, &cf_variant, false) != 0) {
            return 23;
        }
        if (reg("cf_all",
                {ext::kBoolean, ext::kInteger, ext::kBigint, ext::kDouble, ext::kVarchar,
                 ext::kDate, ext::kTimestamp, ext::kInterval},
                ext::kVarchar, &cf_all, false) != 0) {
            return 18;
        }
        if (reg("cf_empty", {}, ext::kVarchar, &cf_empty, false) != 0) {
            return 19;
        }
        if (reg("cf_fill", {ext::kBigint}, ext::kVarchar, &cf_fill, false) != 0) {
            return 20;
        }
        if (reg("cf_fail", {ext::kBigint}, ext::kVarchar, &cf_fail, false) != 0) {
            return 21;
        }
        if (reg("cf_unwind", {}, ext::kBigint, &cf_unwind, false) != 0) {
            return 22;
        }
        if (reg("cf_volatile", {}, ext::kBigint, &cf_volatile, true) != 0) {
            return 23;
        }
        if (reg("cf_thread", {}, ext::kBigint, &cf_thread, false) != 0) {
            return 24;
        }
        return 0;
    });
}

/* ------------------------------------------------------------------------ */
/* Introspection (not part of the ABI; the loader tests read these)          */
/* ------------------------------------------------------------------------ */

extern "C" int64_t pinkdb_conformance_calls(void) {
    return g_calls.load(std::memory_order_relaxed);
}

extern "C" int64_t pinkdb_conformance_foreign_threads(void) {
    return g_foreign_threads.load(std::memory_order_relaxed);
}

extern "C" int64_t pinkdb_conformance_caps(void) { return kCaps; }
