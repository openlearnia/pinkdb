// pinkdb extension C++17 SDK — pinkdb_ext.hpp
//
// A header-only, standard-library-only wrapper over the pinkdb extension ABI
// v1 (minor 6). `include/pinkdb_extension.h` next to this file is the complete
// contract and the single source of truth; this header adds C++ ergonomics on
// top of it and never reinterprets the ABI by itself.
//
// What it does
// ------------
//  * `Host` copies `PinkdbHostApi` during `pinkdb_extension_init`, refuses a
//    mismatched `abi_version` and a `struct_size` below
//    `PINKDB_EXT_API_V1_SIZE`, and exposes `has_flags()` / `has_execute()` for
//    the two append-only additions (volatile registration, `execute_statement`).
//    The original struct the loader passes is **init-scoped**: only this copy
//    (and the per-registration copies it hands to the callbacks) stays valid.
//  * `register_function` / `register_volatile` register one scalar callback
//    with a typed C++ signature, without the `pinkdb_ext_value` plumbing.
//    `register_volatile` throws on a host that has no `register_scalar_flags`:
//    it never falls back to a deterministic registration.
//  * `Value` / `Args` are read-only views over the arguments (typed accessors,
//    INTERVAL decoded from the documented bit packing with `std::memcpy`).
//  * `Text` is owning result text freed through the host's `free` (never
//    `std::free`); `Writer` writes one result value into the host's
//    `pinkdb_ext_value`, allocating VARCHAR bytes with the host's `alloc`.
//  * `guard(...)` contains every exception at the `extern "C"` edge: an
//    exception escaping into C is undefined behavior, so it is caught, reported
//    on stderr, and turned into a non-zero error code.
//  * `PINKDB_EXT_EXPORTS("name")` emits the two constant entry points; `init`
//    (and the optional `pinkdb_ext_serve`) stay hand-written, see README.md.
//
// What it does not do
// -------------------
//  * It does not add ABI surface. The ABI has **no** table-function, scan or
//    copy-sink registration entry point: those extension kinds are linked-only
//    (compiled into the engine), not loadable libraries. Nothing here can
//    register one.
//  * It does not make an extension safe to call from anywhere but the host:
//    a callback must not call back into the engine (use `Host::execute_statement`
//    only where the ABI allows it, i.e. inside `pinkdb_ext_serve`).
//  * It does not check argument types or arity for you: the host guarantees
//    the registered declaration, so the accessors trust the tag.
//  * It does not sandbox. A loaded library runs with the host process's
//    privileges; see the README's security posture.
//
// Threading
// ---------
// Callbacks run CONCURRENTLY on the host's rayon worker threads — a registered
// function may run on any thread, several at a time, and never necessarily on
// the thread that ran init. Everything a callback touches (globals, statics,
// `user_data`, the extension's `Host` copy once init has returned) must
// therefore be thread-safe: atomics, locks or thread-local storage. Read-only
// use of a fully initialized `Host` is safe; `register_*` may only be called
// from inside `pinkdb_extension_init`.
//
// Result text ownership
// ---------------------
// A non-NULL VARCHAR result must be host-allocated (`Host::alloc` /
// `Writer::set_text`). The host copies exactly `text_len` bytes and then frees
// the buffer through the matching `free` — `Text` exists so that the only
// pointers that reach the ABI are host pointers, and so that an abandoned
// buffer is freed by the same allocator.
//
// Build: `-std=c++17 -Iinclude`, no libraries linked.

#ifndef PINKDB_EXT_HPP
#define PINKDB_EXT_HPP

#include "pinkdb_extension.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace pinkdb {
namespace ext {

// Type tags, spelled once for the C++ side.
constexpr int32_t kNull = PINKDB_EXT_TYPE_NULL;
constexpr int32_t kBoolean = PINKDB_EXT_TYPE_BOOLEAN;
constexpr int32_t kInteger = PINKDB_EXT_TYPE_INTEGER;
constexpr int32_t kBigint = PINKDB_EXT_TYPE_BIGINT;
constexpr int32_t kDouble = PINKDB_EXT_TYPE_DOUBLE;
constexpr int32_t kVarchar = PINKDB_EXT_TYPE_VARCHAR;
constexpr int32_t kDate = PINKDB_EXT_TYPE_DATE;
constexpr int32_t kTimestamp = PINKDB_EXT_TYPE_TIMESTAMP;
constexpr int32_t kInterval = PINKDB_EXT_TYPE_INTERVAL;
constexpr int32_t kHugeint = PINKDB_EXT_TYPE_HUGEINT;
constexpr int32_t kUhugeint = PINKDB_EXT_TYPE_UHUGEINT;
constexpr int32_t kVariant = PINKDB_EXT_TYPE_VARIANT;

// Codes `guard` returns when it contains an exception. They are ordinary
// statement-error codes: the host reports them as
// `extension function <name>() failed with code <n>`.
constexpr int32_t kError = 1;              ///< a thrown `Error` keeps its own code
constexpr int32_t kOutOfMemory = 2;        ///< `std::bad_alloc` (result text allocation failed)
constexpr int32_t kUnhandledException = 3; ///< any other exception

namespace detail {
// Bit-preserving double <-> uint64 conversions; the INTERVAL payload and
// bit-for-bit DOUBLE results ride on these. Never pointer punning.
inline uint64_t bits_of(double value) noexcept {
    uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof bits);
    return bits;
}

inline double double_of(uint64_t bits) noexcept {
    double value = 0.0;
    std::memcpy(&value, &bits, sizeof value);
    return value;
}
} // namespace detail

// Compile-time proof that the C++ view of the ABI matches the C layout.
static_assert(std::is_standard_layout<pinkdb_ext_value>::value,
              "pinkdb_ext_value must be standard-layout");
static_assert(std::is_trivially_copyable<pinkdb_ext_value>::value,
              "pinkdb_ext_value must be trivially copyable");
static_assert(offsetof(pinkdb_ext_value, type_tag) == 0, "type_tag offset");
static_assert(offsetof(pinkdb_ext_value, is_null) == 4, "is_null offset");
static_assert(offsetof(pinkdb_ext_value, int64) == 8, "int64 offset");
static_assert(offsetof(pinkdb_ext_value, double_) == 16, "double_ offset");
static_assert(offsetof(pinkdb_ext_value, text_ptr) == 24, "text_ptr offset");
static_assert(offsetof(pinkdb_ext_value, text_len) == 32, "text_len offset");

/// An `Error` thrown inside a callback (or inside init) is an ordinary
/// statement error: `guard` reports `code()` and does not unwind into C.
class Error : public std::runtime_error {
public:
    Error(int32_t code, const std::string &what) : std::runtime_error(what), code_(code) {}
    int32_t code() const noexcept { return code_; }

private:
    int32_t code_;
};

/// DuckDB's three-field calendar interval, decoded from / encoded into the
/// ABI's bit packing (see the INTERVAL note in the vendored header).
struct Interval {
    int32_t months = 0;
    int32_t days = 0;
    int64_t micros = 0;
};

/// The host's alloc/free pair, copied out of `PinkdbHostApi`. Result text must
/// be allocated with `alloc` and is always released by the host with `free`.
struct Allocator {
    void *(*alloc)(size_t) = nullptr;
    void (*free)(void *) = nullptr;
};

// --------------------------------------------------------------------------
// Value / Args — read-only argument views
// --------------------------------------------------------------------------

/// One argument value. A view: the bytes a VARCHAR names are only valid for
/// the duration of the call (copy them into a `std::string` to keep them).
class Value {
public:
    explicit Value(const pinkdb_ext_value &value) noexcept : value_(&value) {}

    int32_t type() const noexcept { return value_->type_tag; }
    bool is_null() const noexcept { return value_->is_null != 0; }

    bool boolean() const noexcept { return value_->int64 != 0; }
    int32_t integer() const noexcept { return static_cast<int32_t>(value_->int64); }
    int64_t bigint() const noexcept { return value_->int64; }
    double double_() const noexcept { return value_->double_; }

    /// VARCHAR bytes: embedded NULs allowed, no terminator assumed.
    std::string_view varchar() const noexcept {
        if (value_->text_ptr == nullptr) {
            return std::string_view();
        }
        return std::string_view(value_->text_ptr, value_->text_len);
    }

    /// VARIANT payload: opaque pinkdb-encoded bytes on the same text channel
    /// as `varchar()`, borrowed for the duration of the call.
    std::string_view variant() const noexcept { return varchar(); }

    int32_t date() const noexcept { return static_cast<int32_t>(value_->int64); } ///< days since 1970-01-01
    int64_t timestamp() const noexcept { return value_->int64; }                  ///< µs since 1970-01-01 UTC

    /// 128-bit integers: low 64 bits in `int64`, high 64 in the `double_` bits.
    __int128 hugeint() const noexcept {
        const unsigned __int128 bits = (static_cast<unsigned __int128>(detail::bits_of(value_->double_)) << 64) |
                                       static_cast<uint64_t>(value_->int64);
        return static_cast<__int128>(bits);
    }
    unsigned __int128 uhugeint() const noexcept {
        return (static_cast<unsigned __int128>(detail::bits_of(value_->double_)) << 64) |
               static_cast<uint64_t>(value_->int64);
    }

    Interval interval() const noexcept {
        const uint64_t bits = detail::bits_of(value_->double_);
        Interval interval;
        interval.months = static_cast<int32_t>(static_cast<uint32_t>(bits >> 32));
        interval.days = static_cast<int32_t>(static_cast<uint32_t>(bits));
        interval.micros = value_->int64;
        return interval;
    }

    /// The raw struct, for the rare case the wrapper has no accessor for.
    const pinkdb_ext_value &raw() const noexcept { return *value_; }

private:
    const pinkdb_ext_value *value_;
};

/// The callback's arguments, in declaration order. A zero-argument function
/// receives `args == NULL` from the host, i.e. an empty `Args`.
class Args {
public:
    Args(const pinkdb_ext_value *values, size_t count) noexcept : values_(values), count_(count) {}

    size_t size() const noexcept { return count_; }
    bool empty() const noexcept { return count_ == 0; }

    Value operator[](size_t index) const {
        if (index >= count_) {
            throw std::out_of_range("pinkdb: argument index out of range");
        }
        return Value(values_[index]);
    }

private:
    const pinkdb_ext_value *values_;
    size_t count_;
};

// --------------------------------------------------------------------------
// Text — owning result text, freed through the host
// --------------------------------------------------------------------------

/// Host-allocated bytes with RAII ownership. Copying is deleted (two owners of
/// one host buffer would double-free); moving transfers it, and handing it to
/// `Writer::set_text(Text&&)` transfers it to the host, which frees it right
/// after the callback returns.
class Text {
public:
    Text() noexcept = default;
    Text(Allocator allocator, char *data, size_t size) noexcept
        : allocator_(allocator), data_(data), size_(size) {}

    Text(const Text &) = delete;
    Text &operator=(const Text &) = delete;

    Text(Text &&other) noexcept
        : allocator_(other.allocator_), data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    Text &operator=(Text &&other) noexcept {
        if (this != &other) {
            reset();
            allocator_ = other.allocator_;
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    ~Text() { reset(); }

    /// Copy `bytes` into a fresh host allocation. Throws `std::bad_alloc` when
    /// the host's `alloc` fails.
    static Text copy(Allocator allocator, std::string_view bytes) {
        if (bytes.empty()) {
            return Text(allocator, nullptr, 0);
        }
        if (allocator.alloc == nullptr) {
            throw std::bad_alloc();
        }
        void *memory = allocator.alloc(bytes.size());
        if (memory == nullptr) {
            throw std::bad_alloc();
        }
        std::memcpy(memory, bytes.data(), bytes.size());
        return Text(allocator, static_cast<char *>(memory), bytes.size());
    }

    const char *data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    std::string_view view() const noexcept {
        return data_ == nullptr ? std::string_view() : std::string_view(data_, size_);
    }

    /// Give up ownership; the caller (the host, through `Writer`) frees it.
    char *release() noexcept {
        char *data = data_;
        data_ = nullptr;
        size_ = 0;
        return data;
    }

    void reset() noexcept {
        if (data_ != nullptr && allocator_.free != nullptr) {
            allocator_.free(data_);
        }
        data_ = nullptr;
        size_ = 0;
    }

private:
    Allocator allocator_{};
    char *data_ = nullptr;
    size_t size_ = 0;
};

// --------------------------------------------------------------------------
// Writer — fill the host's result slot
// --------------------------------------------------------------------------

/// Writes exactly one result into the host's pre-filled (NULL) slot. Call one
/// `set_*` per callback: a second `set_text` would drop the first buffer.
class Writer {
public:
    Writer(pinkdb_ext_value *out, Allocator allocator) noexcept : out_(out), allocator_(allocator) {}

    Writer(const Writer &) = delete;
    Writer &operator=(const Writer &) = delete;

    pinkdb_ext_value *raw() const noexcept { return out_; }
    Allocator allocator() const noexcept { return allocator_; }

    /// The NULL result the host pre-filled: only `is_null` is meaningful, so
    /// this is all a NULL callback has to do.
    void set_null() noexcept { out_->is_null = 1; }

    void set_bool(bool value) noexcept {
        tag(kBoolean);
        out_->int64 = value ? 1 : 0;
    }
    void set_int(int32_t value) noexcept {
        tag(kInteger);
        out_->int64 = value;
    }
    void set_bigint(int64_t value) noexcept {
        tag(kBigint);
        out_->int64 = value;
    }
    void set_double(double value) noexcept {
        tag(kDouble);
        out_->double_ = value;
    }
    void set_date(int32_t days) noexcept {
        tag(kDate);
        out_->int64 = days;
    }
    void set_timestamp(int64_t micros) noexcept {
        tag(kTimestamp);
        out_->int64 = micros;
    }
    void set_hugeint(__int128 value) noexcept {
        tag(kHugeint);
        const unsigned __int128 bits = static_cast<unsigned __int128>(value);
        out_->int64 = static_cast<int64_t>(static_cast<uint64_t>(bits));
        out_->double_ = detail::double_of(static_cast<uint64_t>(bits >> 64));
    }
    void set_uhugeint(unsigned __int128 value) noexcept {
        tag(kUhugeint);
        out_->int64 = static_cast<int64_t>(static_cast<uint64_t>(value));
        out_->double_ = detail::double_of(static_cast<uint64_t>(value >> 64));
    }
    void set_interval(const Interval &interval) noexcept {
        tag(kInterval);
        out_->int64 = interval.micros;
        const uint64_t bits = (static_cast<uint64_t>(static_cast<uint32_t>(interval.months)) << 32) |
                              static_cast<uint64_t>(static_cast<uint32_t>(interval.days));
        out_->double_ = detail::double_of(bits);
    }

    /// Allocate the bytes with the host's `alloc` (the ABI's only legal source
    /// for result text) and publish them. An empty string is a zero-length
    /// VARCHAR with a NULL `text_ptr`: nothing to free, read as "".
    void set_text(std::string_view bytes) { set_bytes(kVarchar, bytes); }

    /// Publish a VARIANT payload (opaque pinkdb-encoded bytes): the same
    /// allocation and ownership rule as `set_text`, under the VARIANT tag.
    void set_variant(std::string_view bytes) { set_bytes(kVariant, bytes); }

    /// Transfer an owned host buffer into the result. From here the host frees
    /// it — also when the callback then returns a non-zero code.
    void set_text(Text &&text) noexcept {
        tag(kVarchar);
        out_->text_ptr = text.data();
        out_->text_len = text.size();
        text.release();
    }

    /// Build owned text without publishing it (e.g. to publish it and then
    /// return an error code).
    Text make_text(std::string_view bytes) const { return Text::copy(allocator_, bytes); }

private:
    void set_bytes(int32_t type_tag, std::string_view bytes) {
        if (bytes.empty()) {
            tag(type_tag);
            out_->text_ptr = nullptr;
            out_->text_len = 0;
            return;
        }
        Text text = Text::copy(allocator_, bytes);
        tag(type_tag);
        out_->text_ptr = text.data();
        out_->text_len = text.size();
        text.release();
    }

    void tag(int32_t type_tag) noexcept {
        out_->type_tag = type_tag;
        out_->is_null = 0;
    }

    pinkdb_ext_value *out_;
    Allocator allocator_;
};

// --------------------------------------------------------------------------
// guard — contain every exception at the extern "C" edge
// --------------------------------------------------------------------------

namespace detail {
inline void report(const char *what, int32_t code, const char *message) noexcept {
    std::fprintf(stderr, "pinkdb extension: %s failed with code %d: %s\n",
                 what == nullptr ? "callback" : what, code,
                 message == nullptr ? "unknown exception" : message);
}
} // namespace detail

/// Run `fn` (returning `int32_t`) and convert any exception into a non-zero
/// code, because unwinding across the host's `extern "C"` boundary is
/// undefined behavior:
///
///   0                  `fn` returned 0
///   `Error::code()`    `fn` threw `Error`
///   kOutOfMemory       `fn` threw `std::bad_alloc`
///   kUnhandledException any other exception
///
/// Every non-zero path also prints one diagnostic line on stderr. Use it as
/// the first and last statement of every callback and of
/// `pinkdb_extension_init`.
template <class Fn>
int32_t guard(const char *what, Fn &&fn) noexcept {
    try {
        return fn();
    } catch (const Error &error) {
        detail::report(what, error.code(), error.what());
        return error.code();
    } catch (const std::bad_alloc &) {
        detail::report(what, kOutOfMemory, "out of memory");
        return kOutOfMemory;
    } catch (const std::exception &error) {
        detail::report(what, kUnhandledException, error.what());
        return kUnhandledException;
    } catch (...) {
        detail::report(what, kUnhandledException, "unknown exception");
        return kUnhandledException;
    }
}

/// The C++ shape of a scalar callback: throw on error (or return a code), write
/// one result into `out`, and never unwind. `guard` is applied for you.
using ScalarFn = int32_t (*)(void *user_data, const Args &args, Writer &out);

namespace detail {
/// One registration's immutable context: the host api copy (so callbacks need
/// no global) plus the author's `user_data`. It lives as long as the process,
/// like every registered function.
struct Registration {
    PinkdbHostApi api{};
    void *ctx = nullptr;
    const char *name = "";
    ScalarFn fn = nullptr;
    void *user_data = nullptr;
};

int32_t dispatch(void *user_data, const pinkdb_ext_value *args, size_t n_args,
                 pinkdb_ext_value *out) noexcept;
} // namespace detail

// --------------------------------------------------------------------------
// Host — the host api copy and the registration entry points
// --------------------------------------------------------------------------

/// A copy of the host's `PinkdbHostApi`, taken with `Host::adopt` during
/// `pinkdb_extension_init` (or during `pinkdb_ext_serve`). The struct the host
/// passes in is **valid only during that call**; this copy is what survives
/// for the process lifetime, and it is safe to read from callback threads.
///
/// A default-constructed `Host` is empty (`valid() == false`); every member
/// that needs the host throws `Error` on it.
class Host {
public:
    Host() noexcept = default;

    /// Validate the host's api and copy it. Throws `Error` when the host is
    /// unusable: a mismatched `abi_version`, a `struct_size` below
    /// `PINKDB_EXT_API_V1_SIZE`, or missing required function pointers.
    /// Fields the host does not have stay zero in the copy.
    static Host adopt(const PinkdbHostApi *api, void *host_ctx) {
        if (api == nullptr) {
            throw Error(kError, "pinkdb: the host passed no api struct");
        }
        if (api->abi_version != PINKDB_EXT_ABI_VERSION) {
            throw Error(kError, "pinkdb: host ABI version " + std::to_string(api->abi_version) +
                                    " does not match extension ABI version " +
                                    std::to_string(PINKDB_EXT_ABI_VERSION));
        }
        if (api->struct_size < PINKDB_EXT_API_V1_SIZE) {
            throw Error(kError, "pinkdb: host api struct_size " + std::to_string(api->struct_size) +
                                    " is below the ABI v1 size " +
                                    std::to_string(PINKDB_EXT_API_V1_SIZE));
        }
        if (api->register_scalar == nullptr || api->alloc == nullptr || api->free == nullptr) {
            throw Error(kError, "pinkdb: host api is missing a required entry point");
        }

        Host host;
        // Copy only what the host says it has: an older host (smaller
        // struct_size) must not be read past its struct, a newer one may carry
        // fields this SDK does not know.
        const size_t size = api->struct_size < sizeof(PinkdbHostApi) ? api->struct_size
                                                                     : sizeof(PinkdbHostApi);
        std::memcpy(&host.api_, api, size);
        host.ctx_ = host_ctx;
        return host;
    }

    /// Non-zero when the copy came from a real host.
    bool valid() const noexcept {
        return api_.register_scalar != nullptr && api_.alloc != nullptr && api_.free != nullptr;
    }

    /// Non-zero when the host has `register_scalar_flags` (ABI v1 addition), so
    /// volatile functions can be declared.
    bool has_flags() const noexcept {
        return valid() && PINKDB_EXT_HOST_HAS_FLAGS(&api_) != 0 &&
               api_.register_scalar_flags != nullptr;
    }

    /// Non-zero when the host has `execute_statement` (ABI minor 2).
    bool has_execute() const noexcept {
        return valid() && PINKDB_EXT_HOST_HAS_EXECUTE(&api_) != 0 &&
               api_.execute_statement != nullptr;
    }

    const PinkdbHostApi &api() const noexcept { return api_; }
    Allocator allocator() const noexcept {
        Allocator allocator;
        allocator.alloc = api_.alloc;
        allocator.free = api_.free;
        return allocator;
    }

    /// Allocate result text through the host. Throws `std::bad_alloc` on NULL.
    void *alloc(size_t size) const {
        void *memory = api_.alloc(size);
        if (memory == nullptr) {
            throw std::bad_alloc();
        }
        return memory;
    }

    /// Free host-allocated result text. Prefer `Text`, which does this in its
    /// destructor.
    void free(void *pointer) const noexcept {
        if (pointer != nullptr) {
            api_.free(pointer);
        }
    }

    /// Register a deterministic (pure) scalar function. Returns the host's code:
    /// 0 registered, non-zero rejected (the load then fails — never assume a
    /// function exists). Call only from `pinkdb_extension_init`.
    int32_t register_function(const char *name, std::initializer_list<int32_t> arg_types,
                              int32_t ret_type, ScalarFn fn, void *user_data = nullptr) const {
        return add(name, arg_types, ret_type, 0, fn, user_data);
    }

    /// Register a `PINKDB_EXT_FLAG_VOLATILE` scalar function (it may read the
    /// clock, a random source, ambient state; the engine refuses to call it in
    /// a statement it WAL-logs). Throws `Error` when the host has no
    /// `register_scalar_flags`: registering it as deterministic would let a
    /// durable store replay a call whose value may change, so there is no
    /// fallback. Returns the host's code otherwise.
    int32_t register_volatile(const char *name, std::initializer_list<int32_t> arg_types,
                              int32_t ret_type, ScalarFn fn, void *user_data = nullptr) const {
        if (!has_flags()) {
            throw Error(kError, std::string("pinkdb: cannot register volatile function \"") +
                                    (name == nullptr ? "" : name) +
                                    "\": the host has no register_scalar_flags");
        }
        return add(name, arg_types, ret_type, PINKDB_EXT_FLAG_VOLATILE, fn, user_data);
    }

    /// ABI minor 2: run a SQL statement in the host (only meaningful inside
    /// `pinkdb_ext_serve`, where the api is wired to the live session). Returns
    /// the host's code: 0 success, 1 statement failed, 2 no live session — also
    /// what a host without `execute_statement` answers. `*out` receives the
    /// result text when code is 0.
    int32_t execute_statement(std::string_view sql, std::string *out) const {
        if (!has_execute()) {
            return 2;
        }
        char *text = nullptr;
        size_t len = 0;
        // `sql` need not be NUL-terminated: exactly sql.size() bytes are read.
        const int32_t code = api_.execute_statement(ctx_, sql.data(), sql.size(), &text, &len);
        Text owned(allocator(), text, len); // freed through the host, whatever the code
        if (out != nullptr && code == 0 && text != nullptr) {
            out->assign(text, len);
        }
        return code;
    }

private:
    int32_t add(const char *name, std::initializer_list<int32_t> arg_types, int32_t ret_type,
                uint32_t flags, ScalarFn fn, void *user_data) const {
        if (!valid()) {
            throw Error(kError, "pinkdb: registration before Host::adopt");
        }
        if (name == nullptr || *name == '\0') {
            throw Error(kError, "pinkdb: a function name must be non-empty");
        }
        if (fn == nullptr) {
            throw Error(kError, std::string("pinkdb: function \"") + name + "\" has no callback");
        }
        if (arg_types.size() > PINKDB_EXT_MAX_ARGS) {
            throw Error(kError, std::string("pinkdb: function \"") + name + "\" has more than " +
                                    std::to_string(PINKDB_EXT_MAX_ARGS) + " arguments");
        }
        const std::vector<int32_t> types(arg_types);
        auto slot = std::make_unique<detail::Registration>();
        slot->api = api_;
        slot->ctx = ctx_;
        slot->name = name;
        slot->fn = fn;
        slot->user_data = user_data;

        const int32_t code =
            flags == 0
                ? api_.register_scalar(ctx_, name, static_cast<uint32_t>(types.size()),
                                       types.empty() ? nullptr : types.data(), ret_type,
                                       &detail::dispatch, slot.get())
                : api_.register_scalar_flags(ctx_, name, static_cast<uint32_t>(types.size()),
                                             types.empty() ? nullptr : types.data(), ret_type,
                                             flags, &detail::dispatch, slot.get());
        if (code == 0) {
            // The slot must outlive every call: keep it for the process.
            slots_.push_back(std::move(slot));
        }
        return code;
    }

    PinkdbHostApi api_{};
    void *ctx_ = nullptr;
    mutable std::vector<std::unique_ptr<detail::Registration>> slots_;
};

namespace detail {
inline int32_t dispatch(void *user_data, const pinkdb_ext_value *args, size_t n_args,
                        pinkdb_ext_value *out) noexcept {
    const Registration *registration = static_cast<const Registration *>(user_data);
    if (registration == nullptr || registration->fn == nullptr || out == nullptr) {
        return kUnhandledException;
    }
    Allocator allocator;
    allocator.alloc = registration->api.alloc;
    allocator.free = registration->api.free;
    const Args view(args, n_args);
    Writer writer(out, allocator);
    return guard(registration->name,
                 [&]() -> int32_t { return registration->fn(registration->user_data, view, writer); });
}
} // namespace detail

} // namespace ext
} // namespace pinkdb

// --------------------------------------------------------------------------
// Exports
// --------------------------------------------------------------------------
//
// The loader looks the entry points up by name, so they must have C linkage.
// Put PINKDB_EXT_EXPORTS("your_ext_name") at file scope, then write init (and,
// if the extension serves a client, the optional pinkdb_ext_serve) by hand:
//
//     PINKDB_EXT_EXPORTS("my_ext")
//
//     extern "C" int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
//         return pinkdb::ext::guard("pinkdb_extension_init", [&]() -> int32_t {
//             g_host = pinkdb::ext::Host::adopt(api, host_ctx);
//             if (g_host.register_function("my_fn", {pinkdb::ext::kVarchar},
//                                          pinkdb::ext::kVarchar, &my_fn) != 0) {
//                 return 10;
//             }
//             return 0;
//         });
//     }
#define PINKDB_EXT_EXPORTS(extension_name_literal)                                             \
    extern "C" uint32_t pinkdb_extension_abi_version(void) { return PINKDB_EXT_ABI_VERSION; }  \
    extern "C" const char *pinkdb_extension_name(void) { return (extension_name_literal); }

#endif // PINKDB_EXT_HPP
