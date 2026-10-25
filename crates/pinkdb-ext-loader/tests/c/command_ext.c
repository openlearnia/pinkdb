/*
 * An extension that uses the ABI minor 4 command entry point: it runs one
 * statement on each channel (its own bookkeeping through execute_internal, a
 * model tool call through execute_statement), streams both answers plus its
 * arguments back through the sink, and returns.
 *
 * The statements are canned: the tests are the host's side of the contract
 * (symbol detection, both channels wired, chunks on both sink channels,
 * frees exactly once), not the engine's policy.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "command_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    (void)host_ctx;
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    return 0;
}

int32_t pinkdb_ext_command(const PinkdbHostApi *api, void *host_ctx,
                           const char *args, PinkdbExtSinkFn sink,
                           void *sink_ctx) {
    if (api == NULL || sink == NULL) {
        return 2;
    }
    if (!PINKDB_EXT_HOST_HAS_INTERNAL(api)) {
        return 3;
    }
    int32_t answer = 0;
    char *text = NULL;
    size_t len = 0;

    const char internal[] = "INSERT INTO pinkdb_agent.messages VALUES (1)";
    int32_t code = api->execute_internal(host_ctx, internal, sizeof(internal) - 1,
                                         &text, &len);
    answer = code;
    if (text != NULL) {
        sink(sink_ctx, 0, text, len);
        api->free(text);
        text = NULL;
        len = 0;
    }

    const char sql[] = "SELECT 2";
    code = api->execute_statement(host_ctx, sql, sizeof(sql) - 1, &text, &len);
    if (answer == 0) {
        answer = code;
    }
    if (text != NULL) {
        sink(sink_ctx, 1, text, len);
        api->free(text);
        text = NULL;
        len = 0;
    }

    if (args != NULL) {
        sink(sink_ctx, 0, args, strlen(args));
    }
    return answer;
}
