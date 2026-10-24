/*
 * An extension that uses the ABI minor 2 capabilities: it registers nothing,
 * exports pinkdb_ext_serve, and on serve asks the host to run one statement,
 * writing the host's answer to the file named by `options` (the tests read
 * it back). Returning straight away also covers "the loop ends when the
 * client disconnects" from the host's side: a serve call is just a call.
 */
#include "pinkdb_extension.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

uint32_t pinkdb_extension_abi_version(void) {
    return PINKDB_EXT_ABI_VERSION;
}

const char *pinkdb_extension_name(void) {
    return "serve_ext";
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    (void)host_ctx;
    if (api == NULL || api->abi_version != PINKDB_EXT_ABI_VERSION) {
        return 1;
    }
    return 0;
}

int32_t pinkdb_ext_serve(const PinkdbHostApi *api, void *host_ctx,
                         const char *transport, const char *options) {
    if (api == NULL) {
        return 1;
    }
    char *text = NULL;
    size_t len = 0;
    /* Exactly the bytes the caller tests: no NUL terminator required. */
    const char sql[] = "SELECT 1";
    int32_t code = api->execute_statement(host_ctx, sql, sizeof(sql) - 1, &text,
                                          &len);
    if (options == NULL || *options == '\0') {
        if (text != NULL) {
            api->free(text);
        }
        return code == 1 ? 1 : 0;
    }
    FILE *out = fopen(options, "w");
    if (out == NULL) {
        if (text != NULL) {
            api->free(text);
        }
        return 1;
    }
    fprintf(out, "transport=%s\ncode=%d\n",
            transport == NULL ? "(null)" : transport, (int)code);
    if (text != NULL) {
        fwrite(text, 1, len, out);
    }
    fputc('\n', out);
    fclose(out);
    /* The host allocated the text; the extension frees it exactly once. */
    if (text != NULL) {
        api->free(text);
    }
    return 0;
}
