#!/usr/bin/env python3
"""Make a Mach-O library loadable on macOS 27's dyld: align its string table.

macOS 27's dyld refuses a library whose symbol string table does not start on
an 8-byte boundary ("mis-aligned LINKEDIT string pool"). Apple's ld
(ld-27037) and LLD both start that table right after the indirect symbol
table, whose entries are 4 bytes each; with an odd number of indirect symbols
the table starts on a 4-byte boundary and the library cannot be loaded. Which
libraries hit it depends on how many functions they import, so it changes as
the code changes (on 2026-10-30 the release builds of the agent and memory
extensions did, the MCP bridge and the FFI library did not).

This script moves the string table to the next 8-byte boundary and re-signs
the library (ad hoc, as the linker signs it). It changes nothing else, and it
does nothing to a library that is already aligned, so it is safe to run on
every build:

    cargo build --release -p pinkdb-agent
    python3 scripts/macos-align-linkedit.py target/release/libpinkdb_agent.dylib

Only 64-bit little-endian Mach-O files are handled (every macOS target pinkdb
builds); anything else is left untouched with a message.
"""

import os
import struct
import subprocess
import sys

MH_MAGIC_64 = 0xFEEDFACF
LC_SEGMENT_64 = 0x19
LC_SYMTAB = 0x2
LC_CODE_SIGNATURE = 0x1D


def load_commands(data):
    """Yield (offset, cmd, size) for every load command."""
    ncmds = struct.unpack_from("<I", data, 16)[0]
    off = 32
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, off)
        yield off, cmd, size
        off += size


def layout(data):
    """The symtab command's offset and fields, the __LINKEDIT segment's, and
    the code signature's data offset (or None)."""
    symtab = linkedit = signature = None
    for off, cmd, _ in load_commands(data):
        if cmd == LC_SYMTAB:
            symtab = (off,) + struct.unpack_from("<IIII", data, off + 8)
        elif cmd == LC_SEGMENT_64:
            name = data[off + 8:off + 24].rstrip(b"\0")
            if name == b"__LINKEDIT":
                linkedit = (off,) + struct.unpack_from("<QQQQ", data, off + 24)
        elif cmd == LC_CODE_SIGNATURE:
            signature = struct.unpack_from("<I", data, off + 8)[0]
    return symtab, linkedit, signature


def is_macho64(data):
    return len(data) >= 32 and struct.unpack_from("<I", data, 0)[0] == MH_MAGIC_64


def codesign(args, path):
    subprocess.run(["codesign", *args, path], check=True, capture_output=True)


def align(path):
    """Align `path`'s string table in place. Returns a one-line report."""
    with open(path, "rb") as f:
        data = bytearray(f.read())
    if not is_macho64(data):
        return f"{path}: not a 64-bit Mach-O file, left as is"
    symtab, linkedit, signature = layout(data)
    if symtab is None or linkedit is None:
        return f"{path}: no symbol table, left as is"
    _, _, _, stroff, strsize = symtab
    if stroff % 8 == 0:
        return f"{path}: already aligned"
    pad = 8 - stroff % 8
    end = stroff + strsize
    # The common case: the linker leaves a gap before the code signature
    # (which starts 16-byte aligned), and the table moves into it.
    room = (signature if signature is not None else len(data)) - end
    if room < pad:
        # No gap: drop the signature, grow the file, and sign again below.
        if signature is not None:
            codesign(["--remove-signature"], path)
            with open(path, "rb") as f:
                data = bytearray(f.read())
            symtab, linkedit, signature = layout(data)
            _, _, _, stroff, strsize = symtab
            end = stroff + strsize
        if len(data) - end < pad:
            data.extend(b"\0" * (pad - (len(data) - end)))
        seg_off, _, vmsize, fileoff, filesize = linkedit
        new_filesize = len(data) - fileoff
        struct.pack_into("<Q", data, seg_off + 24 + 24, new_filesize)
        if new_filesize > vmsize:
            page = 0x4000
            struct.pack_into("<Q", data, seg_off + 24 + 8, (new_filesize + page - 1) // page * page)
    symtab_off = symtab[0]
    new = stroff + pad
    data[new:new + strsize] = data[stroff:stroff + strsize]
    data[stroff:new] = b"\0" * pad
    struct.pack_into("<I", data, symtab_off + 16, new)
    with open(path, "wb") as f:
        f.write(data)
    # The bytes under the signature changed: sign again (ad hoc, like ld).
    codesign(["--force", "--sign", "-"], path)
    return f"{path}: string table moved {stroff} -> {new} and re-signed"


def main(argv):
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        sys.stdout.write(__doc__)
        return 0 if len(argv) >= 2 else 2
    if sys.platform != "darwin":
        print("not macOS: nothing to do")
        return 0
    status = 0
    for path in argv[1:]:
        if not os.path.isfile(path):
            print(f"{path}: no such file", file=sys.stderr)
            status = 1
            continue
        print(align(path))
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv))
