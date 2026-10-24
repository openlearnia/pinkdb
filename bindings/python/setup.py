"""Build the pinkdb wheel with the C ABI shared library bundled inside it.

A wheel must work without a pinkdb checkout or a locally built library, so the
build runs ``cargo build --release -p pinkdb-ffi`` and copies
``libpinkdb_ffi`` into the ``pinkdb`` package. ``pinkdb._find_library`` looks
next to the package first, so an installed wheel loads its own copy.

Set ``PINKDB_FFI_LIB`` to an existing library to skip the cargo build (used by
CI when it already built one).
"""

import os
import shutil
import subprocess
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.build_py import build_py

REPO_ROOT = Path(__file__).resolve().parents[2]
LIBRARY_NAMES = (
    "libpinkdb_ffi.dylib",
    "libpinkdb_ffi.so",
    "pinkdb_ffi.dll",
)


def build_library() -> Path:
    """Return the FFI shared library, building it when it is not provided."""
    override = os.environ.get("PINKDB_FFI_LIB")
    if override:
        library = Path(override).expanduser().resolve()
        if not library.is_file():
            raise SystemExit(
                f"PINKDB_FFI_LIB={override!r} does not point to an existing file"
            )
        return library

    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
            "-p",
            "pinkdb-ffi",
        ],
        check=True,
    )
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", REPO_ROOT / "target")).resolve()
    for name in LIBRARY_NAMES:
        candidate = target_dir / "release" / name
        if candidate.is_file():
            return candidate
    raise SystemExit(
        f"cargo built pinkdb-ffi but produced no library in {target_dir / 'release'}"
    )


class BuildPyWithLibrary(build_py):
    """Copy the shared library into the package after the .py files."""

    def run(self) -> None:
        super().run()
        library = build_library()
        destination = Path(self.build_lib) / "pinkdb" / library.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, destination)


class PlatformDistribution(Distribution):
    """A wheel with a native library is platform specific, not pure Python."""

    def has_ext_modules(self) -> bool:
        return True

    def is_pure(self) -> bool:
        return False


setup(
    cmdclass={"build_py": BuildPyWithLibrary},
    distclass=PlatformDistribution,
)
