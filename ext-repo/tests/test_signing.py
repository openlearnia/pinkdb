#!/usr/bin/env python3
"""Signing tests for ext-repo/publish.py and ext-repo/verify.sh.

Run from the repository root:

    python3 -m unittest discover -s ext-repo/tests -v

The tests generate a throwaway Ed25519 key with OpenSSL in a temporary
directory and put a fake `wrangler` on PATH, so nothing touches the network
or the real extension repository. The private key is never printed.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PUBLISH = ROOT / "ext-repo" / "publish.py"
VERIFY = ROOT / "ext-repo" / "verify.sh"
DEVNULL = subprocess.DEVNULL


def openssl(*args: str, **kwargs) -> bytes:
    return subprocess.run(
        ["openssl", *args], check=True, stdout=subprocess.PIPE, stderr=DEVNULL, **kwargs
    ).stdout


def make_key(path: Path) -> None:
    openssl("genpkey", "-algorithm", "ED25519", "-out", str(path))
    os.chmod(path, 0o600)


def public_pem(key: Path, path: Path) -> None:
    openssl("pkey", "-in", str(key), "-pubout", "-out", str(path))


def key_id(public_pem_path: Path) -> str:
    der = openssl("pkey", "-pubin", "-in", str(public_pem_path), "-outform", "DER")
    return hashlib.sha256(der).hexdigest()[:16]


def sign_index(index: Path, key: Path, signature_out: Path) -> None:
    openssl(
        "pkeyutl",
        "-sign",
        "-rawin",
        "-inkey",
        str(key),
        "-in",
        str(index),
        "-out",
        str(signature_out),
    )


def verify_signature(index: Path, sig_bin: Path, public_pem_path: Path) -> None:
    subprocess.run(
        [
            "openssl",
            "pkeyutl",
            "-verify",
            "-rawin",
            "-pubin",
            "-inkey",
            str(public_pem_path),
            "-in",
            str(index),
            "-sigfile",
            str(sig_bin),
        ],
        check=True,
        stdout=DEVNULL,
        stderr=DEVNULL,
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fake_wrangler(bin_dir: Path, log_path: Path) -> None:
    script = bin_dir / "wrangler"
    script.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> "{log_path}"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    os.chmod(script, 0o755)


def parse_envelope(sig_path: Path) -> tuple[str, bytes]:
    fields = sig_path.read_text(encoding="ascii").strip().split()
    if len(fields) != 3:
        raise AssertionError(f"bad envelope: {fields!r}")
    if fields[0] != "PINKDB-SIG1":
        raise AssertionError(f"bad envelope prefix: {fields[0]!r}")
    return fields[1], base64.b64decode(fields[2], validate=True)


class PublishSigningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pinkdb-publish-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        self.log = self.tmp / "wrangler.log"
        fake_wrangler(self.bin, self.log)
        self.key = self.tmp / "key.pem"
        make_key(self.key)
        self.pub = self.tmp / "pub.pem"
        public_pem(self.key, self.pub)
        self.artifact = self.tmp / "test_ext.dylib"
        self.artifact.write_bytes(b"fake extension bytes for signing tests")

    def publish(self, signing_key: str | None) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PATH"] = f"{self.bin}{os.pathsep}{env.get('PATH', '')}"
        env["WRANGLER_LOG"] = str(self.log)
        env.pop("PINKDB_EXT_SIGNING_KEY", None)
        env.pop("PINKDB_EXT_ALLOW_UNSIGNED", None)
        if signing_key is not None:
            env["PINKDB_EXT_SIGNING_KEY"] = signing_key
        return subprocess.run(
            [
                sys.executable,
                str(PUBLISH),
                str(self.artifact),
                "--name",
                "test_ext",
                "--version",
                "0.1.0",
                "--platform",
                "darwin-arm64",
                "--description",
                "signing test",
                "--repo-dir",
                str(self.repo),
                "--skip-live-check",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def assert_signed(self) -> tuple[str, bytes]:
        proc = self.publish(str(self.key))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        index = self.repo / "index.json"
        sig_path = self.repo / "index.json.sig"
        self.assertTrue(index.is_file())
        self.assertTrue(sig_path.is_file())
        envelope_key_id, signature = parse_envelope(sig_path)
        self.assertEqual(len(signature), 64)
        self.assertEqual(envelope_key_id, key_id(self.pub))
        sig_bin = self.tmp / "sig.bin"
        sig_bin.write_bytes(signature)
        verify_signature(index, sig_bin, self.pub)
        return envelope_key_id, signature

    def test_signs_the_exact_index_bytes_with_a_key_path(self) -> None:
        envelope_key_id, _ = self.assert_signed()
        self.assertEqual(envelope_key_id, key_id(self.pub))

        uploads = self.log.read_text(encoding="utf-8").splitlines()
        artifact_at = next(
            i for i, line in enumerate(uploads) if "/artifacts/" in line
        )
        signature_at = next(
            i for i, line in enumerate(uploads) if "pinkdb-extensions/index.json.sig" in line
        )
        catalog_at = next(
            i
            for i, line in enumerate(uploads)
            if "pinkdb-extensions/index.json --file" in line
        )
        self.assertLess(artifact_at, catalog_at)
        self.assertLess(signature_at, catalog_at)

    def test_accepts_base64_encoded_pem_content(self) -> None:
        encoded = base64.b64encode(self.key.read_bytes()).decode("ascii")
        proc = self.publish(encoded)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        sig_path = self.repo / "index.json.sig"
        self.assertTrue(sig_path.is_file())
        envelope_key_id, signature = parse_envelope(sig_path)
        self.assertEqual(envelope_key_id, key_id(self.pub))
        sig_bin = self.tmp / "sig.bin"
        sig_bin.write_bytes(signature)
        verify_signature(self.repo / "index.json", sig_bin, self.pub)

    def test_accepts_inline_pem_content(self) -> None:
        proc = self.publish(self.key.read_text(encoding="ascii"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        sig_path = self.repo / "index.json.sig"
        _, signature = parse_envelope(sig_path)
        sig_bin = self.tmp / "sig.bin"
        sig_bin.write_bytes(signature)
        verify_signature(self.repo / "index.json", sig_bin, self.pub)

    def test_without_the_key_stays_unsigned_with_a_loud_warning(self) -> None:
        stale = self.repo / "index.json.sig"
        stale.write_text("stale\n", encoding="ascii")
        proc = self.publish(None)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("UNSIGNED", proc.stderr)
        self.assertIn("WILL REFUSE", proc.stderr)
        self.assertFalse(stale.exists(), "a stale local signature must not survive")
        uploads = self.log.read_text(encoding="utf-8")
        self.assertNotIn("index.json.sig", uploads)
        self.assertIn("pinkdb-extensions/index.json --file", uploads)

    def test_malformed_key_material_is_refused_before_upload(self) -> None:
        proc = self.publish("not a key path or base64")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("PINKDB_EXT_SIGNING_KEY", proc.stderr)
        self.assertFalse(self.log.exists(), "nothing may be uploaded")
        self.assertFalse((self.repo / "index.json").exists())

    def test_a_tampered_index_does_not_verify(self) -> None:
        self.assert_signed()
        index = self.repo / "index.json"
        index.write_bytes(index.read_bytes() + b"\n")
        sig_bin = self.tmp / "sig.bin"
        _, signature = parse_envelope(self.repo / "index.json.sig")
        sig_bin.write_bytes(signature)
        with self.assertRaises(subprocess.CalledProcessError):
            verify_signature(index, sig_bin, self.pub)


class VerifyShTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pinkdb-verify-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        self.key = self.tmp / "key.pem"
        make_key(self.key)
        self.pub = self.tmp / "pub.pem"
        public_pem(self.key, self.pub)
        self.index = self.repo / "index.json"
        self.sig = self.repo / "index.json.sig"
        self.artifact = self.repo / "artifacts/test_ext/0.1.0/darwin-arm64/test_ext.dylib"
        self.artifact.parent.mkdir(parents=True)
        self.artifact.write_bytes(b"the artifact bytes")
        self.write_catalog()

    def write_catalog(self) -> None:
        digest = sha256_file(self.artifact)
        catalog = {
            "format_version": 1,
            "updated": "2026-10-20T00:00:00Z",
            "extensions": [
                {
                    "name": "test_ext",
                    "version": "0.1.0",
                    "abi": 1,
                    "platform": "darwin-arm64",
                    "file": "artifacts/test_ext/0.1.0/darwin-arm64/test_ext.dylib",
                    "sha256": digest,
                    "size": self.artifact.stat().st_size,
                    "description": "verify test",
                }
            ],
        }
        self.index.write_text(json.dumps(catalog, indent=2) + "\n", encoding="utf-8")
        sig_bin = self.tmp / "raw.sig"
        sign_index(self.index, self.key, sig_bin)
        self.sig.write_text(
            f"PINKDB-SIG1 {key_id(self.pub)} "
            f"{base64.b64encode(sig_bin.read_bytes()).decode('ascii')}\n",
            encoding="ascii",
        )

    def verify(self, *extra: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "bash",
                str(VERIFY),
                "--index",
                str(self.index),
                "--sig",
                str(self.sig),
                "--pubkey",
                str(self.pub),
                *extra,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def test_positive_local_verification(self) -> None:
        proc = self.verify()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("catalog signature", proc.stdout)
        self.assertIn("sha256", proc.stdout)
        self.assertIn("checked 1 artifact(s), 0 failure(s)", proc.stdout)

    def test_tampered_artifact_fails(self) -> None:
        self.artifact.write_bytes(b"tampered")
        proc = self.verify()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("sha256 mismatch", proc.stderr)

    def test_tampered_index_fails(self) -> None:
        self.index.write_bytes(self.index.read_bytes() + b"\n")
        proc = self.verify()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("does not verify", proc.stderr)

    def test_missing_artifact_fails(self) -> None:
        self.artifact.unlink()
        proc = self.verify()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("artifact missing", proc.stderr)

    def test_missing_signature_fails(self) -> None:
        self.sig.unlink()
        proc = self.verify()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("signature not found", proc.stderr)

    def test_wrong_public_key_fails(self) -> None:
        other = self.tmp / "other.pem"
        other_key = self.tmp / "other-key.pem"
        make_key(other_key)
        public_pem(other_key, other)
        proc = subprocess.run(
            [
                "bash",
                str(VERIFY),
                "--index",
                str(self.index),
                "--sig",
                str(self.sig),
                "--pubkey",
                str(other),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("does not match pinned key", proc.stderr)

    def test_skip_artifacts_still_verifies_the_catalog(self) -> None:
        self.artifact.unlink()
        proc = self.verify("--skip-artifacts")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("skipped artifact verification", proc.stdout)

    def test_remote_mode_verifies_catalog_and_artifact(self) -> None:
        class QuietHandler(SimpleHTTPRequestHandler):
            def log_message(self, *args) -> None:  # no request noise in test output
                pass

        handler = lambda *args, **kwargs: QuietHandler(  # noqa: E731
            *args, directory=str(self.repo), **kwargs
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        url = (
            f"http://127.0.0.1:{server.server_port}/artifacts/test_ext/0.1.0/"
            "darwin-arm64/test_ext.dylib"
        )
        proc = subprocess.run(
            [
                "bash",
                str(VERIFY),
                url,
                f"http://127.0.0.1:{server.server_port}/index.json?ts=1",
                "--pubkey",
                str(self.pub),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("catalog signature", proc.stdout)
        self.assertIn("sha256", proc.stdout)

        # An unsigned remote catalog is refused before the artifact download.
        self.sig.unlink()
        proc = subprocess.run(
            ["bash", str(VERIFY), url, "--pubkey", str(self.pub)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("index.json.sig", proc.stderr)


if __name__ == "__main__":
    unittest.main()
