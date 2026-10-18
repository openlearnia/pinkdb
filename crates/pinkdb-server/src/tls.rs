//! TLS transport support (crate feature `tls`): rustls + PEM certificate/key
//! files, no hand-rolled crypto.
//!
//! The default build has none of this: `tls` pulls in rustls (the audited,
//! pure-Rust TLS implementation already used by the repository client and the
//! lake S3 client) with the same `ring` provider those code paths build, plus
//! a small PEM reader. Nothing here reaches for OpenSSL or a system trust
//! store.
//!
//! # Server
//!
//! [`server_config_from_pem`] loads a PEM certificate chain (leaf first) and
//! one PEM private key (PKCS#8, PKCS#1 or SEC1), and checks that the key
//! belongs to the leaf certificate. [`Server::bind_tls`](crate::Server::bind_tls)
//! calls it at startup: unreadable files, non-PEM bytes, and mismatched
//! pairs refuse startup with a message naming the file and the reason.
//!
//! # Client
//!
//! [`client_config`] builds a client that verifies the server certificate
//! against a PEM trust anchor ([`ClientTrust::CaFile`]): for a self-signed
//! server, pass the server's own certificate. There is deliberately no
//! system/public root store in this wave. [`ClientTrust::Insecure`] skips
//! verification entirely — a local-testing escape hatch that the CLI only
//! reaches through its loudly documented `--tls-insecure` flag.
//!
//! # What TLS does and does not change
//!
//! TLS encrypts the transport. It does **not** add client authentication or
//! authorization: the server still serves every client that can complete a
//! handshake, exactly as the plaintext server does. Client certificates are
//! not requested (`with_no_client_auth`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};

/// A TLS configuration could not be built: a missing/unreadable file, PEM
/// that does not parse, an empty certificate chain or key, or a key that does
/// not match the certificate. The message names the file and the reason.
#[derive(Debug)]
pub struct TlsConfigError {
    message: String,
}

impl TlsConfigError {
    fn new(message: impl Into<String>) -> TlsConfigError {
        TlsConfigError {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TlsConfigError {}

/// How a TLS client decides whether to trust the server's certificate.
#[derive(Debug, Clone)]
pub enum ClientTrust {
    /// PEM file holding the trust anchor(s): a CA certificate, or the
    /// self-signed server certificate itself. Every `CERTIFICATE` block in
    /// the file is trusted.
    CaFile(PathBuf),
    /// **DANGEROUS: accepts any certificate.** No identity check at all,
    /// which defeats TLS's point; only for local tests where generating and
    /// distributing a trust anchor is the thing being avoided. The CLI flag
    /// that selects it prints a warning.
    Insecure,
}

/// Load a PEM certificate chain + private key and validate them as a TLS
/// server identity.
pub fn server_config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<Arc<ServerConfig>, TlsConfigError> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();
    let certs = read_certs(cert_path)?;
    let key = read_private_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            TlsConfigError::new(format!(
                "the certificate(s) in {} and the private key in {} are not a usable pair: {e}",
                cert_path.display(),
                key_path.display()
            ))
        })?;
    Ok(Arc::new(config))
}

/// Build a client configuration for `trust`.
pub fn client_config(trust: ClientTrust) -> Result<Arc<ClientConfig>, TlsConfigError> {
    match trust {
        ClientTrust::CaFile(path) => {
            let mut roots = RootCertStore::empty();
            for cert in read_certs(&path)? {
                roots.add(cert).map_err(|e| {
                    TlsConfigError::new(format!(
                        "cannot use a certificate from {} as a trust anchor: {e}",
                        path.display()
                    ))
                })?;
            }
            Ok(Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ))
        }
        ClientTrust::Insecure => Ok(Arc::new(
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))))
                .with_no_client_auth(),
        )),
    }
}

/// Read every `CERTIFICATE` block in `path` as a DER certificate.
fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let bytes = std::fs::read(path).map_err(|e| {
        TlsConfigError::new(format!(
            "cannot read TLS certificate file {}: {e}",
            path.display()
        ))
    })?;
    let mut certs = Vec::new();
    for cert in rustls_pemfile::certs(&mut bytes.as_slice()) {
        let cert = cert.map_err(|e| {
            TlsConfigError::new(format!(
                "cannot parse a PEM certificate in {}: {e}",
                path.display()
            ))
        })?;
        certs.push(cert);
    }
    if certs.is_empty() {
        return Err(TlsConfigError::new(format!(
            "no PEM certificates found in {} (expected at least one CERTIFICATE block)",
            path.display()
        )));
    }
    Ok(certs)
}

/// Read the first private key in `path` (PKCS#8, PKCS#1 or SEC1 PEM).
fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let bytes = std::fs::read(path).map_err(|e| {
        TlsConfigError::new(format!(
            "cannot read TLS private key file {}: {e}",
            path.display()
        ))
    })?;
    let key = rustls_pemfile::private_key(&mut bytes.as_slice()).map_err(|e| {
        TlsConfigError::new(format!(
            "cannot parse the PEM private key in {}: {e}",
            path.display()
        ))
    })?;
    key.ok_or_else(|| {
        TlsConfigError::new(format!(
            "no PEM private key found in {} (expected PRIVATE KEY, RSA PRIVATE KEY or \
             EC PRIVATE KEY)",
            path.display()
        ))
    })
}

/// `--tls-insecure`'s verifier: accept the certificate whatever it says. The
/// signature checks still run against the presented key, so the handshake
/// remains structurally sound; only the identity/validity checks are skipped.
#[derive(Debug)]
struct AcceptAnyServerCert(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
