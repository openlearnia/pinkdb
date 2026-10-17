//! AWS Signature Version 4 (header authentication) for the S3 client.
//!
//! A dependency-light implementation of the documented algorithm
//! (`AWS4-HMAC-SHA256`, `hmac` + `sha2` only — no aws-sdk, no tokio):
//! canonical request → string to sign → HMAC-SHA256 signing-key chain →
//! `Authorization: AWS4-HMAC-SHA256 Credential=…, SignedHeaders=…,
//! Signature=…`.
//!
//! The caller provides the method, the already-percent-encoded canonical URI
//! and query, the exact header set to sign, the payload hash, the
//! timestamp pair, and the region/service. S3 requires the
//! `x-amz-content-sha256` header and hosts sign `host`; the client adds
//! both. The signer is deterministic given those inputs, which is what the
//! unit tests pin against published AWS SigV4 test vectors.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::time::SystemTime;

use crate::time::amz_date_utc;

type HmacSha256 = Hmac<Sha256>;

/// Long-lived S3 credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
/// optional `AWS_SESSION_TOKEN`).
/// `Debug` redacts the secret access key and the session token.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Credentials {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// One request to sign, with everything SigV4 hashes already in canonical
/// form.
///
/// `headers` must contain exactly the headers the caller sends **and wants
/// signed** (lowercase names; the signer sorts them). The client always
/// includes `host`, `x-amz-date` and `x-amz-content-sha256`, plus
/// `x-amz-security-token` when configured and the conditional write headers
/// (`if-match` / `if-none-match`) so a precondition cannot be tampered with
/// in flight.
#[derive(Debug)]
pub(crate) struct SigningInput<'a> {
    pub(crate) method: &'a str,
    /// Percent-encoded path, starting with `/`.
    pub(crate) canonical_uri: &'a str,
    /// Percent-encoded, key-sorted query (no leading `?`; empty if none).
    pub(crate) canonical_query: &'a str,
    pub(crate) headers: Vec<(&'a str, &'a str)>,
    /// Lowercase hex SHA-256 of the request body (the empty-body hash for
    /// GET/HEAD/DELETE).
    pub(crate) payload_hash: &'a str,
    /// `20150830T123600Z`.
    pub(crate) amz_date: &'a str,
    /// `20150830`.
    pub(crate) date_stamp: &'a str,
    pub(crate) region: &'a str,
    pub(crate) service: &'a str,
}

/// The signed request's authentication material (and the intermediates, so
/// tests can pin the exact canonical request and string to sign).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Signature {
    /// `x-amz-date` value.
    pub(crate) amz_date: String,
    /// The value of the `Authorization` header.
    pub(crate) authorization: String,
    pub(crate) canonical_request: String,
    pub(crate) string_to_sign: String,
    pub(crate) signed_headers: String,
    pub(crate) signature: String,
}

/// SigV4 canonicalization of a header value: trim, and collapse each run of
/// spaces/tabs to a single space (AWS's rule; a signed double space would
/// otherwise `SignatureDoesNotMatch`).
fn canonical_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.trim().chars() {
        if c == ' ' || c == '\t' {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(c);
            in_space = false;
        }
    }
    out
}

/// Sign one request with `credentials`.
pub(crate) fn sign(credentials: &Credentials, input: &SigningInput<'_>) -> Signature {
    let mut headers: Vec<(String, String)> = input
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), canonical_value(value)))
        .collect();
    if let Some(token) = &credentials.session_token {
        // The token is part of the request; make sure it is signed even when
        // the caller forgot to list it.
        if !headers
            .iter()
            .any(|(name, _)| name == "x-amz-security-token")
        {
            headers.push(("x-amz-security-token".to_string(), canonical_value(token)));
        }
    }
    headers.sort();
    headers.dedup_by(|a, b| a.0 == b.0);

    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        input.method,
        input.canonical_uri,
        input.canonical_query,
        canonical_headers,
        signed_headers,
        input.payload_hash
    );
    let scope = format!(
        "{}/{}/{}/aws4_request",
        input.date_stamp, input.region, input.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        input.amz_date,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signature = hex(&hmac_sha256(
        &signing_key(
            &credentials.secret_access_key,
            input.date_stamp,
            input.region,
            input.service,
        ),
        string_to_sign.as_bytes(),
    ));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
         Signature={signature}",
        credentials.access_key_id
    );
    Signature {
        amz_date: input.amz_date.to_string(),
        authorization,
        canonical_request,
        string_to_sign,
        signed_headers,
        signature,
    }
}

/// The AWS4 signing key: `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region),
/// service), "aws4_request")`.
fn signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> [u8; 32] {
    let mut key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    key = hmac_sha256(&key, region.as_bytes());
    key = hmac_sha256(&key, service.as_bytes());
    hmac_sha256(&key, b"aws4_request")
}

/// HMAC-SHA256 (HMAC accepts keys of any length).
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Lowercase hex.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xF) as usize] as char);
    }
    out
}

/// The current timestamp pair for signing (`x-amz-date`, date stamp).
pub(crate) fn now_amz_date() -> (String, String) {
    amz_date_utc(SystemTime::now())
}

/// Lowercase hex SHA-256 of `bytes` (the `x-amz-content-sha256` value).
pub(crate) fn payload_hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    /// The AWS SigV4 test-suite constants: this key pair is public and only
    /// meaningful for the published vectors.
    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn creds() -> Credentials {
        Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
            session_token: None,
        }
    }

    fn at(secs: u64) -> (String, String) {
        amz_date_utc(UNIX_EPOCH + Duration::from_secs(secs))
    }

    #[test]
    fn canonical_header_values_collapse_internal_whitespace() {
        assert_eq!(canonical_value("  a   b\t c  "), "a b c");
        assert_eq!(canonical_value("plain"), "plain");
        assert_eq!(canonical_value("   "), "");
    }

    /// AWS SigV4 test suite `get-vanilla`: the plainest request there is, in
    /// the `service`/`us-east-1` suite (headers `host` + `x-amz-date` only,
    /// so this pins the bare algorithm).
    #[test]
    fn aws_test_vector_get_vanilla() {
        let (amz_date, date_stamp) = at(1_440_938_160); // 2015-08-30T12:36:00Z
        let signed = sign(
            &creds(),
            &SigningInput {
                method: "GET",
                canonical_uri: "/",
                canonical_query: "",
                headers: vec![("host", "example.amazonaws.com"), ("x-amz-date", &amz_date)],
                payload_hash: EMPTY_HASH,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: "us-east-1",
                service: "service",
            },
        );
        assert_eq!(
            signed.canonical_request,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
             host;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            signed.string_to_sign,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n\
             20150830/us-east-1/service/aws4_request\n\
             bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        assert_eq!(
            signed.signature,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// AWS SigV4 test suite `get-vanilla-query-order-key-case`: query keys
    /// are sorted case-sensitively in the canonical request.
    #[test]
    fn aws_test_vector_query_order_key_case() {
        let (amz_date, date_stamp) = at(1_440_938_160);
        let signed = sign(
            &creds(),
            &SigningInput {
                method: "GET",
                canonical_uri: "/",
                canonical_query: "Param1=value1&Param2=value2",
                headers: vec![("host", "example.amazonaws.com"), ("x-amz-date", &amz_date)],
                payload_hash: EMPTY_HASH,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: "us-east-1",
                service: "service",
            },
        );
        assert_eq!(
            signed.canonical_request.lines().nth(2).unwrap(),
            "Param1=value1&Param2=value2"
        );
        assert_eq!(
            signed.signature,
            "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
        );
    }

    /// A realistic conditional catalog `PUT` over S3 (virtual-hosted host,
    /// `If-Match: "…"`, `x-amz-content-sha256`) signed with the public test
    /// credentials; the expected values were generated with an independent
    /// SigV4 implementation (the same four-step algorithm).
    #[test]
    fn conditional_put_vector() {
        let (amz_date, date_stamp) = at(1_791_972_672); // 2026-10-14T10:11:12Z
        assert_eq!(amz_date, "20261014T101112Z");
        let body = br#"{"format_version":2,"version":2,"tables":{}}"#;
        let hash = payload_hash(body);
        assert_eq!(
            hash,
            "4cdbdc97901ade291d828daee607eb93b01b68f8a10c478b9f73dd9e6d5967a5"
        );
        let signed = sign(
            &creds(),
            &SigningInput {
                method: "PUT",
                canonical_uri: "/lakes/mine/catalog.json",
                canonical_query: "",
                headers: vec![
                    ("host", "mybucket.s3.us-east-1.amazonaws.com"),
                    ("if-match", "\"9f86d081884c7d659a2feaa0c55ad015\""),
                    ("x-amz-content-sha256", &hash),
                    ("x-amz-date", &amz_date),
                ],
                payload_hash: &hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: "us-east-1",
                service: "s3",
            },
        );
        assert_eq!(
            signed.canonical_request,
            "PUT\n/lakes/mine/catalog.json\n\n\
             host:mybucket.s3.us-east-1.amazonaws.com\n\
             if-match:\"9f86d081884c7d659a2feaa0c55ad015\"\n\
             x-amz-content-sha256:4cdbdc97901ade291d828daee607eb93b01b68f8a10c478b9f73dd9e6d5967a5\n\
             x-amz-date:20261014T101112Z\n\n\
             host;if-match;x-amz-content-sha256;x-amz-date\n\
             4cdbdc97901ade291d828daee607eb93b01b68f8a10c478b9f73dd9e6d5967a5"
        );
        assert_eq!(
            signed.string_to_sign,
            "AWS4-HMAC-SHA256\n20261014T101112Z\n20261014/us-east-1/s3/aws4_request\n\
             75892741b70bd0c37ebd2aefd5580742722bd1d8f65c9f90dfd5e87572aa8aa9"
        );
        assert_eq!(
            signed.signature,
            "fde843e5cc49606d8df99c7dd7bee0787dc948a3a899a5c073da775ed8f253cf"
        );
    }

    /// A `ListObjectsV2` against a custom endpoint (`host:port`, region
    /// `auto` — the R2 configuration): percent-encoded query values stay
    /// encoded, the port is part of `host`.
    #[test]
    fn list_objects_vector_with_custom_endpoint() {
        let (amz_date, date_stamp) = at(1_791_972_672);
        let signed = sign(
            &creds(),
            &SigningInput {
                method: "GET",
                canonical_uri: "/lakebucket/lakes/mine/",
                canonical_query: "delimiter=%2F&list-type=2&prefix=lakes%2Fmine%2Fsnapshots%2F",
                headers: vec![
                    ("host", "127.0.0.1:9000"),
                    ("x-amz-content-sha256", EMPTY_HASH),
                    ("x-amz-date", &amz_date),
                ],
                payload_hash: EMPTY_HASH,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: "auto",
                service: "s3",
            },
        );
        assert_eq!(
            signed.string_to_sign,
            "AWS4-HMAC-SHA256\n20261014T101112Z\n20261014/auto/s3/aws4_request\n\
             428be35619b0d4d5af9202e842c9df9ac488e62fbf4c6b27c110baab60202d0b"
        );
        assert_eq!(
            signed.signature,
            "66d0130a7f8821406df291697a6d5deb787855462dcf57e43d8d9da8784cf132"
        );
    }

    /// A session token is signed (`x-amz-security-token`) even when the
    /// caller leaves it out of `headers`, and a percent sign in the object
    /// key is percent-encoded once more for the URI (`%252F`).
    #[test]
    fn session_token_is_signed_and_keys_are_encoded() {
        let (amz_date, date_stamp) = at(1_791_972_672);
        let mut credentials = creds();
        credentials.session_token = Some("TOKEN+/=".to_string());
        let body = b"vortex-bytes";
        let hash = payload_hash(body);
        let signed = sign(
            &credentials,
            &SigningInput {
                method: "PUT",
                canonical_uri: "/lakebucket/lakes/mine/data/main.a%252Fb/0123abcd.vortex",
                canonical_query: "",
                headers: vec![
                    ("host", "127.0.0.1:9000"),
                    ("x-amz-content-sha256", &hash),
                    ("x-amz-date", &amz_date),
                ],
                payload_hash: &hash,
                amz_date: &amz_date,
                date_stamp: &date_stamp,
                region: "auto",
                service: "s3",
            },
        );
        assert_eq!(
            signed.canonical_request,
            "PUT\n/lakebucket/lakes/mine/data/main.a%252Fb/0123abcd.vortex\n\n\
             host:127.0.0.1:9000\n\
             x-amz-content-sha256:10e8115208c795e82cb18de3889b990b4dd6e7146b6789a9ca4d8f0d5494574d\n\
             x-amz-date:20261014T101112Z\n\
             x-amz-security-token:TOKEN+/=\n\n\
             host;x-amz-content-sha256;x-amz-date;x-amz-security-token\n\
             10e8115208c795e82cb18de3889b990b4dd6e7146b6789a9ca4d8f0d5494574d"
        );
        assert_eq!(
            signed.signature,
            "2eea23231c52dbb56e4e9f00c21575d2c96f1210bb38037a8c52460c86ef8ede"
        );
    }

    /// The empty-body hash is the documented constant (S3 requires this
    /// value in `x-amz-content-sha256` for bodyless requests).
    #[test]
    fn empty_body_hash_is_the_documented_constant() {
        assert_eq!(payload_hash(b""), EMPTY_HASH);
        assert_eq!(payload_hash(b"abc").len(), 64);
    }
}
