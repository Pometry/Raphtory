use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use jsonwebtoken::{Algorithm, DecodingKey};
use serde::{de, Deserialize, Deserializer, Serialize};
use spki::SubjectPublicKeyInfoRef;
use std::fmt::Debug;

pub const DEFAULT_REQUIRE_AUTH_FOR_READS: bool = true;
pub const PUBLIC_KEY_DECODING_ERR_MSG: &str =
    "Could not decode public key. Provide a base64-encoded DER (X.509 SPKI) public key \
     for Ed25519 or RSA (2048-4096 bit).";

/// Describes one family of asymmetric public-key algorithms that Raphtory can validate JWTs with.
///
/// To add support for a new algorithm family (e.g. EC/ECDSA), append one entry to
/// [`SUPPORTED_ALGORITHMS`] — no other code needs to change.
struct AlgorithmSpec {
    /// X.509 SPKI algorithm OID string (e.g. `"1.3.101.112"` for Ed25519).
    oid: &'static str,
    /// Constructs the `DecodingKey` from the raw subject-public-key bytes extracted from the SPKI
    /// structure (i.e. the inner key bytes, not the full DER-encoded SPKI wrapper).
    make_key: fn(&[u8]) -> DecodingKey,
    /// JWT algorithms accepted for this key family. All listed variants are allowed during
    /// validation; the first entry is used as the `Validation` default.
    algorithms: &'static [Algorithm],
}

/// Registry of supported public-key algorithm families.
///
/// # Adding a new family
/// Append an [`AlgorithmSpec`] entry here. `TryFrom<String> for PublicKey` will pick it up
/// automatically — no other changes required.
const SUPPORTED_ALGORITHMS: &[AlgorithmSpec] = &[
    AlgorithmSpec {
        oid: "1.3.101.112", // id-EdDSA (Ed25519)
        make_key: DecodingKey::from_ed_der,
        algorithms: &[Algorithm::EdDSA],
    },
    AlgorithmSpec {
        oid: "1.2.840.113549.1.1.1", // rsaEncryption (PKCS#1)
        make_key: DecodingKey::from_rsa_der,
        algorithms: &[Algorithm::RS256, Algorithm::RS384, Algorithm::RS512],
    },
];

#[derive(Clone)]
pub struct PublicKey {
    source: String,
    pub(crate) decoding_key: DecodingKey,
    pub(crate) algorithms: Vec<Algorithm>,
}

impl PartialEq for PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.source.eq(&other.source)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PublicKeyError {
    #[error(transparent)]
    Base64(#[from] DecodeError),
    #[error("The provided key is not a valid X.509 Subject Public Key Info ASN.1 structure")]
    Spki,
    #[error("Key algorithm is not supported; see SUPPORTED_ALGORITHMS for accepted OIDs")]
    UnsupportedAlgorithm,
}

impl TryFrom<String> for PublicKey {
    type Error = PublicKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let der = BASE64_STANDARD.decode(&value)?;
        let spki_ref =
            SubjectPublicKeyInfoRef::try_from(der.as_ref()).map_err(|_| PublicKeyError::Spki)?;
        let oid = spki_ref.algorithm.oid.to_string();
        let spec = SUPPORTED_ALGORITHMS
            .iter()
            .find(|s| s.oid == oid.as_str())
            .ok_or(PublicKeyError::UnsupportedAlgorithm)?;
        let raw = spki_ref.subject_public_key.raw_bytes();
        Ok(Self {
            source: value,
            decoding_key: (spec.make_key)(raw),
            algorithms: spec.algorithms.to_vec(),
        })
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        Self::try_from(string).map_err(de::Error::custom)
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.source.serialize(serializer)
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "---hidden-public-key---".fmt(f)
    }
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct AuthConfig {
    pub public_key: Option<PublicKey>,
    pub require_auth_for_reads: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            public_key: None,
            require_auth_for_reads: DEFAULT_REQUIRE_AUTH_FOR_READS,
        }
    }
}
