use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use jsonwebtoken::DecodingKey;
use serde::{de, Deserialize, Deserializer, Serialize};
use spki::SubjectPublicKeyInfoRef;
use std::fmt::Debug;

pub const DEFAULT_AUTH_ENABLED_FOR_READS: bool = true;
pub const PUBLIC_KEY_DECODING_ERR_MSG: &str = "Could not successfully decode the public key. Make sure you use the standard alphabet with padding";

#[derive(Clone)]
pub struct PublicKey {
    source: String,
    pub(crate) decoding_key: DecodingKey,
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
    #[error("The provided key is not a a valid X.509 Subject Public Key Info ASN.1 structure")]
    Spki,
}

impl TryFrom<String> for PublicKey {
    type Error = PublicKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let der = BASE64_STANDARD.decode(&value)?;
        let spki_ref =
            SubjectPublicKeyInfoRef::try_from(der.as_ref()).map_err(|_| PublicKeyError::Spki)?;
        let decoding_key = DecodingKey::from_ed_der(spki_ref.subject_public_key.raw_bytes());
        Ok(Self {
            source: value,
            decoding_key,
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
    pub enabled_for_reads: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            public_key: None,
            enabled_for_reads: DEFAULT_AUTH_ENABLED_FOR_READS,
        }
    }
}
