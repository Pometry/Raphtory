use std::{fmt::Debug, ops::Deref};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Secret(SecretString);

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct AuthConfig {
    pub secret: Option<Secret>,
    pub open_read_access: bool,
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret() == other.expose_secret()
    }
}

impl Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(|string| string.into())
    }
}

impl Serialize for Secret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("---hidden-secret---")
    }
}

impl Deref for Secret {
    type Target = SecretString;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            secret: None,
            open_read_access: false,
        }
    }
}
