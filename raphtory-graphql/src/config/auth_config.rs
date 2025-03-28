use std::fmt::Debug;

use serde::Deserialize;

pub const DEFAULT_CAPACITY: u64 = 30;
pub const DEFAULT_TTI_SECONDS: u64 = 900;

#[derive(Deserialize, PartialEq, Clone, serde::Serialize)]
pub struct AuthConfig {
    pub secret: Option<String>,
    pub require_read_permissions: bool,
}

// implemented manually to avoid leaking the secret // TODO: Try to use secrecy crate for this?
impl Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            // deestructured in this way so i doesn't compile if the fields change
            secret,
            require_read_permissions,
        } = &self;
        let secret = secret.as_ref().map(|_| "...".to_owned());
        f.debug_struct("AuthConfig")
            .field("secret", &secret)
            .field("require_read_permissions", require_read_permissions)
            .finish()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            secret: None,
            require_read_permissions: false,
        }
    }
}
