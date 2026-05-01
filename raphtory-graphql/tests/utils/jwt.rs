use std::sync::LazyLock;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;

// Borrowed from test_permissions.py.
pub const PUB_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

pub static ADMIN_JWT: LazyLock<String> = LazyLock::new(|| {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");

    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "rw", "role": "admin" }),
        &key,
    )
    .expect("encode admin JWT")
});

#[allow(dead_code)]
pub fn user_jwt(role: &str) -> String {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");

    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "ro", "role": role }),
        &key,
    )
    .expect("encode user JWT")
}
