use base64_compat::{decode_config, URL_SAFE_NO_PAD};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AuthorizationCode, CsrfToken,
    PkceCodeChallenge, PkceCodeVerifier, Scope, TokenResponse,
};
use poem::{
    handler,
    http::StatusCode,
    web::{
        cookie::{Cookie, CookieJar},
        Data, Json, Query, Redirect,
    },
    IntoResponse, Response,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    env,
    error::Error,
    sync::{Arc, Mutex},
};

#[derive(Deserialize, Serialize)]
struct AuthRequest {
    code: String,
    state: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Jwks {
    pub(crate) keys: Vec<Jwk>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Jwk {
    pub(crate) kid: String,
    kty: String,
    #[serde(rename = "use")]
    use_: String,
    pub(crate) n: String,
    pub(crate) e: String,
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) oauth_client: Arc<BasicClient>,
    pub(crate) csrf_state: Arc<Mutex<HashMap<String, CsrfToken>>>,
    pub(crate) pkce_verifier: Arc<Mutex<HashMap<String, PkceCodeVerifier>>>,
    pub(crate) jwks: Arc<Jwks>,
}

#[handler]
pub async fn login(data: Data<&AppState>, jar: &CookieJar) -> Redirect {
    let session_id = uuid::Uuid::new_v4().to_string();
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (authorize_url, csrf_state) = data
        .oauth_client
        .authorize_url(CsrfToken::new_random)
        .set_pkce_challenge(pkce_challenge)
        .add_scope(Scope::new("openid".to_string()))
        .add_scope(Scope::new("email".to_string()))
        .add_scope(Scope::new("offline_access".to_string()))
        .add_scope(Scope::new(
            "a10e734e-cb36-46ca-bbfd-c298e15b6327/public-scope".to_string(),
        ))
        .url();

    data.csrf_state
        .lock()
        .unwrap()
        .insert(session_id.clone(), csrf_state);
    data.pkce_verifier
        .lock()
        .unwrap()
        .insert(session_id.clone(), pkce_verifier);

    let mut session_cookie = Cookie::new("session_id", session_id);
    session_cookie.set_path("/");
    session_cookie.set_http_only(true);
    jar.add(session_cookie);

    Redirect::temporary(authorize_url.to_string().as_str())
}

#[handler]
pub async fn auth_callback(
    data: Data<&AppState>,
    query: Query<AuthRequest>,
    jar: &CookieJar,
) -> impl IntoResponse {
    if let Some(session_cookie) = jar.get("session_id") {
        let session_id = session_cookie.value::<String>().unwrap();

        let code = AuthorizationCode::new(query.0.code.clone());
        let pkce_verifier = data
            .pkce_verifier
            .lock()
            .unwrap()
            .remove(&session_id)
            .unwrap();

        let token_result = data
            .oauth_client
            .exchange_code(code)
            .set_pkce_verifier(pkce_verifier)
            .request_async(async_http_client)
            .await;

        match token_result {
            Ok(token) => {
                let access_token = token.access_token();
                let expires_in = token
                    .expires_in()
                    .unwrap_or(core::time::Duration::from_secs(60 * 60 * 24));
                let expiration = Utc::now() + Duration::from_std(expires_in).unwrap();

                let token_data = json!({
                    "access_token_secret": access_token.secret(),
                    "expires_at": expiration.to_rfc3339()
                });

                let mut auth_cookie = Cookie::new("auth_token", token_data.to_string());
                auth_cookie.set_expires(expiration);
                auth_cookie.set_path("/");
                auth_cookie.set_http_only(true);
                jar.add(auth_cookie);
                return Redirect::temporary("/").into_response();
            }
            Err(_err) => {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .content_type("application/json")
                    .body(json!({"error": "Login failed"}).to_string());
            }
        }
    } else {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .content_type("application/json")
            .body(json!({"error": "Session not found. Please login again"}).to_string());
    }
}

pub fn decode_base64_urlsafe(base64_str: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let decoded = decode_config(base64_str, URL_SAFE_NO_PAD)?;
    Ok(decoded)
}

#[handler]
pub async fn secure_endpoint() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "Secured"
    }))
}

#[handler]
pub async fn logout(jar: &CookieJar) -> String {
    if let Some(mut cookie) = jar.get("auth_token") {
        cookie.set_expires(Utc::now() - Duration::days(1));
        jar.remove("auth_token");
    }
    if let Some(mut cookie) = jar.get("session_id") {
        cookie.set_expires(Utc::now() - Duration::days(1));
        jar.remove("session_id");
    }
    "You have been logged out.".to_string()
}

#[handler]
pub async fn verify(data: Data<&AppState>, jar: &CookieJar) -> Json<serde_json::Value> {
    if let Some(_session_cookie) = jar.get("session_id") {
        if let Some(cookie) = jar.get("auth_token") {
            let cookie_value = cookie.value::<String>().expect("Unable to find cookie");
            let token_data: serde_json::Value =
                serde_json::from_str(&cookie_value).expect("Invalid cookie format");
            let token = token_data["access_token_secret"]
                .as_str()
                .expect("No access token found");
            let expires_at_str = token_data["expires_at"]
                .as_str()
                .expect("No expiration time found");

            let expires_at = chrono::DateTime::parse_from_rfc3339(expires_at_str)
                .expect("Invalid expiration format");

            if Utc::now() > expires_at {
                return Json(serde_json::json!({
                    "message": "Access token expired, please login again"
                }));
            }

            let header = decode_header(token).expect("Unable to decode header");
            let kid = header.kid.expect("Token header does not have a kid field");

            let jwk = data
                .jwks
                .keys
                .iter()
                .find(|&jwk| jwk.kid == kid)
                .expect("Key ID not found in JWKS");

            let n = decode_base64_urlsafe(&jwk.n).unwrap();
            let e = decode_base64_urlsafe(&jwk.e).unwrap();

            let decoding_key = DecodingKey::from_rsa_raw_components(&n, &e);

            let validation = Validation::new(Algorithm::RS256);

            let token_data =
                decode::<HashMap<String, serde_json::Value>>(token, &decoding_key, &validation);

            match token_data {
                Ok(_dc) => Json(serde_json::json!({
                    "message": "Valid access token",
                })),
                Err(_err) => Json(serde_json::json!({
                    "message": "No valid auth token found",
                })),
            }
        } else {
            Json(serde_json::json!({
                "message": "No cookie auth_token found, please login"
            }))
        }
    } else {
        Json(serde_json::json!({
            "message": "No session_id found, please login"
        }))
    }
}

pub async fn get_jwks() -> Result<Jwks, Box<dyn Error>> {
    let authority = env::var("AUTHORITY").expect("AUTHORITY not set");
    let jwks_url = format!("{}/discovery/v2.0/keys", authority);
    let client = Client::new();
    let response = client.get(&jwks_url).send().await?;
    let jwks = response.json::<Jwks>().await?;
    Ok(jwks)
}
