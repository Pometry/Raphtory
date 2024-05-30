use crate::azure_auth::common::{decode_base64_urlsafe, AppState};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use poem::{
    http::StatusCode, web::Redirect, Endpoint, Error, IntoResponse, Middleware, Request, Response,
    Result,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct TokenMiddleware {
    app_state: Arc<AppState>,
}

impl TokenMiddleware {
    pub fn new(app_state: Arc<AppState>) -> Self {
        TokenMiddleware { app_state }
    }
}

impl<E: Endpoint> Middleware<E> for TokenMiddleware {
    type Output = TokenMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        TokenMiddlewareImpl {
            ep,
            app_state: self.app_state.clone(),
        }
    }
}

pub struct TokenMiddlewareImpl<E> {
    ep: E,
    app_state: Arc<AppState>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct Token(String);

impl<E: Endpoint> Endpoint for TokenMiddlewareImpl<E> {
    type Output = Response;

    async fn call(&self, mut req: Request) -> Result<Self::Output> {
        println!("Running call");
        let jar = req.cookie().clone();
        println!("Checking for session_id");
        if let Some(_session_cookie) = jar.get("session_id") {
            println!("Checking for auth_token");
            if let Some(auth_cookie) = jar.get("auth_token") {
                println!("Gotten cookie, decoding data");
                let token_data: serde_json::Value = serde_json::from_str(
                    &auth_cookie
                        .value::<String>()
                        .expect("Unable to find cookie"),
                )
                .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;
                println!("Getting access token");
                let access_token = token_data["access_token_secret"]
                    .as_str()
                    .ok_or_else(|| Error::from_status(StatusCode::UNAUTHORIZED))?;
                println!("Expiry");
                let expires_at_str = token_data["expires_at"]
                    .as_str()
                    .ok_or_else(|| Error::from_status(StatusCode::UNAUTHORIZED))?;
                println!("Gotten access token");
                let expires_at = chrono::DateTime::parse_from_rfc3339(expires_at_str)
                    .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;
                if chrono::Utc::now() > expires_at {
                    return Err(Error::from_status(StatusCode::UNAUTHORIZED));
                }

                println!("Getting headers and kit");
                let header = decode_header(access_token)
                    .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;
                let kid = header
                    .kid
                    .ok_or_else(|| Error::from_status(StatusCode::UNAUTHORIZED))?;

                println!("Finding jwk");
                let jwk = self
                    .app_state
                    .jwks
                    .keys
                    .iter()
                    .find(|&jwk| jwk.kid == kid)
                    .ok_or_else(|| Error::from_status(StatusCode::UNAUTHORIZED))?;

                println!("Decoding n and e");
                let n = decode_base64_urlsafe(&jwk.n)
                    .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;
                let e = decode_base64_urlsafe(&jwk.e)
                    .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;

                println!("Decoding key");
                let decoding_key = DecodingKey::from_rsa_raw_components(&n, &e);

                println!("validating");
                let validation = Validation::new(Algorithm::RS256);
                decode::<HashMap<String, serde_json::Value>>(
                    access_token,
                    &decoding_key,
                    &validation,
                )
                .map_err(|_| Error::from_status(StatusCode::UNAUTHORIZED))?;

                println!("Inserting token into request");
                // Insert token data to extensions of request.
                req.extensions_mut().insert(Token(access_token.to_string()));

                println!("Token is valid, proceed with the request");
                return self.ep.call(req).await.map(IntoResponse::into_response);
            }
            println!("unable to find auth_token cookie");
            Ok(Redirect::temporary("/login").into_response())
        } else {
            println!("Unable to find session_id cookie");
            Ok(Redirect::temporary("/login").into_response())
        }
    }
}
