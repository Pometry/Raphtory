//! HTTP interop for the streaming interpreter.
//!
//! ## Why a dedicated endpoint (and not an async-graphql Extension)
//!
//! The ideal would be to let async-graphql parse + validate, then hand the
//! validated document to our interpreter. That isn't reachable in 7.2.1:
//!
//! * async-graphql's validator (`check_rules`) and the `prepare_request`
//!   pipeline are `pub(crate)` — there's no public "validate-only" entry, and
//!   the `Registry` needed to call it isn't exposed for the static schema.
//! * The `Extension::execute` hook *can* intercept after validation, but it
//!   returns a fully-materialised [`async_graphql::Response`] and the
//!   `ExtensionContext` doesn't expose the parsed document. Going through it
//!   would force us to build the whole response in memory — the exact opposite
//!   of the streaming, zero-collect design.
//!
//! Raw-byte streaming can only happen at the HTTP layer (poem owns the response
//! body), so the interpreter is wired as its own poem [`Endpoint`]. Validation
//! is done against `schema.graphql` in [`crate::interpreter::planner`], which
//! keeps the SDL authoritative. If async-graphql later exposes `check_rules`
//! publicly we can swap our validation for theirs without touching this layer.

use crate::{
    data::Data,
    interpreter::{execute, plan_request, streaming_body},
};
use poem::{Body, Endpoint, IntoResponse, Request, Response};
use raphtory::db::api::view::IntoDynamic;
use serde::Deserialize;

/// A GraphQL-over-HTTP request body. `variables`/`operationName` are accepted
/// (so well-formed clients don't break) but ignored by the POC.
#[derive(Deserialize)]
struct HttpRequest {
    query: String,
}

/// Streaming GraphQL endpoint backed by the interpreter. Mounted alongside the
/// async-graphql endpoint on its own route; opt-in, additive.
pub struct InterpreterEndpoint {
    data: Data,
}

impl InterpreterEndpoint {
    pub fn new(data: Data) -> Self {
        Self { data }
    }
}

impl Endpoint for InterpreterEndpoint {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Response> {
        let bytes = req.into_body().into_bytes().await?;

        let parsed: HttpRequest = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => return Ok(graphql_error(format!("invalid request body: {e}"))),
        };

        // request → validate (against schema.graphql) → plan
        let planned = match plan_request(&parsed.query) {
            Ok(p) => p,
            Err(e) => return Ok(graphql_error(e.to_string())),
        };

        // load the root graph (the only async step) before streaming begins
        let graph = match self.data.get_graph_unfiltered(&planned.graph_path).await {
            Ok(g) => g.graph().clone().into_dynamic(),
            Err(e) => return Ok(graphql_error(e.to_string())),
        };

        // execute on the compute pool, streaming chunks straight to the body
        let plan = planned.plan;
        let body = streaming_body(move |sink| execute(&plan, graph, sink));
        Ok(Response::builder()
            .header("content-type", "application/json")
            .body(body))
    }
}

/// A GraphQL error document (`{"data":null,"errors":[{"message":…}]}`) returned
/// with HTTP 200, matching GraphQL-over-HTTP conventions and the client's
/// expectations. Used for failures that happen *before* streaming starts
/// (bad body, validation/plan error, graph load failure).
fn graphql_error(message: String) -> Response {
    let body = serde_json::json!({ "data": null, "errors": [{ "message": message }] });
    poem::web::Json(body).into_response() // Json defaults to HTTP 200
}

#[cfg(test)]
mod tests {
    use crate::{
        client::raphtory_client::RaphtoryGraphQLClient, url_encode::url_encode_graph, GraphServer,
    };
    use raphtory::{
        db::api::storage::storage::Config,
        prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS},
    };
    use serde_json::{json, Value};
    use std::{collections::HashMap, time::Duration};
    use tempfile::TempDir;
    use url::Url;

    /// POST a query to the interpreter endpoint and return the parsed JSON body.
    async fn post_interp(http: &reqwest::Client, port: u16, query: &str) -> Value {
        let resp = http
            .post(format!("http://localhost:{port}/graphql_interp"))
            .json(&json!({ "query": query }))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "status {}", resp.status());
        serde_json::from_str(&resp.text().await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn interp_endpoint_matches_async_graphql() {
        // a graph with nodes + history
        let g = Graph::new();
        g.add_edge(1, "ben", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(2, "haaroon", "hamza", NO_PROPS, None).unwrap();
        g.add_node(3, "ben", NO_PROPS, None, None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        let port = 43934;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        // send the graph through the existing (async-graphql) mutation path
        let gql = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        gql.send_graph("g", &encoded, true).await.unwrap();

        let http = reqwest::Client::new();

        // each query must produce identical output on both engines
        for query in [
            r#"{ graph(path: "g") { nodes { list { id } } } }"#,
            r#"{ graph(path: "g") { nodes { list { name } } } }"#,
            r#"{ graph(path: "g") { node(name: "ben") { history { list { timestamp eventId } } } } }"#,
            // branching: window (Graph→Graph), after/before (Node→Node),
            // neighbours (Node→PathFromNode), neighbours.list, nested history
            r#"{ graph(path: "g") {
                window(start: 1, end: 10) {
                    node(name: "ben") {
                        after(time: 0) {
                            history { list { timestamp eventId } }
                            neighbours {
                                list {
                                    name
                                    before(time: 5) { history { list { timestamp eventId } } }
                                }
                            }
                        }
                    }
                }
            } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap(); // {"graph": {...}}
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_endpoint_rejects_invalid_query() {
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        let port = 43935;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let http = reqwest::Client::new();
        // `bogus` is not a field on Node in schema.graphql → validation error
        let body = post_interp(
            &http,
            port,
            r#"{ graph(path: "g") { nodes { list { bogus } } } }"#,
        )
        .await;
        assert_eq!(body["data"], Value::Null);
        assert!(
            body["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("bogus"),
            "unexpected error body: {body}"
        );
    }
}
