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
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
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
    async fn interp_edge_queries_match_endpoint() {
        // mirrors the layered graph in test_gql_history.py (edges only)
        let g = Graph::new();
        g.add_edge(150, "Dumbledore", "Harry", NO_PROPS, Some("communication"))
            .unwrap();
        g.add_edge(200, "Dumbledore", "Harry", [("weight", 0.5f64)], Some("friendship"))
            .unwrap();
        g.add_edge(300, "Dumbledore", "Harry", [("weight", 0.7f64)], Some("communication"))
            .unwrap();
        g.add_edge(350, "Dumbledore", "Harry", [("weight", 0.9f64)], Some("friendship"))
            .unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43936;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let gql = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        gql.send_graph("g", &encoded, true).await.unwrap();

        let http = reqwest::Client::new();
        for query in [
            // edge + history
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { list { timestamp eventId } } } } }"#,
            // edge endpoints + id
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { id src { name } dst { name } } } }"#,
            // edges collection
            r#"{ graph(path:"g") { edges { list { id } } } }"#,
            // windowed edge that exists in the window
            r#"{ graph(path:"g") { window(start:150, end:300) { edge(src:"Dumbledore", dst:"Harry") { history { list { timestamp eventId } } } } } }"#,
            // windowed edge that is absent → null
            r#"{ graph(path:"g") { window(start:0, end:150) { edge(src:"Dumbledore", dst:"Harry") { history { list { timestamp eventId } } } } } }"#,
            // layered edge
            r#"{ graph(path:"g") { layer(name:"communication") { edge(src:"Dumbledore", dst:"Harry") { history { list { timestamp eventId } } } } } }"#,
            // edge after/before
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { after(time:200) { history { list { timestamp } } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_property_queries_match_endpoint() {
        // mirrors test_graph_properties_query: a node whose temporal props change
        // over time, plus node metadata; a second node with numeric props; an edge
        // with a float property.
        let g = Graph::new();
        g.add_node(1, "n1", [("prop1", "val1"), ("prop2", "val1")], None, None)
            .unwrap();
        g.add_node(2, "n1", [("prop1", "val2"), ("prop2", "val2")], None, None)
            .unwrap();
        let n = g
            .add_node(3, "n1", [("prop1", "val3"), ("prop2", "val3")], None, None)
            .unwrap();
        n.add_metadata([("prop5", "val4")]).unwrap();
        g.add_node(1, "n2", [("score", 42i64)], None, None).unwrap();
        g.add_edge(5, "n1", "n2", [("weight", 0.9f64)], None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43937;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let gql = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        gql.send_graph("g", &encoded, true).await.unwrap();

        let http = reqwest::Client::new();
        for query in [
            // properties.values(keys) + asString, temporal.values(keys).history, metadata.values(keys).value
            r#"{ graph(path:"g") { node(name:"n1") {
                properties {
                    values(keys:["prop1"]) { key asString value }
                    temporal { values(keys:["prop2"]) { key history { list { timestamp eventId } } } }
                }
                metadata { values(keys:["prop5"]) { key value } }
            } } }"#,
            // numeric `value` (no keys → all props)
            r#"{ graph(path:"g") { node(name:"n2") { properties { values { key value asString } } } } }"#,
            // edge properties
            r#"{ graph(path:"g") { edge(src:"n1", dst:"n2") { properties { values(keys:["weight"]) { key value asString } } } } }"#,
            // properties.values over a collection
            r#"{ graph(path:"g") { nodes { list { properties { values { key } } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_history_projections_match_endpoint() {
        // layered graph from test_gql_history.py (edges only suffice here)
        let g = Graph::new();
        g.add_edge(150, "Dumbledore", "Harry", NO_PROPS, Some("communication"))
            .unwrap();
        g.add_edge(200, "Dumbledore", "Harry", [("weight", 0.5f64)], Some("friendship"))
            .unwrap();
        g.add_edge(300, "Dumbledore", "Harry", [("weight", 0.7f64)], Some("communication"))
            .unwrap();
        g.add_edge(350, "Dumbledore", "Harry", [("weight", 0.9f64)], Some("friendship"))
            .unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43938;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let gql = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        gql.send_graph("g", &encoded, true).await.unwrap();

        let http = reqwest::Client::new();
        for query in [
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { timestamps { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { eventId { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { datetimes { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { datetimes(formatString:"%Y-%m-%d") { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { datetimes(formatString:"%Y-%m-%d %H:%M:%S %3fms") { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { list { datetime(formatString:"%Y-%m-%d %H:%M:%S %3fms") } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }

        // invalid datetime format → rejected at plan time, before streaming
        for bad in [
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { datetimes(formatString:"%Y-%m-%d %H:%M:%S %4fms") { list } } } } }"#,
            r#"{ graph(path:"g") { edge(src:"Dumbledore", dst:"Harry") { history { list { datetime(formatString:"%Y-%m-%d %H:%M:%S %4fms") } } } } }"#,
        ] {
            let body = post_interp(&http, port, bad).await;
            assert_eq!(body["data"], Value::Null, "expected error for: {bad}");
            assert!(
                body["errors"][0]["message"]
                    .as_str()
                    .unwrap()
                    .contains("Invalid datetime format string"),
                "unexpected error body: {body}"
            );
        }
    }

    #[tokio::test]
    async fn interp_surface_fields_match_endpoint() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("l1")).unwrap();
        g.add_edge(2, "a", "c", [("w", 1.0f64)], Some("l2")).unwrap();
        g.add_edge(3, "b", "c", NO_PROPS, Some("l1")).unwrap();
        g.add_node(5, "a", NO_PROPS, Some("person"), None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43939;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let gql = RaphtoryGraphQLClient::new(
            Url::parse(&format!("http://localhost:{port}/")).unwrap(),
            None,
        );
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        gql.send_graph("g", &encoded, true).await.unwrap();

        let http = reqwest::Client::new();
        for query in [
            // graph scalars + counts + lookups + time
            r#"{ graph(path:"g") { countNodes countEdges countTemporalEdges uniqueLayers hasNode(name:"a") hasAB: hasEdge(src:"a", dst:"b") hasAZ: hasEdge(src:"a", dst:"z") earliestTime{timestamp} latestTime{timestamp eventId} } }"#,
            // node scalars + time (start has no window → null EventTime path)
            r#"{ graph(path:"g") { node(name:"a") { nodeType degree inDegree outDegree edgeHistoryCount isActive earliestTime{timestamp eventId} latestTime{timestamp} firstUpdate{timestamp} lastUpdate{timestamp} start{timestamp} } } }"#,
            // node traversal collections
            r#"{ graph(path:"g") { node(name:"a") { edges{list{id}} inEdges{list{id}} outEdges{list{id}} neighbours{list{name}} inNeighbours{list{name}} outNeighbours{list{name}} } } }"#,
            // components
            r#"{ graph(path:"g") { node(name:"a") { outComponent{list{name}} inComponent{list{name}} } } }"#,
            // edge scalars/structure/time (start no window → null)
            r#"{ graph(path:"g") { edge(src:"a", dst:"b") { nbr{name} isValid isActive isDeleted isSelfLoop layerNames earliestTime{timestamp} latestTime{timestamp} start{timestamp} } } }"#,
            // edge explode / explodeLayers
            r#"{ graph(path:"g") { edge(src:"a", dst:"b") { explode{list{layerNames}} explodeLayers{list{layerNames}} } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_endpoint_rejects_invalid_query() {
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
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
