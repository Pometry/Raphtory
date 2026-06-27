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
    async fn interp_collection_ops_match_endpoint() {
        // nodes with a sortable property + a few edges
        let g = Graph::new();
        g.add_node(1, "c", [("age", 30i64)], None, None).unwrap();
        g.add_node(2, "a", [("age", 10i64)], None, None).unwrap();
        g.add_node(3, "b", [("age", 20i64)], None, None).unwrap();
        g.add_edge(5, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(6, "b", "c", NO_PROPS, None).unwrap();
        g.add_edge(7, "a", "c", NO_PROPS, None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43940;
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
            // count
            r#"{ graph(path:"g") { nodes { count } edges { count } } }"#,
            // page (offset / pageIndex)
            r#"{ graph(path:"g") { nodes { page(limit:2) { name } } } }"#,
            r#"{ graph(path:"g") { nodes { page(limit:1, offset:1) { name } } } }"#,
            r#"{ graph(path:"g") { edges { page(limit:2) { id } } } }"#,
            // sorted: by id (asc / reverse), by property, by time
            r#"{ graph(path:"g") { nodes { sorted(sortBys:[{id:true}]) { list { name } } } } }"#,
            r#"{ graph(path:"g") { nodes { sorted(sortBys:[{id:true, reverse:true}]) { list { name } } } } }"#,
            r#"{ graph(path:"g") { nodes { sorted(sortBys:[{property:"age"}]) { list { name } } } } }"#,
            r#"{ graph(path:"g") { nodes { sorted(sortBys:[{time: LATEST}]) { list { name } } } } }"#,
            r#"{ graph(path:"g") { edges { sorted(sortBys:[{src:true, dst:true}]) { list { id } } } } }"#,
            // sorted then paged
            r#"{ graph(path:"g") { nodes { sorted(sortBys:[{id:true}]) { page(limit:2) { name } } } } }"#,
            // neighbours count + page
            r#"{ graph(path:"g") { node(name:"a") { neighbours { count page(limit:1) { name } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_graph_views_match_endpoint() {
        // layered, multi-timestamp graph to exercise time + layer + structural views
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("l1")).unwrap();
        g.add_edge(5, "a", "c", NO_PROPS, Some("l2")).unwrap();
        g.add_edge(9, "b", "c", NO_PROPS, Some("l1")).unwrap();
        g.add_node(3, "a", NO_PROPS, Some("person"), None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43941;
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
            // graph time views
            r#"{ graph(path:"g") { at(time:5) { nodes { count } } } }"#,
            r#"{ graph(path:"g") { before(time:5) { edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { after(time:1) { edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { latest { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { snapshotAt(time:5) { countEdges } } }"#,
            r#"{ graph(path:"g") { snapshotLatest { countEdges } } }"#,
            // graph layer views
            r#"{ graph(path:"g") { layers(names:["l1"]) { edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { excludeLayer(name:"l1") { edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { excludeLayers(names:["l2"]) { edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { defaultLayer { countEdges } } }"#,
            // graph structural views
            r#"{ graph(path:"g") { subgraph(nodes:["a","b"]) { nodes { list { name } } edges { list { id } } } } }"#,
            r#"{ graph(path:"g") { subgraphNodeTypes(nodeTypes:["person"]) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { excludeNodes(nodes:["a"]) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { valid { countEdges } } }"#,
            // node + edge time/layer views
            r#"{ graph(path:"g") { node(name:"a") { at(time:3) { earliestTime { timestamp } } } } }"#,
            r#"{ graph(path:"g") { node(name:"a") { latest { name } } } }"#,
            r#"{ graph(path:"g") { edge(src:"a", dst:"b") { snapshotLatest { isValid } layer(name:"l1") { history { list { timestamp } } } } } }"#,
            // chained views
            r#"{ graph(path:"g") { window(start:0, end:10) { layers(names:["l1"]) { edges { count } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_filters_match_endpoint() {
        // nodes with a temporal Age property + edges with a weight property
        let g = Graph::new();
        g.add_node(200, "Dumbledore", [("Age", 50i64)], None, None).unwrap();
        g.add_node(300, "Dumbledore", [("Age", 51i64)], None, None).unwrap();
        g.add_node(250, "Harry", [("Age", 20i64)], None, None).unwrap();
        g.add_node(350, "Harry", [("Age", 21i64)], None, None).unwrap();
        g.add_edge(200, "Dumbledore", "Harry", [("weight", 0.5f64)], None).unwrap();
        g.add_edge(300, "Dumbledore", "Sirius", [("weight", 0.9f64)], None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43942;
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
            // graph.filterNodes / filterEdges (property filters pushed into raphtory)
            r#"{ graph(path:"g") { filterNodes(expr:{property:{name:"Age",where:{ge:{i64:51}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterEdges(expr:{property:{name:"weight",where:{eq:{f64:0.9}}}}) { edges { list { id } } } } }"#,
            // nodes(select:) / edges(select:) on the graph entry points
            r#"{ graph(path:"g") { nodes(select:{property:{name:"Age",where:{lt:{i64:51}}}}) { list { name } } } }"#,
            r#"{ graph(path:"g") { edges(select:{property:{name:"weight",where:{gt:{f64:0.5}}}}) { list { id } } } }"#,
            // collection .filter / .select
            r#"{ graph(path:"g") { nodes { filter(expr:{property:{name:"Age",where:{lt:{i64:51}}}}) { list { name } } } } }"#,
            r#"{ graph(path:"g") { nodes { select(expr:{property:{name:"Age",where:{ge:{i64:21}}}}) { list { name } } } } }"#,
            r#"{ graph(path:"g") { edges { select(expr:{property:{name:"weight",where:{gt:{f64:0.5}}}}) { list { id } } } } }"#,
            // neighbours(select:) — filter the neighbour set by property
            r#"{ graph(path:"g") { node(name:"Dumbledore") { neighbours(select:{property:{name:"Age",where:{ge:{i64:21}}}}) { list { name } } } } }"#,
            // And / Or / Not combinators
            r#"{ graph(path:"g") { filterNodes(expr:{and:[{property:{name:"Age",where:{ge:{i64:21}}}},{property:{name:"Age",where:{lt:{i64:60}}}}]}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{not:{property:{name:"Age",where:{ge:{i64:51}}}}}) { nodes { list { name } } } } }"#,
            // node.filter
            r#"{ graph(path:"g") { node(name:"Dumbledore") { filter(expr:{property:{name:"Age",where:{ge:{i64:51}}}}) { name } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_node_field_filters_match_endpoint() {
        // named, typed nodes to exercise node-field filters (NODE_NAME/NODE_TYPE/NODE_ID)
        let g = Graph::new();
        g.add_node(1, "alice", NO_PROPS, Some("person"), None).unwrap();
        g.add_node(2, "bob", NO_PROPS, Some("person"), None).unwrap();
        g.add_node(3, "server1", NO_PROPS, Some("machine"), None).unwrap();
        g.add_edge(5, "alice", "bob", NO_PROPS, None).unwrap();
        g.add_edge(6, "alice", "server1", NO_PROPS, None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43943;
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
            // node-field filters: by NODE_NAME / NODE_TYPE / NODE_ID (the enum gap, now closed)
            r#"{ graph(path:"g") { filterNodes(expr:{node:{field:NODE_NAME,where:{eq:{str:"alice"}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { nodes(select:{node:{field:NODE_TYPE,where:{eq:{str:"person"}}}}) { list { name } } } }"#,
            r#"{ graph(path:"g") { nodes { filter(expr:{node:{field:NODE_NAME,where:{ne:{str:"alice"}}}}) { list { name } } } } }"#,
            r#"{ graph(path:"g") { node(name:"alice") { neighbours(select:{node:{field:NODE_TYPE,where:{eq:{str:"machine"}}}}) { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{node:{field:NODE_ID,where:{eq:{str:"bob"}}}}) { nodes { list { name } } } } }"#,
        ] {
            let expected =
                serde_json::to_value(gql.query(query, HashMap::new()).await.unwrap()).unwrap();
            let got = post_interp(&http, port, query).await;
            assert_eq!(got["data"], expected, "mismatch for query: {query}");
        }
    }

    #[tokio::test]
    async fn interp_degree_and_time_scoped_filters_match_endpoint() {
        // a hub + spokes (varying degree) and a temporal Age property
        let g = Graph::new();
        g.add_edge(1, "hub", "a", NO_PROPS, None).unwrap();
        g.add_edge(2, "hub", "b", NO_PROPS, None).unwrap();
        g.add_edge(3, "hub", "c", NO_PROPS, None).unwrap();
        g.add_edge(4, "a", "b", NO_PROPS, None).unwrap();
        g.add_node(100, "a", [("Age", 10i64)], None, None).unwrap();
        g.add_node(300, "a", [("Age", 50i64)], None, None).unwrap();
        g.add_node(100, "b", [("Age", 99i64)], None, None).unwrap();

        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let port = 43944;
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
            // degree filters (DegreeDirection enum: BOTH / OUT / IN)
            r#"{ graph(path:"g") { filterNodes(expr:{degree:{direction:BOTH,where:{ge:{u64:2}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{degree:{direction:OUT,where:{ge:{u64:3}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{degree:{direction:IN,where:{eq:{u64:1}}}}) { nodes { list { name } } } } }"#,
            // time-scoped filter expressions (TimeInput leaves: int form)
            r#"{ graph(path:"g") { filterNodes(expr:{window:{start:0,end:200,expr:{property:{name:"Age",where:{lt:{i64:40}}}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{before:{time:200,expr:{property:{name:"Age",where:{lt:{i64:40}}}}}}) { nodes { list { name } } } } }"#,
            r#"{ graph(path:"g") { filterNodes(expr:{after:{time:200,expr:{property:{name:"Age",where:{ge:{i64:40}}}}}}) { nodes { list { name } } } } }"#,
            // time-scoped via object TimeInput form {timestamp, eventId}
            r#"{ graph(path:"g") { filterNodes(expr:{at:{time:{timestamp:100,eventId:0},expr:{property:{name:"Age",where:{ge:{i64:90}}}}}}) { nodes { list { name } } } } }"#,
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
