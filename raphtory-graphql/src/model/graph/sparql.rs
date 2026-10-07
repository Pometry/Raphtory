//! The `sparql` field of `Graph` (feature `rdf`): SPARQL queries over a graph view.
use crate::{
    model::graph::graph::GqlGraph,
    sparql::{endpoint::ProtocolCall, run_sparql, SparqlError},
};
use async_graphql::{Context, Result};
use dynamic_graphql::{ExpandObject, ExpandObjectFields};
use raphtory::{
    errors::GraphError,
    rdf::{RdfViewOps, SparqlFormat},
};
use std::sync::Arc;

/// Adds the `sparql` field to `Graph`.
#[derive(ExpandObject)]
pub struct GqlGraphSparql<'a>(&'a GqlGraph);

#[ExpandObjectFields]
impl<'a> GqlGraphSparql<'a> {
    /// Runs a SPARQL 1.1 query (SELECT, ASK, CONSTRUCT or DESCRIBE) on the RDF triples of this
    /// view and returns the results as one serialized document. The view has one triple per
    /// visible edge and layer (source, layer, destination); a name that is not an IRI, a blank
    /// node or a literal is an IRI under `raphtory:`, a prefix every query can use. On
    /// PERSISTENT graphs the triples are the state at the end of the view, so under
    /// `snapshotAt(time: T)` the state as of T; EVENT graphs ignore deletions (use
    /// `graphType: PERSISTENT`). `GRAPH <raphtory:asof:T>` matches the triples as of T. The
    /// temporal functions (`raphtory:validFrom`, `validTo`, `validFromTime`, `validToTime`) are
    /// unavailable when the caller's access to the graph is row-filtered. Disabled when the
    /// server sets `disable_lists`. Queries longer than the server's `max_sparql_query_length`
    /// (16 KiB by default), with more than `max_sparql_triple_patterns` triple patterns (100 by
    /// default) or with brackets nested more than 128 deep are rejected before they run, and a
    /// query that runs longer than `sparql_timeout` (30 seconds by default) is stopped with an
    /// error.
    async fn sparql(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The SPARQL 1.1 query.")] query: String,
        #[graphql(
            desc = "Result format: a name, file extension or media type. SELECT and ASK: `json` (SPARQL Results JSON, the default), `xml`, `csv` or `tsv`. CONSTRUCT and DESCRIBE: an RDF format, `nt` (N-Triples, the default), `ttl`, `jsonld`, `rdf` (RDF/XML), `nq` or `trig`. `json` and `xml` mean a different format for each form of query; a format that does not fit the query is an error."
        )]
        format: Option<String>,
    ) -> Result<String> {
        // a request of the SPARQL protocol endpoint, which takes the results itself
        if let Some(call) = ctx.data_opt::<Arc<ProtocolCall>>() {
            call.run(ctx, self.0, query).await?;
            return Ok(String::new());
        }
        let results = run_sparql(ctx, self.0, query, None, move |view, query, options| {
            let format = match format {
                Some(format) => SparqlFormat::parse(&format).map_err(GraphError::from)?,
                None => SparqlFormat::default(),
            };
            let mut results = Vec::new();
            view.sparql_to_writer_with(query, &mut results, format, options)?;
            String::from_utf8(results).map_err(|error| {
                SparqlError::Internal(format!("SPARQL results are not UTF-8: {error}"))
            })
        })
        .await?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        auth_policy::{
            auth_policy_tests::FakePolicy, AuthPolicyError, AuthorizationPolicy, GraphPermission,
            NamespacePermission,
        },
        config::concurrency_config::ConcurrencyConfig,
        model::{graph::filtering::GraphAccessFilter, App},
        test_support::{setup_with_graphs, setup_with_policy},
    };
    use async_graphql::dynamic::Schema;
    use dynamic_graphql::{Request, Variables};
    use futures_util::future::BoxFuture;
    use raphtory::{
        db::api::view::MaterializedGraph,
        prelude::*,
        rdf::{QueryResultsFormat, RdfFormat, RdfParser, SparqlFormat},
    };
    use serde_json::{json, Value as Json};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tempfile::TempDir;

    const PEOPLE: &str = r#"
        @prefix ex: <http://ex/> .
        ex:alice ex:knows ex:bob ; ex:age 42 .
        ex:bob ex:knows ex:carol .
        ex:carol ex:name "Carol" .
    "#;
    const KNOWS: &str = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o";
    const COUNT: &str = "SELECT (COUNT(*) AS ?n) { ?s ?p ?o }";
    const KNOWN_BY: &str =
        "PREFIX ex: <http://ex/> CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }";
    /// When `bob knows carol` started holding: at 1.
    const SINCE: &str = "PREFIX ex: <http://ex/> \
        SELECT (raphtory:validFromTime(ex:bob, ex:knows, ex:carol) AS ?t) {}";

    /// Asserted at 1: `alice knows bob`, `alice age 42`, `bob knows carol`, `carol name "Carol"`.
    /// `alice knows bob` is retracted at 5, and `dave knows alice` asserted at 7.
    fn people() -> PersistentGraph {
        let pg = PersistentGraph::new();
        pg.load_rdf(1, PEOPLE.as_bytes(), RdfFormat::Turtle, None)
            .unwrap();
        let triple =
            |s: &str, o: &str| format!("<http://ex/{s}> <http://ex/knows> <http://ex/{o}> .");
        pg.retract_rdf(
            5,
            triple("alice", "bob").as_bytes(),
            RdfFormat::NTriples,
            None,
        )
        .unwrap();
        pg.load_rdf(
            7,
            triple("dave", "alice").as_bytes(),
            RdfFormat::NTriples,
            None,
        )
        .unwrap();
        pg
    }

    /// The same events as [`people`], stored as an event graph.
    fn people_events() -> Graph {
        people().event_graph()
    }

    fn graphs() -> Vec<(&'static str, MaterializedGraph)> {
        vec![("pg", people().into()), ("eg", people_events().into())]
    }

    async fn setup(work_dir: &TempDir) -> Schema {
        setup_with_graphs(&graphs(), work_dir.path()).await.schema
    }

    async fn setup_as(work_dir: &TempDir, permission: GraphPermission) -> Schema {
        let policy = FakePolicy::default()
            .with_namespace("", NamespacePermission::Read)
            .with_graph("pg", permission.clone())
            .with_graph("eg", permission);
        setup_with_policy(&graphs(), work_dir.path(), Arc::new(policy))
            .await
            .schema
    }

    /// A row-filtered read permission (see [`GraphAccessFilter`]).
    fn read_with(filter: Json) -> GraphPermission {
        GraphPermission::Read {
            filter: Some(serde_json::from_value::<GraphAccessFilter>(filter).unwrap()),
        }
    }

    /// Runs `{ graph(path: ...) { view { sparql(query: q, format: f) } } }` and returns the
    /// value of `sparql`, or the first error message. An error must leave `graph` null.
    async fn sparql_on(
        schema: &Schema,
        graph: &str,
        view: &str,
        query: &str,
        format: Option<&str>,
    ) -> Result<String, String> {
        let (open, close) = if view.is_empty() {
            (String::new(), "")
        } else {
            (format!("view: {view} {{"), "}")
        };
        let document = format!(
            "query Q($q: String!, $f: String) {{ graph({graph}) {{ {open} sparql(query: $q, format: $f) {close} }} }}"
        );
        let request = Request::new(document).variables(Variables::from_json(json!({
            "q": query,
            "f": format,
        })));
        let response = schema.execute(request).await;
        let data = response.data.into_json().unwrap();
        match response.errors.first() {
            Some(error) => {
                assert_eq!(data, json!({ "graph": null }), "{}", error.message);
                Err(error.message.clone())
            }
            None => {
                let graph = &data["graph"];
                let value = if view.is_empty() {
                    &graph["sparql"]
                } else {
                    &graph["view"]["sparql"]
                };
                Ok(value
                    .as_str()
                    .unwrap_or_else(|| panic!("no sparql value in {data}"))
                    .to_owned())
            }
        }
    }

    async fn sparql(schema: &Schema, view: &str, query: &str) -> Result<String, String> {
        sparql_on(schema, r#"path: "pg""#, view, query, None).await
    }

    /// The values of the solutions of SPARQL Results JSON, `""` for an unbound variable.
    fn rows(results: &str) -> Vec<Vec<String>> {
        let results: Json = serde_json::from_str(results).unwrap();
        let vars: Vec<&str> = results["head"]["vars"]
            .as_array()
            .unwrap()
            .iter()
            .map(|var| var.as_str().unwrap())
            .collect();
        results["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|solution| {
                vars.iter()
                    .map(|var| solution[var]["value"].as_str().unwrap_or("").to_owned())
                    .collect()
            })
            .collect()
    }

    fn pairs(pairs: &[(&str, &str)]) -> Vec<Vec<String>> {
        pairs
            .iter()
            .map(|(s, o)| vec![format!("http://ex/{s}"), format!("http://ex/{o}")])
            .collect()
    }

    /// The results of `query` on `view` of the local graph, as `sparql_to_writer` writes them.
    fn local(view: &impl RdfViewOps, query: &str, format: impl Into<SparqlFormat>) -> String {
        let mut out = Vec::new();
        view.sparql_to_writer(query, &mut out, format).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[tokio::test]
    async fn select_defaults_to_the_json_of_sparql_to_writer() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let json = sparql(&schema, "", KNOWS).await.unwrap();
        assert_eq!(json, local(&people(), KNOWS, SparqlFormat::default()));
        assert_eq!(json, local(&people(), KNOWS, QueryResultsFormat::Json));
        assert_eq!(rows(&json), pairs(&[("bob", "carol"), ("dave", "alice")]));

        // terms, not names: a literal keeps its datatype
        let json = sparql(&schema, "", "SELECT ?o { ?s <http://ex/age> ?o }")
            .await
            .unwrap();
        let results: Json = serde_json::from_str(&json).unwrap();
        assert_eq!(
            results["results"]["bindings"][0]["o"],
            json!({
                "type": "literal",
                "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                "value": "42",
            })
        );
    }

    #[tokio::test]
    async fn views_restrict_the_triples() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let knows = |view: &'static str| {
            let schema = &schema;
            async move { rows(&sparql(schema, view, KNOWS).await.unwrap()) }
        };
        assert_eq!(
            knows("").await,
            pairs(&[("bob", "carol"), ("dave", "alice")])
        );
        // the retracted triple is still there as of 3, and dave's is not yet
        let as_of_3 = pairs(&[("alice", "bob"), ("bob", "carol")]);
        assert_eq!(knows("snapshotAt(time: 3)").await, as_of_3);
        assert_eq!(knows("window(start: 0, end: 3)").await, as_of_3);
        assert_eq!(knows("before(time: 6)").await, pairs(&[("bob", "carol")]));
        assert_eq!(
            knows(
                r#"filter(expr: { node: { name: { where: { ne: { str: "http://ex/carol" } } } } })"#
            )
            .await,
            pairs(&[("dave", "alice")])
        );
        assert_eq!(
            knows(r#"applyViews(views: [{ snapshotAt: 3 }, { excludeNodes: ["http://ex/bob"] }])"#)
                .await,
            Vec::<Vec<String>>::new()
        );
        assert_eq!(
            knows(
                r#"applyViews(views: [{ snapshotAt: 3 }, { excludeNodes: ["http://ex/carol"] }])"#
            )
            .await,
            pairs(&[("alice", "bob")])
        );

        let count = |view: &'static str| {
            let schema = &schema;
            async move { rows(&sparql(schema, view, COUNT).await.unwrap()) }
        };
        assert_eq!(count("").await, [["4"]]);
        assert_eq!(count(r#"layer(name: "http://ex/knows")"#).await, [["2"]]);
        assert_eq!(
            count(r#"layers(names: ["http://ex/age", "http://ex/name"])"#).await,
            [["2"]]
        );
        assert_eq!(
            count(r#"excludeLayer(name: "http://ex/knows")"#).await,
            [["2"]]
        );
    }

    #[tokio::test]
    async fn event_graphs_ignore_retractions_unless_read_as_persistent() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let ask = "ASK { <http://ex/alice> <http://ex/knows> <http://ex/bob> }";
        let on = |graph: &'static str| {
            let schema = &schema;
            async move { sparql_on(schema, graph, "", ask, None).await.unwrap() }
        };
        let yes = r#"{"head":{},"boolean":true}"#;
        let no = r#"{"head":{},"boolean":false}"#;
        assert_eq!(on(r#"path: "eg""#).await, yes);
        assert_eq!(on(r#"path: "eg", graphType: PERSISTENT"#).await, no);
        assert_eq!(on(r#"path: "pg""#).await, no);
        assert_eq!(on(r#"path: "pg", graphType: EVENT"#).await, yes);
    }

    #[tokio::test]
    async fn time_graphs_match_snapshot_at() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let in_query = sparql(
            &schema,
            "",
            "PREFIX ex: <http://ex/> \
             SELECT ?s ?o { GRAPH raphtory:asof:3 { ?s ex:knows ?o } } ORDER BY ?s ?o",
        )
        .await
        .unwrap();
        let view = sparql(&schema, "snapshotAt(time: 3)", KNOWS).await.unwrap();
        assert_eq!(in_query, view);
        assert_eq!(rows(&view), pairs(&[("alice", "bob"), ("bob", "carol")]));
    }

    #[tokio::test]
    async fn ask_and_graph_results() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        assert_eq!(
            sparql(&schema, "", "ASK { ?s ?p ?o }").await.unwrap(),
            r#"{"head":{},"boolean":true}"#
        );

        let nt = sparql(&schema, "", KNOWN_BY).await.unwrap();
        assert_eq!(nt, local(&people(), KNOWN_BY, RdfFormat::NTriples));
        let mut lines: Vec<_> = nt.lines().collect();
        lines.sort();
        assert_eq!(
            lines,
            [
                "<http://ex/alice> <http://ex/knownBy> <http://ex/dave> .",
                "<http://ex/carol> <http://ex/knownBy> <http://ex/bob> .",
            ]
        );

        let ttl = sparql_on(&schema, r#"path: "pg""#, "", KNOWN_BY, Some("ttl"))
            .await
            .unwrap();
        assert_eq!(ttl, local(&people(), KNOWN_BY, RdfFormat::Turtle));
        let triples = RdfParser::from_format(RdfFormat::Turtle)
            .for_slice(ttl.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(triples.len(), 2);
    }

    #[tokio::test]
    async fn results_formats() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let pg = people();
        let with = |format: &'static str, query: &'static str| {
            let schema = &schema;
            async move { sparql_on(schema, r#"path: "pg""#, "", query, Some(format)).await }
        };
        for format in ["json", "xml", "csv", "tsv", "text/csv", ".srj"] {
            assert_eq!(
                with(format, KNOWS).await.unwrap(),
                local(&pg, KNOWS, SparqlFormat::parse(format).unwrap()),
                "{format}"
            );
        }
        assert_eq!(
            with("csv", KNOWS).await.unwrap(),
            "s,o\r\nhttp://ex/bob,http://ex/carol\r\nhttp://ex/dave,http://ex/alice\r\n"
        );
        assert_eq!(with("tsv", "ASK {}").await.unwrap(), "true");
        for format in ["jsonld", "rdf", "xml", "nt"] {
            assert_eq!(
                with(format, KNOWN_BY).await.unwrap(),
                local(&pg, KNOWN_BY, SparqlFormat::parse(format).unwrap()),
                "{format}"
            );
        }

        let error = with("bogus", KNOWS).await.unwrap_err();
        assert!(
            error.contains("unknown SPARQL results format 'bogus'"),
            "{error}"
        );
        let error = with("csv", KNOWN_BY).await.unwrap_err();
        assert!(
            error.contains("CONSTRUCT results cannot be written as"),
            "{error}"
        );
        let error = with("ttl", KNOWS).await.unwrap_err();
        assert!(
            error.contains("SELECT results cannot be written as"),
            "{error}"
        );
    }

    /// A `SERVICE` call is refused and makes no request, even when oxigraph has its HTTP client.
    #[tokio::test]
    async fn service_calls_make_no_request() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let service = format!("<http://127.0.0.1:{port}/sparql>");
        for query in [
            format!("SELECT * {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
            format!("ASK {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
        ] {
            let error = sparql(&schema, "", &query).await.unwrap_err();
            assert_eq!(
                error,
                format!(
                    "SPARQL evaluation error: SERVICE {service} is not supported: Raphtory never \
                     sends SPARQL queries to other endpoints"
                )
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        // a connection would be waiting to be accepted
        let accepted = listener.accept();
        assert!(
            accepted
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
            "{accepted:?}"
        );
    }

    #[tokio::test]
    async fn errors_null_the_graph() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        // `sparql_on` checks that `graph` is null
        let error = sparql(&schema, "", "SELECT ?x WHERE { ?x")
            .await
            .unwrap_err();
        assert!(error.starts_with("SPARQL syntax error"), "{error}");
        let error = sparql(
            &schema,
            "",
            "ASK { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }",
        )
        .await
        .unwrap_err();
        assert!(error.starts_with("SPARQL evaluation error"), "{error}");
        assert!(
            error.contains("invalid time graph <raphtory:asof:nope>"),
            "{error}"
        );
        let error = sparql(&schema, "", "SELECT (<http://ex/nope>(1) AS ?x) {}")
            .await
            .unwrap_err();
        assert!(error.starts_with("SPARQL evaluation error"), "{error}");
    }

    #[tokio::test]
    async fn temporal_functions_without_a_policy() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        assert_eq!(rows(&sparql(&schema, "", SINCE).await.unwrap()), [["1"]]);
    }

    #[tokio::test]
    async fn row_filters_hide_triples() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(
            &work_dir,
            read_with(json!({
                "filter": { "node": { "name": { "where": { "ne": { "str": "http://ex/carol" } } } } }
            })),
        )
        .await;
        assert_eq!(
            rows(&sparql(&schema, "", KNOWS).await.unwrap()),
            pairs(&[("dave", "alice")])
        );
        assert_eq!(rows(&sparql(&schema, "", COUNT).await.unwrap()), [["2"]]);
        let no = r#"{"head":{},"boolean":false}"#;
        for ask in [
            "ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }",
            "ASK { <http://ex/carol> ?p ?o }",
            "ASK { ?s ?p <http://ex/carol> }",
            "ASK { ?s ?p \"Carol\" }",
            "ASK { GRAPH raphtory:asof:3 { ?s ?p <http://ex/carol> } }",
        ] {
            assert_eq!(sparql(&schema, "", ask).await.unwrap(), no, "{ask}");
        }
        // as of 3, only the triples between visible nodes
        let as_of_3 = "PREFIX ex: <http://ex/> \
            SELECT ?s ?o { GRAPH raphtory:asof:3 { ?s ex:knows ?o } } ORDER BY ?s ?o";
        assert_eq!(
            rows(&sparql(&schema, "", as_of_3).await.unwrap()),
            pairs(&[("alice", "bob")])
        );
        // a hidden node is the same as an IRI that does not exist
        for (hidden, missing) in [
            (
                "SELECT * { <http://ex/carol> ?p ?o }",
                "SELECT * { <http://ex/nobody> ?p ?o }",
            ),
            (
                "SELECT * { ?s ?p <http://ex/carol> }",
                "SELECT * { ?s ?p <http://ex/nobody> }",
            ),
            ("DESCRIBE <http://ex/carol>", "DESCRIBE <http://ex/nobody>"),
        ] {
            assert_eq!(
                sparql(&schema, "", hidden).await.unwrap(),
                sparql(&schema, "", missing).await.unwrap(),
                "{hidden}"
            );
        }
    }

    #[tokio::test]
    async fn window_row_filters_hide_other_times() {
        let work_dir = TempDir::new().unwrap();
        // the caller may only see the events from 3 up to 6
        let schema = setup_as(
            &work_dir,
            read_with(json!({ "filter": { "window": { "start": 3, "end": 6 } } })),
        )
        .await;
        // the state at the end of the window: retracted at 5, dave's not asserted yet
        assert_eq!(
            rows(&sparql(&schema, "", KNOWS).await.unwrap()),
            pairs(&[("bob", "carol")])
        );
        // times outside the window show nothing, inside it what the window shows
        let at = |t: i64| {
            format!(
                "PREFIX ex: <http://ex/> \
                 SELECT ?s ?o {{ GRAPH raphtory:asof:{t} {{ ?s ex:knows ?o }} }} ORDER BY ?s ?o"
            )
        };
        assert_eq!(
            rows(&sparql(&schema, "", &at(2)).await.unwrap()),
            pairs(&[])
        );
        assert_eq!(
            rows(&sparql(&schema, "", &at(9)).await.unwrap()),
            pairs(&[])
        );
        assert_eq!(
            rows(&sparql(&schema, "", &at(4)).await.unwrap()),
            pairs(&[("alice", "bob"), ("bob", "carol")])
        );
        assert_eq!(
            rows(&sparql(&schema, "snapshotAt(time: 9)", KNOWS).await.unwrap()),
            pairs(&[])
        );
    }

    /// Temporal functions are unavailable to a caller whose read is row-filtered.
    #[tokio::test]
    async fn row_filtered_reads_have_no_temporal_functions() {
        let work_dir = TempDir::new().unwrap();
        let window = read_with(json!({ "filter": { "window": { "start": 3, "end": 6 } } }));
        let schema = setup_as(&work_dir, window).await;
        // the triple is visible to the caller
        assert_eq!(
            sparql(
                &schema,
                "",
                "ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }"
            )
            .await
            .unwrap(),
            r#"{"head":{},"boolean":true}"#
        );
        let error = sparql(&schema, "", SINCE).await.unwrap_err();
        assert!(error.starts_with("SPARQL evaluation error"), "{error}");
        assert!(error.contains("validFromTime"), "{error}");
        let error = sparql_on(&schema, r#"path: "eg""#, "", SINCE, None)
            .await
            .unwrap_err();
        assert!(error.contains("validFromTime"), "{error}");

        // also for a row filter that hides nodes, and inside a time graph
        let work_dir = TempDir::new().unwrap();
        let nodes = read_with(json!({
            "filter": { "node": { "name": { "where": { "ne": { "str": "http://ex/alice" } } } } }
        }));
        let schema = setup_as(&work_dir, nodes).await;
        let since_as_of = "PREFIX ex: <http://ex/> SELECT ?t { \
            GRAPH ?g { ex:bob ex:knows ex:carol } \
            BIND(raphtory:validFrom(ex:bob, ex:knows, ex:carol, ?g) AS ?t) \
        } VALUES ?g { raphtory:asof:3 }";
        let error = sparql(&schema, "", since_as_of).await.unwrap_err();
        assert!(error.contains("validFrom"), "{error}");
    }

    #[tokio::test]
    async fn unfiltered_reads_have_temporal_functions() {
        for permission in [
            GraphPermission::Read { filter: None },
            // hiding properties hides nothing RDF shows
            read_with(json!({ "hiddenProperties": { "edge": ["weight"], "node": ["age"] } })),
            read_with(json!({ "hiddenMetadata": { "node": ["kind"] } })),
            GraphPermission::Write,
        ] {
            let work_dir = TempDir::new().unwrap();
            let schema = setup_as(&work_dir, permission).await;
            assert_eq!(rows(&sparql(&schema, "", SINCE).await.unwrap()), [["1"]]);
            assert_eq!(
                rows(&sparql(&schema, "snapshotAt(time: 3)", KNOWS).await.unwrap()),
                pairs(&[("alice", "bob"), ("bob", "carol")])
            );
        }
    }

    #[tokio::test]
    async fn writers_have_temporal_functions_through_update_graph() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, GraphPermission::Write).await;
        let request = Request::new(
            r#"query Q($q: String!) { updateGraph(path: "pg") { graph { sparql(query: $q) } } }"#,
        )
        .variables(Variables::from_json(json!({ "q": SINCE })));
        let response = schema.execute(request).await;
        assert_eq!(response.errors, vec![]);
        let data = response.data.into_json().unwrap();
        let results = data["updateGraph"]["graph"]["sparql"].as_str().unwrap();
        assert_eq!(rows(results), [["1"]]);
    }

    #[tokio::test]
    async fn introspect_only_users_cannot_query() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, GraphPermission::Introspect).await;
        let request = Request::new(r#"{ graph(path: "pg") { sparql(query: "ASK {}") } }"#);
        let response = schema.execute(request).await;
        assert_eq!(response.errors, vec![]);
        assert_eq!(response.data.into_json().unwrap(), json!({ "graph": null }));
    }

    #[tokio::test]
    async fn disable_lists_disables_sparql() {
        let work_dir = TempDir::new().unwrap();
        let data = setup_with_graphs(&graphs(), work_dir.path()).await.data;
        let config = ConcurrencyConfig {
            disable_lists: true,
            ..Default::default()
        };
        let schema = App::create_schema()
            .data(data)
            .data(config)
            .finish()
            .unwrap();
        let error = sparql(&schema, "", "ASK {}").await.unwrap_err();
        assert_eq!(
            error,
            "SPARQL queries are disabled on this server (`disable_lists`)"
        );
    }

    /// A schema whose server has the concurrency configuration `config`.
    async fn setup_with_config(work_dir: &TempDir, config: ConcurrencyConfig) -> Schema {
        let data = setup_with_graphs(&graphs(), work_dir.path()).await.data;
        App::create_schema()
            .data(data)
            .data(config)
            .finish()
            .unwrap()
    }

    /// A schema whose server sets `max_sparql_query_length`.
    async fn setup_with_max_length(work_dir: &TempDir, max_length: Option<usize>) -> Schema {
        let config = ConcurrencyConfig {
            max_sparql_query_length: max_length,
            ..Default::default()
        };
        setup_with_config(work_dir, config).await
    }

    #[tokio::test]
    async fn long_queries_are_rejected() {
        let too_long = |length: usize, max: usize| {
            format!(
                "SPARQL query too long: {length} bytes, the limit is {max} bytes \
                 (`max_sparql_query_length`)"
            )
        };
        let padded = |length: usize| format!("{COUNT}{}", " ".repeat(length - COUNT.len()));
        // 16 KiB by default, also without a configuration
        let work_dirs: Vec<_> = (0..4).map(|_| TempDir::new().unwrap()).collect();
        for schema in [
            setup(&work_dirs[0]).await,
            setup_with_max_length(&work_dirs[1], Some(16 * 1024)).await,
        ] {
            let count = sparql(&schema, "", COUNT).await.unwrap();
            assert_eq!(sparql(&schema, "", &padded(16 * 1024)).await, Ok(count));
            assert_eq!(
                sparql(&schema, "", &padded(16 * 1024 + 1)).await,
                Err(too_long(16 * 1024 + 1, 16 * 1024))
            );
        }
        let schema = setup_with_max_length(&work_dirs[2], Some(COUNT.len())).await;
        assert!(sparql(&schema, "", COUNT).await.is_ok());
        assert_eq!(
            sparql(&schema, "", &padded(COUNT.len() + 1)).await,
            Err(too_long(COUNT.len() + 1, COUNT.len()))
        );
        let schema = setup_with_max_length(&work_dirs[3], None).await;
        assert!(sparql(&schema, "", &padded(100_000)).await.is_ok());
    }

    /// Thousands of nested brackets would overflow the stack and abort the server.
    #[tokio::test]
    async fn deeply_nested_queries_fail_and_the_server_keeps_working() {
        let nested = |depth: usize| {
            format!(
                "SELECT * {{ FILTER({}1{}) }}",
                "(".repeat(depth),
                ")".repeat(depth)
            )
        };
        for max_length in [Some(16 * 1024), None] {
            let work_dir = TempDir::new().unwrap();
            let schema = setup_with_max_length(&work_dir, max_length).await;
            for depth in [5_000, 100_000] {
                let query = nested(depth);
                let error = sparql(&schema, "", &query).await.unwrap_err();
                if max_length.is_some_and(|max| query.len() > max) {
                    assert!(error.starts_with("SPARQL query too long"), "{error}");
                } else {
                    assert_eq!(
                        error,
                        "SPARQL syntax error: brackets nest more than 128 deep at line 1, \
                         column 145"
                    );
                }
            }
            assert!(sparql(&schema, "", &nested(100)).await.is_ok());
            assert_eq!(
                rows(&sparql(&schema, "", COUNT).await.unwrap()),
                [["4"]],
                "the server still answers"
            );
        }
    }

    /// A long, flat query (9,000 unions) does not overflow the stack.
    #[tokio::test]
    async fn long_flat_queries_do_not_overflow_the_stack() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_with_max_length(&work_dir, None).await;
        let query = format!("SELECT * {{ {{}}{} }}", "UNION{}".repeat(9_000));
        let results = sparql(&schema, "", &query).await.unwrap();
        assert_eq!(rows(&results).len(), 9_001);
    }

    #[test]
    fn the_schema_has_sparql_on_graph() {
        let sdl = App::create_schema().finish().unwrap().sdl();
        let start = sdl.find("\ntype Graph {").expect("type Graph");
        let end = start + sdl[start..].find("\n}").unwrap();
        let graph = &sdl[start..end];
        let field = &graph[graph.find("\tsparql(").expect("sparql on Graph")..];
        let field = &field[..field.find("): ").unwrap() + "): String!".len()];
        // `sparql(query: String!, format: String): String!`, with argument descriptions between
        let lines: Vec<_> = field
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("\"\"\""))
            .collect();
        assert_eq!(lines.first(), Some(&"sparql("), "{field}");
        assert!(lines.contains(&"query: String!,"), "{field}");
        assert!(lines.contains(&"format: String"), "{field}");
        assert_eq!(lines.last(), Some(&"): String!"), "{field}");
    }

    /// A policy that grants an unfiltered read once, then fails to answer.
    struct Flaky {
        calls: AtomicUsize,
    }

    impl AuthorizationPolicy for Flaky {
        fn graph_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            _path: &str,
        ) -> Result<Option<GraphPermission>, AuthPolicyError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(Some(GraphPermission::Read { filter: None }))
            } else {
                Err(AuthPolicyError::new("the store went away"))
            }
        }

        fn namespace_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            _path: &str,
        ) -> Result<Option<NamespacePermission>, AuthPolicyError> {
            Ok(Some(NamespacePermission::Read))
        }
    }

    #[tokio::test]
    async fn the_temporal_functions_fail_closed() {
        let work_dir = TempDir::new().unwrap();
        let policy = Arc::new(Flaky {
            calls: AtomicUsize::new(0),
        });
        let schema = setup_with_policy(&graphs(), work_dir.path(), policy)
            .await
            .schema;
        // `graph` resolves (first call), the check for a row filter faults (second call)
        let error = sparql(&schema, "", SINCE).await.unwrap_err();
        assert!(error.contains("validFromTime"), "{error}");
    }

    /// A policy that adds a row filter when it refines an unfiltered read.
    struct Refining;

    impl AuthorizationPolicy for Refining {
        fn graph_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            _path: &str,
        ) -> Result<Option<GraphPermission>, AuthPolicyError> {
            Ok(Some(GraphPermission::Read { filter: None }))
        }

        fn namespace_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            _path: &str,
        ) -> Result<Option<NamespacePermission>, AuthPolicyError> {
            Ok(Some(NamespacePermission::Read))
        }

        fn refine_permission<'a>(
            &'a self,
            _ctx: &'a async_graphql::Context<'_>,
            _path: &'a str,
            _perm: GraphPermission,
        ) -> BoxFuture<'a, Result<GraphPermission, AuthPolicyError>> {
            Box::pin(std::future::ready(Ok(read_with(
                json!({ "filter": { "window": { "start": 3, "end": 6 } } }),
            ))))
        }
    }

    #[tokio::test]
    async fn refined_row_filters_count() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_with_policy(&graphs(), work_dir.path(), Arc::new(Refining))
            .await
            .schema;
        assert_eq!(
            rows(&sparql(&schema, "", KNOWS).await.unwrap()),
            pairs(&[("bob", "carol")])
        );
        let error = sparql(&schema, "", SINCE).await.unwrap_err();
        assert!(error.contains("validFromTime"), "{error}");
    }

    /// `n` triple patterns joined on `?s`.
    fn star(n: usize) -> String {
        let patterns: String = (0..n).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
        format!("SELECT ?s {{ {patterns} }}")
    }

    /// A query over the 4 triples of `pg` that effectively never finishes (4^16 solutions).
    fn slow_count() -> String {
        let patterns: String = (0..16).map(|i| format!("?s{i} ?p{i} ?o{i} . ")).collect();
        format!("SELECT (COUNT(*) AS ?n) {{ {patterns} }}")
    }

    #[tokio::test]
    async fn queries_with_too_many_triple_patterns_are_rejected() {
        let too_many = |count: usize, max: usize| {
            format!(
                "SPARQL query too complex: {count} triple patterns, the limit is {max} (each \
                 triple pattern, property path step, BIND and (... AS ?v) of SELECT or GROUP BY \
                 counts one, and each VALUES block one plus one per variable and per 100 rows) \
                 (`max_sparql_triple_patterns`)"
            )
        };
        // 100 by default, also without a configuration
        let work_dirs: Vec<_> = (0..4).map(|_| TempDir::new().unwrap()).collect();
        for schema in [
            setup(&work_dirs[0]).await,
            setup_with_config(&work_dirs[1], ConcurrencyConfig::default()).await,
        ] {
            assert!(sparql(&schema, "", &star(100)).await.is_ok());
            assert_eq!(
                sparql(&schema, "", &star(101)).await,
                Err(too_many(101, 100))
            );
            // a star whose variables are bound first plans slowly: what binds them counts
            let binds: String = (0..100)
                .map(|i| format!("BIND(1 AS ?o{i}) BIND(raphtory:p AS ?p{i}) "))
                .collect();
            let patterns: String = (0..100).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
            let bound_star = format!("SELECT * {{ {{ SELECT * {{ {binds} }} }} {patterns} }}");
            assert_eq!(
                sparql(&schema, "", &bound_star).await,
                Err(too_many(300, 100))
            );
            // a lookup of many values counts little (13 KB, under the length limit)
            let ids: String = (0..1_000).map(|i| format!("raphtory:{i} ")).collect();
            let lookup = format!("SELECT ?s ?o {{ VALUES ?s {{ {ids} }} ?s ?p ?o }}");
            assert!(sparql(&schema, "", &lookup).await.is_ok());
        }
        let config = ConcurrencyConfig {
            max_sparql_triple_patterns: Some(2),
            ..Default::default()
        };
        let schema = setup_with_config(&work_dirs[2], config).await;
        assert!(sparql(&schema, "", "ASK { ?s ?p ?o . ?o ?q ?r }")
            .await
            .is_ok());
        // a collection of 2 is 5 triple patterns
        assert_eq!(
            sparql(&schema, "", "ASK { ?s ?p (1 2) }").await,
            Err(too_many(5, 2))
        );
        let config = ConcurrencyConfig {
            max_sparql_triple_patterns: None,
            ..Default::default()
        };
        let schema = setup_with_config(&work_dirs[3], config).await;
        assert!(sparql(&schema, "", &star(101)).await.is_ok());
    }

    #[tokio::test]
    async fn slow_queries_time_out_and_the_server_keeps_answering() {
        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            sparql_timeout: Some(0.2),
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        let start = std::time::Instant::now();
        let error = sparql(&schema, "", &slow_count()).await.unwrap_err();
        assert_eq!(
            error,
            "SPARQL query timed out: it ran longer than its time limit of 200ms \
             (`sparql_timeout`)"
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(10));
        assert_eq!(
            rows(&sparql(&schema, "", COUNT).await.unwrap()),
            [["4"]],
            "the server still answers"
        );
        // fast queries do not time out
        assert_eq!(
            rows(&sparql(&schema, "snapshotAt(time: 3)", KNOWS).await.unwrap()),
            pairs(&[("alice", "bob"), ("bob", "carol")])
        );
    }

    #[tokio::test]
    async fn an_invalid_timeout_fails_every_query() {
        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            sparql_timeout: Some(-1.0),
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        assert_eq!(
            sparql(&schema, "", COUNT).await,
            Err(
                "invalid SPARQL timeout -1: expected a finite number of seconds, at least 0 \
                 (`sparql_timeout`)"
                    .to_owned()
            )
        );
    }

    /// Dropping the request future stops its query.
    #[tokio::test]
    async fn dropping_the_request_stops_the_query() {
        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            sparql_timeout: None,
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        // a query no other test runs
        let query = format!("# dropped\n{}", slow_count());
        let wait_for = |running: usize| {
            let query = query.clone();
            async move {
                let start = std::time::Instant::now();
                while crate::sparql::Running::count(&query) != running {
                    assert!(
                        start.elapsed() < std::time::Duration::from_secs(30),
                        "the query runs {} times, expected {running}",
                        crate::sparql::Running::count(&query)
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        };
        let request =
            Request::new(r#"query Q($q: String!) { graph(path: "pg") { sparql(query: $q) } }"#)
                .variables(Variables::from_json(json!({ "q": query })));
        let task = {
            let schema = schema.clone();
            tokio::spawn(async move { schema.execute(request).await })
        };
        wait_for(1).await;
        task.abort(); // drops the request future
        assert!(task.await.unwrap_err().is_cancelled());
        wait_for(0).await;
        assert_eq!(
            rows(&sparql(&schema, "", COUNT).await.unwrap()),
            [["4"]],
            "the server still answers"
        );
    }

    /// A disconnected client's query runs on, holding its `heavy_query_limit` slot, until
    /// `sparql_timeout` stops it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_query_whose_client_disconnects_runs_until_its_timeout() {
        use crate::{config::app_config::AppConfigBuilder, server::GraphServer};
        use raphtory::db::api::storage::storage::Args;
        use std::time::{Duration, Instant};
        use tokio::{io::AsyncWriteExt, net::TcpStream};

        let work_dir = TempDir::new().unwrap();
        people().encode(work_dir.path().join("pg")).unwrap();
        let config = AppConfigBuilder::new()
            .with_sparql_timeout(Some(2.0))
            .with_heavy_query_limit(Some(1))
            .build();
        let server = GraphServer::new(work_dir.path().to_path_buf(), Some(config), Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let port = running.port();

        // a query no other test runs, sent over HTTP/1.1 by a client that then goes away
        let query = format!("# disconnected\n{}", slow_count());
        let body = json!({
            "query": r#"query Q($q: String!) { graph(path: "pg") { sparql(query: $q) } }"#,
            "variables": { "q": query },
        })
        .to_string();
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        client.write_all(request.as_bytes()).await.unwrap();
        let start = Instant::now();
        while crate::sparql::Running::count(&query) == 0 {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "the query never ran"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = Instant::now();
        drop(client);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            crate::sparql::Running::count(&query),
            1,
            "the query runs on after its client disconnected"
        );

        // the next SPARQL request waits for the slot until the first query times out
        let response = reqwest::Client::new()
            .post(format!("http://localhost:{port}/"))
            .json(&json!({
                "query": r#"{ graph(path: "pg") { sparql(query: "ASK { ?s ?p ?o }") } }"#
            }))
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let waited = started.elapsed();
        let response: Json = serde_json::from_str(&response).unwrap();
        assert_eq!(
            response["data"]["graph"]["sparql"], r#"{"head":{},"boolean":true}"#,
            "{response}"
        );
        assert!(
            waited >= Duration::from_millis(1500),
            "the abandoned query held the slot until its timeout, but the next query ran after \
             {waited:?}"
        );
        assert!(waited < Duration::from_secs(30), "{waited:?}");
        assert_eq!(
            crate::sparql::Running::count(&query),
            0,
            "the timeout stopped it"
        );
        running.stop().await;
    }

    #[tokio::test]
    async fn a_timeout_too_long_to_represent_is_no_limit() {
        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            sparql_timeout: Some(1e20),
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        assert_eq!(rows(&sparql(&schema, "", COUNT).await.unwrap()), [["4"]]);
    }

    #[test]
    fn the_limits_are_configurable() {
        use crate::{
            cli::{Args, Commands},
            config::app_config::AppConfigBuilder,
        };
        use clap::Parser;

        let config = AppConfigBuilder::new().build().concurrency;
        assert_eq!(config.sparql_timeout, Some(30.0));
        assert_eq!(config.max_sparql_triple_patterns, Some(100));

        let config = AppConfigBuilder::new()
            .update_from_json(json!({
                "concurrency": { "sparql_timeout": 1.5, "max_sparql_triple_patterns": 7 }
            }))
            .unwrap()
            .build()
            .concurrency;
        assert_eq!(config.sparql_timeout, Some(1.5));
        assert_eq!(config.max_sparql_triple_patterns, Some(7));
        let config = AppConfigBuilder::new()
            .update_from_json(json!({
                "concurrency": { "sparql_timeout": 2, "max_sparql_triple_patterns": null }
            }))
            .unwrap()
            .build()
            .concurrency;
        assert_eq!(config.sparql_timeout, Some(2.0));
        assert_eq!(config.max_sparql_triple_patterns, None);
        let config = AppConfigBuilder::new()
            .update_from_json(json!({ "concurrency": { "sparql_timeout": null } }))
            .unwrap()
            .build()
            .concurrency;
        assert_eq!(config.sparql_timeout, None);
        for invalid in [json!(-1), json!(-0.5), json!("soon")] {
            let error = AppConfigBuilder::new()
                .update_from_json(json!({ "concurrency": { "sparql_timeout": invalid } }))
                .err()
                .unwrap_or_else(|| panic!("{invalid} was accepted"))
                .to_string();
            assert!(error.contains("concurrency.sparql_timeout"), "{error}");
        }
        assert!(AppConfigBuilder::new()
            .update_from_json(json!({ "concurrency": { "max_sparql_triple_patterns": -1 } }))
            .is_err());
        // a finite timeout too long for a `Duration` is a limit that is never reached
        let config = AppConfigBuilder::new()
            .update_from_json(json!({ "concurrency": { "sparql_timeout": 1e20 } }))
            .unwrap()
            .build()
            .concurrency;
        assert_eq!(config.sparql_timeout, Some(1e20));

        let server_config = |args: &[&str]| {
            let args = Args::try_parse_from(["raphtory", "server"].iter().chain(args))?;
            let Commands::Server(server) = args.command else {
                panic!("not the server command")
            };
            Ok::<_, clap::Error>(
                AppConfigBuilder::new_from_args(server.config_args)
                    .unwrap()
                    .build()
                    .concurrency,
            )
        };
        let config = server_config(&[
            "--sparql-timeout",
            "0.25",
            "--max-sparql-triple-patterns",
            "9",
        ])
        .unwrap();
        assert_eq!(config.sparql_timeout, Some(0.25));
        assert_eq!(config.max_sparql_triple_patterns, Some(9));
        assert!(server_config(&["--sparql-timeout", "-3"]).is_err());
        assert!(server_config(&["--sparql-timeout", "inf"]).is_err());
        assert_eq!(
            server_config(&["--sparql-timeout", "1e20"])
                .unwrap()
                .sparql_timeout,
            Some(1e20)
        );
    }
}
