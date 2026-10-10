//! Shared GraphQL test scaffolding: schema/fixture builders and single-mutation
//! runners used by the integration-style tests in `lib.rs`. Gated by `#[cfg(test)]`
//! and only intended for in-crate consumers.

#![allow(dead_code)]

use crate::{
    auth::Access, auth_policy::AuthorizationPolicy, config::app_config::AppConfig, data::Data,
    model::App,
};
use async_graphql::dynamic::Schema;
use dynamic_graphql::Request;
use raphtory::db::api::{storage::storage::Args, view::MaterializedGraph};
use raphtory_api::core::storage::graph_folder::{DIRTY_PATH, ROOT_META_PATH};
use std::{path::Path, sync::Arc};

pub(crate) struct TestSetup {
    pub(crate) data: Data,
    pub(crate) schema: Schema,
}

pub(crate) async fn setup_with_graphs(
    graphs: &[(&str, MaterializedGraph)],
    work_dir: &Path,
) -> TestSetup {
    let data = Data::new(work_dir, &AppConfig::default(), Args::default());
    for (path, graph) in graphs {
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { data, schema }
}

pub(crate) async fn setup_with_policy(
    graphs: &[(&str, MaterializedGraph)],
    work_dir: &Path,
    policy: Arc<dyn AuthorizationPolicy>,
) -> TestSetup {
    let mut data = Data::new(work_dir, &AppConfig::default(), Args::default());
    for (path, graph) in graphs {
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    data.set_auth_policy(policy);
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { data, schema }
}

pub(crate) async fn run_mutation(schema: &Schema, query: &str) -> async_graphql::Response {
    let req = Request::new(query).data(Access::Rw);
    schema.execute(req).await
}

pub(crate) async fn run_mutation_as_user(schema: &Schema, query: &str) -> async_graphql::Response {
    // No `Access::Rw` injected, so the policy decides allow/deny. `FakePolicy` resolves
    // permissions by path alone, so no identity needs to travel in the request.
    let req = Request::new(query);
    schema.execute(req).await
}

/// Assert that `path` is an existing directory that is NOT a graph folder
/// (no `ROOT_META_PATH`) and contains no leftover `DIRTY_PATH` marker.
pub(crate) fn assert_is_namespace_dir(path: &Path) {
    assert!(path.is_dir(), "expected directory at {:?}", path);
    assert!(
        !path.join(ROOT_META_PATH).exists(),
        "{:?} contains a graph metadata file ({}); expected a plain namespace directory",
        path,
        ROOT_META_PATH,
    );
    assert!(
        !path.join(DIRTY_PATH).exists(),
        "{:?} contains a dirty marker ({}); expected a clean namespace directory",
        path,
        DIRTY_PATH,
    );
}

/// Fixtures of the RDF tests.
#[cfg(feature = "rdf")]
pub(crate) mod rdf {
    use crate::{auth_policy::GraphPermission, model::graph::filtering::GraphAccessFilter};
    use oxigraph::io::RdfParser;
    use raphtory::{prelude::*, rdf::RdfFormat};
    use serde_json::Value as Json;

    pub(crate) const PEOPLE: &str = r#"
        @prefix ex: <http://ex/> .
        ex:alice ex:knows ex:bob ; ex:age 42 .
        ex:bob ex:knows ex:carol .
        ex:carol ex:name "Carol" .
    "#;
    pub(crate) const KNOWS: &str =
        "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o";
    pub(crate) const COUNT: &str = "SELECT (COUNT(*) AS ?n) { ?s ?p ?o }";
    pub(crate) const KNOWN_BY: &str =
        "PREFIX ex: <http://ex/> CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }";
    /// When `bob knows carol` started holding: at 1.
    pub(crate) const SINCE: &str = "PREFIX ex: <http://ex/> \
        SELECT (raphtory:validFromTime(ex:bob, ex:knows, ex:carol) AS ?t) {}";
    /// The `knows` pairs of [`people`] now.
    pub(crate) const NOW: &[(&str, &str)] = &[("bob", "carol"), ("dave", "alice")];
    /// The `knows` pairs of [`people`] as of 3.
    pub(crate) const AS_OF_3: &[(&str, &str)] = &[("alice", "bob"), ("bob", "carol")];

    /// Asserted at 1: `alice knows bob`, `alice age 42`, `bob knows carol`, `carol name "Carol"`.
    /// `alice knows bob` is retracted at 5, and `dave knows alice` asserted at 7.
    pub(crate) fn people() -> PersistentGraph {
        let rg = RdfGraph::new();
        rg.load_rdf(1, PEOPLE.as_bytes(), RdfFormat::Turtle, None)
            .unwrap();
        let triple =
            |s: &str, o: &str| format!("<http://ex/{s}> <http://ex/knows> <http://ex/{o}> .");
        rg.retract_rdf(
            5,
            triple("alice", "bob").as_bytes(),
            RdfFormat::NTriples,
            None,
        )
        .unwrap();
        rg.load_rdf(
            7,
            triple("dave", "alice").as_bytes(),
            RdfFormat::NTriples,
            None,
        )
        .unwrap();
        rg.graph().clone()
    }

    /// The values of the solutions of SPARQL Results JSON, `""` for an unbound variable.
    pub(crate) fn rows(results: &str) -> Vec<Vec<String>> {
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

    /// The rows of `knows` pairs.
    pub(crate) fn pairs(pairs: &[(&str, &str)]) -> Vec<Vec<String>> {
        pairs
            .iter()
            .map(|(s, o)| vec![format!("http://ex/{s}"), format!("http://ex/{o}")])
            .collect()
    }

    /// The triples of an RDF document, in N-Triples form and sorted.
    pub(crate) fn triples(document: &str, format: RdfFormat) -> Vec<String> {
        let mut triples: Vec<String> = RdfParser::from_format(format)
            .for_slice(document.as_bytes())
            .map(|quad| format!("{} .", quad.unwrap()))
            .collect();
        triples.sort();
        triples
    }

    /// A row-filtered read permission (see [`GraphAccessFilter`]).
    pub(crate) fn read_with(filter: Json) -> GraphPermission {
        GraphPermission::Read {
            filter: Some(serde_json::from_value::<GraphAccessFilter>(filter).unwrap()),
        }
    }

    /// A query over the triples of [`people`] that effectively never finishes (at least 4^16
    /// solutions), tagged so no other test runs the same query.
    pub(crate) fn slow_count(tag: &str) -> String {
        let patterns: String = (0..16).map(|i| format!("?s{i} ?p{i} ?o{i} . ")).collect();
        format!("# {tag}\nSELECT (COUNT(*) AS ?n) {{ {patterns} }}")
    }
}
