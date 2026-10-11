//! Shared GraphQL test scaffolding: schema/fixture builders and single-mutation
//! runners used by the in-crate tests. Gated by `#[cfg(test)]`.

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
    use oxigraph::{
        io::{RdfFormat as OxRdfFormat, RdfParser},
        model::Graph,
        sparql::results::{QueryResultsFormat, QueryResultsParser, SliceQueryResultsParserOutput},
    };
    use raphtory::prelude::PersistentGraph;
    use raphtory_rdf::{NamedNode, RdfFormat, RdfGraph, Term, Triple};
    use serde_json::{from_value, json};

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
    /// The triples of `KNOWN_BY` on the current state of [`people`], in N-Triples.
    pub(crate) const KNOWN_BY_NOW: &str = "\
        <http://ex/alice> <http://ex/knownBy> <http://ex/dave> .
        <http://ex/carol> <http://ex/knownBy> <http://ex/bob> .";
    /// When `bob knows carol` started holding: at 1.
    pub(crate) const SINCE: &str = "PREFIX ex: <http://ex/> \
        SELECT (raphtory:validFromTime(ex:bob, ex:knows, ex:carol) AS ?t) {}";
    /// The `knows` pairs of [`people`] now.
    pub(crate) const NOW: &[(&str, &str)] = &[("bob", "carol"), ("dave", "alice")];
    /// The `knows` pairs of [`people`] as of 3.
    pub(crate) const AS_OF_3: &[(&str, &str)] = &[("alice", "bob"), ("bob", "carol")];

    /// The IRI `http://ex/<name>`.
    pub(crate) fn ex(name: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("http://ex/{name}"))
    }

    /// Asserted at 1: `alice knows bob`, `alice age 42`, `bob knows carol`, `carol name "Carol"`.
    /// `alice knows bob` is retracted at 5, and `dave knows alice` asserted at 7.
    pub(crate) fn people() -> PersistentGraph {
        let rg = RdfGraph::new();
        rg.load_rdf(1, PEOPLE.as_bytes(), RdfFormat::Turtle, None)
            .unwrap();
        let knows = |s, o| Triple::new(ex(s), ex("knows"), ex(o));
        rg.retract_triple(5, &knows("alice", "bob")).unwrap();
        rg.add_triple(7, &knows("dave", "alice")).unwrap();
        rg.graph().clone()
    }

    /// The values of the solutions of SPARQL Results JSON, `None` for an unbound variable.
    pub(crate) fn rows(results: &str) -> Vec<Vec<Option<Term>>> {
        let parser = QueryResultsParser::from_format(QueryResultsFormat::Json);
        let SliceQueryResultsParserOutput::Solutions(solutions) =
            parser.for_slice(results).unwrap()
        else {
            panic!("not the results of a SELECT query: {results}")
        };
        solutions
            .map(|solution| solution.unwrap().values().to_vec())
            .collect()
    }

    /// The rows of `knows` pairs.
    pub(crate) fn pairs(pairs: &[(&str, &str)]) -> Vec<Vec<Option<Term>>> {
        pairs
            .iter()
            .map(|(s, o)| vec![Some(ex(s).into()), Some(ex(o).into())])
            .collect()
    }

    /// The triples of an RDF document.
    pub(crate) fn graph(document: &str, format: OxRdfFormat) -> Graph {
        RdfParser::from_format(format)
            .for_slice(document.as_bytes())
            .map(|quad| Triple::from(quad.unwrap()))
            .collect()
    }

    /// Read access to every node but `ex:carol` (see [`GraphAccessFilter`]).
    pub(crate) fn without_carol() -> GraphPermission {
        let filter = json!({
            "filter": { "node": { "name": { "where": { "ne": { "str": "http://ex/carol" } } } } }
        });
        GraphPermission::Read {
            filter: Some(from_value::<GraphAccessFilter>(filter).unwrap()),
        }
    }
}
