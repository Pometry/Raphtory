mod bounds;
mod functions;
mod import_export;
mod limits;
mod loading;
mod mapping;
mod oracle;
mod rdf12;
mod results;
mod scan_equivalence;
#[cfg(feature = "shacl")]
mod shacl;
#[cfg(feature = "shacl")]
mod shacl_report;
mod temporal;
mod time_graph;

use crate::{
    mapping::layer_name_of, NamedNode, RdfFormat, RdfView, SparqlOptions, SparqlResults, Term,
    Triple,
};

use oxigraph::{io::RdfParser, model::Graph as OxGraph};
use raphtory::db::api::view::{IntoDynamic, StaticGraphViewOps};
use std::{
    panic,
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::Duration,
};

const FOAF: &str = include_str!("fixtures/foaf.ttl");
const DATATYPES: &str = include_str!("fixtures/datatypes.nt");
const BLANK_NODES: &str = include_str!("fixtures/blank_nodes.ttl");
const SOCIAL: &str = include_str!("fixtures/social.ttl");

/// The layer name of the predicate `iri`.
fn layer_of(iri: &str) -> String {
    layer_name_of(NamedNode::new(iri).unwrap().as_ref().into()).unwrap()
}

/// Solution rows in N-Triples form (`UNDEF` where a variable is unbound), sorted.
fn sorted_rows(rows: &[Vec<Option<Term>>]) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map_or_else(|| "UNDEF".to_owned(), ToString::to_string)
                })
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// Runs a `SELECT` query and returns its [`sorted_rows`].
fn select<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> Vec<Vec<String>> {
    match view
        .sparql(query, &SparqlOptions::default())
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Solutions { rows, .. } => sorted_rows(&rows),
        other => panic!("{query}: not a SELECT result: {other:?}"),
    }
}

/// Runs an `ASK` query.
fn ask<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> bool {
    match view
        .sparql(query, &SparqlOptions::default())
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Boolean(value) => value,
        other => panic!("{query}: not an ASK result: {other:?}"),
    }
}

/// The sorted N-Triples lines `to_rdf` writes for a view.
fn exported<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, RdfFormat::NTriples, &[]).unwrap();
    let mut lines: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

/// Parses a document into a set of triples, keeping blank-node labels.
fn parse_graph(doc: &str, format: RdfFormat) -> OxGraph {
    RdfParser::from_format(format.to_oxigraph())
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap_or_else(|e| panic!("{e}: {doc}"))))
        .collect()
}

/// Runs `f` on another thread and fails (instead of hanging) if it does not finish in time.
fn within_60s<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || tx.send(f()).unwrap());
    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(v) => v,
        Err(RecvTimeoutError::Disconnected) => panic::resume_unwind(handle.join().unwrap_err()),
        Err(RecvTimeoutError::Timeout) => panic!("deadlock: did not finish within 60 s"),
    }
}
