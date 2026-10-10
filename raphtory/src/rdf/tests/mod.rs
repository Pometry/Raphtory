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
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    rdf::{layer_name_of, model::NamedNode, RdfFormat, RdfView, SparqlResults},
};
use oxigraph::io::RdfParser;

/// The layer name of the predicate `iri`.
fn layer_of(iri: &str) -> String {
    layer_name_of(NamedNode::new(iri).unwrap().as_ref().into()).unwrap()
}

/// Whether this build parses RDF 1.2 (triple terms, directional literals). Checked at run time
/// because oxigraph features unify across the workspace.
fn rdf12() -> bool {
    RdfParser::from_format(RdfFormat::NTriples)
        .for_slice("<http://e/a> <http://e/p> <<( <http://e/s> <http://e/p> <http://e/o> )>> .")
        .all(|quad| quad.is_ok())
}

/// Runs a `SELECT` query and returns its rows in N-Triples form (`UNDEF` where a variable is
/// unbound), sorted.
fn select<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> Vec<Vec<String>> {
    match view
        .sparql(query)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Solutions { rows, .. } => {
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
        other => panic!("{query}: not a SELECT result: {other:?}"),
    }
}

/// Runs an `ASK` query.
fn ask<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> bool {
    match view
        .sparql(query)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Boolean(value) => value,
        other => panic!("{query}: not an ASK result: {other:?}"),
    }
}

/// Runs `f` on another thread and fails (instead of hanging) if it does not finish in time.
fn within_60s<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || tx.send(f()).unwrap());
    rx.recv_timeout(std::time::Duration::from_secs(60))
        .expect("deadlock: did not finish within 60 s")
}
