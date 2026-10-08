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

use crate::rdf::{RdfViewOps, SparqlResults};

/// Runs a `SELECT` query and returns its rows in N-Triples form (`UNDEF` where a variable is
/// unbound), sorted.
fn select<G: RdfViewOps>(view: &G, query: &str) -> Vec<Vec<String>> {
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
fn ask<G: RdfViewOps>(view: &G, query: &str) -> bool {
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
