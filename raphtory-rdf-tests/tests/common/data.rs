//! What the readers of the downloaded RDF data share: where the data is, and N-Triples in and
//! out.
//!
//! Included with `#[path]` by `raphtory-rdf-tests/tests/bear.rs` and the `rdf-bench` benchmarks
//! next to their data readers, so it only uses `raphtory`, `oxigraph` and `std`.
#![allow(dead_code)]
use oxigraph::io::RdfParser;
use raphtory::rdf::{model::Triple, RdfFormat};
use std::path::PathBuf;

/// The environment variable that points at the directory with the downloaded RDF data (the
/// `RDF_DATA_DIR` of the `rdf-bench` Makefile).
pub const DATA_ENV: &str = "RAPHTORY_RDF_DATA";

/// The subdirectory `name` of `$RAPHTORY_RDF_DATA`, or why it is not set.
pub fn data_dir(name: &str) -> Result<PathBuf, String> {
    std::env::var_os(DATA_ENV)
        .map(|root| PathBuf::from(root).join(name))
        .ok_or_else(|| {
            format!(
                "{DATA_ENV} is not set; set it to the directory the rdf-bench Makefile downloads \
                 to (~/.cache/raphtory-rdf by default)"
            )
        })
}

/// The triples of a document.
pub fn parse(doc: &[u8], format: RdfFormat) -> Result<Vec<Triple>, String> {
    RdfParser::from_format(format)
        .for_slice(doc)
        .map(|quad| quad.map(Triple::from).map_err(|e| e.to_string()))
        .collect()
}

/// Triples as an N-Triples document.
pub fn ntriples<'a>(triples: impl IntoIterator<Item = &'a Triple>) -> Vec<u8> {
    let mut doc = String::new();
    for triple in triples {
        doc.push_str(&triple.to_string());
        doc.push_str(" .\n");
    }
    doc.into_bytes()
}
