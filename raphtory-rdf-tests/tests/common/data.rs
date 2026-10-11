//! What the readers of the downloaded RDF data share: where the data is, and N-Triples in and
//! out.
//!
//! Included with `#[path]` by `tests/bear.rs` and the benchmarks in `benches/` next to their data
//! readers, so it only uses `raphtory-rdf`, `oxigraph` and `std`.
#![allow(dead_code)]
use oxigraph::io::{RdfFormat as OxRdfFormat, RdfParser};
use raphtory_rdf::{RdfFormat, Triple};
use std::{env, path::PathBuf};

/// The environment variable that points at the directory with the downloaded RDF data (the
/// `RDF_DATA_DIR` of the Makefile).
pub const DATA_ENV: &str = "RAPHTORY_RDF_DATA";

/// The subdirectory `name` of `$RAPHTORY_RDF_DATA`, or why it is not set.
pub fn data_dir(name: &str) -> Result<PathBuf, String> {
    env::var_os(DATA_ENV)
        .map(|root| PathBuf::from(root).join(name))
        .ok_or_else(|| {
            format!(
                "{DATA_ENV} is not set; set it to the directory the `make *-fetch` targets \
                 download to (~/.cache/raphtory-rdf by default)"
            )
        })
}

/// The triples of a document.
pub fn parse(doc: &[u8], format: RdfFormat) -> Result<Vec<Triple>, String> {
    let format = OxRdfFormat::from_media_type(format.media_type()).expect("an RDF media type");
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
