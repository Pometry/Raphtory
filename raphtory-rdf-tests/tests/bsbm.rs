//! The BSBM dataset of 1,000 products (`dataset-1000.nt.bz2` of Zenodo record 12663333) through
//! `load_rdf` and `to_rdf`: the export is the same set of triples as the source.
//!
//! `make rust-test-rdf-bsbm` downloads the data into `~/.cache/raphtory-rdf/bsbm` and runs this
//! with `RAPHTORY_RDF_DATA` set.
#[path = "common/data.rs"]
mod data;

use bzip2::read::MultiBzDecoder;
use data::{data_dir, parse};
use oxigraph::model::{dataset::CanonicalizationAlgorithm, Graph as OxGraph};
use raphtory_rdf::{RdfFormat, RdfGraph};
use std::{fs::File, io::Read};

/// The triples of an N-Triples document, with canonical blank-node labels.
fn canonical(doc: &[u8]) -> OxGraph {
    let mut graph: OxGraph = parse(doc, RdfFormat::NTriples)
        .unwrap()
        .into_iter()
        .collect();
    graph.canonicalize(CanonicalizationAlgorithm::Unstable);
    graph
}

#[test]
#[ignore = "needs the BSBM download: make rust-test-rdf-bsbm"]
fn bsbm_1000_to_rdf_round_trip() {
    let path = data_dir("bsbm")
        .unwrap_or_else(|e| panic!("{e}"))
        .join("dataset-1000.nt.bz2");
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut source = Vec::new();
    MultiBzDecoder::new(file).read_to_end(&mut source).unwrap();

    let rg = RdfGraph::new();
    rg.load_rdf(1, source.as_slice(), RdfFormat::NTriples, None)
        .unwrap();
    let mut exported = Vec::new();
    let stats = rg.to_rdf(&mut exported, RdfFormat::NTriples).unwrap();

    let expected = canonical(&source);
    assert_eq!((stats.written, stats.skipped), (expected.len(), 0));
    assert!(
        canonical(&exported) == expected,
        "the export is not the set of triples of {}",
        path.display()
    );
}
