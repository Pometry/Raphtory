//! `load_rdf` of N-Triples: the fast naming path.
use crate::{
    db::{
        api::view::{IntoDynamic, StaticGraphViewOps},
        graph::assertions::assert_graph_equal,
    },
    prelude::*,
    rdf::{model::Triple, RdfFormat},
};
use oxigraph::io::RdfParser;
use std::collections::BTreeSet;

const NT: RdfFormat = RdfFormat::NTriples;

/// The sorted N-Triples lines `to_rdf` writes for a view.
fn exported<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, NT, &[]).unwrap();
    let mut lines: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

/// The N-Triples fast naming path gives every term and predicate the name and layer name the
/// full mapping gives it, so a load matches `add_triple` (including `raphtory:` IRIs,
/// look-alikes, escapes, literals and escape-heavy predicates).
#[test]
fn n_triples_names_match_the_full_mapping() {
    let doc = r#"<http://ex/a> <http://ex/p> <http://ex/b> .
<raphtory:a> <http://ex/p> <raphtory:Alice%20Smith> .
<http://ex/a> <raphtory:_default> <raphtory:caf%C3%A9> .
<raphtory:> <raphtory:a%20b> <raphtory:%22x> .
<Raphtory:a> <raphtoryx:p> <raphtory-a:b> .
<urn:raphtory:a> <http://ex/p> <mailto:a@example.org> .
<http://ex/caf%C3%A9> <http://ex/p> <http://ex/café> .
<http://ex/é> <http://ex/p> <http://ex/\U0001F600> .
<http://ex/a> <http://ex/p> "x" .
<http://ex/a> <http://ex/p> "x"@en .
<http://ex/a> <http://ex/p> "042"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/a> <http://ex/p> "<raphtory:a>" .
<http://ex/a> <http://ex/p> <http://ex/a> .
<http://ex/alice> <http://ex/a%2Fb> <http://ex/c%2Fd> .
<http://ex/alice> <http://ex/100%25> "1" .
<http://ex/bob> <http://ex/q?a=*&b=%3A> "3" .
"#;
    let triples: Vec<Triple> = RdfParser::from_format(NT)
        .for_reader(doc.as_bytes())
        .map(|q| q.unwrap().into())
        .collect();
    assert_eq!(triples.len(), 16);
    let reference = RdfGraph::new();
    for triple in &triples {
        reference.add_triple(1, triple).unwrap();
    }
    let names: BTreeSet<String> = reference.graph().nodes().iter().map(|n| n.name()).collect();
    assert!(names.contains("Alice Smith") && names.contains("café"));
    assert!(names.contains("Raphtory:a") && names.contains("raphtory-a:b"));
    let rg = RdfGraph::new();
    // one load per line, in the same order as the `add_triple` calls above
    for line in doc.lines() {
        rg.load_rdf(1, line.as_bytes(), NT, None).unwrap();
    }
    assert_graph_equal(rg.graph(), reference.graph());
    let rg = RdfGraph::new();
    assert_eq!(rg.load_rdf(1, doc.as_bytes(), NT, None).unwrap(), 16);
    assert_eq!(exported(&rg), exported(&reference));
}
