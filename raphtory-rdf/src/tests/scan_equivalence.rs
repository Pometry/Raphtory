//! Export and SPARQL see exactly the `(edge, layer)` pairs of Raphtory's edge iterator over a
//! view, for a full scan and for every combination of bound terms.
use super::{exported, select};
use crate::{mapping::iri_of, term_of, RdfView};
use raphtory::{
    db::api::view::{DynamicGraph, IntoDynamic},
    prelude::*,
};

const NODES: [&str; 3] = ["a", "b", "c"];
const LAYERS: [&str; 3] = ["p", "q", "r"];

type Spo = (String, String, String);

fn view_triples(view: &DynamicGraph) -> Vec<Spo> {
    let mut triples: Vec<Spo> = view
        .edges()
        .explode_layers()
        .into_iter()
        .map(|e| {
            (
                term_of(&e.src().name()).to_string(),
                iri_of(&e.layer_name().unwrap()).to_string(),
                term_of(&e.dst().name()).to_string(),
            )
        })
        .collect();
    triples.sort();
    triples
}

// as SPARQL rows of the unbound variables among `?s ?p ?o`
fn rows(
    triples: &[Spo],
    keep: impl Fn(&Spo) -> bool,
    bound: (bool, bool, bool),
) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = triples
        .iter()
        .filter(|t| keep(t))
        .map(|(s, p, o)| {
            [(s, bound.0), (p, bound.1), (o, bound.2)]
                .into_iter()
                .filter(|(_, bound)| !bound)
                .map(|(term, _)| term.clone())
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn reads_match_the_edges_of_the_view() {
    let pg = PersistentGraph::new();
    for (t, s, o, layer) in [
        (1, "a", "b", "p"),
        (1, "a", "b", "q"),
        (2, "b", "c", "p"),
        (2, "c", "a", "r"),
        (3, "a", "a", "q"),
        (8, "c", "b", "p"),
    ] {
        pg.add_edge(t, s, o, NO_PROPS, Some(layer)).unwrap();
    }
    pg.delete_edge(4, "b", "c", Some("p")).unwrap();
    let view = pg.window(0, 6).valid_layers(["p", "q"]).into_dynamic();
    let triples = view_triples(&view);
    let rdf = RdfView::from_view(view).unwrap();

    let lines: Vec<String> = triples
        .iter()
        .map(|(s, p, o)| format!("{s} {p} {o} ."))
        .collect();
    assert_eq!(exported(&rdf), lines, "to_rdf");
    assert_eq!(
        select(&rdf, "SELECT ?s ?p ?o { ?s ?p ?o }"),
        rows(&triples, |_| true, (false, false, false))
    );
    for node in NODES {
        let term = term_of(node).to_string();
        let query = format!("SELECT ?p ?o {{ {term} ?p ?o }}");
        let expected = rows(&triples, |(s, _, _)| *s == term, (true, false, false));
        assert_eq!(select(&rdf, &query), expected, "{query}");
        let query = format!("SELECT ?s ?p {{ ?s ?p {term} }}");
        let expected = rows(&triples, |(_, _, o)| *o == term, (false, false, true));
        assert_eq!(select(&rdf, &query), expected, "{query}");
        for object in NODES {
            let object = term_of(object).to_string();
            let query = format!("SELECT ?p {{ {term} ?p {object} }}");
            let expected = rows(
                &triples,
                |(s, _, o)| *s == term && *o == object,
                (true, false, true),
            );
            assert_eq!(select(&rdf, &query), expected, "{query}");
        }
    }
    for layer in LAYERS {
        let term = iri_of(layer).to_string();
        let query = format!("SELECT ?s ?o {{ ?s {term} ?o }}");
        let expected = rows(&triples, |(_, p, _)| *p == term, (false, true, false));
        assert_eq!(select(&rdf, &query), expected, "{query}");
    }
}
