//! RDF import and export, storage semantics and formats.
use super::{exported, parse_graph, BLANK_NODES, DATATYPES, FOAF};
use crate::{shacl::ShaclShapes, RdfError, RdfFormat, RdfGraph, RdfView};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};
use std::{collections::BTreeSet, fs, path::Path};

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn export<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    format: RdfFormat,
) -> Result<String, RdfError> {
    view.to_rdf_string(format)
}

fn node_names<G: StaticGraphViewOps>(g: &G) -> BTreeSet<String> {
    g.nodes().into_iter().map(|n| n.name()).collect()
}

fn layer_names<G: StaticGraphViewOps>(g: &G) -> BTreeSet<String> {
    g.unique_layers().map(|l| l.to_string()).collect()
}

#[test]
fn load_foaf_turtle() {
    let rg = RdfGraph::new();
    let n = rg
        .load_rdf(1, FOAF.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    assert_eq!(n, 15);
    let pg = rg.graph();

    // 15 triples on 14 (src, dst) pairs: alice knows and works with bob.
    assert_eq!(pg.count_edges(), 14);
    let layered_edges: usize = pg
        .edges()
        .into_iter()
        .map(|e| e.explode_layers().into_iter().count())
        .sum();
    assert_eq!(layered_edges, 15);
    let alice_bob = pg
        .edge(
            "http://example.org/people/alice",
            "http://example.org/people/bob",
        )
        .unwrap();
    assert_eq!(
        alice_bob
            .layer_names()
            .into_iter()
            .map(|l| l.to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "http://example.org/rel/worksWith".to_owned(),
            "http://xmlns.com/foaf/0.1/knows".to_owned(),
        ])
    );

    // `42` (shorthand) and `"42"^^xsd:integer` are the same literal, so one node.
    let age = format!("\"42\"^^<{XSD_INTEGER}>");
    assert_eq!(pg.node(age.as_str()).unwrap().in_degree(), 2);
    assert_eq!(alice_bob.history().t().collect(), vec![1, 1]);
}

#[test]
fn blank_nodes() {
    let rg = RdfGraph::new();
    rg.load_rdf(1, BLANK_NODES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    rg.load_rdf(2, BLANK_NODES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let blanks: Vec<String> = node_names(rg.graph())
        .into_iter()
        .filter(|n| n.starts_with("_:"))
        .collect();
    // `_:x` and `[]` in each of the two loads
    assert_eq!(blanks.len(), 4, "{blanks:?}");
    assert!(!blanks.contains(&"_:x".to_string()));
    assert_eq!(exported(&rg).len(), 4);

    let line = exported(&rg)
        .into_iter()
        .find(|l| l.contains("<http://ex/p>"))
        .unwrap();
    let label = line.split(' ').next().unwrap().to_owned();
    assert!(label.starts_with("_:"));
    let n = rg
        .retract_rdf(3, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(n, 1);

    let after = exported(&rg.valid());
    assert_eq!(after.len(), 3);
    assert!(!after.contains(&line));
    assert_eq!(
        after.iter().filter(|l| l.contains("<http://ex/p>")).count(),
        1
    );
    // the node keeps its label
    assert!(rg.graph().node(label.as_str()).is_some());
}

#[test]
fn failed_loads_keep_earlier_triples() {
    // integer node ids cannot hold names
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> .";
    let err = RdfGraph::from(g.clone())
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(matches!(err, RdfError::Graph(_)), "{err:?}");
    assert_eq!(g.count_edges(), 1);

    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .
<raphtory:%61> <http://ex/p> <http://ex/o> .
<http://ex/c> <http://ex/p> <http://ex/d> .";
    let rg = RdfGraph::new();
    let err = rg
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(matches!(err, RdfError::NonCanonicalTerm(_)), "{err:?}");
    assert_eq!(rg.graph().count_edges(), 1);
}

#[test]
fn base_iri() {
    let rg = RdfGraph::new();
    rg.load_rdf(
        1,
        "<s> <p> <o> .".as_bytes(),
        RdfFormat::Turtle,
        Some("http://ex/dir/doc"),
    )
    .unwrap();
    assert_eq!(
        exported(&rg),
        ["<http://ex/dir/s> <http://ex/dir/p> <http://ex/dir/o> ."]
    );
}

#[test]
fn export_round_trips_blank_free_fixtures() {
    for (data, format) in [(FOAF, RdfFormat::Turtle), (DATATYPES, RdfFormat::NTriples)] {
        let input = parse_graph(data, format);
        let rg = RdfGraph::new();
        let n = rg.load_rdf(1, data.as_bytes(), format, None).unwrap();
        assert!(n >= input.len());

        assert_eq!(exported(&rg).len(), input.len());
        for format in [RdfFormat::NTriples, RdfFormat::RdfXml] {
            let doc = export(&rg, format).unwrap();
            assert_eq!(parse_graph(&doc, format), input, "{format}");
        }
    }
}

#[test]
fn plain_graph_round_trip() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
    g.add_edge(2, "Alice Smith", "12:30", NO_PROPS, Some("has time"))
        .unwrap();
    g.add_edge(
        3,
        "http://ex/carol",
        "Alice",
        NO_PROPS,
        Some("http://ex/knows"),
    )
    .unwrap();
    g.add_edge(
        4,
        "user:42",
        "raphtory:x",
        NO_PROPS,
        Some("rel:with:colons"),
    )
    .unwrap();
    g.add_edge(5, "", "\"x\"@EN", NO_PROPS, Some("42")).unwrap();
    g.add_edge(6, "Bob", "\"lit\"", NO_PROPS, Some("_default"))
        .unwrap();
    g.add_edge(7, "Bob", "Bob", NO_PROPS, Some("self")).unwrap();
    // `:` is not allowed in Turtle or SPARQL blank-node labels, so these are not blank nodes
    g.add_edge(8, "_:a:b", "_::x", NO_PROPS, Some("_:p:q"))
        .unwrap();

    let u64_graph = Graph::new();
    u64_graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    u64_graph.add_edge(2, 42, 1, NO_PROPS, Some("x y")).unwrap();
    u64_graph.add_edge(3, 2, 2, NO_PROPS, None).unwrap();

    for original in [RdfGraph::from(g), RdfGraph::from(u64_graph)] {
        let first = export(&original, RdfFormat::NTriples).unwrap();
        assert_eq!(first.lines().count(), original.graph().count_edges());

        let reloaded = RdfGraph::new();
        reloaded
            .load_rdf(0, first.as_bytes(), RdfFormat::NTriples, None)
            .unwrap_or_else(|e| panic!("export does not load: {e}\n{first}"));
        assert_eq!(node_names(reloaded.graph()), node_names(original.graph()));
        assert_eq!(layer_names(reloaded.graph()), layer_names(original.graph()));

        assert_eq!(exported(&reloaded), exported(&original));

        // the export is also a retraction document for every triple it contains
        let n = reloaded
            .retract_rdf(1, first.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
        assert_eq!(n, first.lines().count());
        assert!(exported(&reloaded.valid()).is_empty());
    }
}

#[test]
fn rdf_formats() {
    for (s, expected) in [
        (" .TTL ", RdfFormat::Turtle),
        ("n-triples", RdfFormat::NTriples),
        ("text/turtle; charset=utf-8", RdfFormat::Turtle),
    ] {
        assert_eq!(RdfFormat::parse(s).unwrap(), expected, "{s}");
    }
    // a lone `"` as a parameter value would make oxigraph panic
    let s = "text/turtle;profile=\"";
    let err = RdfFormat::parse(s).unwrap_err();
    assert!(
        matches!(&err, RdfError::UnknownFormat(f) if f == s),
        "{err:?}"
    );
}

#[test]
fn files() {
    let dir = tempfile::tempdir().unwrap();
    let ttl = dir.path().join("foaf.ttl");
    let bare = dir.path().join("foaf");
    let rg = RdfGraph::new();
    let n = rg
        .load_rdf(1, FOAF.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    rg.to_rdf_path(&ttl, None).unwrap();
    rg.to_rdf_path(&bare, None).unwrap();
    let read = |path: &Path| fs::read_to_string(path).unwrap();
    assert_ne!(read(&ttl), read(&bare));
    assert_eq!(
        parse_graph(&read(&ttl), RdfFormat::Turtle),
        parse_graph(&read(&bare), RdfFormat::NTriples)
    );
    assert_eq!(RdfFormat::from_path(&ttl).unwrap(), RdfFormat::Turtle);

    let copy = RdfGraph::new();
    let error = copy.load_rdf_path(1, &bare, None, None).unwrap_err();
    assert!(matches!(error, RdfError::NoPathFormat(_)), "{error:?}");
    assert_eq!(
        copy.load_rdf_path(1, &bare, RdfFormat::NTriples, None)
            .unwrap(),
        n
    );
    assert_eq!(exported(&copy), exported(&rg));
    assert_eq!(copy.retract_rdf_path(2, &ttl, None, None).unwrap(), n);
    assert!(exported(&copy.valid()).is_empty());

    let shapes = dir.path().join("shapes.ttl");
    fs::write(
        &shapes,
        "<http://ex/S> a <http://www.w3.org/ns/shacl#NodeShape> .",
    )
    .unwrap();
    assert!(ShaclShapes::parse_path(&shapes, None, None).is_ok());
}
