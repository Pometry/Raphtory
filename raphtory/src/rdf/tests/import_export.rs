//! RDF import and export, storage semantics, scans and formats.
use crate::{
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{self, BlankNode, Literal, NamedNode, Triple, TripleRef},
        parse_rdf_format,
        scan::{Dir, EdgeScan},
        serializer_with_prefixes, RdfError, RdfFormat, RdfParser, RdfSerializer,
    },
};
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, VID};
use std::collections::BTreeSet;

const FOAF: &str = include_str!("fixtures/foaf.ttl");
const DATATYPES: &str = include_str!("fixtures/datatypes.nt");
const BLANK_NODES: &str = include_str!("fixtures/blank_nodes.ttl");
const DEFAULT_GRAPH_TRIG: &str = include_str!("fixtures/default_graph.trig");

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn export<G: RdfViewOps>(view: &G, serializer: impl Into<RdfSerializer>) -> String {
    let mut out = Vec::new();
    view.to_rdf(&mut out, serializer).unwrap();
    String::from_utf8(out).unwrap()
}

fn nt<G: RdfViewOps>(view: &G) -> String {
    export(view, RdfFormat::NTriples)
}

fn sorted_lines(s: &str) -> Vec<&str> {
    let mut lines: Vec<_> = s.lines().collect();
    lines.sort_unstable();
    lines
}

/// Parses a document into an `oxrdf::Graph` (a set of triples), keeping blank-node labels.
fn parse_graph(data: &str, format: RdfFormat) -> model::Graph {
    let mut graph = model::Graph::new();
    for quad in RdfParser::from_format(format).for_reader(data.as_bytes()) {
        let triple: Triple = quad.unwrap().into();
        graph.insert(&triple);
    }
    graph
}

fn node_names<G: StaticGraphViewOps>(g: &G) -> BTreeSet<String> {
    g.nodes().into_iter().map(|n| n.name()).collect()
}

fn layer_names<G: StaticGraphViewOps>(g: &G) -> BTreeSet<String> {
    g.unique_layers().map(|l| l.to_string()).collect()
}

fn triple<'a>(s: &'a NamedNode, p: &'a NamedNode, o: &'a NamedNode) -> TripleRef<'a> {
    TripleRef::new(s, p, o)
}

fn iri(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

/// A FOAF Turtle document.
#[test]
fn load_foaf_turtle() {
    let pg = PersistentGraph::new();
    let n = pg
        .load_rdf(1, FOAF.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    assert_eq!(n, 15);

    let age = format!("\"42\"^^<{XSD_INTEGER}>");
    let expected_nodes: BTreeSet<String> = [
        "http://example.org/people/alice",
        "http://example.org/people/bob",
        "http://example.org/people/carol",
        "http://xmlns.com/foaf/0.1/Person",
        "\"Alice\"@en",
        age.as_str(),
        "mailto:alice@example.org",
        "\"Bob\"",
        "\"Carol\"@en",
        &format!("\"37\"^^<{XSD_INTEGER}>"),
        "http://carol.example.org/",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(node_names(&pg), expected_nodes);

    let expected_layers: BTreeSet<String> = [
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://xmlns.com/foaf/0.1/name",
        "http://xmlns.com/foaf/0.1/age",
        "http://xmlns.com/foaf/0.1/knows",
        "http://xmlns.com/foaf/0.1/mbox",
        "http://xmlns.com/foaf/0.1/homepage",
        "http://example.org/rel/worksWith",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(layer_names(&pg), expected_layers);

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
            "http://example.org/rel/worksWith".to_string(),
            "http://xmlns.com/foaf/0.1/knows".to_string()
        ])
    );

    // `42` (shorthand) and `"42"^^xsd:integer` are the same literal, so one node.
    let age_node = pg.node(age.as_str()).unwrap();
    assert_eq!(age_node.in_degree(), 2);
    assert_eq!(age_node.out_degree(), 0);
    assert_eq!(
        pg.node("http://xmlns.com/foaf/0.1/Person")
            .unwrap()
            .in_degree(),
        3
    );
    // the stored history is the load time
    assert_eq!(alice_bob.history().t().collect(), vec![1, 1]);
}

/// Blank nodes are renamed on load and matched as written on retraction.
#[test]
fn blank_nodes() {
    let pg = PersistentGraph::new();
    pg.load_rdf(1, BLANK_NODES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    pg.load_rdf(2, BLANK_NODES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let blanks: Vec<String> = node_names(&pg)
        .into_iter()
        .filter(|n| n.starts_with("_:"))
        .collect();
    // `_:x` and `[]` in each of the two loads
    assert_eq!(blanks.len(), 4, "{blanks:?}");
    assert!(!blanks.contains(&"_:x".to_string()));
    assert_eq!(nt(&pg).lines().count(), 4);

    // retract one of the `_:x ex:p ex:o` triples using its label from the export
    let exported = nt(&pg);
    let line = exported
        .lines()
        .find(|l| l.contains("<http://ex/p>"))
        .unwrap()
        .to_owned();
    let label = line.split(' ').next().unwrap().to_owned();
    assert!(label.starts_with("_:"));
    let n = pg
        .retract_rdf(3, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(n, 1);

    let after = nt(&pg);
    assert_eq!(after.lines().count(), 3);
    assert!(!after.contains(&line));
    assert_eq!(
        after
            .lines()
            .filter(|l| l.contains("<http://ex/p>"))
            .count(),
        1
    );
    // the retracted triple is still visible as of t = 2
    assert!(nt(&pg.snapshot_at(2)).contains(&line));
    // the node keeps its label
    assert!(pg.node(label.as_str()).is_some());

    // add_triple / delete_triple also use labels as written
    let g = Graph::new();
    let b = BlankNode::new("b1").unwrap();
    let p = iri("http://ex/p");
    let o = iri("http://ex/o");
    g.add_triple(1, TripleRef::new(&b, &p, &o)).unwrap();
    assert!(g.node("_:b1").is_some());
    assert_eq!(nt(&g), "_:b1 <http://ex/p> <http://ex/o> .\n");
}

/// Named graphs are rejected; TriG with only the default graph loads.
#[test]
fn named_graphs() {
    let pg = PersistentGraph::new();
    let err = pg
        .load_rdf(
            1,
            "<http://e/s> <http://e/p> <http://e/o> <http://e/g> .".as_bytes(),
            RdfFormat::NQuads,
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::Parse(_))),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("Named graphs are not allowed"),
        "{err}"
    );
    assert_eq!(pg.count_nodes(), 0);
    assert_eq!(pg.count_edges(), 0);

    let trig = "@prefix ex: <http://e/> . ex:g { ex:s ex:p ex:o . }";
    let err = pg
        .load_rdf(1, trig.as_bytes(), RdfFormat::TriG, None)
        .unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::Parse(_))));
    assert_eq!(pg.count_nodes(), 0);

    let n = pg
        .load_rdf(1, DEFAULT_GRAPH_TRIG.as_bytes(), RdfFormat::TriG, None)
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        sorted_lines(&nt(&pg)),
        vec![
            "<http://ex/a> <http://ex/p> <http://ex/b> .",
            "<http://ex/b> <http://ex/p> <http://ex/c> .",
        ]
    );

    // N-Quads in the default graph load too
    let nq = PersistentGraph::new();
    nq.load_rdf(
        1,
        "<http://e/s> <http://e/p> <http://e/o> .".as_bytes(),
        RdfFormat::NQuads,
        None,
    )
    .unwrap();
    assert_eq!(nq.count_edges(), 1);
}

/// U64-id graphs and non-canonical `raphtory:` IRIs are rejected without writing.
#[test]
fn rejected_inputs_leave_the_graph_unchanged() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> .";
    let err = g
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::NonStringIds)),
        "{err:?}"
    );
    let err = g
        .retract_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::NonStringIds)));
    let (s, p, o) = (iri("http://ex/s"), iri("http://ex/p"), iri("http://ex/o"));
    let err = g.add_triple(1, triple(&s, &p, &o)).unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::NonStringIds)));
    let err = g.delete_triple(1, triple(&s, &p, &o)).unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::NonStringIds)));
    assert_eq!(g.count_nodes(), 2);
    assert_eq!(g.count_edges(), 1);
    assert_eq!(layer_names(&g), BTreeSet::from(["_default".to_string()]));

    let pg = PersistentGraph::new();
    for doc in [
        "<raphtory:%61> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <raphtory:asof:2024> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> <raphtory:%2f> .",
    ] {
        let err = pg
            .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
            .unwrap_err();
        assert!(
            matches!(err, GraphError::Rdf(RdfError::NonCanonicalTerm(_))),
            "{err:?}"
        );
    }
    let err = pg
        .add_triple(1, triple(&iri("raphtory:%61"), &p, &o))
        .unwrap_err();
    match err {
        GraphError::Rdf(RdfError::NonCanonicalTerm(term)) => assert_eq!(term, "<raphtory:%61>"),
        err => panic!("unexpected {err:?}"),
    }
    assert_eq!(pg.count_nodes(), 0);
    assert_eq!(pg.unique_layers().count(), 0);

    // a canonical `raphtory:` IRI is fine
    pg.add_triple(1, triple(&iri("raphtory:a"), &p, &o))
        .unwrap();
    assert!(pg.node("a").is_some());
}

/// Loads are not atomic: triples before an error stay in the graph.
#[test]
fn loads_are_not_atomic() {
    let pg = PersistentGraph::new();
    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .\nthis is not n-triples\n";
    let err = pg
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::Parse(_))));
    assert_eq!(pg.count_edges(), 1);
}

/// `base_iri` resolves relative IRIs; a bad base is an error.
#[test]
fn base_iri() {
    let pg = PersistentGraph::new();
    pg.load_rdf(
        1,
        "<s> <p> <o> . <#frag> <p> <../up> .".as_bytes(),
        RdfFormat::Turtle,
        Some("http://ex/dir/doc"),
    )
    .unwrap();
    assert_eq!(
        sorted_lines(&nt(&pg)),
        vec![
            "<http://ex/dir/doc#frag> <http://ex/dir/p> <http://ex/up> .",
            "<http://ex/dir/s> <http://ex/dir/p> <http://ex/dir/o> .",
        ]
    );

    let err = pg
        .load_rdf(
            1,
            "<s> <p> <o> .".as_bytes(),
            RdfFormat::Turtle,
            Some("not a base iri"),
        )
        .unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::Iri(_))), "{err:?}");

    // without a base, relative IRIs cannot be resolved
    let err = pg
        .load_rdf(2, "<s> <p> <o> .".as_bytes(), RdfFormat::Turtle, None)
        .unwrap_err();
    assert!(matches!(err, GraphError::Rdf(RdfError::Parse(_))));
}

/// Export of a blank-free document parses back to the same RDF graph.
#[test]
fn export_round_trips_blank_free_fixtures() {
    for (data, format) in [(FOAF, RdfFormat::Turtle), (DATATYPES, RdfFormat::NTriples)] {
        let input = parse_graph(data, format);
        let pg = PersistentGraph::new();
        let n = pg.load_rdf(1, data.as_bytes(), format, None).unwrap();
        assert!(n >= input.len());

        let exported = nt(&pg);
        assert_eq!(exported.lines().count(), input.len());
        assert_eq!(parse_graph(&exported, RdfFormat::NTriples), input);

        let serializer = RdfSerializer::from_format(RdfFormat::Turtle)
            .with_prefix("foaf", "http://xmlns.com/foaf/0.1/")
            .unwrap()
            .with_prefix("ex", "http://ex/")
            .unwrap()
            .with_prefix("raphtory", "raphtory:")
            .unwrap();
        let turtle = export(&pg, serializer);
        assert!(turtle.contains("@prefix ex: <http://ex/>"), "{turtle}");
        assert_eq!(parse_graph(&turtle, RdfFormat::Turtle), input);

        // other serialisations parse back too
        for format in [RdfFormat::RdfXml, RdfFormat::NQuads, RdfFormat::TriG] {
            assert_eq!(parse_graph(&export(&pg, format), format), input, "{format}");
        }
    }
}

/// The datatype zoo keeps every literal exactly, one node per distinct term.
#[test]
fn literals_are_kept_exactly() {
    let pg = PersistentGraph::new();
    pg.load_rdf(1, DATATYPES.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    for name in [
        format!("\"42\"^^<{XSD_INTEGER}>"),
        format!("\"042\"^^<{XSD_INTEGER}>"),
        "\"1\"^^<http://www.w3.org/2001/XMLSchema#int>".to_string(),
        "\"hello\"@en".to_string(),
        "\"Hello\"@en-gb".to_string(), // parsers lower-case language tags
        "\"typed\"".to_string(),       // xsd:string literals are simple literals
        "\"line1\\nline2 \\\"quoted\\\" back\\\\slash\\ttab\"".to_string(),
        "\"café 日本\"".to_string(),
        "Alice Smith".to_string(),
        "12:30".to_string(),
        "urn:isbn:0451450523".to_string(),
    ] {
        assert!(pg.node(name.as_str()).is_some(), "missing node {name}");
    }
    assert!(layer_names(&pg).contains("_default"));
    // an IRI used as subject, predicate and object is one node and one layer
    let knows = pg.node("http://ex/knows").unwrap();
    assert_eq!(knows.in_degree(), 1);
    assert_eq!(knows.out_degree(), 1);
    assert!(layer_names(&pg).contains("http://ex/knows"));
    // the duplicate triple is stored twice, but exported once
    let ab = pg.edge("http://ex/a", "http://ex/b").unwrap();
    assert_eq!(ab.history().t().collect().len(), 3);
    let exported = nt(&pg);
    assert_eq!(
        exported
            .lines()
            .filter(|l| *l == "<http://ex/a> <http://ex/knows> <http://ex/b> .")
            .count(),
        1
    );
    // the self-loop is exported once
    assert_eq!(
        exported
            .lines()
            .filter(|l| *l == "<http://ex/a> <http://ex/knows> <http://ex/a> .")
            .count(),
        1
    );
}

/// Plain (non-RDF) graphs export and re-load to the same names.
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

    for original in [g.into_dynamic(), u64_graph.into_dynamic()] {
        let first = nt(&original);
        assert_eq!(first.lines().count(), original.count_edges());

        for format in [RdfFormat::NTriples, RdfFormat::Turtle] {
            let doc = export(&original, format);
            let reloaded = Graph::new();
            reloaded
                .load_rdf(0, doc.as_bytes(), format, None)
                .unwrap_or_else(|e| panic!("{format} export does not load: {e}\n{doc}"));
            assert_eq!(node_names(&reloaded), node_names(&original), "{format}");
            assert_eq!(layer_names(&reloaded), layer_names(&original), "{format}");

            let second = nt(&reloaded);
            assert_eq!(sorted_lines(&second), sorted_lines(&first), "{format}");
            assert_eq!(
                parse_graph(&second, RdfFormat::NTriples),
                parse_graph(&first, RdfFormat::NTriples),
                "{format}"
            );

            // the export is also a retraction document for every triple it contains
            let n = reloaded
                .retract_rdf(1, doc.as_bytes(), format, None)
                .unwrap();
            assert_eq!(n, first.lines().count(), "{format}");
            assert_eq!(nt(&reloaded.persistent_graph()), "", "{format}");
        }
    }

    let u64_graph = Graph::new();
    u64_graph.add_edge(1, 42, 7, NO_PROPS, None).unwrap();
    assert_eq!(
        nt(&u64_graph),
        "<raphtory:42> <raphtory:_default> <raphtory:7> .\n"
    );
}

/// Generalized triples are skipped and counted.
#[test]
fn generalized_triples_are_skipped() {
    let g = Graph::new();
    g.add_edge(1, "\"lit\"", "Bob", NO_PROPS, Some("http://ex/p"))
        .unwrap();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("_:l"))
        .unwrap();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("http://ex/p"))
        .unwrap();
    let mut out = Vec::new();
    let stats = g.to_rdf(&mut out, RdfFormat::NTriples).unwrap();
    assert_eq!(stats.triples, 1);
    assert_eq!(stats.skipped, 2);
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "<raphtory:Alice> <http://ex/p> <raphtory:Bob> .\n"
    );
}

/// The element names of an XML document (start and end tags) that are not a QName: an NCName,
/// optionally with an NCName prefix (such as the empty local name of `<oxprefix:>`), or that
/// have the prefix `xmlns`, which no element can have (such as `<xmlns:foo>`).
fn malformed_xml_names(doc: &str) -> Vec<&str> {
    let ncname = |part: &str| {
        part.chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
            && part
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
    };
    doc.split('<')
        .skip(1)
        .map(|tag| tag.strip_prefix('/').unwrap_or(tag))
        .filter(|tag| !tag.starts_with('?'))
        .map(|tag| {
            tag.split(|c: char| c.is_whitespace() || c == '/' || c == '>')
                .next()
                .unwrap()
        })
        .filter(|name| {
            let parts: Vec<_> = name.split(':').collect();
            parts.len() > 2
                || (parts.len() == 2 && parts[0] == "xmlns")
                || !parts.into_iter().all(ncname)
        })
        .collect()
}

/// RDF/XML skips (and counts) triples whose predicate cannot be an element name or is an RDF/XML
/// syntax term; an `rdf:type` whose object cannot name an element is written after another
/// triple of its subject.
#[test]
fn rdf_xml_export_is_well_formed() {
    const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const XMLNS_FOO: &str = "http://www.w3.org/2000/xmlns/foo";
    let g = Graph::new();
    let edge = |s: &str, o: &str, p: &str| {
        g.add_edge(1, s, o, NO_PROPS, Some(p)).unwrap();
    };
    // predicates without a local name
    edge("http://ex/a", "http://ex/b", "http://ex/42");
    edge("http://ex/a", "http://ex/b", "http://ex/");
    edge("http://ex/a", "http://ex/b", "urn:x:1.5");
    // the local name can be a suffix of the last segment
    edge("http://ex/a", "http://ex/b", "http://ex/4p");
    // the RDF/XML syntax terms cannot be predicates (the serializer fails on them, after it
    // started the element of `a`), and an IRI in the `xmlns` namespace would be written as an
    // `xmlns:foo` element
    let syntax_terms = [
        "Description",
        "li",
        "RDF",
        "ID",
        "about",
        "parseType",
        "resource",
        "nodeID",
        "datatype",
    ]
    .map(|term| format!("{RDF_NS}{term}"));
    for predicate in &syntax_terms {
        edge("http://ex/a", "http://ex/b", predicate);
    }
    edge("http://ex/a", "http://ex/b", XMLNS_FOO);
    // the first triple of `s` is an `rdf:type` whose object has no local name
    edge("http://ex/s", "http://ex/42", RDF_TYPE);
    edge("http://ex/s", "http://ex/o", "http://ex/p");
    // the only triple of `t` is one
    edge("http://ex/t", "http://ex/42", RDF_TYPE);
    // an `rdf:type` object with a local name names the subject's element
    edge("http://ex/u", "http://ex/Person", RDF_TYPE);
    // an `rdf:type` object in the `xmlns` namespace cannot name it: held back while it would be
    // the first triple of `y`, skipped as the only triple of `x`
    edge("http://ex/x", XMLNS_FOO, RDF_TYPE);
    edge("http://ex/y", XMLNS_FOO, RDF_TYPE);
    edge("http://ex/y", "http://ex/z", "http://ex/p");
    // a syntax term can be the object (the serializer writes an `rdf:Description` element)
    edge("http://ex/r", &syntax_terms[1], RDF_TYPE);
    // blank nodes whose label is not an NCName (it starts with a digit) are renamed, and so are
    // the labels they could collide with
    edge("_:1", "http://ex/o", "http://ex/p");
    edge("http://ex/v", "_:42abc", "http://ex/p");
    edge("http://ex/v", "_:x1", "http://ex/p");
    edge("http://ex/v", "_:x", "http://ex/p");
    edge("http://ex/v", "_:b1", "http://ex/p");
    // literals with characters XML cannot hold, or that XML parsers change (a CR becomes a LF)
    let literal = |value: &str| Literal::new_simple_literal(value).to_string();
    for value in [
        "bell\u{7}",
        "nul\u{0}",
        "a\rb",
        "\u{FFFE}",
        "tab\tnewline\n",
    ] {
        edge("http://ex/w", &literal(value), "http://ex/p");
    }

    let all = nt(&g);
    // the `rdf:type` triples of `s` and `y` come first
    let s_type = format!("<http://ex/s> <{RDF_TYPE}> <http://ex/42> .");
    assert!(
        all.find(&s_type).unwrap() < all.find("<http://ex/s> <http://ex/p>").unwrap(),
        "{all}"
    );
    let y_type = format!("<http://ex/y> <{RDF_TYPE}> <{XMLNS_FOO}> .");
    assert!(
        all.find(&y_type).unwrap() < all.find("<http://ex/y> <http://ex/p>").unwrap(),
        "{all}"
    );
    let expected = [
        "<http://ex/a> <http://ex/4p> <http://ex/b> .".to_owned(),
        s_type,
        "<http://ex/s> <http://ex/p> <http://ex/o> .".to_owned(),
        format!("<http://ex/u> <{RDF_TYPE}> <http://ex/Person> ."),
        y_type,
        "<http://ex/y> <http://ex/p> <http://ex/z> .".to_owned(),
        format!("<http://ex/r> <{RDF_TYPE}> <{RDF_NS}li> ."),
        "_:x1 <http://ex/p> <http://ex/o> .".to_owned(),
        "<http://ex/v> <http://ex/p> _:x42abc .".to_owned(),
        "<http://ex/v> <http://ex/p> _:xx1 .".to_owned(),
        "<http://ex/v> <http://ex/p> _:x .".to_owned(),
        "<http://ex/v> <http://ex/p> _:b1 .".to_owned(),
        format!(
            "<http://ex/w> <http://ex/p> {} .",
            literal("tab\tnewline\n")
        ),
    ]
    .join("\n");
    let expected = parse_graph(&expected, RdfFormat::NTriples);

    for serializer in [
        RdfSerializer::from_format(RdfFormat::RdfXml),
        serializer_with_prefixes(RdfFormat::RdfXml, [("ex", "http://ex/")]).unwrap(),
        serializer_with_prefixes(RdfFormat::RdfXml, [("", "http://ex/")]).unwrap(),
    ] {
        let mut out = Vec::new();
        let stats = g.to_rdf(&mut out, serializer).unwrap();
        let doc = String::from_utf8(out).unwrap();
        assert_eq!(stats.triples, 13, "{doc}");
        assert_eq!(stats.skipped, 19, "{doc}");
        assert_eq!(malformed_xml_names(&doc), Vec::<&str>::new(), "{doc}");
        assert!(!doc.contains("<xmlns:"), "{doc}");
        assert!(
            doc.contains(&format!("<rdf:type rdf:resource=\"{XMLNS_FOO}\"/>")),
            "{doc}"
        );
        // every rdf:nodeID is an NCName, and the document holds only XML characters (no CR)
        let node_ids: Vec<&str> = doc
            .split("rdf:nodeID=\"")
            .skip(1)
            .map(|rest| rest.split('"').next().unwrap())
            .collect();
        assert_eq!(node_ids.len(), 5, "{doc}");
        let ncname = |id: &str| {
            id.starts_with(|c: char| c.is_alphabetic() || c == '_')
                && id
                    .chars()
                    .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
        };
        assert!(node_ids.iter().all(|id| ncname(id)), "{node_ids:?}");
        assert!(
            doc.chars().all(|c| matches!(c,
                '\t' | '\n' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..)),
            "{doc:?}"
        );
        assert!(
            doc.contains("<rdf:type rdf:resource=\"http://ex/42\"/>"),
            "{doc}"
        );
        assert_eq!(parse_graph(&doc, RdfFormat::RdfXml), expected, "{doc}");
    }

    // other formats write every triple, with the blank-node labels as stored
    for format in [RdfFormat::NTriples, RdfFormat::Turtle] {
        let mut out = Vec::new();
        let stats = g.to_rdf(&mut out, format).unwrap();
        assert_eq!((stats.triples, stats.skipped), (32, 0), "{format}");
        let doc = String::from_utf8(out).unwrap();
        assert_eq!(
            parse_graph(&doc, format),
            parse_graph(&all, RdfFormat::NTriples)
        );
        assert!(doc.contains("_:1 ") && doc.contains("_:x1"), "{doc}");
    }
}

/// RDF/XML renames the blank nodes whose label is not an NCName without merging any two.
#[test]
fn rdf_xml_blank_node_labels_stay_distinct() {
    let labels = [
        "1", "x1", "xx1", "1x", "x1x", "x", "xx", "xa", "a", "_1", "x_1", "0.5", "x0.5", "9-",
    ];
    let g = Graph::new();
    for label in labels {
        let node = format!("_:{label}");
        g.add_edge(
            1,
            "http://ex/s",
            node.as_str(),
            NO_PROPS,
            Some("http://ex/p"),
        )
        .unwrap();
        g.add_edge(
            1,
            node.as_str(),
            "http://ex/o",
            NO_PROPS,
            Some("http://ex/p"),
        )
        .unwrap();
    }
    let doc = export(&g, RdfFormat::RdfXml);
    let read = parse_graph(&doc, RdfFormat::RdfXml);
    assert_eq!(read.len(), 2 * labels.len(), "{doc}");
    let objects: BTreeSet<String> = read
        .iter()
        .filter(|t| t.subject.to_string() == "<http://ex/s>")
        .map(|t| t.object.to_string())
        .collect();
    assert_eq!(objects.len(), labels.len(), "{doc}");
    assert!(objects.iter().all(|o| o.starts_with("_:")), "{objects:?}");
    // each blank node is the subject of its own triple, under the same label
    for object in &objects {
        let subject = read
            .iter()
            .filter(|t| t.subject.to_string() == *object)
            .count();
        assert_eq!(subject, 1, "{object}: {doc}");
    }
    // NCName labels that cannot collide are kept
    for kept in ["xa", "a", "_1", "x", "xx", "x_1"] {
        assert!(
            objects.contains(&format!("_:{kept}")),
            "{kept}: {objects:?}"
        );
    }
}

/// `serializer_with_prefixes` rejects the prefix names a format cannot write (the oxigraph
/// serializers write them as is, giving documents that do not parse).
#[test]
fn prefix_names_are_checked() {
    let g = Graph::new();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http://ex/p"),
    )
    .unwrap();
    let expected = parse_graph(&nt(&g), RdfFormat::NTriples);

    let turtle_like = [RdfFormat::Turtle, RdfFormat::TriG, RdfFormat::N3];
    let valid: &[(&[RdfFormat], &[&str])] = &[
        (
            &[RdfFormat::Turtle, RdfFormat::TriG],
            &["", "ex", "e.x", "a-b_c1", "\u{e9}t\u{e9}"],
        ),
        (
            &[RdfFormat::RdfXml],
            &["", "ex", "_x", "e.x", "ex.", "a-b_c1", "\u{e9}t\u{e9}"],
        ),
    ];
    for (formats, names) in valid {
        for &format in *formats {
            for name in *names {
                let serializer = serializer_with_prefixes(format, [(*name, "http://ex/")]).unwrap();
                let doc = export(&g, serializer);
                let declaration = match (format, *name) {
                    (RdfFormat::RdfXml, "") => "xmlns=\"http://ex/\"".to_owned(),
                    (RdfFormat::RdfXml, name) => format!("xmlns:{name}=\"http://ex/\""),
                    (_, name) => format!("@prefix {name}: <http://ex/>"),
                };
                assert!(doc.contains(&declaration), "{format} {name:?}: {doc}");
                assert_eq!(
                    parse_graph(&doc, format),
                    expected,
                    "{format} {name:?}: {doc}"
                );
            }
        }
    }

    let invalid: &[(&[RdfFormat], &[&str])] = &[
        (
            &turtle_like,
            &[
                "a b", "1x", "a:b", "ex.", "_x", "-x", ".x", "a/b", "\u{e9} t",
            ],
        ),
        (
            &[RdfFormat::RdfXml],
            &["a b", "1x", "a:b", "-x", ".x", "a/b", "xml", "xmlns"],
        ),
    ];
    for (formats, names) in invalid {
        for &format in *formats {
            for &name in *names {
                let err = serializer_with_prefixes(format, [(name, "http://ex/")])
                    .err()
                    .unwrap();
                assert!(
                    matches!(&err, RdfError::InvalidPrefix { name: n, format: f, .. } if n == name && *f == format),
                    "{format} {name:?}: {err:?}"
                );
            }
        }
    }
    let err = serializer_with_prefixes(
        RdfFormat::Turtle,
        [("ex", "http://ex/"), ("a b", "http://ex/")],
    )
    .err()
    .unwrap();
    assert_eq!(
        err.to_string(),
        "invalid prefix name 'a b' for Turtle: it must be empty, or start with a letter, contain \
         only letters, digits, '_', '-' and '.', and not end with '.'"
    );

    // the first invalid name is reported
    let err = serializer_with_prefixes(
        RdfFormat::Turtle,
        [
            ("ok", "http://ex/"),
            ("a b", "http://ex/"),
            ("1x", "http://ex/"),
        ],
    )
    .err()
    .unwrap();
    assert!(
        matches!(&err, RdfError::InvalidPrefix { name, .. } if name == "a b"),
        "{err:?}"
    );
    // the serializer chooses the order of the prefixes, and a repeated name keeps its last IRI
    let prefixed = Graph::new();
    prefixed
        .add_edge(
            1,
            "http://ex/a",
            "http://xmlns.com/foaf/0.1/b",
            NO_PROPS,
            Some("http://ex/p"),
        )
        .unwrap();
    for format in [RdfFormat::Turtle, RdfFormat::TriG] {
        let prefixes = [
            ("ex", "http://other/"),
            ("ex", "http://ex/"),
            ("foaf", "http://xmlns.com/foaf/0.1/"),
        ];
        let doc = export(
            &prefixed,
            serializer_with_prefixes(format, prefixes).unwrap(),
        );
        let foaf = doc
            .find("@prefix foaf: <http://xmlns.com/foaf/0.1/> .")
            .unwrap();
        let ex = doc.find("@prefix ex: <http://ex/> .").unwrap();
        assert!(foaf < ex, "{format}: {doc}");
        assert!(!doc.contains("http://other/"), "{format}: {doc}");
    }

    // invalid IRIs
    let err = serializer_with_prefixes(RdfFormat::Turtle, [("ex", "not an iri")])
        .err()
        .unwrap();
    assert!(matches!(err, RdfError::Iri(_)), "{err:?}");

    // formats without prefixes ignore them
    for format in [
        RdfFormat::NTriples,
        RdfFormat::NQuads,
        RdfFormat::JsonLd {
            profile: Default::default(),
        },
    ] {
        let serializer = serializer_with_prefixes(format, [("a b", "http://ex/")]).unwrap();
        assert_eq!(export(&g, serializer), export(&g, format), "{format}");
    }
}

/// Assertions are edge additions, retractions are edge deletions, and the view decides what is
/// visible.
#[test]
fn retraction_and_views() {
    let s = iri("http://ex/s");
    let p = iri("http://ex/p");
    let q = iri("http://ex/q");
    let o = iri("http://ex/o");
    let line = "<http://ex/s> <http://ex/p> <http://ex/o> .";

    let pg = PersistentGraph::new();
    pg.add_triple(1, triple(&s, &p, &o)).unwrap();
    pg.add_triple(1, triple(&s, &q, &o)).unwrap();
    pg.delete_triple(5, triple(&s, &p, &o)).unwrap();

    // storage: one edge, two layers, one deletion in layer p
    let e = pg.edge("http://ex/s", "http://ex/o").unwrap();
    assert_eq!(
        e.layers("http://ex/p").unwrap().deletions().t().collect(),
        vec![5]
    );
    assert_eq!(
        e.layers("http://ex/q").unwrap().deletions().t().collect(),
        Vec::<i64>::new()
    );

    assert!(!nt(&pg.snapshot_at(0)).contains(line));
    for t in 1..5 {
        assert!(nt(&pg.snapshot_at(t)).contains(line), "t = {t}");
    }
    for t in 5..8 {
        assert!(!nt(&pg.snapshot_at(t)).contains(line), "t = {t}");
    }
    assert!(!nt(&pg).contains(line));
    // the other predicate on the same (s, o) stays
    assert!(nt(&pg).contains("<http://ex/q>"));
    // window(a, b) is as of b - 1
    assert!(nt(&pg.window(0, 5)).contains(line));
    assert!(!nt(&pg.window(0, 6)).contains(line));
    // layer views combine
    assert_eq!(nt(&pg.layers("http://ex/q").unwrap()).lines().count(), 1);
    assert_eq!(
        nt(&pg.layers("http://ex/p").unwrap().snapshot_at(3)),
        format!("{line}\n")
    );

    // re-assertion makes it visible again; same-t assert then retract is not visible
    pg.add_triple(8, triple(&s, &p, &o)).unwrap();
    assert!(nt(&pg).contains(line));
    pg.add_triple(10, triple(&s, &p, &o)).unwrap();
    pg.delete_triple(10, triple(&s, &p, &o)).unwrap();
    assert!(!nt(&pg.snapshot_at(10)).contains(line));
    pg.delete_triple(12, triple(&s, &p, &o)).unwrap();
    pg.add_triple(12, triple(&s, &p, &o)).unwrap();
    assert!(nt(&pg.snapshot_at(12)).contains(line));

    // a retraction of a triple that was never asserted is never visible
    let orphan = PersistentGraph::new();
    orphan
        .retract_rdf(3, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(orphan.count_nodes(), 2);
    for t in 0..6 {
        assert_eq!(nt(&orphan.snapshot_at(t)), "");
    }
    assert_eq!(nt(&orphan), "");

    // an event graph ignores retractions; its persistent view sees them
    let g = Graph::new();
    g.load_rdf(1, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    g.retract_rdf(5, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(nt(&g), format!("{line}\n"));
    assert_eq!(nt(&g.window(5, 10)), "");
    assert_eq!(nt(&g.persistent_graph()), "");
    assert_eq!(
        nt(&g.persistent_graph().snapshot_at(3)),
        format!("{line}\n")
    );

    // explicit times can be strings and datetimes like add_edge
    let dates = PersistentGraph::new();
    dates
        .load_rdf("2024-01-01", line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let e = dates.edge("http://ex/s", "http://ex/o").unwrap();
    assert_eq!(e.history().t().collect(), vec![1704067200000]);
    let err = dates
        .load_rdf("not a time", line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(matches!(err, GraphError::ParseTime { .. }), "{err:?}");
}

/// Writes get increasing event ids: across calls (so for equal `t` the
/// later call wins) and within one document (in document order).
#[test]
fn document_order_decides_ties() {
    let pg = PersistentGraph::new();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> .";
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    pg.retract_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(nt(&pg.snapshot_at(1)), "");
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(nt(&pg.snapshot_at(1)), format!("{doc}\n"));

    // within one document, ids increase in document order (p, q, p, r)
    let pg = PersistentGraph::new();
    let in_order = [
        "<http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/q> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/r> <http://ex/o> .",
    ]
    .join("\n");
    pg.load_rdf(1, in_order.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let e = pg.edge("http://ex/s", "http://ex/o").unwrap();
    let ids = |l: &str| e.layers(l).unwrap().history().event_id().collect();
    let (p_ids, q_ids, r_ids) = (ids("http://ex/p"), ids("http://ex/q"), ids("http://ex/r"));
    assert_eq!((p_ids.len(), q_ids.len(), r_ids.len()), (2, 1, 1));
    assert!(
        p_ids[0] < q_ids[0] && q_ids[0] < p_ids[1] && p_ids[1] < r_ids[0],
        "{p_ids:?} {q_ids:?} {r_ids:?}"
    );
    // the triples of a retraction document get increasing ids in document order too
    let pg = PersistentGraph::new();
    let line = "<http://ex/s> <http://ex/p> <http://ex/o> .";
    pg.load_rdf(1, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    pg.retract_rdf(1, in_order.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let e = pg.edge("http://ex/s", "http://ex/o").unwrap();
    let deletions = e
        .layers("http://ex/p")
        .unwrap()
        .deletions()
        .event_id()
        .collect();
    assert_eq!(deletions.len(), 2);
    assert!(deletions[0] < deletions[1], "{deletions:?}");
    // out-of-order loads: timestamps decide
    let late_first = PersistentGraph::new();
    late_first
        .retract_rdf(5, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    late_first
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(nt(&late_first.snapshot_at(3)), format!("{doc}\n"));
    assert_eq!(nt(&late_first), "");
}

fn scan_set(scan: EdgeScan) -> Vec<(VID, LayerId, VID)> {
    let items: Vec<_> = scan.collect();
    let set: BTreeSet<_> = items.iter().map(|(s, l, o)| (s.0, l.0, o.0)).collect();
    assert_eq!(set.len(), items.len(), "duplicate scan items");
    let mut items = items;
    items.sort_by_key(|(s, l, o)| (s.0, l.0, o.0));
    items
}

#[test]
fn edge_scans() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, "a", "b", NO_PROPS, Some("p")).unwrap();
    pg.add_edge(1, "a", "b", NO_PROPS, Some("q")).unwrap();
    pg.add_edge(1, "a", "a", NO_PROPS, Some("p")).unwrap();
    pg.add_edge(1, "b", "c", NO_PROPS, Some("q")).unwrap();
    pg.delete_edge(2, "b", "c", Some("q")).unwrap();
    let view: DynamicGraph = pg.valid().into_dynamic();
    let v = |n: &str| pg.node(n).unwrap().node;
    let l = |n: &str| pg.get_layer_id(n).unwrap();
    let (a, b, c, p, q) = (v("a"), v("b"), v("c"), l("p"), l("q"));

    let mut all = vec![(a, p, a), (a, p, b), (a, q, b)];
    all.sort_by_key(|(s, l, o)| (s.0, l.0, o.0));
    assert_eq!(scan_set(EdgeScan::all(view.clone(), None)), all);
    assert_eq!(
        scan_set(EdgeScan::all(view.clone(), Some(q))),
        vec![(a, q, b)]
    );
    // the scan restricts the layer itself, so a restricted view gives the same result
    assert_eq!(
        scan_set(EdgeScan::all(
            pg.valid_layers("q").valid().into_dynamic(),
            Some(q)
        )),
        vec![(a, q, b)]
    );
    assert_eq!(
        scan_set(EdgeScan::all(view.clone(), Some(STATIC_GRAPH_LAYER_ID))),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), a, Dir::Out, None)),
        all
    );
    let mut into_b = vec![(a, p, b), (a, q, b)];
    into_b.sort_by_key(|(s, l, o)| (s.0, l.0, o.0));
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), b, Dir::In, None)),
        into_b
    );
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), b, Dir::In, Some(p))),
        vec![(a, p, b)]
    );
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), a, Dir::In, Some(p))),
        vec![(a, p, a)]
    );
    // b -> c was retracted
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), c, Dir::In, None)),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::between(view.clone(), a, b, None)),
        into_b
    );
    assert_eq!(
        scan_set(EdgeScan::between(view.clone(), a, b, Some(q))),
        vec![(a, q, b)]
    );
    assert_eq!(
        scan_set(EdgeScan::between(view.clone(), a, a, Some(q))),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::between(view.clone(), b, c, None)),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::between(pg.valid().into_dynamic(), b, a, None)),
        vec![]
    );
    // the unwrapped persistent graph includes the deleted edge's layer
    assert_eq!(
        scan_set(EdgeScan::between(pg.clone().into_dynamic(), b, c, None)),
        vec![(b, q, c)]
    );

    // a -> a is only in p, so an out-scan of a restricted to q must not report it
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), a, Dir::Out, Some(q))),
        vec![(a, q, b)]
    );
    let mut out_of_a_in_p = vec![(a, p, a), (a, p, b)];
    out_of_a_in_p.sort_by_key(|(s, l, o)| (s.0, l.0, o.0));
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), a, Dir::Out, Some(p))),
        out_of_a_in_p
    );
    // b -> a does not exist, so an in-scan of a restricted to q is empty
    assert_eq!(
        scan_set(EdgeScan::around(view.clone(), a, Dir::In, Some(q))),
        vec![]
    );
    // a view already restricted to another layer yields nothing (`valid_layers` intersects)
    let only_q: DynamicGraph = pg.valid_layers("q").valid().into_dynamic();
    assert_eq!(scan_set(EdgeScan::all(only_q.clone(), Some(p))), vec![]);
    assert_eq!(
        scan_set(EdgeScan::around(only_q.clone(), a, Dir::Out, Some(p))),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::around(only_q.clone(), b, Dir::In, Some(p))),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::between(only_q.clone(), a, b, Some(p))),
        vec![]
    );
    assert_eq!(
        scan_set(EdgeScan::between(only_q.clone(), a, a, Some(p))),
        vec![]
    );
    // ... while its own layer is still scanned
    assert_eq!(
        scan_set(EdgeScan::around(only_q, a, Dir::Out, Some(q))),
        vec![(a, q, b)]
    );
}

/// db4 VIDs are segment-strided with gaps, so full scans must not walk `0..num_nodes`.
/// Tiny node pages create the gaps whatever the number of rayon threads.
#[test]
fn scans_cover_segment_strided_vids() {
    let g = Graph::new_with_config(Args::default().with_max_node_page_len(3)).unwrap();
    for i in 0..5 {
        g.add_edge(
            1,
            format!("http://ex/a{i}"),
            format!("http://ex/b{i}"),
            NO_PROPS,
            Some("http://ex/p"),
        )
        .unwrap();
    }
    let mut vids: Vec<usize> = g.nodes().into_iter().map(|n| n.node.0).collect();
    vids.sort_unstable();
    assert_eq!(vids.len(), 10);
    assert!(vids.windows(2).any(|w| w[1] > w[0] + 1), "{vids:?}");
    assert!(
        *vids.last().unwrap() >= g.count_nodes(),
        "0..num_nodes would miss nodes: {vids:?}"
    );

    let scanned: BTreeSet<(usize, usize)> = EdgeScan::all(g.valid().into_dynamic(), None)
        .map(|(s, _, o)| (s.0, o.0))
        .collect();
    let expected: BTreeSet<(usize, usize)> = g
        .edges()
        .into_iter()
        .map(|e| (e.edge.src().0, e.edge.dst().0))
        .collect();
    assert_eq!(scanned.len(), 5);
    assert_eq!(scanned, expected);

    let mut out = Vec::new();
    assert_eq!(g.to_rdf(&mut out, RdfFormat::NTriples).unwrap().triples, 5);
    let exported = String::from_utf8(out).unwrap();
    for i in 0..5 {
        assert!(
            exported.contains(&format!(
                "<http://ex/a{i}> <http://ex/p> <http://ex/b{i}> ."
            )),
            "{exported}"
        );
    }
}

/// Runs `f` on another thread and fails (instead of hanging) if it does not finish in time.
fn within_60s<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || tx.send(f()).unwrap());
    rx.recv_timeout(std::time::Duration::from_secs(60))
        .expect("deadlock: the scan blocked a writer")
}

/// The scan holds no lock between items, so writing from inside the loop cannot deadlock.
#[test]
fn edge_scan_allows_writes_between_items() {
    let g = Graph::new();
    for i in 0..200u64 {
        g.add_edge(
            i as i64,
            format!("n{i}"),
            format!("n{}", (i + 1) % 200),
            NO_PROPS,
            Some("p"),
        )
        .unwrap();
    }
    let seen = within_60s({
        let g = g.clone();
        move || {
            let mut seen = 0;
            for _ in EdgeScan::all(g.valid().into_dynamic(), None) {
                g.add_edge(
                    1000,
                    format!("new{seen}"),
                    "n0".to_string(),
                    NO_PROPS,
                    Some("p"),
                )
                .unwrap();
                seen += 1;
            }
            seen
        }
    });
    // nodes created during the scan are not anchors of it
    assert_eq!(seen, 200);
    assert_eq!(g.count_edges(), 200 + seen);

    // the export writer can write to the graph as well
    struct WritingSink(Graph, usize);
    impl std::io::Write for WritingSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.1 += 1;
            self.0
                .add_edge(
                    2000,
                    format!("sink{}", self.1),
                    "n0".to_string(),
                    NO_PROPS,
                    Some("p"),
                )
                .map_err(std::io::Error::other)?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let (stats, writes) = within_60s({
        let g = g.clone();
        move || {
            let mut sink = WritingSink(g.clone(), 0);
            let stats = g.to_rdf(&mut sink, RdfFormat::NTriples).unwrap();
            (stats, sink.1)
        }
    });
    assert_eq!(stats.triples, 200 + seen);
    assert_eq!(stats.skipped, 0);
    // the buffered writer wrote to the sink (and so to the graph) during the scan
    assert!(writes > 1, "{writes}");
}

/// A full scan that reads the edge storage locks an edge segment only while it collects a chunk
/// of edge ids, so writing to the same layer (and segment) between items cannot deadlock. Edges
/// added during the scan are not read, so it ends.
#[test]
fn edge_storage_scan_allows_writes_between_items() {
    let g = Graph::new();
    for i in 0..200u64 {
        g.add_edge(
            i as i64,
            format!("n{i}"),
            format!("n{}", (i + 1) % 200),
            NO_PROPS,
            Some("p"),
        )
        .unwrap();
    }
    let p = g.get_layer_id("p").unwrap();
    for (name, view) in [
        ("graph", g.clone().into_dynamic()),
        ("window", g.window(0, 1000).into_dynamic()),
    ] {
        for (layer, first_chunk) in [Some(p), None]
            .into_iter()
            .flat_map(|layer| [1, 7, 1 << 16].map(|first_chunk| (layer, first_chunk)))
        {
            // the edges when the scan starts (earlier scans added some)
            let before = g.count_edges();
            let scan = EdgeScan::all_in_chunks(view.clone(), layer, first_chunk).only_valid();
            assert!(scan.reads_edge_storage(), "{name}");
            let seen = within_60s({
                let g = g.clone();
                move || {
                    let mut seen = 0;
                    for (s, l, o) in scan {
                        assert_eq!(l, p);
                        // a new edge of the layer (between existing nodes and with a new node),
                        // in the segment being scanned
                        let (s, o) = (g.node(s).unwrap().name(), g.node(o).unwrap().name());
                        g.add_edge(500, s, o.clone(), NO_PROPS, Some("p")).unwrap();
                        g.add_edge(500, o, format!("x{seen}"), NO_PROPS, Some("p"))
                            .unwrap();
                        seen += 1;
                    }
                    seen
                }
            });
            assert_eq!(seen, before, "{name} {layer:?} in chunks of {first_chunk}");
            assert!(g.count_edges() > before);
        }
    }
}

#[test]
fn rdf_formats() {
    for (s, expected) in [
        ("ttl", RdfFormat::Turtle),
        (".ttl", RdfFormat::Turtle),
        (" TTL ", RdfFormat::Turtle),
        ("turtle", RdfFormat::Turtle),
        ("text/turtle", RdfFormat::Turtle),
        ("nt", RdfFormat::NTriples),
        ("n-triples", RdfFormat::NTriples),
        ("application/n-triples", RdfFormat::NTriples),
        ("nq", RdfFormat::NQuads),
        ("trig", RdfFormat::TriG),
        ("rdf", RdfFormat::RdfXml),
        ("xml", RdfFormat::RdfXml),
        ("n3", RdfFormat::N3),
    ] {
        assert_eq!(parse_rdf_format(s).unwrap(), expected, "{s}");
    }
    assert!(matches!(
        parse_rdf_format("jsonld").unwrap(),
        RdfFormat::JsonLd { .. }
    ));
    // media-type parameters
    assert_eq!(
        parse_rdf_format("text/turtle; charset=utf-8").unwrap(),
        RdfFormat::Turtle
    );
    assert_eq!(
        parse_rdf_format("turtle;profile=\"x\"").unwrap(),
        RdfFormat::Turtle
    );
    let streaming = "application/ld+json;profile=\"http://www.w3.org/ns/json-ld#streaming\"";
    let format = parse_rdf_format(streaming).unwrap();
    assert_eq!(Some(format), RdfFormat::from_media_type(streaming));
    assert_ne!(
        format,
        RdfFormat::JsonLd {
            profile: Default::default()
        }
    );
    for s in [
        "csv",
        "",
        "foo/bar",
        ".",
        "text/turtle; charset=latin1",
        // a lone `"` as a parameter value
        "text/turtle;profile=\"",
        "turtle;profile=\"",
        "application/ld+json; profile = \" ",
        "ttl;x=\"",
    ] {
        let err = parse_rdf_format(s).unwrap_err();
        assert!(
            matches!(&err, RdfError::UnknownFormat(f) if f == s),
            "{err:?}"
        );
        assert_eq!(err.to_string(), format!("unknown RDF format '{s}'"));
    }
}

/// Every supported input format loads.
#[test]
fn input_formats() {
    let expected = parse_graph(DEFAULT_GRAPH_TRIG, RdfFormat::TriG);
    let pg = PersistentGraph::new();
    pg.load_rdf(1, DEFAULT_GRAPH_TRIG.as_bytes(), RdfFormat::TriG, None)
        .unwrap();
    for format in [
        RdfFormat::NTriples,
        RdfFormat::NQuads,
        RdfFormat::Turtle,
        RdfFormat::TriG,
        RdfFormat::N3,
        RdfFormat::RdfXml,
        RdfFormat::JsonLd {
            profile: Default::default(),
        },
    ] {
        let doc = export(&pg, format);
        let g = Graph::new();
        let n = g.load_rdf(1, doc.as_bytes(), format, None).unwrap();
        assert_eq!(n, 2, "{format}");
        assert_eq!(
            parse_graph(&nt(&g), RdfFormat::NTriples),
            expected,
            "{format}"
        );
    }
}

#[test]
fn literal_and_blank_terms_through_add_triple() {
    let g = Graph::new();
    let s = iri("http://ex/s");
    let p = iri("http://ex/p");
    let o = Literal::new_typed_literal("042", oxigraph::model::vocab::xsd::INTEGER);
    let triple = Triple::new(s.clone(), p.clone(), o);
    g.add_triple(1, &triple).unwrap();
    assert!(g
        .node(format!("\"042\"^^<{XSD_INTEGER}>").as_str())
        .is_some());
    assert_eq!(nt(&g), format!("{triple} .\n"));
}
