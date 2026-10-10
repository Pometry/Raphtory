//! RDF import and export, storage semantics, scans and formats.
use super::{layer_of, rdf12, within_60s};
#[cfg(feature = "io")]
use crate::db::api::storage::storage::PersistenceStrategy;
use crate::{
    db::api::view::{internal::CoreGraphOps, IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{self, Literal, Triple},
        parse_rdf_format,
        scan::EdgeScan,
        RdfError, RdfFormat,
    },
};
use oxigraph::io::RdfParser;
use std::collections::BTreeSet;
#[cfg(feature = "io")]
use storage::Extension;
#[cfg(feature = "io")]
use tempfile::tempdir;

const FOAF: &str = include_str!("fixtures/foaf.ttl");
const DATATYPES: &str = include_str!("fixtures/datatypes.nt");
const BLANK_NODES: &str = include_str!("fixtures/blank_nodes.ttl");

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn export<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    format: RdfFormat,
    prefixes: &[(&str, &str)],
) -> Result<String, GraphError> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, format, prefixes)?;
    Ok(String::from_utf8(out).unwrap())
}

fn nt<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>) -> String {
    export(view, RdfFormat::NTriples, &[]).unwrap()
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

/// A FOAF Turtle document: one edge per (subject, object), one layer per predicate, literal
/// objects are nodes, and the load time is the history.
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
            layer_of("http://example.org/rel/worksWith"),
            layer_of("http://xmlns.com/foaf/0.1/knows"),
        ])
    );

    // `42` (shorthand) and `"42"^^xsd:integer` are the same literal, so one node.
    let age = format!("\"42\"^^<{XSD_INTEGER}>");
    assert_eq!(pg.node(age.as_str()).unwrap().in_degree(), 2);
    // the stored history is the load time
    assert_eq!(alice_bob.history().t().collect(), vec![1, 1]);
}

/// Blank nodes are renamed on load and matched as written on retraction.
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
    assert_eq!(nt(&rg).lines().count(), 4);

    // retract one of the `_:x ex:p ex:o` triples using its label from the export
    let exported = nt(&rg);
    let line = exported
        .lines()
        .find(|l| l.contains("<http://ex/p>"))
        .unwrap()
        .to_owned();
    let label = line.split(' ').next().unwrap().to_owned();
    assert!(label.starts_with("_:"));
    let n = rg
        .retract_rdf(3, line.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(n, 1);

    let after = nt(&rg.valid());
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
    assert!(nt(&rg.at(2)).contains(&line));
    // the node keeps its label
    assert!(rg.graph().node(label.as_str()).is_some());
}

/// An RDF view needs persistent semantics, so a view of an event graph is refused.
#[test]
fn views_of_event_graphs_are_refused() {
    assert!(matches!(
        RdfView::from_view(Graph::new()),
        Err(GraphError::Rdf(RdfError::NotPersistent))
    ));
}

/// Inputs that cannot be stored fail with their error. Loads are not atomic: the triples before
/// the bad one stay, and nothing after it is written.
#[test]
fn rejected_inputs_leave_the_graph_unchanged() {
    let g = Graph::new();
    g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    let doc = "<http://ex/s> <http://ex/p> <http://ex/o> .";
    let err = RdfGraph::from(g.clone())
        .load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::NonStringIds)),
        "{err:?}"
    );
    assert_eq!(g.count_edges(), 1);

    let rows: [(RdfFormat, &str, fn(&RdfError) -> bool); 5] = [
        (
            RdfFormat::NTriples,
            "<raphtory:%61> <http://ex/p> <http://ex/o> .",
            |e| matches!(e, RdfError::NonCanonicalTerm(_)),
        ),
        (
            RdfFormat::NTriples,
            "<http://ex/s> <raphtory:a%2Fb> <http://ex/o> .",
            |e| matches!(e, RdfError::UnsafeLayerName(_)),
        ),
        (
            RdfFormat::NTriples,
            "<http://ex/a> <http://ex/p> <<( <http://ex/s> <http://ex/q> <http://ex/o> )>> .",
            |e| {
                if rdf12() {
                    matches!(e, RdfError::Rdf12Term(_))
                } else {
                    matches!(e, RdfError::Parse(_))
                }
            },
        ),
        (RdfFormat::NTriples, "this is not n-triples", |e| {
            matches!(e, RdfError::Parse(_))
        }),
        (
            RdfFormat::NQuads,
            "<http://ex/s> <http://ex/p> <http://ex/o> <http://ex/g> .",
            |e| matches!(e, RdfError::Parse(_)),
        ),
    ];
    for (format, bad, expected) in rows {
        let doc = format!(
            "<http://ex/a> <http://ex/p> <http://ex/b> .\n{bad}\n<http://ex/c> <http://ex/p> <http://ex/d> .\n"
        );
        let rg = RdfGraph::new();
        match rg.load_rdf(1, doc.as_bytes(), format, None).unwrap_err() {
            GraphError::Rdf(err) => assert!(expected(&err), "{bad}: {err:?}"),
            err => panic!("{bad}: {err:?}"),
        }
        assert_eq!(rg.graph().count_edges(), 1, "{bad}");
    }
}

/// `base_iri` resolves relative IRIs.
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
        nt(&rg),
        "<http://ex/dir/s> <http://ex/dir/p> <http://ex/dir/o> .\n"
    );
}

/// Export of a blank-free document parses back to the same RDF graph.
#[test]
fn export_round_trips_blank_free_fixtures() {
    for (data, format) in [(FOAF, RdfFormat::Turtle), (DATATYPES, RdfFormat::NTriples)] {
        let input = parse_graph(data, format);
        let rg = RdfGraph::new();
        let n = rg.load_rdf(1, data.as_bytes(), format, None).unwrap();
        assert!(n >= input.len());

        let exported = nt(&rg);
        assert_eq!(exported.lines().count(), input.len());
        assert_eq!(parse_graph(&exported, RdfFormat::NTriples), input);
        let xml = export(&rg, RdfFormat::RdfXml, &[]).unwrap();
        assert_eq!(parse_graph(&xml, RdfFormat::RdfXml), input);
    }
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
        Some("http%3A%2F%2Fex%2Fknows"),
    )
    .unwrap();
    g.add_edge(
        4,
        "user:42",
        "raphtory:x",
        NO_PROPS,
        Some("rel%3Awith%3Acolons"),
    )
    .unwrap();
    g.add_edge(5, "", "\"x\"@EN", NO_PROPS, Some("42")).unwrap();
    g.add_edge(6, "Bob", "\"lit\"", NO_PROPS, Some("_default"))
        .unwrap();
    g.add_edge(7, "Bob", "Bob", NO_PROPS, Some("self")).unwrap();
    // `:` is not allowed in Turtle or SPARQL blank-node labels, so these are not blank nodes
    g.add_edge(8, "_:a:b", "_::x", NO_PROPS, Some("_%3Ap%3Aq"))
        .unwrap();

    let u64_graph = Graph::new();
    u64_graph.add_edge(1, 1, 2, NO_PROPS, None).unwrap();
    u64_graph.add_edge(2, 42, 1, NO_PROPS, Some("x y")).unwrap();
    u64_graph.add_edge(3, 2, 2, NO_PROPS, None).unwrap();

    for original in [RdfGraph::from(g), RdfGraph::from(u64_graph)] {
        let first = nt(&original);
        assert_eq!(first.lines().count(), original.graph().count_edges());

        let reloaded = RdfGraph::new();
        reloaded
            .load_rdf(0, first.as_bytes(), RdfFormat::NTriples, None)
            .unwrap_or_else(|e| panic!("export does not load: {e}\n{first}"));
        assert_eq!(node_names(reloaded.graph()), node_names(original.graph()));
        assert_eq!(layer_names(reloaded.graph()), layer_names(original.graph()));

        let second = nt(&reloaded);
        assert_eq!(sorted_lines(&second), sorted_lines(&first));
        assert_eq!(
            parse_graph(&second, RdfFormat::NTriples),
            parse_graph(&first, RdfFormat::NTriples)
        );

        // the export is also a retraction document for every triple it contains
        let n = reloaded
            .retract_rdf(1, first.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
        assert_eq!(n, first.lines().count());
        assert_eq!(nt(&reloaded.valid()), "");
    }
}

/// RDF/XML skips (and counts) triples whose predicate cannot be an element name or is an RDF/XML
/// syntax term, and literals XML cannot hold; an `rdf:type` whose object cannot name an element
/// is written after another triple of its subject; blank-node labels that are not NCNames are
/// renamed without merging any two.
#[test]
fn rdf_xml_export_is_well_formed() {
    const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const XMLNS_FOO: &str = "http://www.w3.org/2000/xmlns/foo";
    let g = Graph::new();
    let edge = |s: &str, o: &str, p: &str| {
        g.add_edge(1, s, o, NO_PROPS, Some(layer_of(p).as_str()))
            .unwrap();
    };
    // a predicate without a local name
    edge("http://ex/a", "http://ex/b", "http://ex/42");
    // the local name can be a suffix of the last segment
    edge("http://ex/a", "http://ex/b", "http://ex/4p");
    // an RDF/XML syntax term (the serializer fails on it after it started the element of `a`)
    edge(
        "http://ex/a",
        "http://ex/b",
        &format!("{RDF_NS}Description"),
    );
    // an IRI in the `xmlns` namespace would be written as an `xmlns:foo` element
    edge("http://ex/a", "http://ex/b", XMLNS_FOO);
    // the first triple of `s` is an `rdf:type` whose object has no local name
    edge("http://ex/s", "http://ex/42", RDF_TYPE);
    edge("http://ex/s", "http://ex/o", "http://ex/p");
    // the only triple of `t` is one
    edge("http://ex/t", "http://ex/42", RDF_TYPE);
    // an `rdf:type` object with a local name names the subject's element
    edge("http://ex/u", "http://ex/Person", RDF_TYPE);
    // a syntax term can be the object (the serializer writes an `rdf:Description` element)
    edge("http://ex/r", &format!("{RDF_NS}li"), RDF_TYPE);
    // blank nodes whose label is not an NCName (it starts with a digit) are renamed, and so are
    // the labels they could collide with
    edge("_:1", "http://ex/o", "http://ex/p");
    edge("http://ex/v", "_:x1", "http://ex/p");
    edge("http://ex/v", "_:x", "http://ex/p");
    // literals with a character XML cannot hold, or without one
    let literal = |value: &str| Literal::new_simple_literal(value).to_string();
    edge("http://ex/w", &literal("bell\u{7}"), "http://ex/p");
    edge("http://ex/w", &literal("tab\tnewline\n"), "http://ex/p");
    let g = RdfGraph::from(g);

    let all = nt(&g);
    // the `rdf:type` triple of `s` comes first
    let s_type = format!("<http://ex/s> <{RDF_TYPE}> <http://ex/42> .");
    assert!(
        all.find(&s_type).unwrap() < all.find("<http://ex/s> <http://ex/p>").unwrap(),
        "{all}"
    );
    let expected = [
        "<http://ex/a> <http://ex/4p> <http://ex/b> .".to_owned(),
        s_type,
        "<http://ex/s> <http://ex/p> <http://ex/o> .".to_owned(),
        format!("<http://ex/u> <{RDF_TYPE}> <http://ex/Person> ."),
        format!("<http://ex/r> <{RDF_TYPE}> <{RDF_NS}li> ."),
        "_:x1 <http://ex/p> <http://ex/o> .".to_owned(),
        "<http://ex/v> <http://ex/p> _:xx1 .".to_owned(),
        "<http://ex/v> <http://ex/p> _:x .".to_owned(),
        format!(
            "<http://ex/w> <http://ex/p> {} .",
            literal("tab\tnewline\n")
        ),
    ]
    .join("\n");
    let expected = parse_graph(&expected, RdfFormat::NTriples);

    let mut out = Vec::new();
    let stats = g.to_rdf(&mut out, RdfFormat::RdfXml, &[]).unwrap();
    let doc = String::from_utf8(out).unwrap();
    assert_eq!((stats.written, stats.skipped), (9, 5), "{doc}");
    assert!(!doc.contains("<xmlns:"), "{doc}");
    assert_eq!(parse_graph(&doc, RdfFormat::RdfXml), expected, "{doc}");
}

/// `to_rdf` rejects the prefix names a format cannot write (the oxigraph serializers write them
/// as is, giving documents that do not parse).
#[test]
fn prefix_names_are_checked() {
    let g = Graph::new();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http%3A%2F%2Fex%2Fp"),
    )
    .unwrap();
    let g = RdfGraph::from(g);
    let expected = parse_graph(&nt(&g), RdfFormat::NTriples);

    for (format, name, valid) in [
        (RdfFormat::Turtle, "ex", true),
        (RdfFormat::Turtle, "", true),
        (RdfFormat::Turtle, "_x", false),
        (RdfFormat::Turtle, "ex.", false),
        (RdfFormat::RdfXml, "_x", true),
        (RdfFormat::RdfXml, "xmlns", false),
    ] {
        match export(&g, format, &[(name, "http://ex/")]) {
            Ok(doc) => {
                assert!(valid, "{format} {name:?} is accepted");
                assert_eq!(
                    parse_graph(&doc, format),
                    expected,
                    "{format} {name:?}: {doc}"
                );
            }
            Err(err) => {
                assert!(!valid, "{format} {name:?}: {err:?}");
                assert!(
                    matches!(&err, GraphError::Rdf(RdfError::InvalidPrefix { name: n, format: f, .. }) if n == name && *f == format),
                    "{format} {name:?}: {err:?}"
                );
            }
        }
    }

    // N-Triples has no prefixes, so any name is ignored
    assert_eq!(
        export(&g, RdfFormat::NTriples, &[("a b", "http://ex/")]).unwrap(),
        nt(&g)
    );
}

/// Within one document, writes get increasing event ids in document order.
#[test]
fn document_order_decides_ties() {
    let rg = RdfGraph::new();
    let in_order = [
        "<http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/q> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <http://ex/r> <http://ex/o> .",
    ]
    .join("\n");
    rg.load_rdf(1, in_order.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let e = rg.graph().edge("http://ex/s", "http://ex/o").unwrap();
    let ids = |p: &str| {
        e.layers(layer_of(p).as_str())
            .unwrap()
            .history()
            .event_id()
            .collect()
    };
    let (p_ids, q_ids, r_ids) = (ids("http://ex/p"), ids("http://ex/q"), ids("http://ex/r"));
    assert_eq!((p_ids.len(), q_ids.len(), r_ids.len()), (2, 1, 1));
    assert!(
        p_ids[0] < q_ids[0] && q_ids[0] < p_ids[1] && p_ids[1] < r_ids[0],
        "{p_ids:?} {q_ids:?} {r_ids:?}"
    );
}

/// Full scans hold no storage lock between items, so writing to the same layer between items
/// cannot deadlock. Edges added during a scan are not read, so it ends. The export holds no lock
/// while it writes to its output either.
#[test]
fn scans_allow_writes_between_items() {
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
    let modes = [Some(p), None]
        .into_iter()
        .flat_map(|layer| [None, Some(1), Some(1 << 16)].map(|first_chunk| (layer, first_chunk)));
    for (run, (layer, first_chunk)) in modes.enumerate() {
        let view = match layer {
            Some(_) => g.valid_layers("p").into_dynamic(),
            None => g.clone().into_dynamic(),
        };
        // `None` reads the out-edges of every node
        let scan = match first_chunk {
            Some(first_chunk) => EdgeScan::all_in_chunks(view, layer, first_chunk),
            None => EdgeScan::all_by_nodes(view, layer),
        };
        assert_eq!(scan.reads_by_edge_id(), first_chunk.is_some());
        // the edges when the scan starts (earlier scans added some)
        let before = g.count_edges();
        let seen = within_60s({
            let g = g.clone();
            move || {
                let mut seen = 0;
                for (s, l, o) in scan {
                    assert_eq!(l, p);
                    // a new event on the pair being read, and a new edge of the layer from a new
                    // node, which is not an anchor of a scan by nodes
                    let (s, o) = (g.node(s).unwrap().name(), g.node(o).unwrap().name());
                    g.add_edge(500, s, o.clone(), NO_PROPS, Some("p")).unwrap();
                    g.add_edge(500, format!("x{run}_{seen}"), o, NO_PROPS, Some("p"))
                        .unwrap();
                    seen += 1;
                }
                seen
            }
        });
        assert_eq!(seen, before, "{layer:?} in chunks of {first_chunk:?}");
        assert!(g.count_edges() > before);
    }

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
    let edges = g.count_edges();
    let (stats, writes) = within_60s({
        let g = g.clone();
        move || {
            let mut sink = WritingSink(g.clone(), 0);
            let stats = RdfGraph::from(g)
                .to_rdf(&mut sink, RdfFormat::NTriples, &[])
                .unwrap();
            (stats, sink.1)
        }
    });
    assert_eq!(stats.written, edges);
    assert_eq!(stats.skipped, 0);
    // the buffered writer wrote to the sink (and so to the graph) during the scan
    assert!(writes > 1, "{writes}");
}

/// An `RdfGraph` shares the storage of the `Graph` it wraps.
#[test]
fn writes_are_seen_by_the_wrapped_graph() {
    let g = Graph::new();
    let rg = RdfGraph::from(g.clone());
    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let edge = g.edge("http://ex/a", "http://ex/b").unwrap();
    assert_eq!(
        edge.layer_names()
            .into_iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>(),
        [layer_of("http://ex/p")]
    );
    g.add_edge(
        2,
        "http://ex/b",
        "http://ex/c",
        NO_PROPS,
        Some(layer_of("http://ex/q").as_str()),
    )
    .unwrap();
    assert_eq!(
        sorted_lines(&nt(&rg)),
        [doc, "<http://ex/b> <http://ex/q> <http://ex/c> ."]
    );
}

/// A retraction is an event of its triple even if the triple does not hold: the whole graph
/// and a window holding it show the triple, `valid()` and `at(t)` do not.
#[test]
fn retracting_a_triple_that_does_not_hold() {
    let rg = RdfGraph::new();
    let never = "<http://ex/a> <http://ex/p> <http://ex/b> .";
    let twice = "<http://ex/a> <http://ex/q> <http://ex/c> .";
    rg.retract_rdf(5, never.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    rg.load_rdf(10, twice.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    for t in [20, 30] {
        rg.retract_rdf(t, twice.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
    }
    assert_eq!(sorted_lines(&nt(&rg)), [never, twice]);
    assert_eq!(nt(&rg.window(25, 60)), format!("{twice}\n"));
    assert_eq!(nt(&rg.valid()), "");
    assert_eq!(nt(&rg.at(3)), "");
    assert_eq!(nt(&rg.at(35)), "");
}

/// A stored graph, encoded or (with disk storage) on disk, opened with `RdfGraph::load` keeps
/// the retractions written through the wrapper.
#[cfg(feature = "io")]
#[test]
fn load_a_stored_graph() {
    let write = |g: &Graph| {
        let rg = RdfGraph::from(g.clone());
        let retracted = "<http://ex/a> <http://ex/q> <http://ex/c> .";
        let doc = format!("<http://ex/a> <http://ex/p> <http://ex/b> .\n{retracted}");
        rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
        rg.retract_rdf(2, retracted.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
    };
    let encoded = tempdir().unwrap();
    let g = Graph::new();
    write(&g);
    g.encode(encoded.path()).unwrap();
    let mut dirs = vec![encoded];
    if Extension::disk_storage_enabled() {
        let disk = tempdir().unwrap();
        write(&Graph::new_at_path(disk.path()).unwrap());
        dirs.push(disk);
    }
    for dir in &dirs {
        let loaded = RdfGraph::load(dir.path()).unwrap();
        assert_eq!(
            nt(&loaded.valid()),
            "<http://ex/a> <http://ex/p> <http://ex/b> .\n"
        );
    }
}

#[test]
fn rdf_formats() {
    for (s, expected) in [
        (" .TTL ", RdfFormat::Turtle),
        ("n-triples", RdfFormat::NTriples),
        ("text/turtle; charset=utf-8", RdfFormat::Turtle),
    ] {
        assert_eq!(parse_rdf_format(s).unwrap(), expected, "{s}");
    }
    // a lone `"` as a parameter value would make oxigraph panic
    for s in ["csv", "text/turtle;profile=\""] {
        let err = parse_rdf_format(s).unwrap_err();
        assert!(
            matches!(&err, RdfError::UnknownFormat(f) if f == s),
            "{err:?}"
        );
    }
}
