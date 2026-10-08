//! `load_rdf` and `retract_rdf`: one edge event per triple in document order, partial loads on
//! error, N-Triples naming, and loads concurrent with queries and other writers.
use super::{ask, select, within_60s};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::GraphError,
    prelude::*,
    rdf::{model::Triple, RdfError, RdfFormat, RdfParser, RdfViewOps},
};
use std::{
    collections::{BTreeSet, HashSet},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const NT: RdfFormat = RdfFormat::NTriples;
const NQ: RdfFormat = RdfFormat::NQuads;
const TTL: RdfFormat = RdfFormat::Turtle;

/// `n` distinct N-Triples lines.
fn lines(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "<http://ex/s{}> <http://ex/p{}> <http://ex/o{i}> .",
                i % 97,
                i % 5
            )
        })
        .collect()
}

/// The sorted N-Triples lines `to_rdf` writes for a view.
fn exported<G: RdfViewOps>(view: &G) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, NT).unwrap();
    let mut lines: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

/// The number of edge events (assertions and retractions) in a graph.
fn event_count(pg: &PersistentGraph) -> usize {
    pg.edges()
        .iter()
        .flat_map(|e| e.explode_layers())
        .map(|e| e.history().collect().len() + e.deletions().collect().len())
        .sum()
}

/// Every edge event (with its event id) per layer, and the node names.
fn events<G: StaticGraphViewOps>(g: &G) -> (BTreeSet<String>, Vec<String>) {
    let nodes = g.nodes().iter().map(|n| n.name()).collect();
    let mut events = Vec::new();
    for e in g.edges().iter() {
        for layer in e.layer_names() {
            let el = e.layers(layer.as_ref()).unwrap();
            events.push(format!(
                "{} {layer} {} +{:?} -{:?}",
                e.src().name(),
                e.dst().name(),
                el.history().collect(),
                el.deletions().collect()
            ));
        }
    }
    events.sort();
    (nodes, events)
}

/// A parse error keeps the triples before it (in N-Triples and in Turtle, for assertions and
/// retractions) and writes nothing after it.
#[test]
fn parse_errors_keep_the_triples_before_them() {
    for (format, bad) in [
        (NT, "<http://ex/s> <http://ex/p> \"unterminated ."),
        (NT, "this is not n-triples"),
        (TTL, "<http://ex/s> <http://ex/p> ex:undeclared ."),
    ] {
        for at in [0, 1, 1500] {
            let mut doc = lines(at);
            doc.push(bad.to_owned());
            doc.extend(lines(20).into_iter().map(|l| l.replace("ex/s", "ex/after")));
            let doc = doc.join("\r\n");
            for retract in [false, true] {
                let pg = PersistentGraph::new();
                let err = if retract {
                    pg.retract_rdf(1, doc.as_bytes(), format, None)
                } else {
                    pg.load_rdf(1, doc.as_bytes(), format, None)
                }
                .unwrap_err();
                assert!(
                    matches!(err, GraphError::Rdf(RdfError::Parse(_))),
                    "{err:?}"
                );
                assert_eq!(event_count(&pg), at, "{bad} at {at}, retract: {retract}");
                assert!(pg.node("http://ex/after0").is_none());
                if !retract {
                    assert_eq!(exported(&pg).len(), at);
                }
            }
        }
    }
}

/// A term that cannot be stored keeps the triples before it and writes nothing after it.
#[test]
fn non_canonical_terms_keep_the_triples_before_them() {
    for bad in [
        "<raphtory:%61> <http://ex/p> <http://ex/o> .",
        "<http://ex/s> <raphtory:asof:2024> <http://ex/o> .",
        "<http://ex/s> <http://ex/p> <raphtory:%2f> .",
    ] {
        let term = bad.split(' ').find(|t| t.contains("raphtory:")).unwrap();
        for at in [0, 3, 1500] {
            let mut doc = lines(at);
            doc.push(bad.to_owned());
            doc.extend(lines(10).into_iter().map(|l| l.replace("ex/s", "ex/after")));
            let doc = doc.join("\n");
            for (format, retract) in [(NT, false), (NT, true), (TTL, false), (TTL, true)] {
                let pg = PersistentGraph::new();
                let err = if retract {
                    pg.retract_rdf(1, doc.as_bytes(), format, None)
                } else {
                    pg.load_rdf(1, doc.as_bytes(), format, None)
                }
                .unwrap_err();
                match err {
                    GraphError::Rdf(RdfError::NonCanonicalTerm(t)) => assert_eq!(t, term),
                    err => panic!("unexpected {err:?}"),
                }
                assert_eq!(
                    event_count(&pg),
                    at,
                    "{bad} at {at} {format}, retract: {retract}"
                );
                assert!(pg.node("http://ex/after0").is_none());
            }
        }
    }
}

/// Line endings, comments, a missing final newline and empty documents. A byte order mark is
/// not part of the RDF syntaxes, so a document that starts with one fails to parse and writes
/// nothing.
#[test]
fn document_shapes() {
    let body = lines(30);
    let expected = {
        let pg = PersistentGraph::new();
        pg.load_rdf(1, body.join("\n").as_bytes(), NT, None)
            .unwrap();
        exported(&pg)
    };
    assert_eq!(expected.len(), 30);
    let docs = [
        (body.join("\r\n") + "\r\n", 30),
        (body.join("\r"), 30),
        (
            format!("# a comment\n{}\n# the end", body.join("\n  # between\n")),
            30,
        ),
        (
            format!("{} # trailing comment\n", body.join(" # comment\n")),
            30,
        ),
        (String::new(), 0),
        ("\n\n  \r\n".to_owned(), 0),
        ("# only a comment".to_owned(), 0),
    ];
    for (doc, triples) in &docs {
        for format in [NT, NQ, TTL] {
            let pg = PersistentGraph::new();
            let read = pg.load_rdf(1, doc.as_bytes(), format, None).unwrap();
            assert_eq!(read, *triples, "{doc:?}");
            let stored = exported(&pg);
            if *triples == 0 {
                assert!(stored.is_empty() && pg.count_nodes() == 0, "{doc:?}");
            } else {
                assert_eq!(stored, expected, "{doc:?} {format}");
            }
        }
    }
    // a byte order mark is a parse error on line 1, column 1
    let bom = format!("\u{feff}{}\n", body.join("\n"));
    for format in [NT, NQ, TTL] {
        for retract in [false, true] {
            let pg = PersistentGraph::new();
            let err = if retract {
                pg.retract_rdf(1, bom.as_bytes(), format, None)
            } else {
                pg.load_rdf(1, bom.as_bytes(), format, None)
            }
            .unwrap_err();
            match &err {
                GraphError::Rdf(RdfError::Parse(e)) => {
                    assert!(e.to_string().contains("line 1 "), "{format}: {e}")
                }
                err => panic!("{format}: unexpected {err:?}"),
            }
            assert_eq!(event_count(&pg), 0, "{format}");
            assert_eq!(pg.count_nodes(), 0, "{format}");
        }
    }
}

/// The N-Triples/N-Quads fast naming path gives every term the name `name_of` gives it, so a
/// load matches `add_triple` (including `raphtory:` IRIs, look-alikes, escapes, literals and
/// blank nodes).
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
"#;
    let triples: Vec<Triple> = RdfParser::from_format(NT)
        .for_reader(doc.as_bytes())
        .map(|q| q.unwrap().into())
        .collect();
    assert_eq!(triples.len(), 13);
    let reference = PersistentGraph::new();
    for triple in &triples {
        reference.add_triple(1, triple).unwrap();
    }
    let expected = events(&reference);
    assert!(expected.0.contains("Alice Smith") && expected.0.contains("café"));
    assert!(expected.0.contains("Raphtory:a") && expected.0.contains("raphtory-a:b"));
    for format in [NT, NQ, TTL] {
        let pg = PersistentGraph::new();
        // one load per line, in the same order as the `add_triple` calls above
        for line in doc.lines() {
            pg.load_rdf(1, line.as_bytes(), format, None).unwrap();
        }
        assert_eq!(events(&pg), expected, "{format}");
        let pg = PersistentGraph::new();
        assert_eq!(pg.load_rdf(1, doc.as_bytes(), format, None).unwrap(), 13);
        assert_eq!(exported(&pg), exported(&reference), "{format}");
    }

    // blank nodes keep their labels on retraction, as `delete_triple` keeps them
    let blank = "_:x <http://ex/p> _:y .\n_:y <raphtory:q> <raphtory:a> .\n";
    let reference = PersistentGraph::new();
    for q in RdfParser::from_format(NT).for_reader(blank.as_bytes()) {
        let triple: Triple = q.unwrap().into();
        reference.delete_triple(1, &triple).unwrap();
    }
    let pg = PersistentGraph::new();
    for line in blank.lines() {
        pg.retract_rdf(1, line.as_bytes(), NT, None).unwrap();
    }
    assert_eq!(events(&pg), events(&reference));
    assert!(pg.node("_:x").is_some() && pg.node("a").is_some());

    // with a base IRI, N-Triples use the full mapping too
    let pg = PersistentGraph::new();
    pg.load_rdf(1, doc.as_bytes(), NT, Some("http://base/"))
        .unwrap();
    assert_eq!(exported(&pg), exported(&reference_of(doc)));
}

/// The graph `add_triple` writes for the triples of an N-Triples document.
fn reference_of(doc: &str) -> PersistentGraph {
    let g = PersistentGraph::new();
    for q in RdfParser::from_format(NT).for_reader(doc.as_bytes()) {
        let triple: Triple = q.unwrap().into();
        g.add_triple(1, &triple).unwrap();
    }
    g
}

/// A document of `n` triples (rounded up to a multiple of 4) about `n / 4` people, with
/// literals and blank nodes.
fn people(n: usize) -> String {
    let mut doc = String::with_capacity(n * 90);
    for i in 0..n.div_ceil(4) {
        let s = format!("<http://example.org/person/{i}>");
        doc += &format!("{s} <http://xmlns.com/foaf/0.1/name> \"Person {i}\" .\n");
        doc += &format!(
            "{s} <http://xmlns.com/foaf/0.1/knows> <http://example.org/person/{}> .\n",
            (i * 7919) % (n / 4 + 1)
        );
        doc += &format!(
            "{s} <http://xmlns.com/foaf/0.1/age> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
            i % 90
        );
        doc += &format!("{s} <http://example.org/address> _:a{i} .\n");
    }
    doc
}

/// A load of 100,000 triples while SPARQL queries run on other threads: neither waits for the
/// other forever, and the queries see consistent nodes.
#[test]
fn queries_run_during_a_load() {
    let pg = PersistentGraph::new();
    pg.load_rdf(0, people(2000).as_bytes(), NT, None).unwrap();
    let doc = people(100_000);
    let expected = doc.lines().count();
    let (read, queries) = within_60s({
        let pg = pg.clone();
        move || {
            let done = AtomicBool::new(false);
            let queries = AtomicUsize::new(0);
            std::thread::scope(|scope| {
                let loader = scope.spawn(|| {
                    let read = pg.load_rdf(1, doc.as_bytes(), NT, None);
                    done.store(true, Ordering::Relaxed);
                    read.unwrap()
                });
                for _ in 0..2 {
                    scope.spawn(|| {
                        while !done.load(Ordering::Relaxed) {
                            assert!(ask(&pg, "ASK { <http://example.org/person/1> ?p ?o }"));
                            select(
                                &pg,
                                "SELECT ?s { ?s <http://xmlns.com/foaf/0.1/age> 42 } LIMIT 5",
                            );
                            queries.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
                (loader.join().unwrap(), queries.load(Ordering::Relaxed))
            })
        }
    });
    assert_eq!(read, expected);
    assert!(queries > 0);
    let count = select(
        &pg,
        "SELECT (COUNT(*) AS ?n) { ?s <http://xmlns.com/foaf/0.1/name> ?o }",
    );
    // the people of the first document are people of the second
    assert_eq!(
        count,
        vec![vec![format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            expected / 4
        )]]
    );
}

/// The nodes a load creates can be read, by name and by SPARQL, at any time during the load: a
/// node is never visible without its name.
#[test]
fn new_nodes_are_read_during_a_load() {
    let pg = PersistentGraph::new();
    let doc = people(100_000);
    let expected = doc.lines().count();
    let (read, scans, asks) = within_60s({
        let pg = pg.clone();
        move || {
            let done = AtomicBool::new(false);
            std::thread::scope(|scope| {
                let loader = scope.spawn(|| {
                    let read = pg.load_rdf(1, doc.as_bytes(), NT, None);
                    done.store(true, Ordering::Relaxed);
                    read.unwrap()
                });
                let scans = scope.spawn(|| {
                    // SPARQL scans (which hold no storage lock between nodes) name every node
                    // they reach, and lookups by name find the nodes with their names
                    let mut scans = 0;
                    while !done.load(Ordering::Relaxed) {
                        let rows = select(
                            &pg,
                            "SELECT ?s ?o { ?s <http://example.org/address> ?o } LIMIT 2000",
                        );
                        assert!(rows.iter().flatten().all(|term| term.len() > 2));
                        let k = 7919 * scans % 25_000;
                        if let Some(node) = pg.node(format!("http://example.org/person/{k}")) {
                            assert_eq!(node.name(), format!("http://example.org/person/{k}"));
                        }
                        scans += 1;
                    }
                    scans
                });
                let asks = scope.spawn(|| {
                    let (mut asks, mut k) = (0, 0);
                    while !done.load(Ordering::Relaxed) {
                        ask(
                            &pg,
                            &format!("ASK {{ <http://example.org/person/{k}> ?p ?o }}"),
                        );
                        k = (k + 7919) % 25_000;
                        asks += 1;
                    }
                    asks
                });
                (
                    loader.join().unwrap(),
                    scans.join().unwrap(),
                    asks.join().unwrap(),
                )
            })
        }
    });
    assert_eq!(read, expected);
    assert!(scans > 0 && asks > 0);
    assert_eq!(pg.count_temporal_edges(), expected);
}

/// Loads that share new nodes, running at the same time as each other and as `add_edge` calls
/// on the same edges, create every node once and keep every event.
#[test]
fn concurrent_writers_create_each_node_once() {
    const N: usize = 5_000;
    let doc = |layer: usize| -> String {
        (0..N)
            .map(|i| {
                format!(
                    "<http://ex/n{i}> <http://ex/p{layer}> <http://ex/m{}> .\n",
                    i % 777
                )
            })
            .collect()
    };
    let (a, b) = (doc(0), doc(1));
    for _ in 0..3 {
        let pg = PersistentGraph::new();
        within_60s({
            let (pg, a, b) = (pg.clone(), a.clone(), b.clone());
            move || {
                std::thread::scope(|scope| {
                    scope.spawn(|| pg.load_rdf(1, a.as_bytes(), NT, None).unwrap());
                    scope.spawn(|| pg.load_rdf(1, b.as_bytes(), NT, None).unwrap());
                    scope.spawn(|| pg.load_rdf(3, a.as_bytes(), NT, None).unwrap());
                    scope.spawn(|| {
                        for i in 0..N {
                            let (s, o) =
                                (format!("http://ex/n{i}"), format!("http://ex/m{}", i % 777));
                            pg.add_edge(2, s.as_str(), o.as_str(), NO_PROPS, Some("http://ex/p0"))
                                .unwrap();
                        }
                    });
                })
            }
        });
        let names: HashSet<String> = pg.nodes().iter().map(|n| n.name()).collect();
        assert_eq!((pg.count_nodes(), names.len()), (N + 777, N + 777));
        assert_eq!(pg.count_edges(), N);
        for e in pg.edges().iter() {
            let times = |layer: &str| e.layers(layer).unwrap().history().t().collect();
            assert_eq!(times("http://ex/p0"), vec![1, 2, 3]);
            assert_eq!(times("http://ex/p1"), vec![1]);
        }
    }
}
