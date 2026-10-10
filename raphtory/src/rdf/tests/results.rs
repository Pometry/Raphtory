//! Serialized SPARQL results: `SparqlFormat`, `sparql_to_writer` and `SparqlResults::write`.
use super::layer_of;
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{Literal, Triple},
        QueryResultsFormat, RdfError, RdfFormat, SparqlFormat, SparqlOptions, WriteStats,
    },
};
use oxigraph::io::RdfParser;
use std::collections::BTreeSet;

/// What a write gave: the document and the counts.
type Written = Result<(String, WriteStats), GraphError>;

fn written(write: impl FnOnce(&mut Vec<u8>) -> Result<WriteStats, GraphError>) -> Written {
    let mut out = Vec::new();
    let stats = write(&mut out)?;
    Ok((String::from_utf8(out).expect("output is UTF-8"), stats))
}

/// `sparql_to_writer` into a string.
fn stream<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
    format: impl Into<SparqlFormat>,
) -> Written {
    written(|out| view.sparql_to_writer(query, out, format))
}

/// `sparql()?.write()` into a string.
fn collect<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
    format: impl Into<SparqlFormat>,
) -> Written {
    let results = view.sparql(query)?;
    written(|out| results.write(out, format))
}

/// `sparql_to_writer` and `sparql()?.write()`, which must succeed and write the same.
fn same_written<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
    format: impl Into<SparqlFormat>,
) -> (String, WriteStats) {
    let format = format.into();
    let streamed = stream(view, query, format.clone()).unwrap_or_else(|e| panic!("{query}: {e}"));
    let collected = collect(view, query, format).unwrap_or_else(|e| panic!("{query}: {e}"));
    assert_eq!(streamed, collected, "{query}");
    streamed
}

fn format(name: &str) -> SparqlFormat {
    SparqlFormat::parse(name).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn rdf_error(error: GraphError) -> RdfError {
    match error {
        GraphError::Rdf(error) => error,
        other => panic!("not an RDF error: {other}"),
    }
}

/// Parses an RDF document back into a set of triples in N-Triples form.
fn parse_graph(doc: &str, format: RdfFormat) -> BTreeSet<String> {
    RdfParser::from_format(format)
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap_or_else(|e| panic!("{e}: {doc}"))).to_string())
        .collect()
}

/// One name gives each slot the format it means for it.
#[test]
fn format_names() {
    let jsonld = RdfFormat::from_extension("jsonld").unwrap();
    // names of JSON-LD can stand for different profiles, so compare kinds of RDF formats
    let kind = |format: RdfFormat| match format {
        RdfFormat::JsonLd { .. } => "JSON-LD",
        format => format.name(),
    };
    let slots = |name: &str| {
        let format = format(name);
        (format.results, format.graph.map(kind))
    };
    use QueryResultsFormat::{Csv, Json, Xml};
    let table: &[(&str, Option<QueryResultsFormat>, Option<RdfFormat>)] = &[
        // both slots
        ("json", Some(Json), Some(jsonld)),
        ("xml", Some(Xml), Some(RdfFormat::RdfXml)),
        ("txt", Some(Csv), Some(RdfFormat::NTriples)),
        // only SELECT and ASK
        ("csv", Some(Csv), None),
        ("application/sparql-results+json", Some(Json), None),
        // only CONSTRUCT and DESCRIBE
        ("nt", None, Some(RdfFormat::NTriples)),
        ("text/turtle", None, Some(RdfFormat::Turtle)),
        // spellings: a leading dot, case and media-type parameters
        (".JSON", Some(Json), Some(jsonld)),
        ("text/csv; charset=utf-8", Some(Csv), None),
    ];
    for (name, results, graph) in table {
        assert_eq!(slots(name), (*results, graph.map(kind)), "{name}");
    }

    // neither, including a malformed media-type parameter
    for name in ["foo", "text/turtle;profile=\""] {
        match SparqlFormat::parse(name) {
            Err(RdfError::UnknownSparqlFormat(given)) => assert_eq!(given, name),
            other => panic!("{name}: {other:?}"),
        }
    }
}

/// A format of the wrong kind is rejected before the query is evaluated, so nothing is
/// written; collected results check their own kind.
#[test]
fn wrong_kind_of_format() {
    let rg = RdfGraph::new();
    rg.graph().add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    let construct = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }";
    let mut out = Vec::new();
    let error = rdf_error(
        rg.sparql_to_writer(construct, &mut out, format("csv"))
            .unwrap_err(),
    );
    assert!(
        matches!(error, RdfError::WrongResultsFormat { .. }),
        "{error:?}"
    );
    assert!(out.is_empty());
    // with a format of the right kind, the same query fails in evaluation
    let error = rdf_error(stream(&rg, construct, format("nt")).unwrap_err());
    assert!(matches!(error, RdfError::SparqlEvaluation(_)), "{error:?}");

    let select = rg.sparql("SELECT * { ?s ?p ?o }").unwrap();
    let error = rdf_error(select.write(&mut out, RdfFormat::Turtle).unwrap_err());
    assert!(matches!(
        error,
        RdfError::WrongResultsFormat { form: "SELECT", .. }
    ));
    assert!(out.is_empty());
}

/// SPARQL Results XML cannot hold a literal with a control character other than tab and line
/// feed: such a result fails instead of being written or dropped.
#[test]
fn xml_unsafe_literals() {
    let rg = RdfGraph::new();
    let g = rg.graph();
    let literal = |value: &str| Literal::new_simple_literal(value).to_string();
    for value in ["bell\u{1}", "tab\tnewline\n"] {
        g.add_edge(
            1,
            "http://ex/s",
            literal(value).as_str(),
            NO_PROPS,
            Some("http%3A%2F%2Fex%2Fp"),
        )
        .unwrap();
    }
    let query = "SELECT ?o { <http://ex/s> <http://ex/p> ?o }";
    for error in [
        stream(&rg, query, QueryResultsFormat::Xml).unwrap_err(),
        collect(&rg, query, QueryResultsFormat::Xml).unwrap_err(),
    ] {
        let error = rdf_error(error);
        let RdfError::XmlUnsafeLiteral(value) = &error else {
            panic!("{error:?}")
        };
        assert_eq!(value, &literal("bell\u{1}"));
    }
    // a literal computed by the query, too
    let error = rdf_error(
        stream(
            &rg,
            "SELECT ?x { BIND(\"a\\rb\" AS ?x) }",
            QueryResultsFormat::Xml,
        )
        .unwrap_err(),
    );
    assert!(matches!(error, RdfError::XmlUnsafeLiteral(_)), "{error:?}");
    // tab and line feed are fine
    let safe = "SELECT ?o { <http://ex/s> <http://ex/p> ?o FILTER(CONTAINS(?o, \"tab\")) }";
    assert!(stream(&rg, safe, QueryResultsFormat::Xml).is_ok());
}

/// RDF/XML CONSTRUCT results are grouped by subject, so every subject gets one element and an
/// unnameable `rdf:type` is skipped only when its subject has no other triple, as in `to_rdf`.
#[test]
fn construct_rdf_xml_groups_by_subject() {
    let rg = RdfGraph::new();
    let g = rg.graph();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http%3A%2F%2Fex%2Fp"),
    )
    .unwrap();
    g.add_edge(
        1,
        "http://ex/c",
        "http://ex/d",
        NO_PROPS,
        Some("http%3A%2F%2Fex%2Fp"),
    )
    .unwrap();
    let query = "PREFIX ex: <http://ex/>
        CONSTRUCT { ?s ex:q ?o . ?o ex:r ?s . ?s a ex:42 } WHERE { ?s ex:p ?o }";
    let (nt, stats) = stream(&rg, query, RdfFormat::NTriples).unwrap();
    assert_eq!((stats.written, stats.skipped), (6, 0), "{nt}");
    // the triples of `a` are not consecutive
    let lines: Vec<&str> = nt.lines().collect();
    let of_a: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].starts_with("<http://ex/a> "))
        .collect();
    assert_eq!(of_a.len(), 2, "{nt}");
    assert_ne!(of_a[1], of_a[0] + 1, "{nt}");

    let (doc, stats) = stream(&rg, query, RdfFormat::RdfXml).unwrap();
    assert_eq!((stats.written, stats.skipped), (6, 0), "{doc}");
    assert_eq!(
        parse_graph(&doc, RdfFormat::RdfXml),
        parse_graph(&nt, RdfFormat::NTriples),
        "{doc}"
    );
    for subject in ["a", "b", "c", "d"] {
        assert_eq!(
            doc.matches(&format!("rdf:about=\"http://ex/{subject}\""))
                .count(),
            1,
            "{doc}"
        );
    }
    assert_eq!(
        doc.matches("<rdf:type rdf:resource=\"http://ex/42\"/>")
            .count(),
        2,
        "{doc}"
    );
    // the same as `to_rdf` writes for these triples
    let loaded = RdfGraph::new();
    loaded
        .load_rdf(1, nt.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let mut exported = Vec::new();
    let export_stats = loaded
        .to_rdf(&mut exported, RdfFormat::RdfXml, &[])
        .unwrap();
    let exported = String::from_utf8(exported).unwrap();
    assert_eq!((export_stats.written, export_stats.skipped), (6, 0));
    assert_eq!(
        parse_graph(&exported, RdfFormat::RdfXml),
        parse_graph(&doc, RdfFormat::RdfXml)
    );
    // collected results write the same bytes, and other formats keep the order of the template
    assert_eq!(
        collect(&rg, query, RdfFormat::RdfXml).unwrap(),
        (doc, stats)
    );
    assert_eq!(collect(&rg, query, RdfFormat::NTriples).unwrap().0, nt);

    // CONSTRUCT of every triple writes and skips what `to_rdf` does
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some(layer_of("http://ex/42").as_str()),
    )
    .unwrap();
    let all = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
    let mut exported = Vec::new();
    let export_stats = rg.to_rdf(&mut exported, RdfFormat::RdfXml, &[]).unwrap();
    let exported = String::from_utf8(exported).unwrap();
    let (doc, stats) = stream(&rg, all, RdfFormat::RdfXml).unwrap();
    assert_eq!(stats, export_stats);
    assert_eq!((stats.written, stats.skipped), (2, 1), "{doc}");
    assert_eq!(
        parse_graph(&doc, RdfFormat::RdfXml),
        parse_graph(&exported, RdfFormat::RdfXml)
    );
}

/// An evaluation error while results are written is returned, not a truncated success.
#[test]
fn evaluation_errors_while_writing() {
    let rg = RdfGraph::new();
    rg.graph().add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    // the second time graph IRI is only built (and found invalid) after the first solutions
    let select = "SELECT ?t ?s { VALUES ?t { \"1\" \"nope\" } \
        BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) LATERAL { GRAPH ?g { ?s ?p ?o } } }";
    let construct = "CONSTRUCT { ?s ?p ?o } WHERE { VALUES ?t { \"1\" \"nope\" } \
        BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) LATERAL { GRAPH ?g { ?s ?p ?o } } }";
    // RDF/XML groups the triples by subject before it writes any
    for (query, name) in [(select, "json"), (construct, "nt"), (construct, "rdf")] {
        let error = rdf_error(stream(&rg, query, format(name)).unwrap_err());
        let RdfError::SparqlEvaluation(inner) = &error else {
            panic!("{query} as {name}: {error:?}")
        };
        assert!(inner.to_string().contains("raphtory:asof:nope"), "{inner}");
        assert!(matches!(
            collect(&rg, query, format(name)).map_err(rdf_error),
            Err(RdfError::SparqlEvaluation(_))
        ));
    }
}

/// The counts of `WriteStats`; streamed and collected results write the same.
#[test]
fn write_stats() {
    let rg = RdfGraph::new();
    let g = rg.graph();
    for (s, o, p) in [
        ("http://ex/a", "http://ex/b", "http://ex/p"),
        ("http://ex/a", "http://ex/c", "http://ex/p"),
        ("http://ex/b", "http://ex/c", "http://ex/42"),
    ] {
        g.add_edge(1, s, o, NO_PROPS, Some(layer_of(p).as_str()))
            .unwrap();
    }
    let stats = |query: &str, format: SparqlFormat| {
        let (_, stats) = same_written(&rg, query, format);
        (stats.written, stats.skipped)
    };
    assert_eq!(stats("SELECT * { ?s ?p ?o }", format("json")), (3, 0));
    assert_eq!(
        stats("SELECT * { ?s ?p ?o FILTER(false) }", format("json")),
        (0, 0)
    );
    assert_eq!(stats("ASK { ?s ?p ?o }", format("json")), (0, 0));
    // duplicates are not counted
    let construct = "CONSTRUCT { ?s <http://ex/q> <http://ex/o> } WHERE { ?s ?p ?o }";
    assert_eq!(stats(construct, format("nt")), (2, 0));
    let all = "CONSTRUCT WHERE { ?s ?p ?o }";
    assert_eq!(stats(all, format("ttl")), (3, 0));
    // RDF/XML skips the predicate without a local name
    assert_eq!(stats(all, format("rdf")), (2, 1));
    assert_eq!(stats("DESCRIBE <http://ex/b>", format("xml")), (0, 1));
    assert_eq!(stats("DESCRIBE <http://ex/b>", format("json")), (1, 0));

    // stored blank nodes would repeat CONSTRUCT and DESCRIBE triples
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/p> _:b .
        <http://ex/c> <http://ex/p> _:b .
        _:b <http://ex/q> \"x\" .
        _:b <http://ex/q> _:1 .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    for (query, n) in [
        ("CONSTRUCT WHERE { ?s ?p ?o . ?x ?y ?o }", 4),
        (
            "CONSTRUCT { ?s <http://ex/p> ?o } WHERE { ?s ?x ?y . ?s <http://ex/p> ?o }",
            2,
        ),
        ("DESCRIBE ?o WHERE { ?s <http://ex/p> ?o }", 2),
    ] {
        let (nt, stats) = stream(&rg, query, RdfFormat::NTriples).unwrap();
        assert_eq!((stats.written, stats.skipped), (n, 0), "{query}: {nt}");
        assert_eq!(nt.lines().count(), n, "{query}: {nt}");
        let (xml, stats) = stream(&rg, query, RdfFormat::RdfXml).unwrap();
        assert_eq!((stats.written, stats.skipped), (n, 0), "{query}: {xml}");
        assert_eq!(
            parse_graph(&xml, RdfFormat::RdfXml).len(),
            n,
            "{query}: {xml}"
        );
    }

    // with prefixes, and with the options
    let rg = RdfGraph::new();
    let g = rg.graph();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http%3A%2F%2Fex%2Fp"),
    )
    .unwrap();
    let query = "CONSTRUCT WHERE { ?s ?p ?o }";
    let format = SparqlFormat::from(RdfFormat::Turtle).with_prefixes([("ex", "http://ex/")]);
    let streamed = same_written(&rg, query, format.clone());
    assert_eq!(streamed.0, "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\n");
    let options = SparqlOptions::default();
    let with = written(|out| rg.sparql_to_writer_with(query, out, format, &options)).unwrap();
    assert_eq!(with, streamed);
}
