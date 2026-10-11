//! Serialized SPARQL results: `SparqlFormat`, `sparql_to_writer` and `SparqlResults::write`.
use super::parse_graph;
use crate::{
    QueryForm,
    QueryResultsFormat::{self, Csv, Json, Xml},
    RdfError, RdfFormat, RdfGraph, RdfView, SparqlFormat, SparqlOptions, WriteStats,
};
use oxigraph::sparql::QueryEvaluationError;
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};

/// What a write gave: the document and the counts.
type Written = Result<(String, WriteStats), RdfError>;

fn written(write: impl FnOnce(&mut Vec<u8>) -> Result<WriteStats, RdfError>) -> Written {
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
    written(|out| view.sparql_to_writer(query, out, format, &SparqlOptions::default()))
}

/// `sparql()?.write()` into a string.
fn collect<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
    format: impl Into<SparqlFormat>,
) -> Written {
    let results = view.sparql(query, &SparqlOptions::default())?;
    written(|out| results.write(out, format))
}

/// `sparql_to_writer` and `sparql()?.write()`, which must succeed and write the same.
fn same_written<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
    format: impl Into<SparqlFormat>,
) -> (String, WriteStats) {
    let format = format.into();
    let streamed = stream(view, query, format).unwrap_or_else(|e| panic!("{query}: {e}"));
    let collected = collect(view, query, format).unwrap_or_else(|e| panic!("{query}: {e}"));
    assert_eq!(streamed, collected, "{query}");
    streamed
}

fn format(name: &str) -> SparqlFormat {
    SparqlFormat::parse(name).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// One name gives each slot the format it means for it.
#[test]
fn format_names() {
    let slots = |name: &str| {
        let format = format(name);
        (format.results, format.graph)
    };
    let table: &[(&str, Option<QueryResultsFormat>, Option<RdfFormat>)] = &[
        // both slots
        ("json", Some(Json), Some(RdfFormat::JsonLd)),
        ("xml", Some(Xml), Some(RdfFormat::RdfXml)),
        ("txt", Some(Csv), Some(RdfFormat::NTriples)),
        // only SELECT and ASK
        ("csv", Some(Csv), None),
        ("application/sparql-results+json", Some(Json), None),
        // only CONSTRUCT and DESCRIBE
        ("nt", None, Some(RdfFormat::NTriples)),
        ("text/turtle", None, Some(RdfFormat::Turtle)),
        // spellings: a leading dot, case and media-type parameters
        (".JSON", Some(Json), Some(RdfFormat::JsonLd)),
        ("text/csv; charset=utf-8", Some(Csv), None),
    ];
    for (name, results, graph) in table {
        assert_eq!(slots(name), (*results, *graph), "{name}");
    }

    // neither, including a malformed media-type parameter
    for name in ["foo", "text/turtle;profile=\""] {
        match SparqlFormat::parse(name) {
            Err(RdfError::UnknownSparqlFormat(given)) => assert_eq!(given, name),
            other => panic!("{name}: {other:?}"),
        }
    }
}

/// A format of the wrong kind is rejected before anything is written, streamed or collected.
#[test]
fn wrong_kind_of_format() {
    let rg = RdfGraph::new();
    rg.graph().add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    let construct = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }";
    let mut out = Vec::new();
    let error = rg
        .sparql_to_writer(
            construct,
            &mut out,
            format("csv"),
            &SparqlOptions::default(),
        )
        .unwrap_err();
    assert!(
        matches!(error, RdfError::WrongResultsFormat { .. }),
        "{error:?}"
    );
    assert!(out.is_empty());
    // with a format of the right kind, the same query fails in evaluation
    let error = stream(&rg, construct, format("nt")).unwrap_err();
    assert!(matches!(error, RdfError::SparqlEvaluation(_)), "{error:?}");

    let select = rg
        .sparql("SELECT * { ?s ?p ?o }", &SparqlOptions::default())
        .unwrap();
    let error = select.write(&mut out, RdfFormat::Turtle).unwrap_err();
    assert!(matches!(
        error,
        RdfError::WrongResultsFormat {
            form: QueryForm::Select,
            ..
        }
    ));
    assert!(out.is_empty());
}

/// A literal SPARQL Results XML cannot hold fails the write instead of being dropped.
#[test]
fn xml_unsafe_literals() {
    let rg = RdfGraph::new();
    let doc = r#"<http://ex/s> <http://ex/p> "bell\u0001" .
        <http://ex/s> <http://ex/p> "tab\tnewline\n" ."#;
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let query = "SELECT ?o { <http://ex/s> <http://ex/p> ?o }";
    let error = stream(&rg, query, QueryResultsFormat::Xml).unwrap_err();
    let RdfError::XmlUnsafeLiteral(value) = &error else {
        panic!("{error:?}")
    };
    assert_eq!(value, r#""bell\u0001""#);
    // a literal computed by the query, too
    let error = stream(
        &rg,
        "SELECT ?x { BIND(\"a\\rb\" AS ?x) }",
        QueryResultsFormat::Xml,
    )
    .unwrap_err();
    assert!(matches!(error, RdfError::XmlUnsafeLiteral(_)), "{error:?}");
    // tab and line feed are fine
    let safe = "SELECT ?o { <http://ex/s> <http://ex/p> ?o FILTER(CONTAINS(?o, \"tab\")) }";
    assert!(stream(&rg, safe, QueryResultsFormat::Xml).is_ok());
}

/// RDF/XML CONSTRUCT results are grouped by subject, so every subject gets one element.
#[test]
fn construct_rdf_xml_groups_by_subject() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .
        <http://ex/c> <http://ex/p> <http://ex/d> .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
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
}

/// RDF/XML results with a triple RDF/XML cannot write fail, and nothing is written.
#[test]
fn rdf_xml_fails_on_triples_it_cannot_hold() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/123> <http://ex/b> .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let query = "CONSTRUCT WHERE { ?s ?p ?o }";
    let mut out = Vec::new();
    let streamed = rg.sparql_to_writer(
        query,
        &mut out,
        RdfFormat::RdfXml,
        &SparqlOptions::default(),
    );
    let results = rg.sparql(query, &SparqlOptions::default()).unwrap();
    let collected = results.write(&mut out, RdfFormat::RdfXml);
    for error in [streamed.unwrap_err(), collected.unwrap_err()] {
        let error = error;
        assert!(
            matches!(
                error,
                RdfError::RdfXmlSkipped {
                    skipped: 1,
                    triples: 1
                }
            ),
            "{error:?}"
        );
    }
    assert!(out.is_empty());
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
        let error = stream(&rg, query, format(name)).unwrap_err();
        let RdfError::SparqlEvaluation(QueryEvaluationError::Dataset(inner)) = &error else {
            panic!("{query} as {name}: {error:?}")
        };
        assert!(matches!(
            inner.downcast_ref::<RdfError>(),
            Some(RdfError::InvalidTimeGraph { iri, .. }) if iri.as_str() == "raphtory:asof:nope"
        ));
    }
}

/// The counts of `WriteStats`; streamed and collected results write the same.
#[test]
fn write_stats() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .
        <http://ex/a> <http://ex/p> <http://ex/c> .
        <http://ex/b> <http://ex/42> <http://ex/c> .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
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
}
