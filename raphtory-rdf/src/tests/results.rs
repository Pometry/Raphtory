//! Serialized SPARQL results: `SparqlFormat` and `sparql_to_writer`.
use super::parse_graph;
use crate::{
    QueryForm,
    QueryResultsFormat::{self, Csv, Json, Tsv, Xml},
    RdfError, RdfFormat, RdfGraph, RdfView, SparqlFormat, SparqlOptions, WriteStats,
};
use raphtory::db::api::view::{IntoDynamic, StaticGraphViewOps};

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

/// A row of `formats_from_media_types`: the ranges, then the format and media type for
/// `SELECT` and `ASK` and for `CONSTRUCT` and `DESCRIBE`.
type Case = (
    &'static [(&'static str, f32)],
    Option<(QueryResultsFormat, &'static str)>,
    Option<(RdfFormat, &'static str)>,
);

/// The format for a form is the first media type, in order of preference, that names one, and
/// is served as a media type the ranges accept.
#[test]
fn formats_from_media_types() {
    let table: &[Case] = &[
        (
            &[("text/turtle", 1.0), ("text/csv", 1.0)],
            Some((Csv, "text/csv; charset=utf-8")),
            Some((RdfFormat::Turtle, "text/turtle")),
        ),
        (
            &[("application/xml", 1.0)],
            Some((Xml, "application/xml")),
            Some((RdfFormat::RdfXml, "application/xml")),
        ),
        (
            &[("*/*", 1.0)],
            Some((Json, "application/sparql-results+json")),
            Some((RdfFormat::Turtle, "text/turtle")),
        ),
        (
            &[("text/*", 1.0)],
            Some((Xml, "text/xml")),
            Some((RdfFormat::Turtle, "text/turtle")),
        ),
        (
            &[("image/*", 1.0), ("text/html", 1.0), ("Application/*", 0.5)],
            Some((Json, "application/sparql-results+json")),
            Some((RdfFormat::Turtle, "application/x-turtle")),
        ),
        // a refused or less preferred media type is skipped in a wider range
        (
            &[("text/*", 1.0), ("text/xml", 0.0), ("text/csv", 0.5)],
            Some((Tsv, "text/tab-separated-values; charset=utf-8")),
            Some((RdfFormat::Turtle, "text/turtle")),
        ),
        (
            &[("*/*", 1.0), ("application/*", 0.0), ("text/turtle", 0.0)],
            Some((Xml, "text/xml")),
            Some((RdfFormat::RdfXml, "text/xml")),
        ),
        (
            &[("application/sparql-results+json", 0.0), ("*/*", 1.0)],
            Some((Json, "application/json")),
            Some((RdfFormat::Turtle, "text/turtle")),
        ),
        (&[("text/csv", 0.0), ("*/*", 0.0)], None, None),
        (&[("text/html", 1.0)], None, None),
        (&[], None, None),
    ];
    for (ranges, results, graph) in table {
        let chosen = |form| SparqlFormat::from_media_types(form, ranges.iter().copied());
        for form in [QueryForm::Select, QueryForm::Ask] {
            let expected = results.map(|(format, media_type)| (format.into(), media_type.into()));
            assert_eq!(chosen(form), expected, "{ranges:?}");
        }
        for form in [QueryForm::Construct, QueryForm::Describe] {
            let expected = graph.map(|(format, media_type)| (format.into(), media_type.into()));
            assert_eq!(chosen(form), expected, "{ranges:?}");
        }
    }
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

/// The counts of `WriteStats`.
#[test]
fn write_stats() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .
        <http://ex/a> <http://ex/p> <http://ex/c> .
        <http://ex/b> <http://ex/42> <http://ex/c> .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let stats = |query: &str, format: SparqlFormat| {
        let (_, stats) = stream(&rg, query, format).unwrap_or_else(|e| panic!("{query}: {e}"));
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
    assert!(matches!(
        stream(&rg, all, format("csv")),
        Err(RdfError::WrongResultsFormat { .. })
    ));

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
