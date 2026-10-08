//! Serialized SPARQL results: `SparqlFormat`, `sparql_to_writer` and `SparqlResults::write`.
use super::oracle::{
    parse, BLANK_NODES, DATATYPES, FOAF, PREFIXES, QUERIES, RANDOM_QUERIES, SOCIAL,
    TIME_GRAPH_QUERIES,
};
use crate::{
    db::api::view::{DynamicGraph, IntoDynamic},
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{vocab::xsd, BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple},
        serializer_with_prefixes, QueryResults, QueryResultsFormat, RdfError, RdfFormat, RdfParser,
        RdfViewOps, SparqlFormat, SparqlOptions, SparqlResults, SparqlWriteStats,
    },
};
use oxigraph::sparql::results::QueryResultsParser;
use std::collections::BTreeSet;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// The SPARQL results formats, by name.
const RESULTS_FORMATS: [&str; 4] = ["json", "xml", "csv", "tsv"];
/// RDF formats, by name.
const GRAPH_FORMATS: [&str; 6] = ["nt", "ttl", "jsonld", "rdf", "nq", "trig"];

/// What a write gave: the document and the counts.
type Written = Result<(String, SparqlWriteStats), GraphError>;

fn written(write: impl FnOnce(&mut Vec<u8>) -> Result<SparqlWriteStats, GraphError>) -> Written {
    let mut out = Vec::new();
    let stats = write(&mut out)?;
    Ok((String::from_utf8(out).expect("output is UTF-8"), stats))
}

/// `sparql_to_writer` into a string.
fn stream<G: RdfViewOps>(view: &G, query: &str, format: impl Into<SparqlFormat>) -> Written {
    written(|out| view.sparql_to_writer(query, out, format))
}

/// `sparql()?.write()` into a string.
fn collect<G: RdfViewOps>(view: &G, query: &str, format: impl Into<SparqlFormat>) -> Written {
    let results = view.sparql(query)?;
    written(|out| results.write(out, format))
}

/// `sparql_to_writer` into a string, which must succeed.
fn doc<G: RdfViewOps>(view: &G, query: &str, format: impl Into<SparqlFormat>) -> String {
    stream(view, query, format)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .0
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

fn iri(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

/// Results in a comparable form: the variables and the sorted rows, a boolean, or a set of
/// triples, all in N-Triples form (`UNDEF` where a variable is unbound).
#[derive(Debug, PartialEq, Eq)]
enum Normalized {
    Rows(Vec<String>, Vec<Vec<String>>),
    Boolean(bool),
    Triples(BTreeSet<String>),
}

impl Normalized {
    fn new(results: &SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => {
                let mut rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| v.as_ref().map_or_else(|| "UNDEF".into(), Term::to_string))
                            .collect()
                    })
                    .collect();
                rows.sort();
                Self::Rows(variables.iter().map(|v| v.to_string()).collect(), rows)
            }
            SparqlResults::Boolean(value) => Self::Boolean(*value),
            SparqlResults::Graph(triples) => {
                Self::Triples(triples.iter().map(|t| t.to_string()).collect())
            }
        }
    }
}

/// Parses a SPARQL results document back into results.
fn parse_results(doc: &str, format: QueryResultsFormat) -> SparqlResults {
    let parsed = QueryResultsParser::from_format(format)
        .for_slice(doc.as_bytes())
        .unwrap_or_else(|e| panic!("{e}: {doc}"));
    SparqlResults::from_query_results(QueryResults::from(parsed))
        .unwrap_or_else(|e| panic!("{e}: {doc}"))
}

/// Parses an RDF document back into a set of triples in N-Triples form, keeping blank-node
/// labels, except that the `x` RDF/XML puts in front of a label that is not an XML name is
/// removed again.
fn parse_graph(doc: &str, format: RdfFormat) -> BTreeSet<String> {
    RdfParser::from_format(format)
        .for_reader(doc.as_bytes())
        .map(|quad| {
            let triple = Triple::from(quad.unwrap_or_else(|e| panic!("{e}: {doc}")));
            if format != RdfFormat::RdfXml {
                return triple.to_string();
            }
            let unrename = |node: &BlankNode| {
                let label = node.as_str();
                match label.strip_prefix('x') {
                    Some(rest)
                        if rest
                            .trim_start_matches('x')
                            .starts_with(|c: char| c.is_ascii_digit()) =>
                    {
                        BlankNode::new_unchecked(rest)
                    }
                    _ => node.clone(),
                }
            };
            let subject = match &triple.subject {
                NamedOrBlankNode::BlankNode(node) => unrename(node).into(),
                subject => subject.clone(),
            };
            let object = match &triple.object {
                Term::BlankNode(node) => unrename(node).into(),
                object => object.clone(),
            };
            Triple::new(subject, triple.predicate, object).to_string()
        })
        .collect()
}

/// The graphs of the oracle corpus: each fixture and their union, as persistent graphs.
fn fixture_graphs() -> Vec<(&'static str, PersistentGraph)> {
    let fixtures = [
        ("foaf", parse(FOAF, RdfFormat::Turtle)),
        ("datatypes", parse(DATATYPES, RdfFormat::NTriples)),
        ("blank_nodes", parse(BLANK_NODES, RdfFormat::Turtle)),
        ("social", parse(SOCIAL, RdfFormat::Turtle)),
    ];
    let union: Vec<Triple> = fixtures.iter().flat_map(|(_, t)| t.clone()).collect();
    fixtures
        .into_iter()
        .chain([("union", union)])
        .map(|(name, triples)| {
            let pg = PersistentGraph::new();
            for triple in &triples {
                pg.add_triple(0, triple).unwrap();
            }
            (name, pg)
        })
        .collect()
}

/// The graph of the time graph queries of the oracle corpus.
fn time_graph_fixture() -> PersistentGraph {
    let pg = PersistentGraph::new();
    let ex = |local: &str| iri(&format!("http://ex/{local}"));
    let triple = |s: &str, p: &str, o: Term| Triple::new(ex(s), ex(p), o);
    let [ab, ac, ax, bc, ca] = [
        triple("a", "p", ex("b").into()),
        triple("a", "p", ex("c").into()),
        triple("a", "q", Literal::new_simple_literal("x").into()),
        triple("b", "q", ex("c").into()),
        triple("c", "p", ex("a").into()),
    ];
    for t in [&ab, &ac, &ax] {
        pg.add_triple(1, t).unwrap();
    }
    pg.delete_triple(3, &ac).unwrap();
    pg.add_triple(3, &bc).unwrap();
    pg.add_triple(5, &ca).unwrap();
    pg.delete_triple(5, &ab).unwrap();
    pg
}

/// A graph over the vocabulary of the random oracle queries, with every combination of terms
/// whose index sum is even.
fn random_query_fixture() -> PersistentGraph {
    let subjects: Vec<NamedOrBlankNode> = vec![
        iri("http://ex/a").into(),
        iri("http://ex/b").into(),
        iri("http://ex/p").into(),
        BlankNode::new_unchecked("b1").into(),
    ];
    let predicates = [iri("http://ex/p"), iri("http://ex/q")];
    let objects: Vec<Term> = vec![
        iri("http://ex/a").into(),
        iri("http://ex/b").into(),
        iri("http://ex/p").into(),
        BlankNode::new_unchecked("b1").into(),
        Literal::new_typed_literal("1", xsd::INTEGER).into(),
        Literal::new_typed_literal("01", xsd::INTEGER).into(),
        Literal::new_language_tagged_literal_unchecked("x", "en").into(),
    ];
    let pg = PersistentGraph::new();
    for (i, s) in subjects.iter().enumerate() {
        for (j, p) in predicates.iter().enumerate() {
            for (k, o) in objects.iter().enumerate() {
                if (i + j + k) % 2 == 0 {
                    pg.add_triple(0, &Triple::new(s.clone(), p.clone(), o.clone()))
                        .unwrap();
                }
            }
        }
    }
    pg
}

/// Every query of the oracle corpus, with the views to run it on.
fn corpus() -> Vec<(DynamicGraph, String)> {
    let mut corpus = Vec::new();
    for (_, pg) in fixture_graphs() {
        for (query, _) in QUERIES {
            corpus.push((pg.clone().into_dynamic(), format!("{PREFIXES}{query}")));
        }
    }
    let random = random_query_fixture();
    for query in RANDOM_QUERIES {
        corpus.push((random.clone().into_dynamic(), query.to_string()));
    }
    let timed = time_graph_fixture();
    for view in [
        timed.clone().into_dynamic(),
        timed.event_graph().into_dynamic(),
    ] {
        for (query, _, _) in TIME_GRAPH_QUERIES {
            corpus.push((view.clone(), format!("PREFIX ex: <http://ex/>\n{query}")));
        }
    }
    corpus
}

/// One name gives each slot the format it means for it.
#[test]
fn format_names() {
    let jsonld = RdfFormat::from_extension("jsonld").unwrap();
    // the JSON-LD serializer writes the streaming profile, so compare kinds of RDF formats
    let kind = |format: RdfFormat| match format {
        RdfFormat::JsonLd { .. } => "JSON-LD",
        format => format.name(),
    };
    let slots = |name: &str| {
        let format = format(name);
        (format.results, format.graph.map(|s| kind(s.format())))
    };
    use QueryResultsFormat::{Csv, Json, Tsv, Xml};
    let table: &[(&str, Option<QueryResultsFormat>, Option<RdfFormat>)] = &[
        // both slots
        ("json", Some(Json), Some(jsonld)),
        ("xml", Some(Xml), Some(RdfFormat::RdfXml)),
        ("txt", Some(Csv), Some(RdfFormat::NTriples)),
        ("application/json", Some(Json), Some(jsonld)),
        ("text/xml", Some(Xml), Some(RdfFormat::RdfXml)),
        ("text/plain", Some(Csv), Some(RdfFormat::NTriples)),
        // only SELECT and ASK
        ("csv", Some(Csv), None),
        ("tsv", Some(Tsv), None),
        ("srj", Some(Json), None),
        ("srx", Some(Xml), None),
        ("application/sparql-results+json", Some(Json), None),
        ("application/sparql-results+xml", Some(Xml), None),
        ("text/csv", Some(Csv), None),
        ("text/tab-separated-values", Some(Tsv), None),
        ("sparql-results+json", Some(Json), None),
        // only CONSTRUCT and DESCRIBE
        ("nt", None, Some(RdfFormat::NTriples)),
        ("ttl", None, Some(RdfFormat::Turtle)),
        ("turtle", None, Some(RdfFormat::Turtle)),
        ("rdf", None, Some(RdfFormat::RdfXml)),
        ("jsonld", None, Some(jsonld)),
        ("nq", None, Some(RdfFormat::NQuads)),
        ("trig", None, Some(RdfFormat::TriG)),
        // the N3 serializer writes Turtle (which is N3)
        ("n3", None, Some(RdfFormat::Turtle)),
        ("n-triples", None, Some(RdfFormat::NTriples)),
        ("text/turtle", None, Some(RdfFormat::Turtle)),
        ("application/n-triples", None, Some(RdfFormat::NTriples)),
        ("application/ld+json", None, Some(jsonld)),
        ("application/rdf+xml", None, Some(RdfFormat::RdfXml)),
        // spellings: a leading dot, case, whitespace and media-type parameters
        (".srj", Some(Json), None),
        (".JSON", Some(Json), Some(jsonld)),
        (" Xml ", Some(Xml), Some(RdfFormat::RdfXml)),
        ("TSV", Some(Tsv), None),
        ("TTL", None, Some(RdfFormat::Turtle)),
        ("Application/Sparql-Results+JSON", Some(Json), None),
        ("text/csv; charset=utf-8", Some(Csv), None),
        (
            "application/sparql-results+json;charset=utf-8",
            Some(Json),
            None,
        ),
        ("text/turtle; charset=utf-8", None, Some(RdfFormat::Turtle)),
    ];
    for (name, results, graph) in table {
        assert_eq!(slots(name), (*results, graph.map(kind)), "{name}");
    }

    // neither, including malformed media-type parameters
    for name in [
        "foo",
        "",
        ".",
        "a;b=\"",
        "text/turtle;profile=\"",
        "turtle;profile=\"",
        "text/turtle; charset=latin1",
        "application/sparql-query",
    ] {
        match SparqlFormat::parse(name) {
            Err(RdfError::UnknownSparqlFormat(given)) => assert_eq!(given, name),
            other => panic!("{name}: {other:?}"),
        }
    }
    assert_eq!(
        SparqlFormat::parse("foo").unwrap_err().to_string(),
        "unknown SPARQL results format 'foo': expected json, xml, csv, tsv or an RDF format such \
         as nt or ttl"
    );
    // `parse_rdf_format` keeps its own error
    assert!(matches!(
        crate::rdf::parse_rdf_format("csv"),
        Err(RdfError::UnknownFormat(_))
    ));

    // the conversions and the default
    let default = SparqlFormat::default();
    assert_eq!(
        (default.results, default.graph.map(|s| s.format())),
        (Some(Json), Some(RdfFormat::NTriples))
    );
    let typed = SparqlFormat::from(Tsv);
    assert_eq!((typed.results, typed.graph.is_none()), (Some(Tsv), true));
    let typed = SparqlFormat::from(RdfFormat::Turtle);
    assert_eq!(
        (typed.results, typed.graph.map(|s| s.format())),
        (None, Some(RdfFormat::Turtle))
    );
    assert_eq!(
        format!("{:?}", format("xml")),
        "SparqlFormat { results: Some(Xml), graph: Some(RdfXml) }"
    );
}

/// A node with a space, a blank node, an integer literal and an unbound variable, as SPARQL
/// Results JSON.
#[test]
fn select_json_golden() {
    let g = Graph::new();
    g.add_edge(1, "Alice Smith", "_:b1", NO_PROPS, Some("knows"))
        .unwrap();
    let int_42 = format!("\"42\"^^<{XSD_INTEGER}>");
    g.add_edge(1, "Alice Smith", int_42.as_str(), NO_PROPS, Some("age"))
        .unwrap();
    let query = "SELECT ?s ?o ?u { ?s ?p ?o OPTIONAL { ?o ?q ?u } } ORDER BY ?p";
    let expected = concat!(
        r#"{"head":{"vars":["s","o","u"]},"results":{"bindings":["#,
        r#"{"s":{"type":"uri","value":"raphtory:Alice%20Smith"},"#,
        r#""o":{"type":"literal","value":"42","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},"#,
        r#"{"s":{"type":"uri","value":"raphtory:Alice%20Smith"},"o":{"type":"bnode","value":"b1"}}"#,
        r#"]}}"#
    );
    for format in [
        SparqlFormat::from(QueryResultsFormat::Json),
        SparqlFormat::default(),
        self::format("json"),
        self::format("application/sparql-results+json"),
    ] {
        let (doc, stats) = stream(&g, query, format.clone()).unwrap();
        assert_eq!(doc, expected);
        assert_eq!(
            stats,
            SparqlWriteStats {
                written: 2,
                skipped: 0
            }
        );
        assert_eq!(collect(&g, query, format).unwrap().0, expected);
    }
}

/// JSON, XML and TSV keep every term: parsed back, they are the results of `sparql()`.
#[test]
fn lossless_formats_round_trip() {
    let mut checked = 0;
    for (view, query) in corpus() {
        let results = view.sparql(&query).unwrap();
        if matches!(results, SparqlResults::Graph(_)) {
            continue;
        }
        let expected = Normalized::new(&results);
        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Tsv,
        ] {
            let (doc, _) = stream(&view, &query, format).unwrap_or_else(|e| panic!("{query}: {e}"));
            assert_eq!(
                Normalized::new(&parse_results(&doc, format)),
                expected,
                "{query} as {format}: {doc}"
            );
            checked += 1;
        }
    }
    assert!(checked > 300, "{checked}");
}

/// CSV loses the kind of terms, the datatypes and the languages, as the W3C specification
/// says; fields with a comma, a quote or a line break are quoted.
#[test]
fn csv_golden() {
    let g = PersistentGraph::new();
    let turtle = r#"
        @prefix ex: <http://ex/> .
        ex:alice ex:age 42 ; ex:name "Alice"@en ; ex:knows _:b1 ; ex:quote "say \"hi\", then\nbye" .
    "#;
    g.load_rdf(1, turtle.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let blank = doc_select_blank(&g);
    let (csv, stats) = stream(
        &g,
        "SELECT ?p ?o ?u { <http://ex/alice> ?p ?o OPTIONAL { ?o ?q ?u } } ORDER BY ?p",
        QueryResultsFormat::Csv,
    )
    .unwrap();
    assert_eq!(stats.written, 4);
    assert_eq!(
        csv,
        format!(
            "p,o,u\r\nhttp://ex/age,42,\r\nhttp://ex/knows,{blank},\r\nhttp://ex/name,Alice,\r\n\
             http://ex/quote,\"say \"\"hi\"\", then\nbye\",\r\n"
        )
    );
    // the media-type spelling gives the same document
    let query = "SELECT ?p ?o ?u { <http://ex/alice> ?p ?o OPTIONAL { ?o ?q ?u } } ORDER BY ?p";
    assert_eq!(doc(&g, query, format("text/csv")), csv);
    assert_eq!(doc(&g, query, format("txt")), csv);
    // TSV keeps the N-Triples forms (with integers as bare numbers)
    assert_eq!(
        doc(&g, query, format("tsv")),
        format!(
            "?p\t?o\t?u\n<http://ex/age>\t42\t\n<http://ex/knows>\t{blank}\t\n\
             <http://ex/name>\t\"Alice\"@en\t\n<http://ex/quote>\t\"say \\\"hi\\\", then\\nbye\"\t\n"
        )
    );
}

/// The stored label of the only blank node of a graph, in N-Triples form.
fn doc_select_blank<G: RdfViewOps>(g: &G) -> String {
    let SparqlResults::Solutions { rows, .. } = g
        .sparql("SELECT ?b { ?s ?p ?b FILTER(isBlank(?b)) }")
        .unwrap()
    else {
        unreachable!()
    };
    rows[0][0].as_ref().unwrap().to_string()
}

/// ASK results in every SPARQL results format.
#[test]
fn ask_golden() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
    for (value, query) in [
        (true, "ASK { raphtory:Alice ?p raphtory:Bob }"),
        (false, "ASK { raphtory:Bob ?p raphtory:Alice }"),
    ] {
        let expected = [
            ("json", format!(r#"{{"head":{{}},"boolean":{value}}}"#)),
            (
                "xml",
                format!(
                    r#"<?xml version="1.0"?><sparql xmlns="http://www.w3.org/2005/sparql-results#"><head></head><boolean>{value}</boolean></sparql>"#
                ),
            ),
            ("csv", value.to_string()),
            ("tsv", value.to_string()),
        ];
        for (name, expected) in expected {
            let (doc, stats) = stream(&g, query, format(name)).unwrap();
            assert_eq!(doc, expected, "{name}");
            assert_eq!(stats, SparqlWriteStats::default(), "{name}");
            assert_eq!(
                collect(&g, query, format(name)).unwrap().0,
                expected,
                "{name}"
            );
        }
        // JSON and XML parse back
        for results_format in [QueryResultsFormat::Json, QueryResultsFormat::Xml] {
            let doc = doc(&g, query, results_format);
            assert_eq!(
                parse_results(&doc, results_format),
                SparqlResults::Boolean(value)
            );
        }
    }
}

/// CONSTRUCT and DESCRIBE results in RDF formats parse back to the triples of `sparql()`,
/// without duplicates, also when stored blank nodes would repeat them.
#[test]
fn graph_results_round_trip() {
    let g = PersistentGraph::new();
    let doc = "<http://ex/a> <http://ex/p> _:b .
        <http://ex/c> <http://ex/p> _:b .
        _:b <http://ex/q> \"x\" .
        _:b <http://ex/q> _:1 .";
    g.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    for (query, n) in [
        ("CONSTRUCT WHERE { ?s ?p ?o . ?x ?y ?o }", 4),
        (
            "CONSTRUCT { ?s <http://ex/p> ?o } WHERE { ?s ?x ?y . ?s <http://ex/p> ?o }",
            2,
        ),
        ("DESCRIBE ?o WHERE { ?s <http://ex/p> ?o }", 2),
    ] {
        let SparqlResults::Graph(triples) = g.sparql(query).unwrap() else {
            unreachable!()
        };
        assert_eq!(triples.len(), n, "{query}");
        let expected: BTreeSet<String> = triples.iter().map(Triple::to_string).collect();
        for name in ["nt", "ttl", "jsonld", "rdf", "nq", "trig"] {
            let rdf_format = format(name).graph.unwrap().format();
            let (doc, stats) = stream(&g, query, format(name)).unwrap();
            assert_eq!(
                stats,
                SparqlWriteStats {
                    written: n,
                    skipped: 0
                },
                "{query} as {name}: {doc}"
            );
            assert_eq!(
                parse_graph(&doc, rdf_format),
                expected,
                "{query} as {name}: {doc}"
            );
        }
        // N-Triples: one line per triple
        assert_eq!(doc_lines(&g, query), n, "{query}");
    }

    // over the oracle corpus
    let mut checked = 0;
    for (view, query) in corpus() {
        let SparqlResults::Graph(triples) = view.sparql(&query).unwrap() else {
            continue;
        };
        let expected: BTreeSet<String> = triples.iter().map(Triple::to_string).collect();
        for name in ["nt", "ttl", "jsonld", "rdf"] {
            let rdf_format = format(name).graph.unwrap().format();
            let (doc, stats) = stream(&view, &query, format(name)).unwrap();
            assert_eq!(stats.written + stats.skipped, triples.len(), "{query}");
            let read = parse_graph(&doc, rdf_format);
            assert_eq!(read.len(), stats.written, "{query} as {name}: {doc}");
            if stats.skipped == 0 {
                assert_eq!(read, expected, "{query} as {name}: {doc}");
            } else {
                assert_eq!(rdf_format, RdfFormat::RdfXml, "{query} as {name}");
                assert!(read.is_subset(&expected), "{query} as {name}: {doc}");
            }
            checked += 1;
        }
    }
    assert!(checked > 40, "{checked}");
}

fn doc_lines<G: RdfViewOps>(g: &G, query: &str) -> usize {
    doc(g, query, RdfFormat::NTriples).lines().count()
}

/// A format of the wrong kind is rejected before the query is evaluated, so nothing is
/// written; a syntax error still comes first.
#[test]
fn wrong_kind_of_format() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    let construct = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }";
    let cases = [
        (
            construct,
            "csv",
            "CONSTRUCT results cannot be written as SPARQL Results in CSV; use an RDF format \
             such as nt, ttl, jsonld or rdf",
        ),
        (
            "DESCRIBE ?s WHERE { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }",
            "srj",
            "DESCRIBE results cannot be written as SPARQL Results in JSON; use an RDF format \
             such as nt, ttl, jsonld or rdf",
        ),
        (
            "SELECT * { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }",
            "ttl",
            "SELECT results cannot be written as Turtle; use a SPARQL results format: json, \
             xml, csv or tsv",
        ),
        (
            "ASK { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }",
            "nt",
            "ASK results cannot be written as N-Triples; use a SPARQL results format: json, \
             xml, csv or tsv",
        ),
        (
            "SELECT * { ?s ?p ?o }",
            "application/ld+json",
            "SELECT results cannot be written as JSON-LD; use a SPARQL results format: json, \
             xml, csv or tsv",
        ),
    ];
    for (query, name, message) in cases {
        let mut out = Vec::new();
        let error = rdf_error(
            pg.sparql_to_writer(query, &mut out, format(name))
                .unwrap_err(),
        );
        assert!(
            matches!(error, RdfError::WrongResultsFormat { .. }),
            "{query}: {error:?}"
        );
        assert_eq!(error.to_string(), message);
        assert!(out.is_empty());
    }
    // with a format of the right kind, the same queries fail in evaluation
    for (query, name) in [
        (construct, "nt"),
        (
            "SELECT * { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }",
            "csv",
        ),
    ] {
        let error = rdf_error(stream(&pg, query, format(name)).unwrap_err());
        assert!(
            matches!(error, RdfError::SparqlEvaluation(_)),
            "{query}: {error:?}"
        );
    }
    // a syntax error comes first
    let error = rdf_error(stream(&pg, "CONSTRUCT WHERE {", format("csv")).unwrap_err());
    assert!(matches!(error, RdfError::SparqlSyntax(_)), "{error:?}");
    // an empty format fits no query
    let empty = SparqlFormat {
        results: None,
        graph: None,
    };
    let error = rdf_error(stream(&pg, "ASK {}", empty.clone()).unwrap_err());
    assert_eq!(
        error.to_string(),
        "ASK results cannot be written as no format; use a SPARQL results format: json, xml, \
         csv or tsv"
    );

    // collected results check their own kind
    let select = pg.sparql("SELECT * { ?s ?p ?o }").unwrap();
    let mut out = Vec::new();
    let error = rdf_error(select.write(&mut out, RdfFormat::Turtle).unwrap_err());
    assert!(matches!(
        error,
        RdfError::WrongResultsFormat { form: "SELECT", .. }
    ));
    assert!(out.is_empty());
    let graph = pg.sparql("CONSTRUCT WHERE { ?s ?p ?o }").unwrap();
    let error = rdf_error(graph.write(&mut out, QueryResultsFormat::Json).unwrap_err());
    assert_eq!(
        error.to_string(),
        "CONSTRUCT and DESCRIBE results cannot be written as SPARQL Results in JSON; use an RDF \
         format such as nt, ttl, jsonld or rdf"
    );
    let error = rdf_error(
        SparqlResults::Boolean(true)
            .write(&mut out, empty)
            .unwrap_err(),
    );
    assert!(matches!(
        error,
        RdfError::WrongResultsFormat { form: "ASK", .. }
    ));
    assert!(out.is_empty());
}

/// `sparql_to_writer` and `sparql()?.write()` write the same bytes (and give the same counts
/// or the same error) for every query of the oracle corpus in every format.
#[test]
fn streamed_equals_collected() {
    let mut checked = 0;
    for (view, query) in corpus() {
        let names: &[&str] = match view.sparql(&query).unwrap() {
            SparqlResults::Graph(_) => &GRAPH_FORMATS,
            _ => &RESULTS_FORMATS,
        };
        for name in names {
            let streamed = stream(&view, &query, format(name)).map_err(|e| e.to_string());
            let collected = collect(&view, &query, format(name)).map_err(|e| e.to_string());
            assert_eq!(streamed, collected, "{query} as {name}");
            assert!(streamed.is_ok(), "{query} as {name}: {streamed:?}");
            checked += 1;
        }
    }
    assert!(checked > 700, "{checked}");

    // with a serializer that writes prefixes, and with the options
    let g = Graph::new();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http://ex/p"),
    )
    .unwrap();
    let serializer = serializer_with_prefixes(RdfFormat::Turtle, [("ex", "http://ex/")]).unwrap();
    let query = "CONSTRUCT WHERE { ?s ?p ?o }";
    let streamed = stream(&g, query, serializer.clone()).unwrap();
    assert_eq!(streamed.0, "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\n");
    assert_eq!(collect(&g, query, serializer.clone()).unwrap(), streamed);
    let with =
        written(|out| g.sparql_to_writer_with(query, out, serializer, &SparqlOptions::default()))
            .unwrap();
    assert_eq!(with, streamed);
}

/// SPARQL Results XML cannot hold a literal with a control character (other than tab and line
/// feed) or a carriage return: such a result fails instead of being written or dropped. JSON
/// holds it exactly.
#[test]
fn xml_unsafe_literals() {
    let g = Graph::new();
    let literal = |value: &str| Literal::new_simple_literal(value).to_string();
    for value in ["bell\u{1}", "a\rb", "\u{FFFE}"] {
        g.add_edge(
            1,
            "http://ex/s",
            literal(value).as_str(),
            NO_PROPS,
            Some("http://ex/p"),
        )
        .unwrap();
    }
    g.add_edge(
        1,
        "http://ex/s",
        literal("tab\tnewline\n").as_str(),
        NO_PROPS,
        Some("http://ex/p"),
    )
    .unwrap();
    let query = "SELECT ?o { <http://ex/s> <http://ex/p> ?o }";
    let unsafe_values = ["bell\u{1}", "a\rb", "\u{FFFE}"].map(literal);
    for error in [
        stream(&g, query, QueryResultsFormat::Xml).unwrap_err(),
        collect(&g, query, QueryResultsFormat::Xml).unwrap_err(),
    ] {
        let error = rdf_error(error);
        let RdfError::XmlUnsafeLiteral(value) = &error else {
            panic!("{error:?}")
        };
        // the message holds the literal in N-Triples form, so with its control characters
        // escaped
        assert!(unsafe_values.contains(value), "{value}");
        assert!(!value.contains(['\u{1}', '\r']), "{value:?}");
        assert!(
            error
                .to_string()
                .ends_with("cannot be written in SPARQL Results XML, which cannot hold its control characters unchanged; use JSON"),
            "{error}"
        );
    }
    // a literal computed by the query, too
    for query in [
        "SELECT ?x { BIND(\"a\\rb\" AS ?x) }",
        "SELECT ?x ?y { BIND(\"ok\" AS ?x) BIND(CONCAT(\"bell\", \"\\u0001\") AS ?y) }",
    ] {
        let error = rdf_error(stream(&g, query, QueryResultsFormat::Xml).unwrap_err());
        assert!(matches!(error, RdfError::XmlUnsafeLiteral(_)), "{error:?}");
    }
    // tab and line feed are fine, and JSON and TSV hold everything exactly
    let safe = "SELECT ?o { <http://ex/s> <http://ex/p> ?o FILTER(CONTAINS(?o, \"tab\")) }";
    let xml = doc(&g, safe, QueryResultsFormat::Xml);
    assert_eq!(
        Normalized::new(&parse_results(&xml, QueryResultsFormat::Xml)),
        Normalized::new(&g.sparql(safe).unwrap())
    );
    let expected = Normalized::new(&g.sparql(query).unwrap());
    let Normalized::Rows(_, rows) = &expected else {
        unreachable!()
    };
    assert_eq!(rows.len(), 4);
    for results_format in [QueryResultsFormat::Json, QueryResultsFormat::Tsv] {
        let doc = doc(&g, query, results_format);
        assert_eq!(
            Normalized::new(&parse_results(&doc, results_format)),
            expected,
            "{doc}"
        );
    }
}

/// CONSTRUCT results in RDF/XML follow the rules of `to_rdf`.
#[test]
fn construct_rdf_xml_follows_to_rdf() {
    const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    const XMLNS_FOO: &str = "http://www.w3.org/2000/xmlns/foo";
    let g = Graph::new();
    let edge = |s: &str, o: &str, p: &str| {
        g.add_edge(1, s, o, NO_PROPS, Some(p)).unwrap();
    };
    // predicates without a local name
    edge("http://ex/a", "http://ex/b", "http://ex/42");
    edge("http://ex/a", "http://ex/b", "http://ex/");
    edge("http://ex/a", "http://ex/b", "http://ex/4p");
    // predicates the serializer refuses (syntax terms) or writes as an `xmlns:` element
    edge("http://ex/a", "http://ex/b", &format!("{RDF_NS}li"));
    edge("http://ex/a", "http://ex/b", &format!("{RDF_NS}about"));
    edge("http://ex/a", "http://ex/b", XMLNS_FOO);
    // an `rdf:type` without a local name, first and only triple of a subject
    edge("http://ex/s", "http://ex/42", RDF_TYPE);
    edge("http://ex/s", "http://ex/o", "http://ex/p");
    edge("http://ex/t", "http://ex/42", RDF_TYPE);
    edge("http://ex/u", "http://ex/Person", RDF_TYPE);
    // an `rdf:type` in the `xmlns` namespace, the only triple of a subject, and a syntax term
    // as an `rdf:type`, which is fine
    edge("http://ex/x", XMLNS_FOO, RDF_TYPE);
    edge("http://ex/r", &format!("{RDF_NS}Description"), RDF_TYPE);
    // blank nodes whose label is not an XML name
    edge("_:1", "http://ex/o", "http://ex/p");
    edge("http://ex/v", "_:x1", "http://ex/p");
    // a literal XML cannot hold
    let cr = Literal::new_simple_literal("a\rb").to_string();
    edge("http://ex/w", &cr, "http://ex/p");

    let all = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
    let mut exported = Vec::new();
    let export_stats = g.to_rdf(&mut exported, RdfFormat::RdfXml).unwrap();
    let exported = String::from_utf8(exported).unwrap();
    let (doc, stats) = stream(&g, all, RdfFormat::RdfXml).unwrap();
    assert_eq!(
        (stats.written, stats.skipped),
        (export_stats.triples, export_stats.skipped)
    );
    assert_eq!((stats.written, stats.skipped), (7, 8), "{doc}");
    assert_eq!(
        parse_graph(&doc, RdfFormat::RdfXml),
        parse_graph(&exported, RdfFormat::RdfXml)
    );
    assert!(doc.contains("rdf:nodeID=\"x1\""), "{doc}");
    assert!(doc.contains("rdf:nodeID=\"xx1\""), "{doc}");
    assert!(
        doc.contains("<rdf:type rdf:resource=\"http://ex/42\"/>"),
        "{doc}"
    );
    assert!(!doc.contains('\r'), "{doc:?}");
    assert!(!doc.contains("xmlns:foo"), "{doc}");
    assert!(
        doc.contains(&format!("<rdf:type rdf:resource=\"{RDF_NS}Description\"/>")),
        "{doc}"
    );
    // other formats write every triple
    let (nt, stats) = stream(&g, all, RdfFormat::NTriples).unwrap();
    assert_eq!((stats.written, stats.skipped), (15, 0), "{nt}");
    // the syntax terms and the `xmlns` namespace in a template (one solution: `s p o`)
    for (template, written, skipped) in [
        (format!("?s <{RDF_NS}li> ?o"), 1, 1),
        (format!("?s <{RDF_NS}nodeID> ?o"), 1, 1),
        (format!("?s <{XMLNS_FOO}> ?o"), 1, 1),
        (format!("?s a <{XMLNS_FOO}>"), 1, 1),
        (format!("?s a <{XMLNS_FOO}> ; <http://ex/q> ?o"), 3, 0),
        (
            format!("?s <{RDF_NS}li> ?o ; a <{XMLNS_FOO}> ; <{RDF_NS}about> ?o"),
            1,
            3,
        ),
    ] {
        let query = format!(
            "CONSTRUCT {{ ?o <http://ex/q> ?s . {template} }} \
             WHERE {{ ?s <http://ex/p> ?o FILTER(isIRI(?s) && isIRI(?o)) }}"
        );
        let (doc, stats) = stream(&g, &query, RdfFormat::RdfXml).unwrap();
        assert_eq!(
            (stats.written, stats.skipped),
            (written, skipped),
            "{query}: {doc}"
        );
        assert!(!doc.contains("xmlns:foo"), "{doc}");
        let (nt, _) = stream(&g, &query, RdfFormat::NTriples).unwrap();
        let read = parse_graph(&doc, RdfFormat::RdfXml);
        assert_eq!(read.len(), written, "{query}: {doc}");
        assert!(
            read.is_subset(&parse_graph(&nt, RdfFormat::NTriples)),
            "{query}: {doc}"
        );
    }

    // runs of a subject: the serializer starts an element for every run, so an `rdf:type`
    // without a local name is held back in every run that it would start
    let typed = "PREFIX ex: <http://ex/>
        CONSTRUCT { ?s a ex:42 . ?s ex:q ?o } WHERE { ?s <http://ex/p> ?o FILTER(!isLiteral(?o)) }";
    let (doc, stats) = stream(&g, typed, RdfFormat::RdfXml).unwrap();
    assert_eq!((stats.written, stats.skipped), (6, 0), "{doc}");
    let read = parse_graph(&doc, RdfFormat::RdfXml);
    assert_eq!(read.len(), 6, "{doc}");
    assert_eq!(
        doc.matches("<rdf:type rdf:resource=\"http://ex/42\"/>")
            .count(),
        3,
        "{doc}"
    );
    let only_types = "PREFIX ex: <http://ex/>
        CONSTRUCT { ?s a ex:42 . ?o ex:q ?s } WHERE { ?s <http://ex/p> ?o FILTER(isIRI(?o)) }";
    let (doc, stats) = stream(&g, only_types, RdfFormat::RdfXml).unwrap();
    // the type triples are the only triples of their subjects, so they are skipped
    assert_eq!((stats.written, stats.skipped), (2, 2), "{doc}");
    let read = parse_graph(&doc, RdfFormat::RdfXml);
    assert_eq!(
        read,
        BTreeSet::from([
            "<http://ex/o> <http://ex/q> <http://ex/s>".to_owned(),
            "<http://ex/o> <http://ex/q> _:1".to_owned(),
        ]),
        "{doc}"
    );
    // ... while other formats write them
    let (_, stats) = stream(&g, only_types, RdfFormat::Turtle).unwrap();
    assert_eq!((stats.written, stats.skipped), (4, 0));
}

/// RDF/XML CONSTRUCT results are grouped by subject, so every subject gets one element and an
/// unnameable `rdf:type` is skipped only when its subject has no other triple, as in `to_rdf`.
#[test]
fn construct_rdf_xml_groups_by_subject() {
    let g = Graph::new();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http://ex/p"),
    )
    .unwrap();
    g.add_edge(
        1,
        "http://ex/c",
        "http://ex/d",
        NO_PROPS,
        Some("http://ex/p"),
    )
    .unwrap();
    let query = "PREFIX ex: <http://ex/>
        CONSTRUCT { ?s ex:q ?o . ?o ex:r ?s . ?s a ex:42 } WHERE { ?s ex:p ?o }";
    let (nt, stats) = stream(&g, query, RdfFormat::NTriples).unwrap();
    assert_eq!((stats.written, stats.skipped), (6, 0), "{nt}");
    // the triples of `a` are not consecutive
    let lines: Vec<&str> = nt.lines().collect();
    let of_a: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].starts_with("<http://ex/a> "))
        .collect();
    assert_eq!(of_a.len(), 2, "{nt}");
    assert_ne!(of_a[1], of_a[0] + 1, "{nt}");

    let (doc, stats) = stream(&g, query, RdfFormat::RdfXml).unwrap();
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
    let loaded = Graph::new();
    loaded
        .load_rdf(1, nt.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let mut exported = Vec::new();
    let export_stats = loaded.to_rdf(&mut exported, RdfFormat::RdfXml).unwrap();
    let exported = String::from_utf8(exported).unwrap();
    assert_eq!((export_stats.triples, export_stats.skipped), (6, 0));
    assert_eq!(
        parse_graph(&exported, RdfFormat::RdfXml),
        parse_graph(&doc, RdfFormat::RdfXml)
    );
    // collected results write the same bytes, and other formats keep the order of the template
    assert_eq!(collect(&g, query, RdfFormat::RdfXml).unwrap(), (doc, stats));
    assert_eq!(collect(&g, query, RdfFormat::NTriples).unwrap().0, nt);
}

/// Graphs that were not loaded from RDF give `raphtory:` IRIs; time graphs are IRIs too.
#[test]
fn non_rdf_graphs() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob Smith", NO_PROPS, Some("knows"))
        .unwrap();
    assert_eq!(
        doc(&g, "SELECT ?s ?p ?o { ?s ?p ?o }", format("tsv")),
        "?s\t?p\t?o\n<raphtory:Alice>\t<raphtory:knows>\t<raphtory:Bob%20Smith>\n"
    );
    assert_eq!(
        doc(&g, "SELECT ?s { ?s ?p ?o }", format("csv")),
        "s\r\nraphtory:Alice\r\n"
    );
    assert_eq!(
        doc(&g, "CONSTRUCT WHERE { ?s ?p ?o }", format("nt")),
        "<raphtory:Alice> <raphtory:knows> <raphtory:Bob%20Smith> .\n"
    );
    assert_eq!(
        doc(
            &g,
            "SELECT ?g { BIND(raphtory:asof:2024-01-01 AS ?g) }",
            format("json")
        ),
        r#"{"head":{"vars":["g"]},"results":{"bindings":[{"g":{"type":"uri","value":"raphtory:asof:2024-01-01"}}]}}"#
    );
    // a u64-id graph
    let ids = Graph::new();
    ids.add_edge(1, 42, 43, NO_PROPS, None).unwrap();
    assert_eq!(
        doc(&ids, "CONSTRUCT WHERE { ?s ?p ?o }", format("ttl")),
        "<raphtory:42> <raphtory:_default> <raphtory:43> .\n"
    );
    // a generalized triple (a literal subject) is a solution, but CONSTRUCT drops it
    g.add_edge(1, "\"x\"", "Bob Smith", NO_PROPS, Some("knows"))
        .unwrap();
    assert_eq!(
        doc(
            &g,
            "SELECT ?s { ?s raphtory:knows ?o } ORDER BY ?s",
            format("tsv")
        ),
        "?s\n<raphtory:Alice>\n\"x\"\n"
    );
    let (nt, stats) = stream(&g, "CONSTRUCT WHERE { ?s ?p ?o }", format("nt")).unwrap();
    assert_eq!(
        nt,
        "<raphtory:Alice> <raphtory:knows> <raphtory:Bob%20Smith> .\n"
    );
    assert_eq!(stats.written, 1);
}

/// An evaluation error while results are written is returned, not a truncated success.
#[test]
fn evaluation_errors_while_writing() {
    let pg = PersistentGraph::new();
    pg.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    // the second time graph IRI is only built (and found invalid) after the first solutions
    let select = "SELECT ?t ?s { VALUES ?t { \"1\" \"nope\" } \
        BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) LATERAL { GRAPH ?g { ?s ?p ?o } } }";
    let construct = "CONSTRUCT { ?s ?p ?o } WHERE { VALUES ?t { \"1\" \"nope\" } \
        BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) LATERAL { GRAPH ?g { ?s ?p ?o } } }";
    for (query, names) in [
        (select, &RESULTS_FORMATS[..]),
        (construct, &GRAPH_FORMATS[..]),
    ] {
        for name in names {
            let mut out = Vec::new();
            let error = rdf_error(
                pg.sparql_to_writer(query, &mut out, format(name))
                    .unwrap_err(),
            );
            let RdfError::SparqlEvaluation(inner) = &error else {
                panic!("{query} as {name}: {error:?}")
            };
            assert!(inner.to_string().contains("raphtory:asof:nope"), "{inner}");
            // the writer may hold the start of the document; RDF/XML groups the triples by
            // subject before it writes any
            assert_eq!(out.is_empty(), *name == "rdf", "{query} as {name}");
            assert!(matches!(
                collect(&pg, query, format(name)).map_err(rdf_error),
                Err(RdfError::SparqlEvaluation(_))
            ));
        }
    }
}

/// As-of results through `snapshot_at` and through `GRAPH <raphtory:asof:T>`.
#[test]
fn as_of_results() {
    let pg = PersistentGraph::new();
    let works_for = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
    pg.load_rdf(1, works_for.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    pg.retract_rdf(5, works_for.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let select = "SELECT ?who { ?who <http://ex/worksFor> <http://ex/acme> }";
    let in_graph =
        "SELECT ?who { GRAPH raphtory:asof:3 { ?who <http://ex/worksFor> <http://ex/acme> } }";
    let as_of_3 = r#"{"head":{"vars":["who"]},"results":{"bindings":[{"who":{"type":"uri","value":"http://ex/alice"}}]}}"#;
    assert_eq!(doc(&pg.snapshot_at(3), select, format("json")), as_of_3);
    assert_eq!(doc(&pg, in_graph, format("json")), as_of_3);
    assert_eq!(
        doc(&pg, select, format("json")),
        r#"{"head":{"vars":["who"]},"results":{"bindings":[]}}"#
    );
    // the event graph ignores the retraction
    assert_eq!(doc(&pg.event_graph(), select, format("json")), as_of_3);

    let construct = "CONSTRUCT WHERE { ?s ?p ?o }";
    assert_eq!(
        doc(&pg.snapshot_at(3), construct, format("nt")),
        format!("{works_for}\n")
    );
    assert_eq!(
        doc(
            &pg,
            "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH raphtory:asof:3 { ?s ?p ?o } }",
            format("nt")
        ),
        format!("{works_for}\n")
    );
    assert_eq!(doc(&pg, construct, format("nt")), "");
    assert_eq!(
        doc(&pg.snapshot_at(3), "ASK { ?s ?p ?o }", format("csv")),
        "true"
    );
    assert_eq!(doc(&pg, "ASK { ?s ?p ?o }", format("csv")), "false");
}

/// The counts of `SparqlWriteStats`.
#[test]
fn write_stats() {
    let g = Graph::new();
    for (s, o, p) in [
        ("http://ex/a", "http://ex/b", "http://ex/p"),
        ("http://ex/a", "http://ex/c", "http://ex/p"),
        ("http://ex/b", "http://ex/c", "http://ex/42"),
    ] {
        g.add_edge(1, s, o, NO_PROPS, Some(p)).unwrap();
    }
    let stats = |query: &str, format: SparqlFormat| {
        let (_, streamed) = stream(&g, query, format.clone()).unwrap();
        let (_, collected) = collect(&g, query, format).unwrap();
        assert_eq!(streamed, collected, "{query}");
        (streamed.written, streamed.skipped)
    };
    let select = "SELECT * { ?s ?p ?o }";
    for name in RESULTS_FORMATS {
        assert_eq!(stats(select, format(name)), (3, 0), "{name}");
        assert_eq!(
            stats("SELECT * { ?s ?p ?o FILTER(false) }", format(name)),
            (0, 0)
        );
        assert_eq!(stats("ASK { ?s ?p ?o }", format(name)), (0, 0), "{name}");
    }
    // duplicates are not counted
    let construct = "CONSTRUCT { ?s <http://ex/q> <http://ex/o> } WHERE { ?s ?p ?o }";
    assert_eq!(stats(construct, format("nt")), (2, 0));
    let all = "CONSTRUCT WHERE { ?s ?p ?o }";
    assert_eq!(stats(all, format("ttl")), (3, 0));
    // RDF/XML skips the predicate without a local name
    assert_eq!(stats(all, format("rdf")), (2, 1));
    assert_eq!(stats("DESCRIBE <http://ex/b>", format("xml")), (0, 1));
    assert_eq!(stats("DESCRIBE <http://ex/b>", format("json")), (1, 0));
}
