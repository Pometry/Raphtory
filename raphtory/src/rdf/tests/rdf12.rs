//! Behaviour with and without RDF 1.2 / SPARQL 1.2 (enabled by the `shacl` feature), and the
//! refusal of SPARQL `SERVICE` calls.
//!
//! Every test passes with `--features rdf` and `--features shacl`; where they differ, the test
//! checks [`rdf12`] to pick the expected behaviour.
use super::{ask, within_60s};
use crate::{
    errors::GraphError,
    prelude::*,
    rdf::{
        evaluator, literal_to_prop,
        model::{Literal, Term, TermRef},
        name_of, term_of, QueryResultsFormat, RaphtoryDataset, RdfError, RdfFormat, RdfMutationOps,
        RdfParser, RdfViewOps, SparqlFormat, SparqlResults,
    },
};
use oxigraph::sparql::{QueryEvaluationError, QueryResults};
use std::{
    io::Read,
    net::TcpListener,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

/// Whether this build parses RDF 1.2 (triple terms, directional literals).
fn rdf12() -> bool {
    RdfParser::from_format(RdfFormat::NTriples)
        .for_slice("<http://e/a> <http://e/p> <<( <http://e/s> <http://e/p> <http://e/o> )>> .")
        .all(|quad| quad.is_ok())
}

/// Whether this build parses SPARQL 1.2.
fn sparql12() -> bool {
    spargebra::SparqlParser::new()
        .parse_query("SELECT * { ?s ?p <<( ?a ?b ?c )>> }")
        .is_ok()
}

#[test]
fn the_build_has_both_or_neither() {
    assert_eq!(rdf12(), sparql12());
    #[cfg(feature = "shacl")]
    assert!(rdf12(), "the shacl feature turns on RDF 1.2");
}

fn rdf_error(error: GraphError) -> RdfError {
    match error {
        GraphError::Rdf(error) => error,
        other => panic!("not an RDF error: {other:?}"),
    }
}

/// Checks the error of reading an RDF 1.2 term: [`RdfError::Rdf12Term`] in a build that parses
/// RDF 1.2, a parse error in one that does not.
fn assert_rdf12_rejected(error: GraphError, term: &str) {
    let error = rdf_error(error);
    if rdf12() {
        let RdfError::Rdf12Term(found) = &error else {
            panic!("expected Rdf12Term, got {error:?}");
        };
        assert_eq!(found, term);
        assert!(
            error
                .to_string()
                .ends_with("cannot be stored: RDF 1.2 triple terms (including annotations and reifiers) and directional language-tagged strings are not supported"),
            "{error}"
        );
    } else {
        assert!(matches!(error, RdfError::Parse(_)), "{error:?}");
    }
}

// ------------------------------------------------------------------------------------------
// SERVICE

/// Accepts connections on a local port and counts them.
fn listener() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            counter.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut stream) = stream {
                let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                let _ = stream.read(&mut [0; 1024]);
            }
        }
    });
    (port, hits)
}

/// Gives a connection that a request may have started time to be accepted.
fn connections(hits: &AtomicUsize) -> usize {
    std::thread::sleep(Duration::from_millis(300));
    hits.load(Ordering::SeqCst)
}

fn assert_service_refused(error: GraphError, port: u16) {
    let error = rdf_error(error);
    assert!(
        matches!(
            &error,
            RdfError::SparqlEvaluation(QueryEvaluationError::Service(_))
        ),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        format!(
            "SPARQL evaluation error: SERVICE <http://127.0.0.1:{port}/sparql> is not \
             supported: Raphtory never sends SPARQL queries to other endpoints"
        )
    );
}

fn small_graph() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, Some("p")).unwrap();
    g
}

#[test]
fn service_is_refused_by_sparql() {
    let g = small_graph();
    let (port, hits) = listener();
    let service = format!("<http://127.0.0.1:{port}/sparql>");
    for query in [
        format!("SELECT * {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
        format!("ASK {{ ?a ?b ?c SERVICE {service} {{ ?s ?p ?o }} }}"),
        format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
        format!("SELECT * {{ GRAPH <raphtory:asof:1> {{ SERVICE {service} {{ ?s ?p ?o }} }} }}"),
    ] {
        assert_service_refused(g.sparql(&query).unwrap_err(), port);
    }
    // a service named by a variable is never called (spareval only calls constant services)
    let error = g
        .sparql(&format!(
            "SELECT * {{ VALUES ?x {{ {service} }} SERVICE ?x {{ ?s ?p ?o }} }}"
        ))
        .unwrap_err();
    assert!(matches!(
        rdf_error(error),
        RdfError::SparqlEvaluation(QueryEvaluationError::UnboundService)
    ),);
    // a SILENT service ignores the error: one empty solution
    let results = g
        .sparql(&format!(
            "SELECT * {{ SERVICE SILENT {service} {{ ?s ?p ?o }} }}"
        ))
        .unwrap();
    let SparqlResults::Solutions { rows, .. } = results else {
        panic!("not a SELECT result")
    };
    assert_eq!(rows, vec![vec![None, None, None]]);
    assert_eq!(connections(&hits), 0);
}

#[test]
fn service_is_refused_by_sparql_to_writer() {
    let g = small_graph();
    let (port, hits) = listener();
    let service = format!("<http://127.0.0.1:{port}/sparql>");
    for (query, format) in [
        (
            format!("SELECT * {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
            SparqlFormat::from(QueryResultsFormat::Json),
        ),
        (
            format!("CONSTRUCT {{ ?s ?p ?o }} WHERE {{ SERVICE {service} {{ ?s ?p ?o }} }}"),
            SparqlFormat::from(RdfFormat::NTriples),
        ),
    ] {
        let mut out = Vec::new();
        let error = g.sparql_to_writer(&query, &mut out, format).unwrap_err();
        assert_service_refused(error, port);
    }
    assert_eq!(connections(&hits), 0);
}

#[test]
fn service_is_refused_by_the_evaluator() {
    let g = small_graph();
    let (port, hits) = listener();
    let query = format!("SELECT * {{ SERVICE <http://127.0.0.1:{port}/sparql> {{ ?s ?p ?o }} }}");
    let evaluated = evaluator()
        .parse_query(&query)
        .unwrap()
        .on_queryable_dataset(RaphtoryDataset::new(g.clone()))
        .execute()
        .map(|results| match results {
            QueryResults::Solutions(mut solutions) => solutions.try_for_each(|s| s.map(|_| ())),
            _ => Ok(()),
        });
    let error = match evaluated {
        Ok(Err(error)) | Err(error) => error,
        Ok(Ok(())) => panic!("the SERVICE call succeeded"),
    };
    assert!(
        matches!(error, QueryEvaluationError::Service(_)),
        "{error:?}"
    );
    assert_eq!(connections(&hits), 0);
}

// ------------------------------------------------------------------------------------------
// RDF 1.2 input

#[test]
fn triple_terms_are_rejected() {
    let term = "<<( <http://ex/s> <http://ex/q> <http://ex/o> )>>";
    for (doc, format) in [
        (
            format!("<http://ex/a> <http://ex/p> {term} ."),
            RdfFormat::NTriples,
        ),
        (
            "@prefix ex: <http://ex/> . ex:a ex:p <<( ex:s ex:q ex:o )>> .".to_owned(),
            RdfFormat::Turtle,
        ),
    ] {
        let pg = PersistentGraph::new();
        let error = pg.load_rdf(1, doc.as_bytes(), format, None).unwrap_err();
        assert_rdf12_rejected(error, term);
        assert_eq!(pg.count_edges(), 0);
        assert_eq!(pg.count_nodes(), 0);
        let error = pg.retract_rdf(1, doc.as_bytes(), format, None).unwrap_err();
        assert_rdf12_rejected(error, term);
        assert_eq!(pg.count_edges(), 0);
    }
}

/// An annotation asserts its triple, then the reifier triple fails: the load is not atomic, so
/// the asserted triple stays (in a build that parses RDF 1.2).
#[test]
fn annotations_and_reifiers_are_rejected() {
    let triple = "<<( <http://ex/a> <http://ex/p> <http://ex/b> )>>";
    let pg = PersistentGraph::new();
    let error = pg
        .load_rdf(
            1,
            "@prefix ex: <http://ex/> . ex:a ex:p ex:b {| ex:since 2020 |} .".as_bytes(),
            RdfFormat::Turtle,
            None,
        )
        .unwrap_err();
    assert_rdf12_rejected(error, triple);
    if rdf12() {
        assert_eq!(pg.count_edges(), 1);
        assert!(ask(
            &pg,
            "ASK { <http://ex/a> <http://ex/p> <http://ex/b> }"
        ));
    }
    let pg = PersistentGraph::new();
    let error = pg
        .load_rdf(
            1,
            "@prefix ex: <http://ex/> . << ex:a ex:p ex:b ~ ex:r >> ex:since 2020 .".as_bytes(),
            RdfFormat::Turtle,
            None,
        )
        .unwrap_err();
    assert_rdf12_rejected(error, triple);
    assert!(!ask(&pg, "ASK { ?s <http://ex/since> ?o }"));
}

#[test]
fn version_directive() {
    let pg = PersistentGraph::new();
    let read = pg.load_rdf(
        1,
        "VERSION \"1.2\"\n<http://ex/a> <http://ex/p> <http://ex/b> .".as_bytes(),
        RdfFormat::Turtle,
        None,
    );
    if rdf12() {
        assert_eq!(read.unwrap(), 1);
    } else {
        assert!(matches!(rdf_error(read.unwrap_err()), RdfError::Parse(_)));
    }
}

/// A name in the form of a directional literal is the same `raphtory:` IRI in both builds.
#[test]
fn directional_literal_names_are_iris() {
    let name = "\"hi\"@en--ltr";
    let term = term_of(name);
    assert_eq!(term.to_string(), "<raphtory:%22hi%22%40en--ltr>");
    assert_eq!(name_of(term.as_ref()).as_deref(), Some(name));
    // a graph not loaded from RDF that has such a node exports it as that IRI
    let g = Graph::new();
    g.add_edge(1, "http://ex/a", name, NO_PROPS, Some("http://ex/label"))
        .unwrap();
    let mut out = Vec::new();
    g.to_rdf(&mut out, RdfFormat::NTriples).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "<http://ex/a> <http://ex/label> <raphtory:%22hi%22%40en--ltr> .\n"
    );
    assert!(ask(
        &g,
        "ASK { <http://ex/a> <http://ex/label> <raphtory:%22hi%22%40en--ltr> }"
    ));
    // ordinary language-tagged literals are still literals
    assert!(matches!(term_of("\"hi\"@en"), Term::Literal(_)));
    assert!(matches!(term_of("\"hi\"@en-gb"), Term::Literal(_)));
}

#[test]
fn directional_literals_are_not_names() {
    // only a build with RDF 1.2 has directional literals
    let Ok(literal) = Literal::from_str("\"hi\"@en--ltr") else {
        assert!(!rdf12());
        return;
    };
    assert!(rdf12());
    assert_eq!(literal.language(), Some("en"));
    assert_eq!(name_of(TermRef::Literal(literal.as_ref())), None);
    assert_eq!(literal_to_prop(&literal), None);
}

#[test]
fn directional_literals_are_rejected() {
    let pg = PersistentGraph::new();
    let error = pg
        .load_rdf(
            1,
            "<http://ex/a> <http://ex/label> \"hi\"@en--ltr .".as_bytes(),
            RdfFormat::NTriples,
            None,
        )
        .unwrap_err();
    assert_rdf12_rejected(error, "\"hi\"@en--ltr");
    assert_eq!(pg.count_nodes(), 0);

    // JSON-LD @direction
    let pg = PersistentGraph::new();
    let doc = r#"{"@id": "http://ex/a",
        "http://ex/label": {"@value": "hi", "@language": "en", "@direction": "rtl"}}"#;
    let read = pg.load_rdf(
        1,
        doc.as_bytes(),
        RdfFormat::JsonLd {
            profile: Default::default(),
        },
        None,
    );
    if rdf12() {
        let error = rdf_error(read.unwrap_err());
        assert!(
            matches!(&error, RdfError::Rdf12Term(term) if term == "\"hi\"@en--rtl"),
            "{error:?}"
        );
        assert_eq!(pg.count_nodes(), 0);
    } else {
        // the RDF 1.1 parser drops the direction
        assert_eq!(read.unwrap(), 1);
        assert!(pg.node("\"hi\"@en").is_some());
    }
}

// ------------------------------------------------------------------------------------------
// SPARQL 1.2

fn sparql_syntax_error(result: Result<SparqlResults, GraphError>) -> bool {
    matches!(result, Err(GraphError::Rdf(RdfError::SparqlSyntax(_))))
}

#[test]
fn sparql12_patterns_match_nothing() {
    let pg = PersistentGraph::new();
    pg.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let no_rows = |query: &str| {
        let result = pg.sparql(query);
        if sparql12() {
            match result.unwrap() {
                SparqlResults::Solutions { rows, .. } => assert!(rows.is_empty(), "{query}"),
                other => panic!("{query}: {other:?}"),
            }
        } else {
            assert!(sparql_syntax_error(result), "{query}");
        }
    };
    no_rows("SELECT * { ?s ?p <<( ?a ?b ?c )>> }");
    no_rows("SELECT * { << ?s ?p ?o >> ?q ?v }");
    no_rows("SELECT * { ?s ?p ?o FILTER(isTRIPLE(?o)) }");
    no_rows("SELECT * { ?s ?p ?o ~ ?r }");
    let result = pg.sparql("VERSION \"1.2\" ASK { ?s ?p ?o }");
    if sparql12() {
        assert_eq!(result.unwrap(), SparqlResults::Boolean(true));
    } else {
        assert!(sparql_syntax_error(result));
    }
}

/// A triple term built by a query (`TRIPLE(..)`, SPARQL 1.2) is never written as XML: its nested
/// literals are not checked for characters XML cannot hold. JSON-LD cannot write one either.
#[test]
fn triple_terms_built_by_queries() {
    let g = small_graph();
    let select = "SELECT (TRIPLE(?s, ?p, \"a\\u0001b\") AS ?t) { ?s ?p ?o }";
    let construct = "CONSTRUCT { ?s ?p <<( ?s ?p ?o )>> } WHERE { ?s ?p ?o }";
    let write = |query: &str, format: &str| {
        let mut out = Vec::new();
        g.sparql_to_writer(query, &mut out, SparqlFormat::parse(format).unwrap())
            .map(|stats| (stats, String::from_utf8(out).unwrap()))
    };
    if !sparql12() {
        for query in [select, construct] {
            assert!(sparql_syntax_error(g.sparql(query)));
        }
        return;
    }
    let error = rdf_error(write(select, "xml").unwrap_err());
    assert!(
        matches!(&error, RdfError::XmlTripleTerm(t) if t.starts_with("<<(")),
        "{error:?}"
    );
    // the error names the triple term, not control characters it does not have
    let safe = "SELECT (TRIPLE(?s, ?p, ?o) AS ?t) { ?s ?p ?o }";
    let error = rdf_error(write(safe, "xml").unwrap_err());
    assert!(matches!(&error, RdfError::XmlTripleTerm(_)), "{error:?}");
    let message = error.to_string();
    assert!(
        message.ends_with(
            ")>> is an RDF 1.2 triple term, which Raphtory does not write in SPARQL Results \
             XML; use JSON, CSV or TSV"
        ),
        "{message}"
    );
    assert!(!message.contains("control characters"), "{message}");
    for format in ["csv", "tsv"] {
        let (stats, _) = write(safe, format).unwrap();
        assert_eq!(stats.written, 1, "{format}");
    }
    let (stats, json) = write(select, "json").unwrap();
    assert_eq!(stats.written, 1);
    assert!(json.contains(r#""type":"triple""#), "{json}");

    let (stats, nt) = write(construct, "nt").unwrap();
    assert_eq!(stats.written, 1);
    assert_eq!(
        nt,
        "<raphtory:a> <raphtory:p> <<( <raphtory:a> <raphtory:p> <raphtory:b> )>> .\n"
    );
    let (stats, _) = write(construct, "rdf").unwrap();
    assert_eq!((stats.written, stats.skipped), (0, 1));
    let error = write(construct, "jsonld").unwrap_err();
    assert!(error.to_string().contains("JSON-LD"), "{error}");
}

/// Loads, queries and exports still work alongside a writer in both builds (smoke test of the
/// shared code paths changed for RDF 1.2).
#[test]
fn rdf11_documents_load_the_same() {
    within_60s(|| {
        let pg = PersistentGraph::new();
        let doc = r#"
            @prefix ex: <http://ex/> .
            ex:a ex:label "hi"@en , "x"@fr , "plain" , 42 ; ex:knows _:b .
        "#;
        assert_eq!(
            pg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None)
                .unwrap(),
            5
        );
        assert!(pg.node("\"hi\"@en").is_some());
        assert!(pg.node("\"x\"@fr").is_some());
        let mut out = Vec::new();
        assert_eq!(pg.to_rdf(&mut out, RdfFormat::RdfXml).unwrap().triples, 5);
    });
}
