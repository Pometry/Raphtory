//! The W3C SPARQL 1.0/1.1 query and Turtle evaluation tests (test-suites/rdf-tests) on `RdfGraph`,
//! skipping named graphs (Raphtory does not store them) and the syntax and CSV serialization tests.
use oxigraph::{
    io::{RdfFormat as OxRdfFormat, RdfParser},
    model::{
        dataset::CanonicalizationAlgorithm, vocab::rdf, BlankNode, BlankNodeRef, Graph, Literal,
        NamedNode, NamedNodeRef, NamedOrBlankNodeRef, TermRef, Triple, TripleRef,
    },
    sparql::{
        results::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput},
        Variable,
    },
};
use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults};
use spargebra::SparqlParser;
use std::{collections::HashSet, error::Error, fs};

const SUITE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test-suites/rdf-tests/");
// files are named by their published URL, so relative IRIs resolve as the expected results assume
const TESTS_URL: &str = "https://w3c.github.io/rdf-tests/";
const MF: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const QT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#";
const RS: &str = "http://www.w3.org/2001/sw/DataAccess/tests/result-set#";
const RDFT: &str = "http://www.w3.org/ns/rdftest#";

const KNOWN_FAILURES: &[&str] = &[
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-2", // spareval compares dates with and without a timezone
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1", // spareval: STR() of a number is canonical
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2", // spareval: STR() of a number is canonical
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-groupconcat-04", // spareval: GROUP_CONCAT keeps a shared language tag
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-groupconcat-06", // spareval: GROUP_CONCAT keeps a shared language tag
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-sum-02", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-avg-02", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-min-01", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-min-02", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-max-01", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-max-02", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-err-02", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-max-distinct", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-min-distinct", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/cast/manifest#cast-float", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/cast/manifest#cast-double", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/cast/manifest#cast-decimal", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#plus-1-corrected", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#coalesce01", // spareval: non-canonical number form
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#bnode01", // spareval: BNODE(str) is not scoped to one solution
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_more_set_start", // spareval: zero-length path skips constants absent from the data
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_more_set_end", // spareval: zero-length path skips constants absent from the data
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_one_set_start", // spareval: zero-length path skips constants absent from the data
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_one_set_end", // spareval: zero-length path skips constants absent from the data
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#tsv03", // the expected TSV writes the double "1.0E6" as 1.0e6
];

type Outcome = Option<Result<(), Box<dyn Error>>>;

#[test]
fn sparql10_evaluation() {
    run("sparql/sparql10/manifest-evaluation.ttl", (282, 248, 3, 31));
}

#[test]
fn sparql11_query() {
    run(
        "sparql/sparql11/manifest-sparql11-query.ttl",
        (328, 188, 21, 119),
    );
}

#[test]
fn sparql11_results() {
    run(
        "sparql/sparql11/manifest-sparql11-results.ttl",
        (10, 6, 1, 3),
    );
}

#[test]
fn turtle() {
    run("rdf/rdf11/rdf-turtle/manifest.ttl", (313, 149, 0, 164));
}

fn run(manifest: &str, counts: (usize, usize, usize, usize)) {
    let mut outcomes = Vec::new();
    read_manifest(&format!("{TESTS_URL}{manifest}"), &mut outcomes);
    let (mut pass, mut known, mut skipped, mut problems) = (0, 0, 0, Vec::new());
    for (id, outcome) in &outcomes {
        match (outcome, KNOWN_FAILURES.contains(&id.as_str())) {
            (None, _) => skipped += 1,
            (Some(Ok(())), false) => pass += 1,
            (Some(Err(_)), true) => known += 1,
            (Some(Ok(())), true) => problems.push(format!("{id} passes: unlist it")),
            (Some(Err(e)), false) => problems.push(format!("{id}: {e}")),
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert_eq!((outcomes.len(), pass, known, skipped), counts);
}

fn iri(namespace: &str, local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{namespace}{local}"))
}

fn read(url: &str) -> Vec<u8> {
    let path = format!("{SUITE}{}", url.strip_prefix(TESTS_URL).unwrap());
    fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}; run make w3c-tests-init"))
}

fn extension(url: &str) -> &str {
    url.rsplit_once('.').map_or("", |(_, extension)| extension)
}

fn parse(data: &[u8], format: OxRdfFormat, base: &str) -> Vec<Triple> {
    RdfParser::from_format(format)
        .with_base_iri(base)
        .unwrap()
        .for_slice(data)
        .map(|quad| quad.unwrap_or_else(|e| panic!("{base}: {e}")).into())
        .collect()
}

fn triples(url: &str) -> Vec<Triple> {
    parse(
        &read(url),
        OxRdfFormat::from_extension(extension(url)).unwrap(),
        url,
    )
}

fn iris<'a, 'b>(
    g: &'a Graph,
    subject: impl Into<NamedOrBlankNodeRef<'b>>,
    namespace: &str,
    local: &str,
) -> Vec<&'a str> {
    let subject = subject.into();
    g.objects_for_subject_predicate(subject, &iri(namespace, local))
        .filter_map(|object| match object {
            TermRef::NamedNode(object) => Some(object.as_str()),
            _ => None,
        })
        .collect()
}

fn list<'a>(g: &'a Graph, manifest: NamedOrBlankNodeRef<'_>, local: &str) -> Vec<&'a str> {
    let mut items = Vec::new();
    let mut cell = g.object_for_subject_predicate(manifest, &iri(MF, local));
    while let Some(TermRef::BlankNode(node)) = cell {
        if let Some(TermRef::NamedNode(item)) = g.object_for_subject_predicate(node, rdf::FIRST) {
            items.push(item.as_str());
        }
        cell = g.object_for_subject_predicate(node, rdf::REST);
    }
    items
}

fn read_manifest(url: &str, outcomes: &mut Vec<(String, Outcome)>) {
    let g: Graph = triples(url).into_iter().collect();
    // not always `<>`: some manifests are blank nodes
    let manifest = g
        .subject_for_predicate_object(rdf::TYPE, &iri(MF, "Manifest"))
        .unwrap();
    for include in list(&g, manifest, "include") {
        read_manifest(include, outcomes);
    }
    for test in list(&g, manifest, "entries") {
        outcomes.push((
            test.to_owned(),
            run_test(&g, NamedNodeRef::new_unchecked(test)),
        ));
    }
}

fn run_test(g: &Graph, test: NamedNodeRef<'_>) -> Outcome {
    let is = |namespace, local| g.contains(TripleRef::new(test, rdf::TYPE, &iri(namespace, local)));
    let result = || iris(g, test, MF, "result")[0];
    match g.object_for_subject_predicate(test, &iri(MF, "action"))? {
        TermRef::NamedNode(document) if is(RDFT, "TestTurtleEval") => {
            Some(turtle_eval(document.as_str(), result()))
        }
        TermRef::NamedNode(document) if is(RDFT, "TestTurtleNegativeEval") => {
            let url = document.as_str();
            let loaded =
                RdfGraph::new().load_rdf(0, read(url).as_slice(), RdfFormat::Turtle, Some(url));
            Some(loaded.map_or(Ok(()), |_| {
                Err("loaded a document that must be rejected".into())
            }))
        }
        TermRef::BlankNode(action)
            if is(MF, "QueryEvaluationTest") && iris(g, action, QT, "graphData").is_empty() =>
        {
            let lax = g.contains(TripleRef::new(
                test,
                &iri(MF, "resultCardinality"),
                &iri(MF, "LaxCardinality"),
            ));
            let query_url = iris(g, action, QT, "query")[0];
            let query = format!(
                "BASE <{query_url}>\n{}",
                String::from_utf8(read(query_url)).unwrap()
            );
            if SparqlParser::new()
                .parse_query(&query)
                .is_ok_and(|parsed| parsed.dataset().is_some())
            {
                return None;
            }
            Some(query_eval(
                &query,
                &iris(g, action, QT, "data"),
                result(),
                lax,
            ))
        }
        _ => None,
    }
}

fn query_eval(query: &str, data: &[&str], result: &str, lax: bool) -> Result<(), Box<dyn Error>> {
    let rg = RdfGraph::new();
    for url in data {
        let format = RdfFormat::from_extension(extension(url));
        rg.load_rdf(0, read(url).as_slice(), format, Some(url))?;
    }
    let actual = rg.sparql(query, &SparqlOptions::default())?;
    let (expected, ordered) = expected(result, matches!(actual, SparqlResults::Graph(_)));
    compare(expected, actual, lax, ordered)
}

fn turtle_eval(url: &str, result: &str) -> Result<(), Box<dyn Error>> {
    let rg = RdfGraph::new();
    rg.load_rdf(0, read(url).as_slice(), RdfFormat::Turtle, Some(url))?;
    let ours = rg.to_rdf_string(RdfFormat::NTriples)?;
    compare(
        SparqlResults::Graph(triples(result)),
        SparqlResults::Graph(parse(ours.as_bytes(), OxRdfFormat::NTriples, url)),
        false,
        false,
    )
}

fn expected(url: &str, graph_form: bool) -> (SparqlResults, bool) {
    if let Some(format) = QueryResultsFormat::from_extension(extension(url)) {
        let parser = QueryResultsParser::from_format(format);
        let results = match parser.for_reader(read(url).as_slice()).unwrap() {
            ReaderQueryResultsParserOutput::Boolean(value) => SparqlResults::Boolean(value),
            ReaderQueryResultsParserOutput::Solutions(solutions) => SparqlResults::Solutions {
                variables: solutions.variables().to_vec(),
                rows: solutions.map(|s| s.unwrap().values().to_vec()).collect(),
            },
        };
        return (results, false);
    }
    if graph_form {
        return (SparqlResults::Graph(triples(url)), false);
    }
    result_set(&triples(url).into_iter().collect())
}

fn result_set(g: &Graph) -> (SparqlResults, bool) {
    let rs = |local| iri(RS, local);
    let set = g
        .subject_for_predicate_object(rdf::TYPE, &rs("ResultSet"))
        .unwrap();
    if let Some(value) = g.object_for_subject_predicate(set, &rs("boolean")) {
        return (SparqlResults::Boolean(literal(value) == "true"), false);
    }
    let variables: Vec<_> = g
        .objects_for_subject_predicate(set, &rs("resultVariable"))
        .map(|v| Variable::new_unchecked(literal(v)))
        .collect();
    let mut rows: Vec<(Option<u64>, Vec<_>)> = g
        .objects_for_subject_predicate(set, &rs("solution"))
        .map(|solution| {
            let index = g
                .object_for_subject_predicate(node(solution), &rs("index"))
                .map(|index| literal(index).parse().unwrap());
            let bindings: Vec<_> = g
                .objects_for_subject_predicate(node(solution), &rs("binding"))
                .map(|b| {
                    let get = |local| g.object_for_subject_predicate(node(b), &rs(local));
                    (literal(get("variable").unwrap()), get("value").unwrap())
                })
                .collect();
            let row = variables
                .iter()
                .map(|v| bindings.iter().find(|(name, _)| *name == v.as_str()))
                .map(|binding| binding.map(|(_, value)| value.into_owned()))
                .collect();
            (index, row)
        })
        .collect();
    rows.sort_by_key(|(index, _)| *index);
    let ordered = rows.iter().any(|(index, _)| index.is_some());
    let rows = rows.into_iter().map(|(_, row)| row).collect();
    (SparqlResults::Solutions { variables, rows }, ordered)
}

fn node(term: TermRef<'_>) -> BlankNodeRef<'_> {
    match term {
        TermRef::BlankNode(node) => node,
        _ => panic!("{term} is not a blank node"),
    }
}

fn literal(term: TermRef<'_>) -> &str {
    match term {
        TermRef::Literal(literal) => literal.value(),
        _ => panic!("{term} is not a literal"),
    }
}

fn compare(
    expected: SparqlResults,
    actual: SparqlResults,
    lax: bool,
    ordered: bool,
) -> Result<(), Box<dyn Error>> {
    let (expected, actual) = (
        canonical(expected, lax, ordered),
        canonical(actual, lax, ordered),
    );
    if expected == actual {
        return Ok(());
    }
    Err(format!("expected\n{expected}\ngot\n{actual}").into())
}

// each solution is its own blank node, or canonicalization would permute alike solutions
fn canonical(results: SparqlResults, lax: bool, ordered: bool) -> Graph {
    let set = iri(RS, "ResultSet");
    let triples = match results {
        SparqlResults::Graph(triples) => triples,
        SparqlResults::Boolean(value) => {
            vec![Triple::new(set, iri(RS, "boolean"), Literal::from(value))]
        }
        SparqlResults::Solutions {
            variables,
            mut rows,
        } => {
            if lax {
                let mut seen = HashSet::new();
                rows.retain(|row| seen.insert(row.clone()));
            }
            let mut triples: Vec<_> = variables
                .iter()
                .map(|v| {
                    let name = Literal::new_simple_literal(v.as_str());
                    Triple::new(set.clone(), iri(RS, "resultVariable"), name)
                })
                .collect();
            for (position, row) in rows.into_iter().enumerate() {
                let solution = BlankNode::default();
                triples.push(Triple::new(
                    solution.clone(),
                    rdf::TYPE,
                    iri(RS, "Solution"),
                ));
                if ordered {
                    let index = Literal::from(position as u64);
                    triples.push(Triple::new(solution.clone(), iri(RS, "index"), index));
                }
                for (variable, value) in variables.iter().zip(row) {
                    if let Some(value) = value {
                        let predicate = iri(RS, &format!("value/{}", variable.as_str()));
                        triples.push(Triple::new(solution.clone(), predicate, value));
                    }
                }
            }
            triples
        }
    };
    let mut graph: Graph = triples.into_iter().collect();
    graph.canonicalize(CanonicalizationAlgorithm::Unstable);
    graph
}
