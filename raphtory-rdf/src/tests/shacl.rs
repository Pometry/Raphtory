//! SHACL validation of graph views.
use crate::{
    shacl::{
        limits::{run_guarded, MAX_LIST_LENGTH, MAX_NESTING},
        ShaclLimit, ShaclOptions, ShaclReport, ShaclShapes,
    },
    RdfError, RdfFormat, RdfGraph, Triple,
};
use oxigraph::{
    io::RdfParser,
    model::{vocab::rdf, Term},
};
use raphtory::prelude::*;
use rudof_rdf::{rdf_core::vocabs::ShaclVocab, rdf_impl::OxigraphInMemory};
use shacl::validator::report::ValidationReport;
use std::error::Error;

const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .
ex:PersonShape a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:property [ sh:path ex:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string ] ;
  sh:property [ sh:path ex:age ; sh:maxCount 1 ; sh:datatype xsd:integer ; sh:minInclusive 0 ] ;
  sh:property [ sh:path ex:knows ; sh:class ex:Person ] ;
  sh:property [ sh:path ( ex:worksFor ex:name ) ; sh:minLength 2 ] .
"#;

fn shapes(ttl: &str) -> ShaclShapes {
    ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None)
        .unwrap_or_else(|error| panic!("{error}\n{ttl}"))
}

fn validate(rg: &RdfGraph, ttl: &str) -> ShaclReport {
    rg.validate_shacl(&shapes(ttl), &ShaclOptions::default())
        .unwrap()
}

// (focus node, constraint component without the SHACL namespace, path, value)
fn results(report: &ShaclReport) -> Vec<(String, String, Option<String>, Option<String>)> {
    report
        .results()
        .into_iter()
        .map(|result| {
            (
                result.focus_node,
                result.constraint_component.replace(ShaclVocab::SH, ""),
                result.path,
                result.value,
            )
        })
        .collect()
}

fn result(
    focus: &str,
    component: &str,
    path: &str,
    value: Option<&str>,
) -> (String, String, Option<String>, Option<String>) {
    (
        focus.to_owned(),
        component.to_owned(),
        Some(path.to_owned()),
        value.map(str::to_owned),
    )
}

// alice and bob are people, alice knows bob and works for acme. At 5 alice loses her name, bob
// gets a negative age and knows acme (not a person); at 7 acme's name becomes "A" (too short).
fn temporal_graph() -> RdfGraph {
    let rg = RdfGraph::new();
    let doc = r#"
        @prefix ex: <http://ex/> .
        ex:alice a ex:Person ; ex:name "Alice" ; ex:age 30 ; ex:knows ex:bob ; ex:worksFor ex:acme .
        ex:bob a ex:Person ; ex:name "Bob" .
        ex:acme ex:name "ACME" .
    "#;
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let nt = |t: i64, doc: &str, retract: bool| {
        if retract {
            rg.retract_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
        } else {
            rg.load_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
        }
        .unwrap()
    };
    nt(5, "<http://ex/alice> <http://ex/name> \"Alice\" .", true);
    nt(
        5,
        "<http://ex/bob> <http://ex/age> -3 . <http://ex/bob> <http://ex/knows> <http://ex/acme> .",
        false,
    );
    nt(7, "<http://ex/acme> <http://ex/name> \"ACME\" .", true);
    nt(7, "<http://ex/acme> <http://ex/name> \"A\" .", false);
    rg
}

#[test]
fn temporal_snapshots() {
    let pg = temporal_graph();
    let alice_name = result(
        "http://ex/alice",
        "MinCountConstraintComponent",
        "http://ex/name",
        None,
    );
    let alice_employer = result(
        "http://ex/alice",
        "MinLengthConstraintComponent",
        "(http://ex/worksFor / http://ex/name)",
        Some("\"A\""),
    );
    let bob_knows = result(
        "http://ex/bob",
        "ClassConstraintComponent",
        "http://ex/knows",
        Some("http://ex/acme"),
    );
    let bob_age = result(
        "http://ex/bob",
        "MinInclusiveConstraintComponent",
        "http://ex/age",
        Some("\"-3\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    );
    let expected = [
        (3, vec![]),
        (
            6,
            vec![alice_name.clone(), bob_knows.clone(), bob_age.clone()],
        ),
        (8, vec![alice_name, alice_employer, bob_knows, bob_age]),
    ];
    for (t, expected) in expected {
        let report = pg
            .snapshot_at(t)
            .validate_shacl(&shapes(SHAPES), &ShaclOptions::default())
            .unwrap();
        assert_eq!(results(&report), expected, "as of {t}");
        assert_eq!(report.conforms(), expected.is_empty());
    }
}

#[test]
fn plain_graphs() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Person", NO_PROPS, Some(rdf::TYPE.as_str()))
        .unwrap();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    // a generalized triple: a literal subject
    g.add_edge(1, "\"lit\"", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    let ttl = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix raphtory: <raphtory:> .
        <http://ex/Known> sh:targetObjectsOf raphtory:knows ;
            sh:property [ sh:path [ sh:inversePath raphtory:knows ] ; sh:maxCount 1 ] .
        <http://ex/Closed> sh:targetClass raphtory:Person ; sh:closed true ;
            sh:ignoredProperties ( <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ) ;
            sh:property [ sh:path raphtory:knows ; sh:minCount 2 ] .
    "#;
    let report = validate(&RdfGraph::from(g), ttl);
    assert_eq!(
        results(&report),
        [result(
            "Alice",
            "MinCountConstraintComponent",
            "knows",
            None
        )]
    );
}

#[test]
fn written_reports_keep_data_blank_nodes_apart() {
    let g = Graph::new();
    g.add_edge(1, "_:1", "http://ex/o", NO_PROPS, Some("http://ex/p"))
        .unwrap();
    let ttl = "@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetSubjectsOf <http://ex/p> ;
            sh:property [ sh:path <http://ex/q> ; sh:minCount 1 ] .";
    let report = validate(&RdfGraph::from(g), ttl)
        .to_rdf_string(RdfFormat::NTriples)
        .unwrap();
    assert!(report.contains("#focusNode> _:1 ."), "{report}");
    assert!(
        !report.lines().any(|line| line.starts_with("_:1 ")),
        "{report}"
    );
}

#[test]
#[cfg(panic = "unwind")]
fn validator_panics_are_errors() {
    assert!(matches!(
        run_guarded::<()>("the SHACL validator", || panic!("boom")),
        Err(RdfError::Panicked(_))
    ));
    assert!(!validate(&temporal_graph(), SHAPES).conforms());
}

#[test]
fn recursive_shapes_can_be_rejected() {
    let g = temporal_graph();
    let recursive = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:alice ; sh:property [ sh:path ex:knows ; sh:node ex:S ] ."#,
    );
    // sh:class of a shape that is also a class is not recursion
    let class = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:Person a sh:NodeShape , <http://www.w3.org/2000/01/rdf-schema#Class> ;
            sh:property [ sh:path ex:knows ; sh:class ex:Person ] ."#,
    );
    let reject = ShaclOptions::default().with_reject_recursive_shapes(true);
    assert!(g
        .validate_shacl(&recursive, &ShaclOptions::default())
        .is_ok());
    assert!(matches!(
        g.validate_shacl(&recursive, &reject),
        Err(RdfError::ShaclLimit(ShaclLimit::RecursiveShapes))
    ));
    assert!(g.validate_shacl(&class, &reject).is_ok());
}

fn assert_rejected(ttl: &str) {
    let error = ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None).unwrap_err();
    assert!(matches!(error, RdfError::ShaclLimit(_)), "{error:?}");
    // the message holds the cause, so an error chain does not repeat it
    assert!(error.source().is_none(), "{error}");
}

#[test]
fn limits_of_the_validator() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/v1> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let in_list = |n: usize| {
        let values: String = (0..n).map(|i| format!("<http://ex/v{i}> ")).collect();
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:in ( {values}) ] ."
        )
    };
    assert!(validate(&g, &in_list(MAX_LIST_LENGTH)).conforms());
    assert_rejected(&in_list(100_000));

    // nested paths: the deepest nesting, in results too (written and dropped on this thread)
    let nested = |n: usize| {
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path {}ex:p{} ; sh:maxCount 0 ] .",
            "[ sh:zeroOrOnePath ".repeat(n),
            " ]".repeat(n)
        )
    };
    // (ex:S is one deeper than its property shape, as deep as its path)
    let report = validate(&g, &nested(MAX_NESTING - 1));
    let results = report.results();
    assert_eq!(results.len(), 1);
    let path = results[0].path.as_ref().unwrap();
    assert_eq!(path.matches('?').count(), MAX_NESTING - 1, "{path}");
    let mut out = Vec::new();
    report.write(&mut out, RdfFormat::Turtle).unwrap();
    drop(report);
    assert_rejected(&nested(100_000));
    // nested property shapes count too
    let properties = |n: usize| {
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; {}sh:path ex:p{} .",
            "sh:property [ ".repeat(n),
            " ]".repeat(n)
        )
    };
    assert!(validate(&g, &properties(MAX_NESTING)).conforms());
    assert_rejected(&properties(100_000));
    // and named shapes
    let named = |n: usize| {
        let chain: String = (0..n)
            .map(|i| format!("ex:S{i} sh:node ex:S{} . ", i + 1))
            .collect();
        format!("{prefix} ex:S0 sh:targetNode ex:a . {chain}")
    };
    assert!(validate(&g, &named(MAX_NESTING)).conforms());
    assert_rejected(&named(MAX_NESTING + 1));

    // a duplicated triple, a sequence and shared sub-paths
    let duplicated: String = (0..=MAX_NESTING)
        .map(|i| format!("_:p{i} sh:zeroOrMorePath _:p{j} , _:p{j} . ", j = i + 1))
        .collect();
    assert_rejected(&format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path _:p0 ; sh:maxCount 1 ] . {duplicated}"
    ));
    let sequence = "ex:p ".repeat(MAX_NESTING + 1);
    assert_rejected(&format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ( {sequence}) ; sh:maxCount 1 ] ."
    ));
    let shared: String = (0..40)
        .map(|i| {
            format!(
                "_:p{i} rdf:first _:p{j} ; rdf:rest ( _:p{j} ) . ",
                j = i + 1
            )
        })
        .collect();
    assert_rejected(&format!(
        "{prefix} @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:S sh:targetNode ex:a ; sh:property [ sh:path _:p0 ; sh:maxCount 1 ] . {shared}"
    ));

    // cycles that are not recursive shapes
    assert_rejected(&format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:in _:l ] .
        _:l <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ex:v1 ;
            <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:m .
        _:m <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ex:v2 ;
            <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:l ."
    ));
    assert_rejected(&format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path _:p ; sh:minCount 1 ] .
        _:p sh:zeroOrMorePath _:p ."
    ));
    // what the validator does not read is not checked
    let unused = format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:maxCount 1 ] .
        _:p sh:zeroOrMorePath _:p ."
    );
    assert!(validate(&g, &unused).conforms());
}

#[test]
fn paths() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        "@prefix ex: <http://ex/> . ex:a ex:p ex:b . ex:b ex:q ex:c . ex:c ex:p ex:d .".as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    let ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:a ;
          sh:property [ sh:path ex:p ; sh:maxCount 0 ] ;
          sh:property [ sh:path ( ex:p ex:q ) ; sh:maxCount 0 ] ;
          sh:property [ sh:path [ sh:alternativePath ( ex:p ex:q ) ] ; sh:maxCount 0 ] ;
          sh:property [ sh:path [ sh:zeroOrMorePath ex:p ] ; sh:maxCount 0 ] ;
          sh:property [ sh:path [ sh:oneOrMorePath [ sh:alternativePath ( ex:p ex:q ) ] ] ; sh:maxCount 0 ] ;
          sh:property [ sh:path [ sh:zeroOrOnePath ( ex:p ex:q ) ] ; sh:maxCount 0 ] .
        ex:T sh:targetNode ex:b ; sh:property [ sh:path [ sh:inversePath ex:p ] ; sh:maxCount 0 ;
            sh:severity sh:Info ; sh:message "none allowed" ] ."#;
    let report = validate(&g, ttl);
    let results = report.results();
    let mut paths: Vec<&str> = results
        .iter()
        .map(|result| result.path.as_deref().unwrap())
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        [
            "((http://ex/p / http://ex/q))?",
            "((http://ex/p | http://ex/q))+",
            "(http://ex/p / http://ex/q)",
            "(http://ex/p | http://ex/q)",
            "(http://ex/p)*",
            "^(http://ex/p)",
            "http://ex/p",
        ]
    );
    let info = results
        .iter()
        .find(|result| result.focus_node == "http://ex/b")
        .unwrap();
    assert_eq!(info.severity, ShaclVocab::SH_INFO);
    assert_eq!(info.messages, ["none allowed"]);

    let mut out = Vec::new();
    let written = report.write(&mut out, RdfFormat::Turtle).unwrap();
    let mut store = OxigraphInMemory::new();
    let mut root = None;
    for quad in RdfParser::from_format(RdfFormat::Turtle.to_oxigraph()).for_slice(&out) {
        let triple = Triple::from(quad.unwrap());
        if matches!(&triple.object, Term::NamedNode(class) if class.as_str() == ShaclVocab::SH_VALIDATION_REPORT)
        {
            root = Some(Term::from(triple.subject.clone()));
        }
        store
            .add_triple_ref(&triple.subject, &triple.predicate, &triple.object)
            .unwrap();
    }
    assert_eq!(store.len(), written);
    assert_eq!(
        ValidationReport::parse(&mut store, root.unwrap()).unwrap(),
        report.0
    );
}
