//! SHACL validation of graph views.
use super::on_small_stack;
use crate::{
    shacl::{ShaclLimit, ShaclOptions, ShaclReport, ShaclShapes},
    RdfError, RdfFormat, RdfGraph, Triple,
};
use oxigraph::{
    io::RdfParser,
    model::{vocab::rdf, Term},
};
use raphtory::prelude::*;
use rudof_rdf::{rdf_core::vocabs::ShaclVocab, rdf_impl::OxigraphInMemory};
use shacl::validator::report::ValidationReport;
use std::{
    io, thread,
    time::{Duration, Instant},
};

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
    ShaclShapes::parse(
        ttl.as_bytes(),
        RdfFormat::Turtle,
        None,
        &ShaclOptions::default(),
    )
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

// at 5 alice loses her name, bob gets a negative age and knows acme; at 7 acme's name is too short
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

#[test]
fn deep_and_long_shapes_run_on_a_small_stack() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .";
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> \"v\" .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let n = 10_000;
    let named: String = (0..n)
        .map(|i| format!("ex:S{i} sh:node ex:S{} . ", i + 1))
        .collect();
    let shapes = [
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; {}sh:path ex:p{} .",
            "sh:property [ ".repeat(n),
            " ]".repeat(n)
        ),
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; {}sh:datatype xsd:integer{} .",
            "sh:not [ ".repeat(n),
            " ]".repeat(n)
        ),
        format!("{prefix} ex:S0 sh:targetNode ex:a . {named}"),
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:in ( {}) ] .",
            "1 ".repeat(n)
        ),
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:or ( {}) ] .",
            "[ sh:datatype xsd:string ] ".repeat(n)
        ),
        format!(
            "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ( {}) ; sh:maxCount 1 ] .",
            "ex:p ".repeat(n)
        ),
    ];
    // parsed, validated and reported on a stack far too small for the validator's recursion
    let conforms = on_small_stack(move || {
        shapes.map(|ttl| {
            let report = validate(&g, &ttl);
            report.to_rdf_string(RdfFormat::Turtle).unwrap();
            report.conforms()
        })
    });
    assert_eq!(conforms, [true, false, true, false, true, true]);
}

#[test]
fn nested_paths_run_on_a_small_stack() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let ttl = |n| {
        format!(
            "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S sh:targetNode ex:a ; sh:property [ sh:path {}ex:p{} ; sh:maxCount 0 ] .",
            "[ sh:zeroOrOnePath ".repeat(n),
            " ]".repeat(n)
        )
    };
    // rudof nests paths up to 64 deep
    let (n, deepest, deeper) = (64, ttl(64), ttl(65));
    on_small_stack(move || {
        let report = validate(&g, &deepest);
        let results = report.results();
        assert_eq!(results.len(), 1);
        let path = results[0].path.as_ref().unwrap();
        assert_eq!(path.matches('?').count(), n, "{path}");
        report.to_rdf_string(RdfFormat::Turtle).unwrap();
        let options = ShaclOptions::default();
        let result = ShaclShapes::parse(deeper.as_bytes(), RdfFormat::Turtle, None, &options);
        assert!(matches!(result, Err(RdfError::Shacl(_))), "{result:?}");
    });
}

#[test]
fn paths_rudof_would_expand_without_end_are_errors() {
    let parse = |shapes: &str| {
        let ttl = format!(
            "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            ex:S sh:targetNode ex:a ; sh:property [ sh:path _:p ; sh:minCount 0 ] . {shapes}"
        );
        ShaclShapes::parse(
            ttl.as_bytes(),
            RdfFormat::Turtle,
            None,
            &ShaclOptions::default(),
        )
        .map(drop)
    };
    // each level is the sequence of the one below twice, 2^30 steps in all
    let shared: String = (1..=30)
        .map(|i| format!("_:l{i} rdf:first _:l{0} ; rdf:rest ( _:l{0} ) . ", i - 1))
        .collect();
    let exploding = format!("_:l0 sh:inversePath ex:q . {shared} _:p sh:inversePath _:l30 .");
    // each aborted the process
    let failing = [
        "_:p sh:zeroOrMorePath _:p .",
        "_:p sh:inversePath [ sh:zeroOrOnePath _:p ] .",
        "_:p rdf:first ex:q ; rdf:rest ( _:p ) .",
        "_:p sh:alternativePath ex:l . ex:l rdf:first _:p ; rdf:rest rdf:nil .",
        "ex:T sh:property [ sh:path ex:a ; sh:subsetOf _:c ] . _:c sh:zeroOrMorePath _:c .",
        "ex:T sh:deactivated [ <http://www.w3.org/ns/shacl-node-expr#pathValues> _:c ] .
        _:c sh:zeroOrMorePath _:c .",
        &exploding,
    ]
    .map(str::to_owned);
    on_small_stack(move || {
        for shapes in failing {
            let result = parse(&shapes);
            assert!(matches!(result, Err(RdfError::Shacl(_))), "{result:?}");
        }
        // an IRI in a path is a predicate, and shapes may refer to themselves
        let ok = "_:p sh:zeroOrMorePath ex:p . ex:p sh:zeroOrMorePath ex:p . ex:S sh:node ex:S .";
        assert!(parse(ok).is_ok());
    });
}

#[test]
fn recursive_shapes_follow_a_data_chain() {
    let g = RdfGraph::new();
    let chain: String = (0..100_000)
        .map(|i| {
            format!(
                "<http://ex/n{i}> <http://ex/next> <http://ex/n{}> .\n",
                i + 1
            )
        })
        .collect();
    g.load_rdf(1, chain.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let ttl = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:n0 ; sh:property [ sh:path ex:next ; sh:node ex:S ] .";
    assert!(on_small_stack(move || validate(&g, ttl).conforms()));
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

// a lookup with no bound term is an error (`triples_matching`), which fails the validation
#[test]
fn validation_never_scans_the_whole_graph() {
    let ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        ex:Person a sh:NodeShape , rdfs:Class ; sh:property [ sh:path ex:name ; sh:minCount 1 ] .
        ex:S sh:targetClass ex:Person ; sh:targetSubjectsOf ex:knows ; sh:targetObjectsOf ex:knows ;
            sh:targetNode ex:acme ; sh:closed true ; sh:ignoredProperties ( ex:knows ex:age ex:worksFor ) ;
            sh:property [ sh:path ex:name ; sh:maxCount 1 ] ;
            sh:property [ sh:path [ sh:zeroOrMorePath ex:knows ] ; sh:class ex:Person ] ."#;
    let report = temporal_graph()
        .snapshot_at(3)
        .validate_shacl(&shapes(ttl), &ShaclOptions::default())
        .unwrap();
    let focus: Vec<String> = report.results().into_iter().map(|r| r.focus_node).collect();
    // closed ex:S does not allow the people's `rdf:type`, and acme is not a person
    assert_eq!(
        focus,
        ["http://ex/acme", "http://ex/alice", "http://ex/bob"]
    );
}

// rudof reads shape literals as values, which must not lose their lexical form ("01")
#[test]
fn shape_literals_find_stored_literals() {
    for datatype in ["integer", "long"] {
        let g = RdfGraph::new();
        let doc = format!(
            r#"<http://ex/a> <http://ex/n> "01"^^<http://www.w3.org/2001/XMLSchema#{datatype}> ."#
        );
        g.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
        let ttl = format!(
            r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S sh:targetNode "01"^^<http://www.w3.org/2001/XMLSchema#{datatype}> ;
                sh:property [ sh:path [ sh:inversePath ex:n ] ; sh:minCount 1 ] ."#
        );
        let report = validate(&g, &ttl);
        assert!(report.conforms(), "{:?}", report.results());
    }
}

#[test]
fn node_expressions_that_read_every_triple_are_errors() {
    let ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix shnex: <http://www.w3.org/ns/shacl-node-expr#> .
        ex:S sh:targetNode ex:alice ; sh:deactivated [ shnex:nodesMatching ex:S ] ."#;
    let (g, shapes) = (temporal_graph(), shapes(ttl));
    for reject in [false, true] {
        let options = ShaclOptions::default().with_reject_recursive_shapes(reject);
        let result = g.validate_shacl(&shapes, &options);
        assert!(matches!(result, Err(RdfError::Shacl(_))), "{result:?}");
    }
}

#[test]
fn cyclic_node_expressions_are_errors() {
    // aborted the process
    let ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix shnex: <http://www.w3.org/ns/shacl-node-expr#> .
        ex:S sh:targetNode ex:alice ; sh:deactivated _:e . _:e shnex:exists _:e ."#;
    let g = temporal_graph();
    let result = on_small_stack(move || {
        ShaclShapes::parse(
            ttl.as_bytes(),
            RdfFormat::Turtle,
            None,
            &ShaclOptions::default(),
        )
        .and_then(|shapes| g.validate_shacl(&shapes, &ShaclOptions::default()))
        .map(|report| report.conforms())
    });
    assert!(matches!(result, Err(RdfError::Shacl(_))), "{result:?}");
}

#[test]
fn explicit_limits_bound_shapes_and_results() {
    let options = ShaclOptions::default().with_max_shapes_length(SHAPES.len());
    let parse = |data: &[u8]| ShaclShapes::parse(data, RdfFormat::Turtle, None, &options);
    assert!(parse(SHAPES.as_bytes()).is_ok());
    let too_long = |result: Result<ShaclShapes, RdfError>| {
        matches!(
            result,
            Err(RdfError::ShaclLimit(ShaclLimit::ShapesTooLong { max })) if max == SHAPES.len()
        )
    };
    assert!(too_long(parse(format!("{SHAPES} ").as_bytes())));
    // the read stops past the limit
    let endless = ShaclShapes::parse(io::repeat(b'x'), RdfFormat::Turtle, None, &options);
    assert!(too_long(endless));

    let g = RdfGraph::new();
    let triples: String = (0..10)
        .map(|i| format!("<http://ex/n{i}> <http://ex/p> \"x\" .\n"))
        .collect();
    g.load_rdf(1, triples.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    // two shapes with 10 results each: together over 15, though each is under it
    let integers = shapes(
        "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:S sh:targetSubjectsOf ex:p ; sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
        ex:T sh:targetSubjectsOf ex:p ; sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .",
    );
    let max_results = |max: usize| ShaclOptions::default().with_max_results(max);
    let result = g.validate_shacl(&integers, &max_results(15));
    assert!(
        matches!(
            result,
            Err(RdfError::ShaclLimit(ShaclLimit::TooManyResults { max: 15 }))
        ),
        "{result:?}"
    );
    let all = g.validate_shacl(&integers, &max_results(20)).unwrap();
    assert_eq!(all.results().len(), 20);
}

#[test]
fn validations_past_their_timeout_fail_quickly() {
    let g = RdfGraph::new();
    let chain: String = (0..5_000)
        .map(|i| {
            format!(
                "<http://ex/n{i}> <http://ex/next> <http://ex/n{}> .\n",
                i + 1
            )
        })
        .collect();
    g.load_rdf(1, chain.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    // the closure from each of 5,000 focus nodes: about 10 s in release without the timeout
    let closure = shapes(
        "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetSubjectsOf ex:next ;
            sh:property [ sh:path [ sh:zeroOrMorePath ex:next ] ; sh:minCount 1 ] .",
    );
    let timeout = Duration::from_millis(50);
    let start = Instant::now();
    match g.validate_shacl(&closure, &ShaclOptions::default().with_timeout(timeout)) {
        Err(RdfError::ShaclLimit(ShaclLimit::TimedOut { timeout: t })) => assert_eq!(t, timeout),
        other => panic!("{other:?}"),
    }
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
}

// rudof drops the errors of target lookups, so with no focus nodes left the run would conform
#[test]
fn validations_past_their_timeout_never_conform() {
    let options = ShaclOptions::default().with_timeout(Duration::ZERO);
    let result = temporal_graph().validate_shacl(&shapes(SHAPES), &options);
    assert!(
        matches!(
            result,
            Err(RdfError::ShaclLimit(ShaclLimit::TimedOut { .. }))
        ),
        "{result:?}"
    );
}

#[test]
fn triple_terms_in_shapes_are_errors() {
    // as deep as the server's default shapes limit allows, on the stack of its compute pool
    let max = 100 << 10;
    let nested = |n| format!("<x:s> <x:p> {}[]{} .", "<<([]a".repeat(n), ")>>".repeat(n));
    let deep = nested((max - nested(0).len()) / 9);
    thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || {
            let options = ShaclOptions::default().with_max_shapes_length(max);
            for ttl in ["<x:s> <x:p> <<( <x:a> <x:b> <x:c> )>> .", deep.as_str()] {
                let result = ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None, &options);
                assert!(matches!(result, Err(RdfError::Rdf12Term(_))), "{result:?}");
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
