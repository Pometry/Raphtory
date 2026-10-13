//! SHACL validation of graph views.
use super::{
    layer_of,
    shacl_report::{actual, expected, key, multiset, Key},
};
use crate::{
    name_of,
    shacl::{
        run_guarded, Proc, ShaclLimit, ShaclOptions, ShaclPath, ShaclReport, ShaclShapes,
        ShaclWarning, MAX_LIST_LENGTH, MAX_NESTING,
    },
    Literal, NamedNode, RdfError, RdfFormat, RdfGraph, RdfView, Term, Triple,
};
use oxigraph::{
    io::RdfParser,
    model::{
        vocab::{rdf, xsd},
        Graph as OxGraph, NamedNodeRef,
    },
};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{vocabs::ShaclVocab, RDFFormat},
    rdf_impl::{OxigraphInMemory, ReaderMode},
};
use rustc_hash::FxHashMap;
use shacl::{
    ir::IRSchema,
    validator::{processor::ShaclProcessor, ShaclConfig, ShaclValidationMode},
};
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

fn shapes_error(ttl: &str) -> RdfError {
    match ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None) {
        Err(error) => error,
        other => panic!("{ttl}: {other:?}"),
    }
}

/// A result as most tests compare them: focus node, constraint component, path and value.
type Summary = (Term, NamedNode, Option<ShaclPath>, Option<Term>);

fn summarise((focus, component, _, path, value, _): Key) -> Summary {
    (focus, component, path, value)
}

/// The results of a report, in order.
fn results(report: &ShaclReport) -> Vec<Summary> {
    report.results.iter().map(key).map(summarise).collect()
}

/// A result with the constraint component `component`, a [`ShaclVocab`] IRI.
fn result(
    focus: impl Into<Term>,
    component: &str,
    path: Option<ShaclPath>,
    value: Option<Term>,
) -> Summary {
    (
        focus.into(),
        NamedNode::new_unchecked(component),
        path,
        value,
    )
}

fn ex(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("http://ex/{local}"))
}

fn predicate(local: &str) -> Option<ShaclPath> {
    Some(ShaclPath::Predicate(ex(local)))
}

fn typed(value: &str, datatype: NamedNodeRef<'_>) -> Term {
    Literal::new_typed_literal(value, datatype).into()
}

/// The reference: export the view and validate it in rudof's own in-memory graph, with shapes
/// compiled by rudof from Turtle.
fn validate_export<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    shapes_ttl: &str,
) -> (bool, FxHashMap<Summary, usize>) {
    let mut document = Vec::new();
    view.to_rdf(&mut document, RdfFormat::NTriples).unwrap();
    let store = OxigraphInMemory::from_reader(
        &mut document.as_slice(),
        "raphtory",
        &RDFFormat::NTriples,
        None,
        &ReaderMode::Strict,
    )
    .unwrap();
    let schema =
        IRSchema::from_str(shapes_ttl, &RDFFormat::Turtle, None, &ReaderMode::Strict).unwrap();
    let report = Proc(store)
        .validate(
            &schema,
            &ShaclValidationMode::Native,
            &ShaclConfig::default(),
        )
        .unwrap();
    let keys = report.results().iter().map(|result| -> Key {
        (
            result.focus_node().clone().into(),
            NamedNode::try_from(Term::from(result.constraint_component().clone())).unwrap(),
            IriS::from(result.severity()).into(),
            result.path().map(ShaclPath::from),
            result.value().cloned().map(Into::into),
            result.source().cloned().map(Into::into),
        )
    });
    (report.conforms(), multiset(keys.map(summarise)))
}

/// Checks that validating `view` gives the results of the export reference, and returns them.
fn validate_checked<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    shapes_ttl: &str,
) -> ShaclReport {
    let report = view
        .validate_shacl(&shapes(shapes_ttl), &ShaclOptions::default())
        .unwrap();
    assert_eq!(
        (report.conforms, multiset(results(&report))),
        validate_export(view, shapes_ttl),
        "the view and its export validate differently"
    );
    report
}

/// alice and bob are people, alice knows bob and works for acme. At 5 alice loses her name, bob
/// gets a negative age and knows acme (not a person); at 7 acme's name becomes "A" (too short).
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
        ex("alice"),
        ShaclVocab::SH_MIN_COUNT_CONSTRAINT_COMPONENT,
        predicate("name"),
        None,
    );
    let alice_employer = result(
        ex("alice"),
        ShaclVocab::SH_MIN_LENGTH_CONSTRAINT_COMPONENT,
        Some(ShaclPath::Sequence(vec![
            ShaclPath::Predicate(ex("worksFor")),
            ShaclPath::Predicate(ex("name")),
        ])),
        Some(Literal::new_simple_literal("A").into()),
    );
    let bob_knows = result(
        ex("bob"),
        ShaclVocab::SH_CLASS_CONSTRAINT_COMPONENT,
        predicate("knows"),
        Some(ex("acme").into()),
    );
    let bob_age = result(
        ex("bob"),
        ShaclVocab::SH_MIN_INCLUSIVE_CONSTRAINT_COMPONENT,
        predicate("age"),
        Some(typed("-3", xsd::INTEGER)),
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
        let report = validate_checked(&pg.snapshot_at(t), SHAPES);
        assert_eq!(results(&report), expected, "as of {t}");
        assert_eq!(report.conforms, expected.is_empty());
        assert!(report.warnings.is_empty());
    }
}

/// Graphs not loaded from RDF: names are `raphtory:` IRIs, and generalized triples are left out.
#[test]
fn plain_graphs() {
    let g = Graph::new();
    g.add_edge(
        1,
        "Alice",
        "Person",
        NO_PROPS,
        Some(layer_of(rdf::TYPE.as_str()).as_str()),
    )
    .unwrap();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    // generalized triples: a literal subject, a layer named by a literal
    g.add_edge(1, "\"lit\"", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("\"notiri\""))
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
    let report = validate_checked(&RdfGraph::from(g), ttl);
    assert_eq!(
        results(&report),
        [result(
            NamedNode::new_unchecked("raphtory:Alice"),
            ShaclVocab::SH_MIN_COUNT_CONSTRAINT_COMPONENT,
            Some(ShaclPath::Predicate(NamedNode::new_unchecked(
                "raphtory:knows"
            ))),
            None,
        )]
    );
}

const SUBCLASSES: &str = r#"
    @prefix ex: <http://ex/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    ex:Student rdfs:subClassOf ex:Person . ex:PhD rdfs:subClassOf ex:Student .
    ex:s1 a ex:Student . ex:s2 a ex:PhD . ex:p1 a ex:Person .
    ex:x ex:knows ex:s1 , ex:s2 , ex:p1 .
"#;

fn subclass_graph() -> RdfGraph {
    let g = RdfGraph::new();
    g.load_rdf(1, SUBCLASSES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    g
}

const TARGET_CLASS: &str = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
    ex:T a sh:NodeShape ; sh:targetClass ex:Person ; sh:property [ sh:path ex:missing ; sh:minCount 1 ] ."#;
const CLASS: &str = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
    ex:C a sh:NodeShape ; sh:targetNode ex:x ; sh:property [ sh:path ex:knows ; sh:class ex:Person ] ."#;
const IMPLICIT: &str = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
    ex:Person a <http://www.w3.org/2000/01/rdf-schema#Class>, sh:NodeShape ;
        sh:property [ sh:path ex:missing ; sh:minCount 1 ] ."#;

/// Shapes that use classes, on data with subclass triples, get a warning: the validator follows
/// `rdfs:subClassOf` only partly.
#[test]
fn subclass_reasoning_is_partial() {
    let g = subclass_graph();
    for shapes_ttl in [TARGET_CLASS, CLASS, IMPLICIT] {
        let report = validate_checked(&g, shapes_ttl);
        assert_eq!(
            report.warnings,
            [ShaclWarning::PartialSubclassReasoning],
            "{shapes_ttl}"
        );
    }
    // no warning without subclass triples (also when they are retracted)...
    g.retract_rdf(2, SUBCLASSES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    assert!(g
        .valid()
        .validate_shacl(&shapes(CLASS), &ShaclOptions::default())
        .unwrap()
        .warnings
        .is_empty());
    assert!(!g
        .snapshot_at(1)
        .validate_shacl(&shapes(CLASS), &ShaclOptions::default())
        .unwrap()
        .warnings
        .is_empty());
    // ... or when the shapes use no classes
    let s = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetNode <http://ex/x> ; sh:property [ sh:path <http://ex/knows> ; sh:minCount 1 ] ."#,
    );
    let report = subclass_graph()
        .validate_shacl(&s, &ShaclOptions::default())
        .unwrap();
    assert!(report.conforms && report.warnings.is_empty());
}

/// Result values are the literals as written in the graph, where that is unambiguous.
#[test]
fn literal_values() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        r#"
        @prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "042"^^xsd:long , "05"^^xsd:short , "1"^^xsd:boolean ,
            "2020-01-01T00:00:00.000Z"^^xsd:dateTime , "colour"@en-gb , "042"^^xsd:integer .
        # the rewritten form is a node too: it is kept
        ex:b ex:v "0"^^xsd:boolean , "false"^^xsd:boolean .
        # two literals with the same rewritten form: it is kept
        ex:c ex:v "01"^^xsd:long , "001"^^xsd:long .
        # literals of focus nodes
        ex:d ex:w "1"^^xsd:boolean .
        "#
        .as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    let in_nothing = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetNode <http://ex/a> , <http://ex/b> , <http://ex/c> ;
            sh:property [ sh:path <http://ex/v> ; sh:in ( ) ] .
        <http://ex/T> sh:targetObjectsOf <http://ex/w> ; sh:datatype <http://ex/none> ."#;
    let report = g
        .validate_shacl(&shapes(in_nothing), &ShaclOptions::default())
        .unwrap();
    let values = |focus: Term| -> Vec<Term> {
        report
            .results
            .iter()
            .filter(|result| result.focus_node == focus)
            .map(|result| result.value.clone().unwrap())
            .collect()
    };
    // every value of a is a node of the graph
    let a = values(ex("a").into());
    assert_eq!(a.len(), 6);
    for value in &a {
        let name = name_of(value.as_ref()).unwrap();
        assert!(g.graph().node(&name).is_some(), "{value}");
    }
    for written in [
        typed("05", xsd::SHORT),
        typed("042", xsd::LONG),
        Literal::new_language_tagged_literal_unchecked("colour", "en-gb").into(),
        typed("2020-01-01T00:00:00.000Z", xsd::DATE_TIME),
        typed("1", xsd::BOOLEAN),
    ] {
        assert!(a.contains(&written), "{written}: {a:?}");
    }
    assert!(values(ex("b").into())
        .iter()
        .all(|value| *value == typed("false", xsd::BOOLEAN)));
    let c = values(ex("c").into());
    assert!(!c.is_empty() && c.iter().all(|value| *value == typed("1", xsd::LONG)));
    // a focus node
    assert_eq!(values(typed("1", xsd::BOOLEAN)), [typed("1", xsd::BOOLEAN)]);
    // sh:targetNode literals are looked up as written: "1"^^xsd:boolean is ex:d's (the validator
    // looks for "true"^^xsd:boolean), and "02"^^xsd:short nobody's
    let s = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <http://ex/V> sh:targetNode "1"^^xsd:boolean , "02"^^xsd:short ;
            sh:property [ sh:path [ sh:inversePath <http://ex/w> ] ; sh:minCount 1 ] ."#,
    );
    let report = g.validate_shacl(&s, &ShaclOptions::default()).unwrap();
    assert_eq!(
        results(&report),
        [result(
            typed("02", xsd::SHORT),
            ShaclVocab::SH_MIN_COUNT_CONSTRAINT_COMPONENT,
            Some(ShaclPath::Inverse(Box::new(ShaclPath::Predicate(ex("w"))))),
            None,
        )]
    );
}

/// The literals the validator rewrites are looked up and given back in the view: data outside
/// the view does not change the report.
#[test]
fn rewritten_literals_follow_the_view() {
    let g = RdfGraph::new();
    let one = typed("1", xsd::BOOLEAN);
    g.load_rdf(
        1,
        format!("<http://ex/d> <http://ex/w> {one} .").as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let s = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <http://ex/V> sh:targetNode "1"^^xsd:boolean ;
            sh:property [ sh:path [ sh:inversePath <http://ex/w> ] ; sh:minCount 1 ] .
        <http://ex/S> sh:targetNode <http://ex/d> ; sh:property [ sh:path <http://ex/w> ; sh:in ( ) ] ."#,
    );
    let before = g
        .at(2)
        .validate_shacl(&s, &ShaclOptions::default())
        .unwrap();
    assert_eq!(
        results(&before),
        [result(
            ex("d"),
            ShaclVocab::SH_IN_CONSTRAINT_COMPONENT,
            predicate("w"),
            Some(one),
        )]
    );
    // the form the validator gives "1"^^xsd:boolean, at a later time
    let rewritten = typed("true", xsd::BOOLEAN);
    g.load_rdf(
        5,
        format!("<http://ex/e> <http://ex/z> {rewritten} .").as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    assert_eq!(
        g.at(2)
            .validate_shacl(&s, &ShaclOptions::default())
            .unwrap(),
        before
    );
}

/// Property shapes apply to literal focus nodes, which have no values.
#[test]
fn literal_focus_nodes_have_no_values() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        r#"@prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:alice ex:age 5 . ex:bob ex:age "042"^^xsd:long ."#
            .as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    let s = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetObjectsOf ex:age ; sh:property [ sh:path ex:unit ; sh:minCount 1 ] ."#,
    );
    let min_count = |focus| {
        result(
            focus,
            ShaclVocab::SH_MIN_COUNT_CONSTRAINT_COMPONENT,
            predicate("unit"),
            None,
        )
    };
    assert_eq!(
        results(&g.validate_shacl(&s, &ShaclOptions::default()).unwrap()),
        [
            min_count(typed("042", xsd::LONG)),
            min_count(typed("5", xsd::INTEGER))
        ]
    );
}

/// Features the validator would silently ignore, and shape literals it would rewrite, are
/// rejected.
#[test]
fn unsupported_features_are_rejected() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .";
    for shapes in [
        "ex:S sh:targetNode ex:a ; sh:sparql ex:C .",
        "ex:S sh:targetNode ex:c ; sh:property [ sh:path [ sh:inversePath ( ex:p ex:q ) ] ; sh:minCount 2 ] .",
        r#"ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:flag ; sh:hasValue "1"^^xsd:boolean ] ."#,
    ] {
        let error = shapes_error(&format!("{prefix} {shapes}"));
        assert!(
            matches!(error, RdfError::ShaclUnsupported { .. }),
            "{shapes}: {error:?}"
        );
    }
    // non-validating properties, prefix declarations, reports, canonical literals and unknown
    // terms are allowed (SHACL ignores unknown terms)
    shapes(&format!(
        "{prefix} ex:S a sh:NodeShape ; sh:name \"S\" ; sh:description \"d\" ; sh:order 1 ;
            sh:group ex:G ; sh:defaultValue 1 ; sh:targetNode ex:a ;
            sh:property [ sh:path ex:flag ; sh:hasValue true ] ;
            sh:declare [ sh:prefix \"ex\" ; sh:namespace \"http://ex/\" ] .
        ex:G a sh:PropertyGroup .
        [] a sh:ValidationReport ; sh:conforms true ; sh:result [ a sh:ValidationResult ;
            sh:focusNode ex:a ; sh:resultSeverity sh:Violation ; sh:sourceShape ex:S ;
            sh:sourceConstraintComponent sh:MinCountConstraintComponent ; sh:value 1 ;
            sh:resultPath ex:p ; sh:resultMessage \"m\" ] .
        ex:T sh:nodeShape ex:S ; sh:minCOunt 1 . ex:U a sh:NodeShap ."
    ));
}

/// A validator panic is reported as an error, and the next validation works.
#[test]
#[cfg(panic = "unwind")]
fn validator_panics_are_errors() {
    assert!(matches!(
        run_guarded::<()>("the SHACL validator", || panic!("boom")),
        Err(RdfError::Panicked(_))
    ));
    assert!(
        temporal_graph()
            .snapshot_at(3)
            .validate_shacl(&shapes(SHAPES), &ShaclOptions::default())
            .unwrap()
            .conforms
    );
}

/// Recursive shapes: on a data cycle, nodes whose conformance depends on themselves are reported
/// with a warning; on acyclic data the results are those of SHACL.
#[test]
fn recursive_shapes_on_cycles() {
    let recursive = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
            sh:property [ sh:path ex:name ; sh:minCount 1 ] ;
            sh:property [ sh:path ex:knows ; sh:node ex:PersonShape ] ."#;
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        r#"@prefix ex: <http://ex/> .
        ex:alice a ex:Person ; ex:name "Alice" ; ex:knows ex:bob .
        ex:bob a ex:Person ; ex:name "Bob" ; ex:knows ex:carol .
        ex:carol a ex:Person ; ex:name "Carol" ."#
            .as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    // a chain: conforms, without a warning
    let report = validate_checked(&g, recursive);
    assert!(report.conforms && report.warnings.is_empty());
    // carol knows alice: a cycle
    g.load_rdf(
        2,
        "<http://ex/carol> <http://ex/knows> <http://ex/alice> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let report = validate_checked(&g, recursive);
    assert_eq!(report.warnings, [ShaclWarning::RecursiveShapes]);
    // recursion through sh:not, sh:or lists, named property shapes and blank-node shapes
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    for shapes_ttl in [
        "ex:S sh:targetNode ex:alice ; sh:or ( ex:T [ sh:node ex:S ] ) . ex:T sh:class ex:None .",
        "ex:S sh:targetNode ex:alice ; sh:property ex:P . ex:P sh:path ex:knows ; sh:node ex:S ; sh:maxCount 0 .",
        "ex:S sh:targetNode ex:alice ; sh:node _:s . _:s sh:property [ sh:path ex:knows ; sh:node _:s ; sh:maxCount 0 ] .",
    ] {
        let report = g
            .validate_shacl(&shapes(&format!("{prefix} {shapes_ttl}")), &ShaclOptions::default())
            .unwrap();
        assert!(!report.conforms, "{shapes_ttl}");
        assert_eq!(report.warnings, [ShaclWarning::RecursiveShapes], "{shapes_ttl}");
    }
    // sh:class of a shape that is also a class is not recursion
    let s = shapes(&format!(
        "{prefix} ex:Person a sh:NodeShape , <http://www.w3.org/2000/01/rdf-schema#Class> ;
            sh:property [ sh:path ex:knows ; sh:class ex:Person ; sh:maxCount 0 ] ."
    ));
    let report = g.validate_shacl(&s, &ShaclOptions::default()).unwrap();
    assert!(!report.conforms && report.warnings.is_empty());

    // a validation can reject recursive shapes, and only those
    let reject = ShaclOptions::default().with_reject_recursive_shapes(true);
    assert!(matches!(
        g.validate_shacl(&shapes(recursive), &reject),
        Err(RdfError::ShaclLimit(ShaclLimit::RecursiveShapes))
    ));
    assert!(g.validate_shacl(&s, &reject).is_ok());
}

fn assert_rejected(ttl: &str) {
    let error = shapes_error(ttl);
    assert!(matches!(error, RdfError::ShaclLimit(_)), "{error:?}");
    // the message holds the cause, so an error chain does not repeat it
    assert!(error.source().is_none(), "{error}");
}

/// Shapes with lists or blank-node nesting deeper than the limits are rejected.
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
    assert!(
        g.validate_shacl(&shapes(&in_list(MAX_LIST_LENGTH)), &ShaclOptions::default())
            .unwrap()
            .conforms
    );
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
    let report = g
        .validate_shacl(&shapes(&nested(MAX_NESTING - 1)), &ShaclOptions::default())
        .unwrap();
    assert_eq!(report.results.len(), 1);
    let path = report.results[0].path.as_ref().unwrap().to_string();
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
    assert!(
        g.validate_shacl(&shapes(&properties(MAX_NESTING)), &ShaclOptions::default())
            .unwrap()
            .conforms
    );
    assert_rejected(&properties(100_000));
    // and named shapes
    let named = |n: usize| {
        let chain: String = (0..n)
            .map(|i| format!("ex:S{i} sh:node ex:S{} . ", i + 1))
            .collect();
        format!("{prefix} ex:S0 sh:targetNode ex:a . {chain}")
    };
    assert!(
        g.validate_shacl(&shapes(&named(MAX_NESTING)), &ShaclOptions::default())
            .unwrap()
            .conforms
    );
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
        _:p sh:zeroOrMorePath _:p . _:q sh:inversePath ( ex:p ex:q ) ."
    );
    assert!(
        g.validate_shacl(&shapes(&unused), &ShaclOptions::default())
            .unwrap()
            .conforms
    );
}

/// Parsing a shapes document twice gives the same blank-node labels, so reports are deterministic.
#[test]
fn blank_node_shapes_are_deterministic() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        "@prefix ex: <http://ex/> . ex:a ex:p 5 .".as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    // two blank property shapes with results on the same focus node, path and value
    let ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:a ;
          sh:property [ sh:path ex:p ; sh:maxInclusive 1 ; sh:message "first" ] ;
          sh:property [ sh:path ex:p ; sh:maxInclusive 2 ; sh:message "second" ] ."#;
    let first = g
        .validate_shacl(&shapes(ttl), &ShaclOptions::default())
        .unwrap();
    assert_eq!(first.results.len(), 2);
    assert_eq!(
        g.validate_shacl(&shapes(ttl), &ShaclOptions::default())
            .unwrap(),
        first
    );
    assert_ne!(first.results[0].source_shape, first.results[1].source_shape);
}

/// Every kind of path, severities and messages, in reports and their RDF.
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
    let report = validate_checked(&g, ttl);
    let mut paths: Vec<String> = report
        .results
        .iter()
        .map(|result| result.path.as_ref().unwrap().to_string())
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        [
            "(<http://ex/p> / <http://ex/q>)",
            "(<http://ex/p> / <http://ex/q>)?",
            "(<http://ex/p> | <http://ex/q>)",
            "(<http://ex/p> | <http://ex/q>)+",
            "<http://ex/p>",
            "<http://ex/p>*",
            "^<http://ex/p>",
        ]
    );
    let info = report
        .results
        .iter()
        .find(|result| result.focus_node == ex("b").into())
        .unwrap();
    assert_eq!(info.severity.as_str(), ShaclVocab::SH_INFO);
    assert_eq!(info.messages, [Literal::new_simple_literal("none allowed")]);
    let names = info.names();
    assert_eq!(names.focus_node, "http://ex/b");
    assert_eq!(names.path.as_deref(), Some("^<http://ex/p>"));
    assert_eq!(names.severity, ShaclVocab::SH_INFO);
    assert_eq!(names.messages, ["none allowed"]);
    assert_eq!(predicate("p").unwrap().name(), "http://ex/p");
    assert_write_round_trip(&report, RdfFormat::Turtle);
}

/// Writes a report, reads it back and checks it holds the same results.
fn assert_write_round_trip(report: &ShaclReport, format: RdfFormat) {
    let mut out = Vec::new();
    let written = report.write(&mut out, format).unwrap();
    let triples: Vec<Triple> = RdfParser::from_format(format.to_oxigraph())
        .for_slice(&out)
        .map(|quad| quad.map(Triple::from))
        .collect::<Result<_, _>>()
        .unwrap_or_else(|error| panic!("{error}\n{}", String::from_utf8_lossy(&out)));
    assert_eq!(triples.len(), written);
    let graph: OxGraph = triples.iter().collect();
    let reports: Vec<_> = graph
        .subjects_for_predicate_object(
            rdf::TYPE,
            NamedNodeRef::new_unchecked(ShaclVocab::SH_VALIDATION_REPORT),
        )
        .collect();
    assert_eq!(reports.len(), 1);
    let root = Term::from(reports[0].into_owned());
    assert_eq!(expected(&graph, &root), actual(report));
}

/// A report with a control character (other than tab and line feed) in a literal fails to
/// write as RDF/XML before anything is written.
#[test]
fn rdf_xml_reports_fail_on_literals_xml_cannot_hold() {
    let g = RdfGraph::new();
    g.load_rdf(
        1,
        r#"<http://ex/a> <http://ex/name> "cr\rx" .
           <http://ex/b> <http://ex/name> "tab\tx" ."#
            .as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let max_length = |node: &str| {
        format!(
            "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S sh:targetNode ex:{node} ; sh:property [ sh:path ex:name ; sh:maxLength 2 ] ."
        )
    };
    let report = validate_checked(&g, &max_length("a"));
    assert_eq!(report.results.len(), 1);
    let mut out = Vec::new();
    match report.write(&mut out, RdfFormat::RdfXml) {
        Err(RdfError::XmlUnsafeReport(term)) => {
            assert_eq!(term, Term::from(Literal::new_simple_literal("cr\rx")))
        }
        other => panic!("{other:?}"),
    }
    assert!(out.is_empty(), "nothing is written");
    // a tab is fine
    let report = validate_checked(&g, &max_length("b"));
    assert_eq!(report.results.len(), 1);
    assert_write_round_trip(&report, RdfFormat::RdfXml);
}
