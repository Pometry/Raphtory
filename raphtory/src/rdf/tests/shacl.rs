//! SHACL validation of graph views (feature `shacl`).
use super::{
    shacl_report::{actual, expected, objects},
    within_60s,
};
use crate::{
    db::api::view::IntoDynamic,
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{vocab::rdf, Graph as RdfGraph, Literal, NamedNode, Term, Triple},
        name_of,
        shacl::{
            run_guarded, ShaclPath, ShaclReport, ShaclShapes, ShaclStore, MAX_LIST_LENGTH,
            MAX_NESTING, RECURSION_WARNING, SUBCLASS_WARNING,
        },
        RdfError, RdfFormat, RdfMutationOps, RdfParser, RdfSerializer, RdfViewOps,
    },
};
use rudof_rdf::{
    rdf_core::{NeighsRDF, RDFFormat},
    rdf_impl::{OxigraphInMemory, ReaderMode},
};
use shacl::{
    ir::IRSchema,
    validator::{
        engine::{Engine, NativeEngine},
        processor::ShaclProcessor,
        ShaclConfig, ShaclValidationMode,
    },
};
use std::{fmt::Debug, time::Instant};

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

const SH: &str = "http://www.w3.org/ns/shacl#";

fn shapes(ttl: &str) -> ShaclShapes {
    ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None)
        .unwrap_or_else(|error| panic!("{error}\n{ttl}"))
}

fn shapes_error(ttl: &str) -> RdfError {
    match ShaclShapes::parse(ttl.as_bytes(), RdfFormat::Turtle, None) {
        Err(GraphError::Rdf(error)) => error,
        other => panic!("{ttl}: {other:?}"),
    }
}

/// The results of a report: focus node, constraint component (local name), path and value.
fn summary(report: &ShaclReport) -> Vec<String> {
    report
        .results
        .iter()
        .map(|result| {
            let component = result.constraint_component.as_str();
            format!(
                "{} {} {} {}",
                result.focus_node,
                component.strip_prefix(SH).unwrap_or(component),
                result
                    .path
                    .as_ref()
                    .map_or_else(|| "-".to_owned(), ShaclPath::to_string),
                result
                    .value
                    .as_ref()
                    .map_or_else(|| "-".to_owned(), Term::to_string),
            )
        })
        .collect()
}

struct Proc<S>(S);

impl<S: NeighsRDF + Debug + Send + Sync + 'static> ShaclProcessor<S> for Proc<S> {
    fn store(&self) -> &S {
        &self.0
    }

    fn runner(_: &ShaclValidationMode, config: &ShaclConfig) -> Box<dyn Engine<S>> {
        Box::new(NativeEngine::new(config.recursion_semantics()))
    }
}

/// The reference: export the view and validate it in rudof's own in-memory graph, with shapes
/// compiled by rudof from Turtle.
fn validate_export<G: RdfViewOps>(view: &G, shapes_ttl: &str) -> (bool, Vec<String>) {
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
    let mut results: Vec<String> = report
        .results()
        .iter()
        .map(|result| {
            let component = Term::from(result.constraint_component().clone()).to_string();
            let component = component.trim_start_matches('<').trim_end_matches('>');
            format!(
                "{} {} {} {}",
                Term::from(result.focus_node().clone()),
                component.strip_prefix(SH).unwrap_or(component),
                result
                    .path()
                    .map_or_else(|| "-".to_owned(), |path| ShaclPath::from(path).to_string()),
                result
                    .value()
                    .map_or_else(|| "-".to_owned(), |v| Term::from(v.clone()).to_string()),
            )
        })
        .collect();
    results.sort();
    (report.conforms(), results)
}

/// Checks that validating `view` gives the results of the export reference, and returns them.
fn validate_checked<G: RdfViewOps>(view: &G, shapes_ttl: &str) -> ShaclReport {
    let report = shapes(shapes_ttl).validate(view).unwrap();
    let mut results = summary(&report);
    results.sort();
    assert_eq!(
        (report.conforms, results),
        validate_export(view, shapes_ttl),
        "the view and its export validate differently"
    );
    report
}

/// alice and bob are people, alice knows bob and works for acme. At 5 alice loses her name, bob
/// gets a negative age and knows acme (not a person); at 7 acme's name becomes "A" (too short).
fn temporal_graph() -> PersistentGraph {
    let pg = PersistentGraph::new();
    let doc = r#"
        @prefix ex: <http://ex/> .
        ex:alice a ex:Person ; ex:name "Alice" ; ex:age 30 ; ex:knows ex:bob ; ex:worksFor ex:acme .
        ex:bob a ex:Person ; ex:name "Bob" .
        ex:acme ex:name "ACME" .
    "#;
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let nt = |t: i64, doc: &str, retract: bool| {
        if retract {
            pg.retract_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
        } else {
            pg.load_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
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
    pg
}

#[test]
fn temporal_snapshots() {
    let pg = temporal_graph();
    let expected: [(i64, &[&str]); 3] = [
        (3, &[]),
        (
            6,
            &[
                "<http://ex/alice> MinCountConstraintComponent <http://ex/name> -",
                "<http://ex/bob> ClassConstraintComponent <http://ex/knows> <http://ex/acme>",
                "<http://ex/bob> MinInclusiveConstraintComponent <http://ex/age> \"-3\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ],
        ),
        (
            8,
            &[
                "<http://ex/alice> MinCountConstraintComponent <http://ex/name> -",
                "<http://ex/alice> MinLengthConstraintComponent (<http://ex/worksFor> / <http://ex/name>) \"A\"",
                "<http://ex/bob> ClassConstraintComponent <http://ex/knows> <http://ex/acme>",
                "<http://ex/bob> MinInclusiveConstraintComponent <http://ex/age> \"-3\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ],
        ),
    ];
    let s = shapes(SHAPES);
    for (t, results) in expected {
        let report = validate_checked(&pg.snapshot_at(t), SHAPES);
        assert_eq!(summary(&report), results, "as of {t}");
        assert_eq!(report.conforms, results.is_empty());
        assert!(report.warnings.is_empty());
    }
    // the present is the state as of 8
    assert_eq!(
        s.validate(&pg).unwrap(),
        s.validate(&pg.snapshot_at(8)).unwrap()
    );
    // validate_at validates the same snapshots, in order
    let reports = s.validate_at(&pg, [8, 3, 6]).unwrap();
    assert_eq!(
        reports.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        [8, 3, 6]
    );
    for (t, report) in &reports {
        assert_eq!(*report, s.validate(&pg.snapshot_at(*t)).unwrap());
    }
    // date-times are times too
    let reports = s.validate_at(&pg, ["1970-01-01T00:00:00.006Z"]).unwrap();
    assert_eq!(reports[0].0, 6);
    assert_eq!(reports[0].1.results.len(), 3);
    assert!(s.validate_at(&pg, ["not a time"]).is_err());
    // the first time the data stopped conforming
    let first = s
        .validate_at(&pg, 0..10)
        .unwrap()
        .into_iter()
        .find(|(_, report)| !report.conforms)
        .map(|(t, _)| t);
    assert_eq!(first, Some(5));
}

/// An event graph never forgets a triple: alice keeps her name, and acme has two.
#[test]
fn event_graph_ignores_retractions() {
    let g = temporal_graph().event_graph();
    let report = validate_checked(&g, SHAPES);
    assert_eq!(
        summary(&report),
        [
            "<http://ex/alice> MinLengthConstraintComponent (<http://ex/worksFor> / <http://ex/name>) \"A\"",
            "<http://ex/bob> ClassConstraintComponent <http://ex/knows> <http://ex/acme>",
            "<http://ex/bob> MinInclusiveConstraintComponent <http://ex/age> \"-3\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ]
    );
    // the persistent view of the same events (blank-node shapes are only the same in one
    // parse of the shapes)
    let s = shapes(SHAPES);
    assert_eq!(
        g.persistent_graph().validate_shacl(&s).unwrap(),
        s.validate(&temporal_graph()).unwrap()
    );
}

#[test]
fn views_restrict_the_data() {
    let pg = temporal_graph();
    // hiding the names: both people miss one
    let view = pg
        .snapshot_at(3)
        .exclude_layers(vec!["http://ex/name"])
        .unwrap();
    let report = validate_checked(&view, SHAPES);
    assert_eq!(
        summary(&report),
        [
            "<http://ex/alice> MinCountConstraintComponent <http://ex/name> -",
            "<http://ex/bob> MinCountConstraintComponent <http://ex/name> -",
        ]
    );
    // a window that starts after the first load holds nothing on an event graph
    let g = pg.event_graph();
    assert!(validate_checked(&g.window(2, 10), SHAPES).conforms);
    // only the events from 5 on: bob's negative age and acme's short name, but bob is no longer
    // known to be a person
    let report = validate_checked(&g.window(5, 10), SHAPES);
    assert!(report.results.is_empty(), "{:?}", summary(&report));
    // a node subgraph
    let report = validate_checked(
        &pg.subgraph(vec!["http://ex/alice", "http://ex/Person", "http://ex/bob"]),
        SHAPES,
    );
    assert_eq!(
        summary(&report),
        [
            "<http://ex/alice> MinCountConstraintComponent <http://ex/name> -",
            "<http://ex/bob> MinCountConstraintComponent <http://ex/name> -",
        ]
    );
    // a dynamic view validates the same
    let s = shapes(SHAPES);
    assert_eq!(
        s.validate(&pg.clone().into_dynamic()).unwrap(),
        s.validate(&pg).unwrap()
    );
}

/// The store holds exactly the triples `to_rdf` writes, and lists only `rdf:type` and
/// `rdfs:subClassOf` ones.
#[test]
fn store_sees_what_to_rdf_writes() {
    let pg = temporal_graph();
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(1, "\"lit\"", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(2, "Alice", "Bob", NO_PROPS, Some("\"notiri\""))
        .unwrap();
    g.add_edge(2, "Alice", "Person", NO_PROPS, Some(rdf::TYPE.as_str()))
        .unwrap();
    pg.load_rdf(
        2,
        "<http://ex/Person> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/Agent> ."
            .as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let views = [
        pg.clone().into_dynamic(),
        pg.snapshot_at(3).into_dynamic(),
        pg.event_graph().into_dynamic(),
        pg.event_graph().window(5, 8).into_dynamic(),
        g.clone().into_dynamic(),
        g.window(2, 3).into_dynamic(),
    ];
    let mut class_triples = 0;
    for view in views {
        let mut nt = Vec::new();
        view.to_rdf(&mut nt, RdfFormat::NTriples).unwrap();
        let mut exported: Vec<String> = RdfParser::from_format(RdfFormat::NTriples)
            .for_slice(&nt)
            .map(|quad| Triple::from(quad.unwrap()).to_string())
            .collect();
        exported.sort();
        let store = ShaclStore::new(view.clone());
        let mut stored: Vec<String> = store
            .all_triples()
            .map(|triple| triple.to_string())
            .collect();
        stored.sort();
        assert_eq!(stored, exported);
        let mut listed: Vec<String> = store
            .triples()
            .unwrap()
            .map(|triple| triple.to_string())
            .collect();
        listed.sort();
        let classes: Vec<String> = exported
            .iter()
            .filter(|triple| {
                triple.contains("> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <")
                    || triple.contains("> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <")
            })
            .cloned()
            .collect();
        class_triples += classes.len();
        assert_eq!(listed, classes);
    }
    assert!(class_triples > 0);
}

/// Graphs not loaded from RDF: names are `raphtory:` IRIs, and generalized triples are left out.
#[test]
fn plain_graphs() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Person", NO_PROPS, Some(rdf::TYPE.as_str()))
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
    let report = validate_checked(&g, ttl);
    assert_eq!(
        summary(&report),
        ["<raphtory:Alice> MinCountConstraintComponent <raphtory:knows> -"]
    );
    let focus = name_of(report.results[0].focus_node.as_ref()).unwrap();
    assert_eq!(focus, "Alice");
    assert!(g.node(focus).is_some());

    // a graph with u64 ids
    let g = Graph::new();
    g.add_edge(1, 1, 2, NO_PROPS, Some(rdf::TYPE.as_str()))
        .unwrap();
    let report = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetClass <raphtory:2> ; sh:property [ sh:path <raphtory:knows> ; sh:minCount 1 ] ."#,
    )
    .validate(&g)
    .unwrap();
    assert_eq!(
        summary(&report),
        ["<raphtory:1> MinCountConstraintComponent <raphtory:knows> -"]
    );
    assert_eq!(name_of(report.results[0].focus_node.as_ref()).unwrap(), "1");
    // an empty graph conforms
    assert!(shapes(SHAPES).validate(&Graph::new()).unwrap().conforms);
}

const SUBCLASSES: &str = r#"
    @prefix ex: <http://ex/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    ex:Student rdfs:subClassOf ex:Person . ex:PhD rdfs:subClassOf ex:Student .
    ex:s1 a ex:Student . ex:s2 a ex:PhD . ex:p1 a ex:Person .
    ex:x ex:knows ex:s1 , ex:s2 , ex:p1 .
"#;

fn subclass_graph() -> PersistentGraph {
    let g = PersistentGraph::new();
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

fn focus_nodes(report: &ShaclReport) -> Vec<String> {
    report
        .results
        .iter()
        .map(|result| match &result.value {
            Some(value) => value.to_string(),
            None => result.focus_node.to_string(),
        })
        .collect()
}

/// The validator's partial subclass reasoning, and the warning that says so.
#[test]
fn subclass_reasoning_is_partial() {
    let g = subclass_graph();
    // sh:targetClass: direct instances only
    let report = validate_checked(&g, TARGET_CLASS);
    assert_eq!(focus_nodes(&report), ["<http://ex/p1>"]);
    assert_eq!(report.warnings, [SUBCLASS_WARNING]);
    // sh:class: one subclass step, so the PhD is reported
    let report = validate_checked(&g, CLASS);
    assert_eq!(focus_nodes(&report), ["<http://ex/s2>"]);
    assert_eq!(report.warnings, [SUBCLASS_WARNING]);
    // implicit class target: one subclass step
    let report = validate_checked(&g, IMPLICIT);
    assert_eq!(focus_nodes(&report), ["<http://ex/p1>", "<http://ex/s1>"]);
    assert_eq!(report.warnings, [SUBCLASS_WARNING]);

    // no warning without subclass triples (also when they are hidden or retracted)...
    let flat = g
        .exclude_layers(vec!["http://www.w3.org/2000/01/rdf-schema#subClassOf"])
        .unwrap();
    assert!(shapes(CLASS).validate(&flat).unwrap().warnings.is_empty());
    g.retract_rdf(2, SUBCLASSES.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    assert!(shapes(CLASS).validate(&g).unwrap().warnings.is_empty());
    assert!(!shapes(CLASS)
        .validate(&g.snapshot_at(1))
        .unwrap()
        .warnings
        .is_empty());
    // ... or when the shapes use no classes
    let report = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetNode <http://ex/x> ; sh:property [ sh:path <http://ex/knows> ; sh:minCount 1 ] ."#,
    )
    .validate(&subclass_graph())
    .unwrap();
    assert!(report.conforms && report.warnings.is_empty());
}

/// What SHACL specifies for subclasses; ignored until the validator follows `rdfs:subClassOf`
/// transitively.
#[test]
#[ignore = "rudof 0.3.24 follows rdfs:subClassOf partly; see subclass_reasoning_is_partial"]
fn subclass_reasoning_as_specified() {
    let g = subclass_graph();
    let report = shapes(TARGET_CLASS).validate(&g).unwrap();
    assert_eq!(
        focus_nodes(&report),
        ["<http://ex/p1>", "<http://ex/s1>", "<http://ex/s2>"]
    );
    assert!(shapes(CLASS).validate(&g).unwrap().conforms);
    let report = shapes(IMPLICIT).validate(&g).unwrap();
    assert_eq!(
        focus_nodes(&report),
        ["<http://ex/p1>", "<http://ex/s1>", "<http://ex/s2>"]
    );
}

/// Result values are the literals as written in the graph, where that is unambiguous.
#[test]
fn literal_values() {
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        r#"
        @prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "042"^^xsd:integer , "1.50"^^xsd:decimal , "0042"^^xsd:int , "abc"^^xsd:integer ,
            "x"@en , "1e3"^^xsd:double , " 5 "^^xsd:integer , "+7"^^xsd:integer , "hello"^^ex:custom ,
            "plain" , "1"^^xsd:boolean , "2020-01-01T00:00:00.000Z"^^xsd:dateTime ,
            "042"^^xsd:long , "05"^^xsd:short , "007"^^xsd:unsignedInt ,
            "+7"^^xsd:nonNegativeInteger , "2021-06-01T12:00:00+00:00"^^xsd:dateTime ,
            "colour"@en-gb .
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
    let report = shapes(in_nothing).validate(&g).unwrap();
    let values = |focus: &str| -> Vec<String> {
        report
            .results
            .iter()
            .filter(|result| result.focus_node.to_string() == focus)
            .map(|result| result.value.as_ref().unwrap().to_string())
            .collect()
    };
    // every value of a is a node of the graph
    let a: Vec<&Term> = report
        .results
        .iter()
        .filter(|result| result.focus_node.to_string() == "<http://ex/a>")
        .map(|result| result.value.as_ref().unwrap())
        .collect();
    assert_eq!(a.len(), 18);
    for value in &a {
        let name = name_of(value.as_ref()).unwrap();
        assert!(g.node(&name).is_some(), "{value}");
    }
    let a = values("<http://ex/a>");
    for written in [
        "\"05\"^^<http://www.w3.org/2001/XMLSchema#short>",
        "\"042\"^^<http://www.w3.org/2001/XMLSchema#long>",
        "\"colour\"@en-gb",
        "\"2021-06-01T12:00:00+00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
        "\"2020-01-01T00:00:00.000Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
        "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
    ] {
        assert!(a.contains(&written.to_owned()), "{written}: {a:?}");
    }
    assert!(values("<http://ex/b>")
        .iter()
        .all(|value| value == "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"));
    assert!(values("<http://ex/c>")
        .iter()
        .all(|value| value == "\"1\"^^<http://www.w3.org/2001/XMLSchema#long>"));
    assert!(!values("<http://ex/c>").is_empty());
    // a focus node
    assert_eq!(
        values("\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"),
        ["\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"]
    );
}

/// `sh:hasValue` and `sh:in` with literals the validator rewrites are rejected; such a
/// `sh:targetNode` is looked up as written.
#[test]
fn shape_literals_the_validator_rewrites() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .";
    let xsd = |local: &str| format!("<http://www.w3.org/2001/XMLSchema#{local}>");
    let cases = [
        (
            r#"ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:flag ; sh:hasValue "1"^^xsd:boolean ] ."#,
            format!("sh:hasValue \"1\"^^{0} (the validator rewrites this literal as \"true\"^^{0}, a different term, so it would never match the data)", xsd("boolean")),
        ),
        (
            r#"ex:S sh:targetNode ex:b ; sh:property [ sh:path ex:n ; sh:in ( 1 "042"^^xsd:long ) ] ."#,
            format!("sh:in \"042\"^^{0} (the validator rewrites this literal as \"42\"^^{0}, a different term, so it would never match the data)", xsd("long")),
        ),
        (
            r#"ex:S sh:targetNode ex:c ; sh:property [ sh:path ex:d ; sh:hasValue "2020-01-01T00:00:00.000Z"^^xsd:dateTime ] ."#,
            format!("sh:hasValue \"2020-01-01T00:00:00.000Z\"^^{0} (the validator rewrites this literal as \"2020-01-01T00:00:00Z\"^^{0}, a different term, so it would never match the data)", xsd("dateTime")),
        ),
        (
            r#"ex:S sh:targetNode ex:e ; sh:property [ sh:path ex:label ; sh:in ( "colour"@en-gb ) ] ."#,
            "sh:in \"colour\"@en-gb (the validator rewrites this literal as \"colour\"@en-GB, a different term, so it would never match the data)".to_owned(),
        ),
    ];
    for (shapes, expected) in cases {
        let error = shapes_error(&format!("{prefix} {shapes}"));
        let RdfError::ShaclUnsupported(found) = &error else {
            panic!("{shapes}: {error:?}")
        };
        assert_eq!(*found, expected);
    }

    // literals in canonical form are compared as terms, as SHACL specifies
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        r#"@prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:flag true . ex:b ex:n "42"^^xsd:long , 7 . ex:c ex:flag "1"^^xsd:boolean ."#
            .as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    let report = validate_checked(
        &g,
        &format!(
            r#"{prefix}
            ex:S sh:targetNode ex:a , ex:c ; sh:property [ sh:path ex:flag ; sh:hasValue true ] .
            ex:T sh:targetNode ex:b ; sh:property [ sh:path ex:n ; sh:in ( 7 "42"^^xsd:long ) ] .
            ex:U sh:targetNode true ; sh:property [ sh:path [ sh:inversePath ex:flag ] ; sh:minCount 1 ] ."#
        ),
    );
    // "1"^^xsd:boolean is not the term true
    assert_eq!(
        summary(&report),
        ["<http://ex/c> HasValueConstraintComponent <http://ex/flag> -"]
    );

    // targets are looked up as written: "1"^^xsd:boolean is ex:c's flag (the validator looks
    // for "true"^^xsd:boolean), and "02"^^xsd:short nobody's
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        r#"<http://ex/c> <http://ex/flag> "1"^^<http://www.w3.org/2001/XMLSchema#boolean> ."#
            .as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let report = shapes(&format!(
        r#"{prefix}
        ex:V sh:targetNode "1"^^xsd:boolean , "02"^^xsd:short ;
            sh:property [ sh:path [ sh:inversePath ex:flag ] ; sh:minCount 1 ] ."#
    ))
    .validate(&g)
    .unwrap();
    assert_eq!(
        summary(&report),
        [format!(
            "\"02\"^^{} MinCountConstraintComponent ^<http://ex/flag> -",
            xsd("short")
        )]
    );
}

#[test]
fn unsupported_features_are_rejected() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    let cases = [
        (
            "ex:S sh:targetNode ex:a ; sh:sparql ex:C .",
            "sh:sparql (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:S sh:select \"SELECT $this {}\" .",
            "sh:select (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:S sh:ask \"ASK {}\" .",
            "sh:ask (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C sh:validator ex:V .",
            "sh:validator (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C sh:nodeValidator ex:V .",
            "sh:nodeValidator (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C sh:propertyValidator ex:V .",
            "sh:propertyValidator (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C a sh:ConstraintComponent .",
            "sh:ConstraintComponent (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C a sh:SPARQLConstraint .",
            "sh:SPARQLConstraint (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:T a sh:SPARQLTarget .",
            "sh:SPARQLTarget (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:T a sh:SPARQLTargetType .",
            "sh:SPARQLTargetType (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:F a sh:SPARQLFunction .",
            "sh:SPARQLFunction (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:V a sh:SPARQLAskValidator .",
            "sh:SPARQLAskValidator (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:V a sh:SPARQLSelectValidator .",
            "sh:SPARQLSelectValidator (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:C sh:parameter [ sh:path ex:p ] .",
            "sh:parameter (SHACL-SPARQL is not supported)",
        ),
        (
            "ex:S sh:rule [ a ex:R ] .",
            "sh:rule (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:R a sh:TripleRule .",
            "sh:TripleRule (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:R a sh:SPARQLRule .",
            "sh:SPARQLRule (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:S sh:target [ a ex:T ] .",
            "sh:target (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:S sh:expression ex:E .",
            "sh:expression (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:S sh:filterShape ex:F .",
            "sh:filterShape (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:S sh:values ex:E .",
            "sh:values (SHACL Advanced Features are not supported)",
        ),
        (
            "ex:S sh:jsFunctionName \"f\" .",
            "sh:jsFunctionName (SHACL-JS is not supported)",
        ),
        (
            "ex:S a sh:JSConstraint .",
            "sh:JSConstraint (SHACL-JS is not supported)",
        ),
        (
            "ex:G sh:entailment <http://www.w3.org/ns/entailment/RDFS> .",
            "sh:entailment (entailment regimes are not supported: validation sees the stored triples only)",
        ),
        // SHACL 1.2
        (
            "ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:singleLine true ] .",
            "sh:singleLine (SHACL 1.2 features are not supported)",
        ),
        (
            "ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:list ; sh:minListLength 2 ] .",
            "sh:minListLength (SHACL 1.2 features are not supported)",
        ),
        (
            "ex:S sh:targetWhere [ sh:path ex:p ] .",
            "sh:targetWhere (SHACL 1.2 features are not supported)",
        ),
        (
            "ex:Person a sh:ShapeClass .",
            "sh:ShapeClass (SHACL 1.2 features are not supported)",
        ),
        (
            "ex:S sh:targetClass ex:C ; sh:closed sh:ByTypes .",
            "sh:closed (SHACL 1.2 features are not supported)",
        ),
    ];
    for (shapes, feature) in cases {
        let error = shapes_error(&format!("{prefix} {shapes}"));
        let RdfError::ShaclUnsupported(found) = &error else {
            panic!("{shapes}: {error:?}")
        };
        assert_eq!(found, feature);
        assert_eq!(
            error.to_string(),
            format!("unsupported SHACL feature: {feature}")
        );
    }
    // non-validating properties, prefix declarations, reports and unknown terms are allowed
    // (SHACL ignores unknown terms)
    shapes(&format!(
        "{prefix} ex:S a sh:NodeShape ; sh:name \"S\" ; sh:description \"d\" ; sh:order 1 ;
            sh:group ex:G ; sh:defaultValue 1 ; sh:targetNode ex:a ;
            sh:declare [ sh:prefix \"ex\" ; sh:namespace \"http://ex/\" ] .
        ex:G a sh:PropertyGroup .
        [] a sh:ValidationReport ; sh:conforms true ; sh:result [ a sh:ValidationResult ;
            sh:focusNode ex:a ; sh:resultSeverity sh:Violation ; sh:sourceShape ex:S ;
            sh:sourceConstraintComponent sh:MinCountConstraintComponent ; sh:value 1 ;
            sh:resultPath ex:p ; sh:resultMessage \"m\" ] .
        ex:T sh:nodeShape ex:S ; sh:minCOunt 1 . ex:U a sh:NodeShap ."
    ));
}

/// Only inverses of predicates are accepted; other inverse paths are rejected.
#[test]
fn inverse_of_complex_paths() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    for path in [
        "( ex:p ex:q )",
        "[ sh:alternativePath ( ex:p ex:q ) ]",
        "[ sh:zeroOrMorePath ex:p ]",
        "[ sh:inversePath ex:p ]",
    ] {
        let ttl = format!(
            "{prefix} ex:S sh:targetNode ex:c ; sh:property [ sh:path [ sh:inversePath {path} ] ; sh:minCount 2 ] ."
        );
        let error = shapes_error(&ttl);
        let RdfError::ShaclUnsupported(found) = &error else {
            panic!("{path}: {error:?}")
        };
        assert_eq!(
            found,
            "sh:inversePath (the validator only supports the inverse of a predicate: write ^(p / q) \
             as (^q / ^p), ^(p | q) as (^p | ^q) and ^(p*) as (^p)*)"
        );
    }
    // ^(p / q) as (^q / ^p)
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/b> .\n<http://ex/b> <http://ex/q> <http://ex/c> ."
            .as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let report = validate_checked(
        &g,
        &format!(
            "{prefix} ex:S sh:targetNode ex:c ;
            sh:property [ sh:path ( [ sh:inversePath ex:q ] [ sh:inversePath ex:p ] ) ; sh:minCount 2 ] ."
        ),
    );
    assert_eq!(
        summary(&report),
        ["<http://ex/c> MinCountConstraintComponent (^<http://ex/q> / ^<http://ex/p>) -"]
    );
}

#[test]
fn invalid_shapes() {
    assert!(matches!(
        shapes_error("this is not turtle"),
        RdfError::Parse(_)
    ));
    // named graphs are rejected
    let error = ShaclShapes::parse(
        "<http://ex/g> { <http://ex/s> <http://ex/p> <http://ex/o> }".as_bytes(),
        RdfFormat::TriG,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, GraphError::Rdf(RdfError::Parse(_))),
        "{error:?}"
    );
    // a relative IRI needs a base
    assert!(matches!(
        shapes_error("<s> <http://www.w3.org/ns/shacl#targetNode> <a> ."),
        RdfError::Parse(_)
    ));
    let s = ShaclShapes::parse(
        "<S> <http://www.w3.org/ns/shacl#targetNode> <a> ; <http://www.w3.org/ns/shacl#property> [ <http://www.w3.org/ns/shacl#path> <p> ; <http://www.w3.org/ns/shacl#minCount> 1 ] .".as_bytes(),
        RdfFormat::Turtle,
        Some("http://ex/"),
    )
    .unwrap();
    let g = Graph::new();
    g.add_edge(
        1,
        "http://ex/a",
        "http://ex/b",
        NO_PROPS,
        Some("http://ex/q"),
    )
    .unwrap();
    assert_eq!(
        summary(&s.validate(&g).unwrap()),
        ["<http://ex/a> MinCountConstraintComponent <http://ex/p> -"]
    );
    assert!(matches!(
        ShaclShapes::parse("".as_bytes(), RdfFormat::Turtle, Some("not an iri")).unwrap_err(),
        GraphError::Rdf(RdfError::Iri(_))
    ));
    // shapes that do not compile
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    for bad in [
        "ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:minCount \"x\" ] .",
        "ex:S a sh:NodeShape ; sh:targetClass \"x\" .",
    ] {
        let error = shapes_error(&format!("{prefix} {bad}"));
        assert!(matches!(error, RdfError::Shacl(_)), "{bad}: {error:?}");
        assert!(error.to_string().starts_with("SHACL error: "), "{error}");
    }
    // recursive shapes compile
    for recursive in [
        "ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:node ex:S ] .",
        "ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:not ex:S .",
    ] {
        shapes(&format!("{prefix} {recursive}"));
    }
    // an empty shapes graph validates anything
    assert!(shapes("").validate(&temporal_graph()).unwrap().conforms);
}

/// A validator panic is reported as an error, and the next validation works.
#[test]
#[cfg(panic = "unwind")]
fn validator_panics_are_errors() {
    let pg = temporal_graph();
    for _ in 0..3 {
        let error = run_guarded::<()>("the SHACL validator", || panic!("boom")).unwrap_err();
        let RdfError::Shacl(message) = &error else {
            panic!("{error:?}")
        };
        assert_eq!(message, "the SHACL validator panicked: boom");
        assert!(
            shapes(SHAPES)
                .validate(&pg.snapshot_at(3))
                .unwrap()
                .conforms
        );
    }
}

/// Recursive shapes: on a data cycle, nodes whose conformance depends on themselves are reported
/// with a warning; on acyclic data the results are those of SHACL.
#[test]
fn recursive_shapes_on_cycles() {
    let recursive = r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
            sh:property [ sh:path ex:name ; sh:minCount 1 ] ;
            sh:property [ sh:path ex:knows ; sh:node ex:PersonShape ] ."#;
    let g = PersistentGraph::new();
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
    assert_eq!(
        summary(&report),
        [
            "<http://ex/alice> NodeConstraintComponent <http://ex/knows> <http://ex/bob>",
            "<http://ex/bob> NodeConstraintComponent <http://ex/knows> <http://ex/carol>",
            "<http://ex/carol> NodeConstraintComponent <http://ex/knows> <http://ex/alice>",
        ]
    );
    assert_eq!(report.warnings, [RECURSION_WARNING]);
    // the warning needs results and recursive shapes
    assert!(validate_checked(&g.snapshot_at(1), recursive)
        .warnings
        .is_empty());
    assert!(shapes(SHAPES).validate(&g).unwrap().warnings.is_empty());
    // recursion through sh:not, sh:or lists, named property shapes and blank-node shapes
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    for shapes_ttl in [
        "ex:S sh:targetNode ex:alice ; sh:or ( ex:T [ sh:node ex:S ] ) . ex:T sh:class ex:None .",
        "ex:S sh:targetNode ex:alice ; sh:property ex:P . ex:P sh:path ex:knows ; sh:node ex:S ; sh:maxCount 0 .",
        "ex:S sh:targetNode ex:alice ; sh:node _:s . _:s sh:property [ sh:path ex:knows ; sh:node _:s ; sh:maxCount 0 ] .",
    ] {
        let report = shapes(&format!("{prefix} {shapes_ttl}"))
            .validate(&g)
            .unwrap();
        assert!(!report.conforms, "{shapes_ttl}");
        assert_eq!(report.warnings, [RECURSION_WARNING], "{shapes_ttl}");
    }
    // sh:class of a shape that is also a class is not recursion
    let report = shapes(&format!(
        "{prefix} ex:Person a sh:NodeShape , <http://www.w3.org/2000/01/rdf-schema#Class> ;
            sh:property [ sh:path ex:knows ; sh:class ex:Person ; sh:maxCount 0 ] ."
    ))
    .validate(&g)
    .unwrap();
    assert!(!report.conforms && report.warnings.is_empty());
}

/// Shapes with lists or blank-node nesting deeper than the limits are rejected.
#[test]
fn limits_of_the_validator() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/v1> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let graph = g.clone();
    within_60s(move || {
        let g = graph;
        let in_list = |n: usize| {
            let values: String = (0..n).map(|i| format!("<http://ex/v{i}> ")).collect();
            format!(
                "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:in ( {values}) ] ."
            )
        };
        // the longest list
        assert!(
            shapes(&in_list(MAX_LIST_LENGTH))
                .validate(&g)
                .unwrap()
                .conforms
        );
        for n in [MAX_LIST_LENGTH + 1, 100_000] {
            let error = shapes_error(&in_list(n));
            assert_eq!(
                error.to_string(),
                format!(
                    "SHACL error: the shapes graph has an RDF list of more than {MAX_LIST_LENGTH} \
                     members, the most the validator accepts"
                )
            );
        }

        // nested paths: the deepest nesting, in results too (written and dropped on this thread)
        let nested = |n: usize| {
            format!(
                "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path {}ex:p{} ; sh:maxCount 0 ] .",
                "[ sh:zeroOrOnePath ".repeat(n),
                " ]".repeat(n)
            )
        };
        // (ex:S is one deeper than its property shape, as deep as its path)
        let report = shapes(&nested(MAX_NESTING - 1)).validate(&g).unwrap();
        assert_eq!(report.results.len(), 1);
        let path = report.results[0].path.as_ref().unwrap().to_string();
        assert_eq!(path.matches('?').count(), MAX_NESTING - 1, "{path}");
        let mut out = Vec::new();
        report.write(&mut out, RdfFormat::Turtle).unwrap();
        drop(report);
        let too_deep = format!(
            "SHACL error: the shapes graph nests shapes and paths more than {MAX_NESTING} deep, \
             the most the validator accepts"
        );
        for n in [MAX_NESTING, 100_000] {
            assert_eq!(shapes_error(&nested(n)).to_string(), too_deep);
        }
        // nested property shapes count too
        let properties = |n: usize| {
            format!(
                "{prefix} ex:S sh:targetNode ex:a ; {}sh:path ex:p{} .",
                "sh:property [ ".repeat(n),
                " ]".repeat(n)
            )
        };
        assert!(
            shapes(&properties(MAX_NESTING))
                .validate(&g)
                .unwrap()
                .conforms
        );
        for n in [MAX_NESTING + 1, 100_000] {
            assert_eq!(shapes_error(&properties(n)).to_string(), too_deep);
        }
        // and named shapes
        let named = |n: usize| {
            let chain: String = (0..n)
                .map(|i| format!("ex:S{i} sh:node ex:S{} . ", i + 1))
                .collect();
            format!("{prefix} ex:S0 sh:targetNode ex:a . {chain}")
        };
        assert!(shapes(&named(MAX_NESTING)).validate(&g).unwrap().conforms);
        assert_eq!(shapes_error(&named(MAX_NESTING + 1)).to_string(), too_deep);
    });

    // cycles that are not recursive shapes
    let cyclic_list = format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:in _:l ] .
        _:l <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ex:v1 ;
            <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:m .
        _:m <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> ex:v2 ;
            <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:l ."
    );
    assert_eq!(
        shapes_error(&cyclic_list).to_string(),
        "SHACL error: the shapes graph has a cyclic RDF list (its rdf:rest links lead back to it)"
    );
    for cyclic in [
        "ex:S sh:targetNode ex:a ; sh:property [ sh:path _:p ; sh:minCount 1 ] . _:p sh:zeroOrMorePath _:p .",
        "ex:S sh:targetNode ex:a ; sh:property [ sh:path _:l ; sh:minCount 1 ] .
            _:l <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:l ;
                <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .",
    ] {
        assert_eq!(
            shapes_error(&format!("{prefix} {cyclic}")).to_string(),
            "SHACL error: the shapes graph has a path that contains itself",
            "{cyclic}"
        );
    }
    // what the validator does not read is not checked
    let unused = format!(
        "{prefix} ex:S sh:targetNode ex:a ; sh:property [ sh:path ex:p ; sh:maxCount 1 ] .
        _:p sh:zeroOrMorePath _:p . _:q sh:inversePath ( ex:p ex:q ) ."
    );
    assert!(shapes(&unused).validate(&g).unwrap().conforms);
}

/// Parsing a shapes document twice gives the same blank-node labels, so the same reports.
#[test]
fn blank_node_shapes_are_deterministic() {
    let g = PersistentGraph::new();
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
    let first = shapes(ttl).validate(&g).unwrap();
    assert_eq!(first.results.len(), 2);
    for _ in 0..10 {
        assert_eq!(shapes(ttl).validate(&g).unwrap(), first);
    }
    let sources: Vec<String> = first
        .results
        .iter()
        .map(|result| result.source_shape.as_ref().unwrap().to_string())
        .collect();
    assert!(sources.iter().all(|source| source.starts_with("_:s")));
    assert_ne!(sources[0], sources[1]);
    // the same document in another format
    let s = shapes(SHAPES);
    let mut nt = Vec::new();
    {
        let mut serializer = RdfSerializer::from_format(RdfFormat::NTriples).for_writer(&mut nt);
        for quad in RdfParser::from_format(RdfFormat::Turtle).for_slice(SHAPES.as_bytes()) {
            serializer.serialize_quad(&quad.unwrap()).unwrap();
        }
        serializer.finish().unwrap();
    }
    let t = ShaclShapes::parse(nt.as_slice(), RdfFormat::NTriples, None).unwrap();
    assert_eq!(
        s.validate(&temporal_graph()).unwrap(),
        t.validate(&temporal_graph()).unwrap()
    );
    // a different document gets different labels
    let other = shapes(&ttl.replace("first", "third")).validate(&g).unwrap();
    assert_ne!(other.results[0].source_shape, first.results[0].source_shape);
}

/// Every kind of path, in reports and their RDF.
#[test]
fn paths() {
    let g = PersistentGraph::new();
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
        ex:T sh:targetNode ex:b ; sh:property [ sh:path [ sh:inversePath ex:p ] ; sh:maxCount 0 ] ."#;
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
    for format in [RdfFormat::Turtle, RdfFormat::NTriples, RdfFormat::RdfXml] {
        assert_write_round_trip(&report, format);
    }
}

/// Writes a report, reads it back and checks it holds the same results.
fn assert_write_round_trip(report: &ShaclReport, format: RdfFormat) {
    let mut out = Vec::new();
    let stats = report.write(&mut out, format).unwrap();
    assert_eq!(stats.skipped, 0);
    let triples: Vec<Triple> = RdfParser::from_format(format)
        .for_slice(&out)
        .map(|quad| quad.map(Triple::from))
        .collect::<Result<_, _>>()
        .unwrap_or_else(|error| panic!("{error}\n{}", String::from_utf8_lossy(&out)));
    assert_eq!(triples.len(), stats.triples);
    let graph: RdfGraph = triples.iter().collect();
    let reports: Vec<_> = graph
        .subjects_for_predicate_object(
            rdf::TYPE,
            NamedNode::new_unchecked(format!("{SH}ValidationReport")).as_ref(),
        )
        .collect();
    assert_eq!(reports.len(), 1);
    let root = Term::from(reports[0].into_owned());
    let results = objects(
        &graph,
        reports[0].into_owned(),
        &NamedNode::new_unchecked(format!("{SH}result")),
    );
    assert_eq!(results.len(), report.results.len());
    assert_eq!(expected(&graph, &root), actual(report));
}

#[test]
fn reports_are_deterministic_and_written_as_rdf() {
    let pg = temporal_graph();
    let s = shapes(SHAPES);
    let a = s.validate(&pg).unwrap();
    for _ in 0..5 {
        assert_eq!(s.validate(&pg).unwrap(), a);
    }
    let mut sorted = a.results.clone();
    sorted.sort_by_key(|result| result.focus_node.to_string());
    assert_eq!(
        sorted.iter().map(|r| &r.focus_node).collect::<Vec<_>>(),
        a.results.iter().map(|r| &r.focus_node).collect::<Vec<_>>()
    );
    for format in [RdfFormat::Turtle, RdfFormat::NTriples, RdfFormat::RdfXml] {
        assert_write_round_trip(&a, format);
    }
    // a report that conforms
    let report = s.validate(&pg.snapshot_at(3)).unwrap();
    let mut out = Vec::new();
    report.write(&mut out, RdfFormat::NTriples).unwrap();
    let out = String::from_utf8(out).unwrap();
    assert_eq!(out.lines().count(), 2, "{out}");
    assert!(out.contains(
        "<http://www.w3.org/ns/shacl#conforms> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ."
    ));
    // with prefixes
    let mut out = Vec::new();
    a.write(
        &mut out,
        crate::rdf::serializer_with_prefixes(RdfFormat::Turtle, [("sh", SH), ("ex", "http://ex/")])
            .unwrap(),
    )
    .unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("sh:ValidationReport"), "{out}");
}

/// A report with a control character (other than tab and line feed) in a literal fails to
/// write as RDF/XML before anything is written; other formats write it.
#[test]
fn rdf_xml_reports_fail_on_literals_xml_cannot_hold() {
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        r#"<http://ex/a> <http://ex/name> "cr\rx" .
           <http://ex/b> <http://ex/name> "tab\tx" ."#
            .as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
    let cr = r#""cr\rx""#;
    let cases = [
        // the literal is the focus node (and the value)
        (
            "ex:S sh:targetObjectsOf ex:name ; sh:maxLength 2 .",
            cr.to_owned(),
        ),
        // the literal is the value
        (
            "ex:S sh:targetSubjectsOf ex:name ; sh:property [ sh:path ex:name ; sh:maxLength 2 ] .",
            cr.to_owned(),
        ),
        // the message
        (
            r#"ex:S sh:targetNode ex:b ;
                sh:property [ sh:path ex:name ; sh:maxCount 0 ; sh:message "bad\u0001" ] ."#,
            "\"bad\\u0001\"".to_owned(),
        ),
    ];
    for (shape, unsafe_term) in cases {
        let ttl = format!("{prefix}\n{shape}");
        let report = validate_checked(&g, &ttl);
        assert!(!report.results.is_empty(), "{ttl}");
        let mut out = Vec::new();
        match report.write(&mut out, RdfFormat::RdfXml) {
            Err(GraphError::Rdf(RdfError::XmlUnsafeReport(term))) => {
                assert_eq!(term, unsafe_term, "{ttl}")
            }
            other => panic!("{ttl}: {other:?}"),
        }
        assert!(out.is_empty(), "{ttl}: nothing is written");
        for format in [RdfFormat::Turtle, RdfFormat::NTriples] {
            assert_write_round_trip(&report, format);
        }
    }
    let max_length = |node: &str| {
        format!(
            "{prefix}\nex:S sh:targetNode ex:{node} ; \
             sh:property [ sh:path ex:name ; sh:maxLength 2 ] ."
        )
    };
    let error = shapes(&max_length("a"))
        .validate(&g)
        .unwrap()
        .write(Vec::new(), RdfFormat::RdfXml)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "{cr} cannot be written in an RDF/XML SHACL report, which cannot hold its control \
             characters unchanged; use Turtle, N-Triples or JSON-LD"
        )
    );
    // a tab is fine
    let report = validate_checked(&g, &max_length("b"));
    assert_eq!(report.results.len(), 1);
    assert_write_round_trip(&report, RdfFormat::RdfXml);
}

/// Severities and messages.
#[test]
fn severities_and_messages() {
    let g = PersistentGraph::new();
    g.load_rdf(
        1,
        "<http://ex/a> <http://ex/p> <http://ex/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let report = shapes(
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:a ; sh:severity sh:Warning ;
            sh:message "zu viele"@de , "too many"@en ; sh:property ex:P .
        ex:P sh:path ex:p ; sh:maxCount 0 ; sh:severity sh:Info ; sh:message "none allowed" .
        ex:D sh:targetNode ex:a ; sh:deactivated true ; sh:property [ sh:path ex:p ; sh:maxCount 0 ] ."#,
    )
    .validate(&g)
    .unwrap();
    assert!(!report.conforms);
    assert_eq!(report.results.len(), 1);
    let result = &report.results[0];
    assert_eq!(result.severity.as_str(), format!("{SH}Info"));
    assert_eq!(
        result.messages,
        [Literal::new_simple_literal("none allowed")]
    );
    assert_eq!(
        result.source_shape.as_ref().map(Term::to_string).as_deref(),
        Some("<http://ex/P>")
    );
    // a count has no value
    assert_eq!(result.value, None);
    assert_write_round_trip(&report, RdfFormat::Turtle);
}

/// Validation holds no lock between the triples it reads: it runs while another thread writes.
#[test]
fn validation_alongside_a_writer() {
    let pg = temporal_graph();
    let s = shapes(SHAPES);
    within_60s(move || {
        let writer = {
            let pg = pg.clone();
            std::thread::spawn(move || {
                for i in 0..20_000 {
                    pg.add_edge(
                        10 + i,
                        format!("http://ex/n{i}"),
                        "http://ex/Person".to_owned(),
                        NO_PROPS,
                        Some(rdf::TYPE.as_str()),
                    )
                    .unwrap();
                }
            })
        };
        let mut validations = 0;
        while !writer.is_finished() || validations == 0 {
            s.validate(&pg).unwrap();
            validations += 1;
        }
        writer.join().unwrap();
        // every new person misses a name
        assert_eq!(s.validate(&pg).unwrap().results.len(), 20_000 + 4);
    });
}

/// Validations from many threads at once share the pool.
#[test]
fn concurrent_validations() {
    let pg = temporal_graph();
    let s = shapes(SHAPES);
    let expected = s.validate(&pg).unwrap();
    within_60s(move || {
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..5 {
                        assert_eq!(s.validate(&pg).unwrap(), expected);
                    }
                });
            }
        });
    });
}

/// A `read_only()` view is validated as it was locked while a writer waits.
#[test]
fn read_only_view_while_a_writer_waits() {
    let pg = temporal_graph();
    let handle = pg.snapshot_at(3);
    let locked = pg.read_only();
    let writer = {
        let pg = pg.clone();
        std::thread::spawn(move || {
            pg.load_rdf(
                20,
                "<http://ex/carol> a <http://ex/Person> .".as_bytes(),
                RdfFormat::Turtle,
                None,
            )
            .unwrap();
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(300));
    let s = shapes(SHAPES);
    let report = within_60s(move || {
        let report = s.validate(&locked).unwrap();
        drop(locked);
        report
    });
    assert_eq!(report.results.len(), 4);
    writer.join().unwrap();
    assert_eq!(shapes(SHAPES).validate(&pg).unwrap().results.len(), 5);
    assert!(shapes(SHAPES).validate(&handle).unwrap().conforms);
}

#[test]
fn shapes_are_send_sync_and_clone() {
    fn check<T: Send + Sync + Clone + Debug>(_: &T) {}
    let s = shapes(SHAPES);
    check(&s);
    let t = s.clone();
    assert_eq!(
        t.validate(&temporal_graph()).unwrap(),
        s.validate(&temporal_graph()).unwrap()
    );
    assert!(format!("{s:?}").starts_with("ShaclShapes"));
}

/// Run with `cargo test --profile build-fast -p raphtory --features shacl --lib
/// rdf::tests::shacl::benchmark -- --ignored --nocapture`.
#[test]
#[ignore = "benchmark"]
fn benchmark() {
    let s = shapes(SHAPES);
    for n in [10_000usize, 50_000] {
        let pg = PersistentGraph::new();
        let mut nt = String::new();
        for i in 0..n {
            nt.push_str(&format!("<http://ex/p{i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Person> .\n"));
            nt.push_str(&format!("<http://ex/p{i}> <http://ex/name> \"P{i}\" .\n"));
            nt.push_str(&format!("<http://ex/p{i}> <http://ex/age> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n", i % 90));
            nt.push_str(&format!(
                "<http://ex/p{i}> <http://ex/knows> <http://ex/p{}> .\n",
                (i + 1) % n
            ));
        }
        pg.load_rdf(1, nt.as_bytes(), RdfFormat::NTriples, None)
            .unwrap();
        let start = Instant::now();
        let report = s.validate(&pg).unwrap();
        let validate = start.elapsed();
        let start = Instant::now();
        let export = validate_export(&pg, SHAPES);
        let export_path = start.elapsed();
        assert!(report.conforms && export.0);
        eprintln!(
            "SHACL benchmark: {} triples: validate {validate:?}, export and validate in memory {export_path:?}",
            4 * n
        );
    }
}
