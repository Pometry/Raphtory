//! The name <-> term bijections (node names and layer names).
use crate::{
    mapping::{iri_of, layer_name_of},
    name_of, term_of, Literal, NamedNode, RdfFormat, Term, Triple, RESERVED_NS,
};
use oxigraph::{
    io::RdfParser,
    model::{vocab::xsd, BlankNode, NamedOrBlankNode},
};
use proptest::prelude::*;

fn iri(s: &str) -> Term {
    NamedNode::new(s).unwrap().into()
}

fn assert_round_trip(name: &str) {
    let term = term_of(name);
    assert_eq!(
        name_of(term.as_ref()).as_deref(),
        Some(name),
        "name {name:?} -> term {term}"
    );
    if let Term::NamedNode(n) = &term {
        // terms minted under `raphtory:` use `new_unchecked`, so check they are valid IRIs
        assert!(NamedNode::new(n.as_str()).is_ok(), "invalid IRI {n}");
    }
    assert_parses_back(&term);
}

fn assert_parses_back(term: &Term) {
    let s = NamedNode::new_unchecked("http://ex/s");
    let p = NamedNode::new_unchecked("http://ex/p");
    let mut triples = vec![Triple::new(s.clone(), p.clone(), term.clone())];
    if let Ok(subject) = NamedOrBlankNode::try_from(term.clone()) {
        triples.push(Triple::new(subject, p.clone(), s.clone()));
    }
    if let Term::NamedNode(predicate) = term {
        triples.push(Triple::new(s.clone(), predicate.clone(), s.clone()));
    }
    for format in [RdfFormat::NTriples, RdfFormat::Turtle] {
        for triple in &triples {
            let doc = format!("{triple} .\n");
            let parsed: Vec<Triple> = RdfParser::from_format(format.to_oxigraph())
                .for_reader(doc.as_bytes())
                .map(|quad| quad.map(Triple::from))
                .collect::<Result<_, _>>()
                .unwrap_or_else(|e| panic!("{doc:?} does not parse as {format}: {e}"));
            assert_eq!(parsed, vec![triple.clone()], "{format}");
        }
    }
}

fn names() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<String>(),
        "\\PC*",
        Just(String::new()),
        any::<String>().prop_map(|s| format!("\"{s}")),
        any::<String>().prop_map(|s| format!("\"{s}\"")),
        "\"[a-zA-Z \\\\\"]{0,6}\"",
        "\"[a-z]{0,4}\"@[a-zA-Z]{1,3}(-[a-zA-Z0-9]{1,4})?",
        "\"[+-]?[0-9]{0,4}\"\\^\\^<http://www\\.w3\\.org/2001/XMLSchema#integer>",
        "\"[a-z]{0,4}\"\\^\\^<http://www\\.w3\\.org/2001/XMLSchema#string>",
        any::<String>().prop_map(|s| format!("_:{s}")),
        "_:[a-zA-Z0-9_.-]{0,8}",
        "_:[a-z0-9:]{0,6}",
        any::<String>().prop_map(|s| format!("{RESERVED_NS}{s}")),
        "raphtory:[a-zA-Z0-9:%/]{0,10}",
        "[a-zA-Z][a-zA-Z0-9+.-]{0,5}:[a-zA-Z0-9/:@%._~ -]{0,12}",
        "[0-9]{1,6}",
    ]
}

fn canonical_terms() -> impl Strategy<Value = Term> {
    prop_oneof![
        "[a-z][a-z0-9+.-]{0,5}:[a-zA-Z0-9/:@._~-]{0,12}".prop_filter_map("valid IRI", |s| {
            (!s.starts_with(RESERVED_NS))
                .then(|| NamedNode::new(s).ok().map(Term::from))
                .flatten()
        }),
        "http://ex/\\PC{0,8}"
            .prop_filter_map("valid IRI", |s| NamedNode::new(s).ok().map(Term::from)),
        any::<String>().prop_map(|n| term_of(&n)),
        "[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,8}"
            .prop_filter_map("valid label", |s| BlankNode::new(s).ok().map(Term::from)),
        any::<u128>().prop_map(|id| BlankNode::new_from_unique_id(id).into()),
        any::<String>().prop_map(|v| Literal::new_simple_literal(v).into()),
        "[\\\\\"\n\r\t\u{8}\u{c}\u{0}\u{7f}a-z]{0,6}"
            .prop_map(|v| Literal::new_simple_literal(v).into()),
        (any::<String>(), "[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8}){0,2}").prop_filter_map(
            "valid language tag",
            |(v, l)| Literal::new_language_tagged_literal(v, l)
                .ok()
                .map(Term::from)
        ),
        "[+-]?0*[0-9]{1,6}".prop_map(|v| Literal::new_typed_literal(v, xsd::INTEGER).into()),
        any::<String>().prop_map(|v| Literal::new_typed_literal(v, xsd::STRING).into()),
        (
            any::<String>(),
            prop_oneof![
                Just(xsd::DECIMAL),
                Just(xsd::DOUBLE),
                Just(xsd::DATE_TIME),
                Just(xsd::INT),
                Just(xsd::BOOLEAN),
            ]
        )
            .prop_map(|(v, dt)| Literal::new_typed_literal(v, dt).into()),
        "\\PC{0,6}".prop_map(|v| Literal::new_typed_literal(
            v,
            NamedNode::new_unchecked("http://ex/type")
        )
        .into()),
    ]
}

proptest! {
    #[test]
    fn name_term_name_round_trip(name in names()) {
        assert_round_trip(&name);
    }

    #[test]
    fn term_name_term_round_trip(term in canonical_terms()) {
        let name = name_of(term.as_ref());
        prop_assert!(name.is_some(), "no name for {}", term);
        prop_assert_eq!(term_of(&name.unwrap()), term.clone());
        assert_parses_back(&term);
    }
}

fn assert_layer_round_trip(name: &str) {
    let predicate = iri_of(name);
    assert_eq!(
        layer_name_of(predicate.as_ref().into()).as_deref(),
        Some(name),
        "layer {name:?} -> {predicate}"
    );
    assert!(
        NamedNode::new(predicate.as_str()).is_ok(),
        "invalid IRI {predicate}"
    );
    assert_parses_back(&predicate.into());
}

fn layer_iris() -> impl Strategy<Value = NamedNode> {
    "[a-z][a-z0-9+.-]{0,5}:[a-zA-Z0-9/:?*@%._~-]{0,12}|http://ex/\\PC{0,8}".prop_filter_map(
        "valid IRI",
        |s| {
            NamedNode::new(s)
                .ok()
                .filter(|iri| !iri.as_str().starts_with(RESERVED_NS))
        },
    )
}

proptest! {
    #[test]
    fn layer_name_term_name_round_trip(name in names()) {
        assert_layer_round_trip(&name);
    }

    #[test]
    fn layer_iri_round_trip(iri in layer_iris()) {
        prop_assert_eq!(layer_name_of(iri.as_ref().into()), Some(iri.as_str().to_owned()));
        prop_assert_eq!(iri_of(iri.as_str()), iri);
    }
}

#[test]
fn layer_mapping_table() {
    let cases = [
        ("http://ex/knows", "http://ex/knows"),
        ("knows", "raphtory:knows"),
        ("_default", "raphtory:_default"),
        ("2024", "raphtory:2024"),
        ("raphtory:x", "raphtory:raphtory%3Ax"),
        ("_:b1", "raphtory:_%3Ab1"),
        ("\"x\"", "raphtory:%22x%22"),
    ];
    for (name, predicate) in cases {
        assert_eq!(iri_of(name).as_str(), predicate, "iri_of({name:?})");
        assert_layer_round_trip(name);
    }
}

#[test]
fn mapping_table() {
    let cases: Vec<(&str, Term)> = vec![
        ("http://ex/alice", iri("http://ex/alice")),
        ("mailto:a@b", iri("mailto:a@b")),
        ("user:42", iri("user:42")),
        ("Alice", iri("raphtory:Alice")),
        ("Alice Smith", iri("raphtory:Alice%20Smith")),
        ("12:30", iri("raphtory:12%3A30")),
        ("42", iri("raphtory:42")),
        ("true", iri("raphtory:true")),
        ("", iri("raphtory:")),
        ("raphtory:x", iri("raphtory:raphtory%3Ax")),
        ("_:b1", BlankNode::new("b1").unwrap().into()),
        ("_:bad label", iri("raphtory:_%3Abad%20label")),
        // `:` is valid in N-Triples labels but not in Turtle or SPARQL ones
        ("_:a:b", iri("raphtory:_%3Aa%3Ab")),
        ("_::x", iri("raphtory:_%3A%3Ax")),
        (
            "\"Alice\"@en",
            Literal::new_language_tagged_literal("Alice", "en")
                .unwrap()
                .into(),
        ),
        ("\"x\"", Literal::new_simple_literal("x").into()),
        (
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            Literal::new_typed_literal("42", xsd::INTEGER).into(),
        ),
        ("\"x\"@EN", iri("raphtory:%22x%22%40EN")),
        // the form of an RDF 1.2 directional literal
        ("\"hi\"@en--ltr", iri("raphtory:%22hi%22%40en--ltr")),
        ("_default", iri("raphtory:_default")),
        (
            "http://xmlns.com/foaf/0.1/knows",
            iri("http://xmlns.com/foaf/0.1/knows"),
        ),
    ];
    for (name, term) in cases {
        assert_eq!(term_of(name), term, "term_of({name:?})");
        assert_eq!(
            name_of(term.as_ref()).as_deref(),
            Some(name),
            "name_of({term})"
        );
        assert_round_trip(name);
    }
    for name in ["raphtory:%61", "\u{0}"] {
        assert_round_trip(name);
    }
    // literals keep their exact lexical form
    assert_ne!(
        term_of("\"042\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        term_of("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
    );
}
