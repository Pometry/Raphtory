//! The name <-> term bijections (node names and layer names) and `literal_to_prop`.
use crate::{
    mapping::{layer_name_of, layer_term, literal_to_prop},
    name_of, term_of, Literal, NamedNode, RdfFormat, Term, Triple, RESERVED_NS,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use oxigraph::{
    io::RdfParser,
    model::{vocab::xsd, BlankNode, NamedNodeRef, NamedOrBlankNode},
};
use proptest::prelude::*;
use raphtory::prelude::Prop;
use spareval::ExpressionTerm;
use std::str::FromStr;

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

/// Every term `term_of` produces can be written in N-Triples and Turtle (as an object, and as
/// a subject or predicate where RDF allows) and read back by the parsers `load_rdf` uses.
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
        // absolute IRIs not under the reserved namespace
        "[a-z][a-z0-9+.-]{0,5}:[a-zA-Z0-9/:@._~-]{0,12}".prop_filter_map("valid IRI", |s| {
            (!s.starts_with(RESERVED_NS))
                .then(|| NamedNode::new(s).ok().map(Term::from))
                .flatten()
        }),
        "http://ex/\\PC{0,8}"
            .prop_filter_map("valid IRI", |s| NamedNode::new(s).ok().map(Term::from)),
        // canonical terms under the reserved namespace
        any::<String>().prop_map(|n| term_of(&n)),
        // blank nodes
        "[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,8}"
            .prop_filter_map("valid label", |s| BlankNode::new(s).ok().map(Term::from)),
        any::<u128>().prop_map(|id| BlankNode::new_from_unique_id(id).into()),
        // literals
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
    /// `name_of(term_of(n)) == Some(n)` for any string.
    #[test]
    fn name_term_name_round_trip(name in names()) {
        assert_round_trip(&name);
    }

    /// `term_of(name_of(t)) == t` for canonical terms.
    #[test]
    fn term_name_term_round_trip(term in canonical_terms()) {
        let name = name_of(term.as_ref());
        prop_assert!(name.is_some(), "no name for {}", term);
        prop_assert_eq!(term_of(&name.unwrap()), term.clone());
        assert_parses_back(&term);
    }
}

/// The printable characters and NUL that Windows or Unix do not allow in file names.
const PATH_UNSAFE: [char; 10] = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\0'];

fn assert_layer_round_trip(name: &str) {
    let term = layer_term(name);
    assert_eq!(
        layer_name_of(term.as_ref()).as_deref(),
        Some(name),
        "layer {name:?} -> term {term}"
    );
    if let Term::NamedNode(n) = &term {
        assert!(NamedNode::new(n.as_str()).is_ok(), "invalid IRI {n}");
        if !n.as_str().starts_with(RESERVED_NS) {
            assert!(
                !name.contains(PATH_UNSAFE),
                "layer name {name:?} of {n} is not path safe"
            );
        }
    }
    if !name.contains(['/', '%', ':', '?', '*']) || name.starts_with('"') || name.starts_with("_:")
    {
        assert_eq!(term, term_of(name), "{name:?} changed term");
    }
    assert_parses_back(&term);
}

/// Names that are, or are close to, escaped IRIs.
fn layer_names() -> impl Strategy<Value = String> {
    prop_oneof![
        names(),
        "[a-z]{1,4}(:|%3A|%3a)([a-z0-9:?*@._~ ]|%2F|%25|%3A|%3F|%2A|%2f|%3a|%3f|%2a|%20|%41|%2|%3|%|/|\\\\){0,8}",
        "raphtory(:|%3A)([a-z]|%2F|%25|%3A|%3F|%2A|/|:|\\?|\\*){0,6}",
        layer_iris().prop_map(|iri| layer_name_of(iri.as_ref().into()).unwrap()),
    ]
}

/// IRIs not under the reserved namespace, including ones with `?`, `*`, `%2F`, `%25`, `%3A` and
/// other escapes.
fn layer_iris() -> impl Strategy<Value = NamedNode> {
    prop_oneof![
        "[a-z][a-z0-9+.-]{0,5}:[a-zA-Z0-9/:?*@._~-]{0,12}",
        "http://ex/([a-z/:?*]|%2F|%25|%3A|%3F|%2A|%2f|%3a|%3f|%2a|%41|%20){0,8}",
        "http://ex/\\PC{0,8}",
        "urn:[a-z]{0,4}(%2[5FA]|%3[AF]){0,3}",
    ]
    .prop_filter_map("valid IRI", |s| {
        (!s.starts_with(RESERVED_NS))
            .then(|| NamedNode::new(s).ok())
            .flatten()
    })
}

proptest! {
    /// `layer_name_of(layer_term(n)) == Some(n)` for any string.
    #[test]
    fn layer_name_term_name_round_trip(name in layer_names()) {
        assert_layer_round_trip(&name);
    }

    /// The layer of every IRI predicate round-trips and is safe as a file name.
    #[test]
    fn layer_iri_round_trip(iri in layer_iris()) {
        let term = Term::from(iri.clone());
        let name = layer_name_of(term.as_ref());
        prop_assert!(name.is_some(), "no layer name for {}", iri);
        let name = name.unwrap();
        prop_assert!(!name.contains(PATH_UNSAFE), "{:?}", name);
        prop_assert_eq!(layer_term(&name), term);
    }
}

/// Rows of the layer mapping table.
#[test]
fn layer_mapping_table() {
    let cases: Vec<(&str, Term)> = vec![
        ("http%3A%2F%2Fex%2Fknows", iri("http://ex/knows")),
        ("http%3A%2F%2Fex%2Fa%252Fb", iri("http://ex/a%2Fb")),
        ("_default", iri("raphtory:_default")),
        // a valid IRI, but not an escaped one
        ("http://ex/p", iri("raphtory:http%3A%2F%2Fex%2Fp")),
        ("urn%3ax", iri("raphtory:urn%253ax")),
        ("_:b1", BlankNode::new("b1").unwrap().into()),
        ("\"x\"", Literal::new_simple_literal("x").into()),
    ];
    for (name, term) in cases {
        assert_eq!(layer_term(name), term, "layer_term({name:?})");
        assert_eq!(
            layer_name_of(term.as_ref()).as_deref(),
            Some(name),
            "layer_name_of({term})"
        );
        assert_layer_round_trip(name);
    }
}

/// Rows of the mapping table.
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

fn typed(value: &str, datatype: NamedNodeRef<'_>) -> Literal {
    Literal::new_typed_literal(value, datatype)
}

/// `literal_to_prop`.
#[test]
fn literal_to_prop_conversions() {
    let utc = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
    let too_long = typed("123456789012345678901.123456789012345678", xsd::DECIMAL);
    let cases: Vec<(Literal, Option<Prop>)> = vec![
        (Literal::new_simple_literal("x"), Some(Prop::str("x"))),
        (
            Literal::new_language_tagged_literal("x", "en").unwrap(),
            Some(Prop::str("x")),
        ),
        (typed("true", xsd::BOOLEAN), Some(Prop::Bool(true))),
        (typed("042", xsd::INTEGER), Some(Prop::I64(42))),
        (
            typed("1.50", xsd::DECIMAL),
            Some(Prop::Decimal(BigDecimal::from_str("1.5").unwrap())),
        ),
        // 38 significant digits fit in a `Prop::Decimal`
        (
            typed("12345678901234567890.123456789012345678", xsd::DECIMAL),
            Some(Prop::Decimal(
                BigDecimal::from_str("12345678901234567890.123456789012345678").unwrap(),
            )),
        ),
        // 39 do not
        (too_long.clone(), None),
        (typed("1.5E0", xsd::DOUBLE), Some(Prop::F64(1.5))),
        (
            typed("2024-01-01T00:00:00Z", xsd::DATE_TIME),
            Some(Prop::DTime(utc("2024-01-01T00:00:00Z"))),
        ),
        (
            typed("2024-01-01T12:34:56", xsd::DATE_TIME),
            Some(Prop::NDTime(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 34, 56)
                    .unwrap(),
            )),
        ),
        (
            typed("x", NamedNode::new("http://ex/type").unwrap().as_ref()),
            None,
        ),
    ];
    for (literal, expected) in cases {
        assert_eq!(literal_to_prop(&literal), expected, "{literal}");
    }
    // the 39-digit decimal is a valid `xsd:decimal`, so its `None` comes from the size check
    assert!(matches!(
        ExpressionTerm::from(Term::from(too_long)),
        ExpressionTerm::DecimalLiteral(_)
    ));
}
