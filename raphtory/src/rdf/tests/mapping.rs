//! The name <-> term bijection and `literal_to_prop`.
use crate::{
    prelude::Prop,
    rdf::{
        literal_to_prop,
        mapping::{pct_decode, pct_encode},
        model::{vocab::xsd, BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple},
        name_of, term_of, RdfFormat, RdfParser, RESERVED_NS,
    },
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use proptest::prelude::*;
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
            let parsed: Vec<Triple> = RdfParser::from_format(format)
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

    #[test]
    fn pct_codec_round_trip(s in any::<String>()) {
        let encoded = pct_encode(&s);
        prop_assert!(encoded.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._~%".contains(&b)));
        prop_assert_eq!(pct_decode(&encoded), Some(s));
    }
}

#[test]
fn round_trip_edge_cases() {
    for name in [
        "",
        "\"",
        "\"\"",
        "\"x",
        "\"x\" ",
        " \"x\"",
        "\"x\"@EN",
        "\"x\"^^<http://www.w3.org/2001/XMLSchema#string>",
        "_:",
        "_:a b",
        "_:a",
        "_:a:b",
        "_::x",
        "_:x:",
        "_:1x",
        "_:a.b",
        "raphtory:",
        "raphtory:a",
        "raphtory:%61",
        "raphtory:asof:2024",
        "RAPHTORY:x",
        "%",
        "%2F",
        "a/b",
        "日本語",
        "\u{0}",
        "Alice\nSmith",
        "http://ex/a b",
        "http://ex/a",
        "42",
        "-1",
        "true",
    ] {
        assert_round_trip(name);
    }
}

/// Every row of the mapping table.
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
    }
    // literals keep their exact lexical form
    assert_ne!(
        term_of("\"042\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        term_of("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
    );
}

/// Non-canonical terms have no name.
#[test]
fn non_canonical_terms_have_no_name() {
    let terms: Vec<Term> = vec![
        iri("raphtory:%61"),
        iri("raphtory:%2f"),
        iri("raphtory:asof:2024"),
        iri("raphtory:a:b"),
        Literal::new_language_tagged_literal_unchecked("x", "EN").into(),
        NamedNode::new_unchecked("http://ex/a b").into(),
        NamedNode::new_unchecked("raphtory:%").into(),
        NamedNode::new_unchecked("raphtory:%FF").into(), // not UTF-8
        BlankNode::new_unchecked("a b").into(),
        // valid in N-Triples, but `_:a:b` is minted under `raphtory:`
        BlankNode::new("a:b").unwrap().into(),
    ];
    for term in terms {
        assert_eq!(name_of(term.as_ref()), None, "{term}");
    }
}

#[test]
fn pct_codec() {
    assert_eq!(pct_encode("Alice Smith"), "Alice%20Smith");
    assert_eq!(pct_encode("a:b/c\"@"), "a%3Ab%2Fc%22%40");
    assert_eq!(pct_encode("é"), "%C3%A9");
    assert_eq!(pct_encode("AZaz09-._~"), "AZaz09-._~");
    assert_eq!(pct_decode("%C3%A9").as_deref(), Some("é"));
    assert_eq!(pct_decode("%2F").as_deref(), Some("/"));
    assert_eq!(pct_decode("%61").as_deref(), Some("a")); // rejected later by name_of
    assert_eq!(pct_decode("%2f"), None);
    assert_eq!(pct_decode("a:b"), None);
    assert_eq!(pct_decode("%2"), None);
    assert_eq!(pct_decode("%G0"), None);
    assert_eq!(pct_decode("%FF"), None);
    assert_eq!(pct_decode("").as_deref(), Some(""));
}

fn typed(value: &str, datatype: oxigraph::model::NamedNodeRef<'_>) -> Literal {
    Literal::new_typed_literal(value, datatype)
}

/// `literal_to_prop`.
#[test]
fn literal_to_prop_conversions() {
    let utc = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
    let cases: Vec<(Literal, Option<Prop>)> = vec![
        (Literal::new_simple_literal("x"), Some(Prop::str("x"))),
        (typed("x", xsd::STRING), Some(Prop::str("x"))),
        (
            Literal::new_language_tagged_literal("x", "en").unwrap(),
            Some(Prop::str("x")),
        ),
        (typed("true", xsd::BOOLEAN), Some(Prop::Bool(true))),
        (typed("0", xsd::BOOLEAN), Some(Prop::Bool(false))),
        (typed("42", xsd::INTEGER), Some(Prop::I64(42))),
        (typed("042", xsd::INTEGER), Some(Prop::I64(42))),
        (typed("-7", xsd::INT), Some(Prop::I64(-7))),
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
        (typed("2.5", xsd::FLOAT), Some(Prop::F64(2.5))),
        (typed("1.5E0", xsd::DOUBLE), Some(Prop::F64(1.5))),
        (
            typed("2024-01-01T00:00:00Z", xsd::DATE_TIME),
            Some(Prop::DTime(utc("2024-01-01T00:00:00Z"))),
        ),
        (
            typed("2024-01-01T01:30:00.5+01:00", xsd::DATE_TIME),
            Some(Prop::DTime(utc("2024-01-01T00:30:00.5Z"))),
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
        // 18 fractional digits fit (trailing zeros do not count)
        (
            typed("1.123456789012345678", xsd::DECIMAL),
            Some(Prop::Decimal(
                BigDecimal::from_str("1.123456789012345678").unwrap(),
            )),
        ),
        (
            typed("1.50000000000000000000000", xsd::DECIMAL),
            Some(Prop::Decimal(BigDecimal::from_str("1.5").unwrap())),
        ),
        (
            typed(&i64::MAX.to_string(), xsd::UNSIGNED_LONG),
            Some(Prop::I64(i64::MAX)),
        ),
        // fallbacks
        (typed("99999999999999999999", xsd::INTEGER), None),
        // an `xsd:unsignedLong` above `i64::MAX`
        (typed("18446744073709551615", xsd::UNSIGNED_LONG), None),
        // spareval keeps at most 18 fractional digits, and absolute values below about 1.7e20
        (typed("1.1234567890123456789", xsd::DECIMAL), None),
        (typed("12345678901234567890123.5", xsd::DECIMAL), None),
        (typed("999999999999999999999", xsd::DECIMAL), None),
        // a valid `xsd:decimal` with 39 significant digits does not fit in a `Prop::Decimal`
        (
            typed("123456789012345678901.123456789012345678", xsd::DECIMAL),
            None,
        ),
        (typed("abc", xsd::INTEGER), None),
        (typed("2024-01-01", xsd::DATE), None),
        (
            typed("x", NamedNode::new("http://ex/type").unwrap().as_ref()),
            None,
        ),
    ];
    for (literal, expected) in cases {
        assert_eq!(literal_to_prop(&literal), expected, "{literal}");
    }
    // the 39-digit decimal is a valid `xsd:decimal`, so its `None` comes from the size check
    let too_long = typed("123456789012345678901.123456789012345678", xsd::DECIMAL);
    assert!(matches!(
        ExpressionTerm::from(Term::from(too_long)),
        ExpressionTerm::DecimalLiteral(_)
    ));
}
