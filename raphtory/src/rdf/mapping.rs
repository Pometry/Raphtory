//! The bijection between Raphtory names (node names and layer names) and RDF terms.
//!
//! [`term_of`] maps every string to an RDF term and [`name_of`] is its checked inverse:
//!
//! - an absolute IRI (anything [`NamedNode::new`] accepts that does not start with
//!   [`RESERVED_NS`]) is kept verbatim,
//! - `_:label` is a blank node if `label` is a blank-node label valid in N-Triples, Turtle and
//!   SPARQL (so it contains no `:`),
//! - a string in the exact (canonical) N-Triples form of a literal is that literal, unless the
//!   literal has a base direction (an RDF 1.2 directional language-tagged string, such as
//!   `"hi"@en--ltr`),
//! - every other string `n` becomes `<raphtory:pct_encode(n)>`.
//!
//! RDF 1.2 terms are never the term of a name, so the mapping does not depend on whether
//! oxigraph was built with RDF 1.2. `name_of(term_of(n).as_ref()) == Some(n)` for every `n`,
//! and `name_of` returns `Some` only for terms `term_of` can produce.
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use oxigraph::model::{vocab::rdf, BlankNode, Literal, LiteralRef, NamedNode, Term, TermRef};
use raphtory_api::core::entities::properties::prop::Prop;
use spareval::ExpressionTerm;
use std::str::FromStr;

/// The IRI prefix used for Raphtory names that are not themselves RDF terms.
///
/// For example the node `Alice Smith` is `<raphtory:Alice%20Smith>` and the default layer is
/// `<raphtory:_default>`.
pub const RESERVED_NS: &str = "raphtory:";

/// Maps a Raphtory name (node name or layer name) to its RDF term. Total and injective.
///
/// A name `_:label` is a blank node only if `label` is valid in N-Triples, Turtle and SPARQL
/// (`_:a:b` is `<raphtory:_%3Aa%3Ab>`).
///
/// # Example
/// ```
/// use raphtory::rdf::{model::Term, term_of};
/// assert_eq!(term_of("http://ex/alice").to_string(), "<http://ex/alice>");
/// assert_eq!(term_of("Alice Smith").to_string(), "<raphtory:Alice%20Smith>");
/// assert_eq!(term_of("_:b1").to_string(), "_:b1");
/// assert!(matches!(term_of("\"x\"@en"), Term::Literal(_)));
/// ```
pub fn term_of(name: &str) -> Term {
    if name.starts_with('"') {
        if let Ok(literal) = Literal::from_str(name) {
            if !is_directional(literal.as_ref()) && literal.to_string() == name {
                return literal.into();
            }
        }
    } else if let Some(id) = name.strip_prefix("_:") {
        // `BlankNode::new` accepts `:` in labels but Turtle and SPARQL do not.
        if !id.contains(':') {
            if let Ok(blank) = BlankNode::new(id) {
                return blank.into();
            }
        }
    } else if !name.starts_with(RESERVED_NS) {
        if let Ok(iri) = NamedNode::new(name) {
            return iri.into();
        }
    }
    NamedNode::new_unchecked(format!("{RESERVED_NS}{}", pct_encode(name))).into()
}

/// Maps an RDF term back to the Raphtory name it stands for.
///
/// This is the checked inverse of [`term_of`]: it returns `Some(n)` only if
/// `term_of(&n) == t`. Non-canonical terms (for example `<raphtory:%61>`, lower-case
/// percent-escapes, a raw `:` after `raphtory:`, or terms built with `*_unchecked`
/// constructors that are not valid) give `None`.
pub fn name_of(t: TermRef<'_>) -> Option<String> {
    let candidate = match t {
        TermRef::NamedNode(n) => match n.as_str().strip_prefix(RESERVED_NS) {
            Some(rest) => pct_decode(rest)?,
            None => n.as_str().to_owned(),
        },
        TermRef::BlankNode(b) => format!("_:{}", b.as_str()),
        TermRef::Literal(l) => l.to_string(),
        #[allow(unreachable_patterns)]
        _ => return None, // RDF 1.2 triple terms
    };
    (term_of(&candidate).as_ref() == t).then_some(candidate)
}

/// The predicate IRI of the layer named `name`, or `None` if its term ([`term_of`]) is not an
/// IRI (a layer named like a literal or a blank node gives generalized triples, which RDF
/// exports and SHACL leave out).
pub(crate) fn layer_predicate(name: &str) -> Option<NamedNode> {
    match term_of(name) {
        Term::NamedNode(iri) => Some(iri),
        _ => None,
    }
}

/// Whether `literal` is an RDF 1.2 directional language-tagged string (`"hi"@en--ltr`).
pub(crate) fn is_directional(literal: LiteralRef<'_>) -> bool {
    literal.language().is_some() && literal.datatype() != rdf::LANG_STRING
}

/// Whether `t` is an RDF 1.1 term (an IRI, a blank node or a literal without a base
/// direction); only those can be stored.
pub(crate) fn is_rdf11_term(t: TermRef<'_>) -> bool {
    match t {
        TermRef::NamedNode(_) | TermRef::BlankNode(_) => true,
        TermRef::Literal(literal) => !is_directional(literal),
        #[allow(unreachable_patterns)]
        _ => false, // RDF 1.2 triple terms
    }
}

#[inline]
fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

/// Percent-encodes every byte outside `[A-Za-z0-9-._~]` as upper-case `%XX`.
pub(crate) fn pct_encode(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out
}

/// Inverse of [`pct_encode`]. Accepts only unreserved bytes and `%XX` escapes with upper-case
/// hex digits, and the decoded bytes must be valid UTF-8; anything else gives `None`.
pub(crate) fn pct_decode(s: &str) -> Option<String> {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let hi = hex(*bytes.get(i + 1)?)?;
            let lo = hex(*bytes.get(i + 2)?)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else if is_unreserved(b) {
            out.push(b);
            i += 1;
        } else {
            return None;
        }
    }
    String::from_utf8(out).ok()
}

/// Converts a literal to a Raphtory [`Prop`] by its value, or `None` if its datatype is not
/// supported or its lexical form is invalid.
///
/// | Literal | `Prop` |
/// |---|---|
/// | simple, `xsd:string`, language-tagged (tag dropped) | `Str` |
/// | `xsd:boolean` | `Bool` |
/// | `xsd:integer` and its subtypes (when it fits in an `i64`, so not an `xsd:unsignedLong` above `i64::MAX`) | `I64` |
/// | `xsd:decimal` (when it has at most 18 digits after the decimal point, ignoring trailing zeros, at most 38 significant digits and an absolute value below 1.7 × 10^20) | `Decimal` |
/// | `xsd:float`, `xsd:double` | `F64` |
/// | `xsd:dateTime` | `DTime` with a timezone, otherwise `NDTime` |
pub fn literal_to_prop(literal: &Literal) -> Option<Prop> {
    match ExpressionTerm::from(Term::Literal(literal.clone())) {
        ExpressionTerm::StringLiteral(value) => Some(Prop::str(value)),
        ExpressionTerm::LangStringLiteral { value, .. } => Some(Prop::str(value)),
        ExpressionTerm::BooleanLiteral(value) => Some(Prop::Bool(value.into())),
        ExpressionTerm::IntegerLiteral(value) => Some(Prop::I64(value.into())),
        // spareval's decimal is an i128 with 18 fractional digits; `Prop::Decimal` holds 38.
        ExpressionTerm::DecimalLiteral(value) => BigDecimal::from_str(&value.to_string())
            .ok()
            .and_then(|bd| Prop::try_from_bd(bd).ok()),
        ExpressionTerm::FloatLiteral(value) => Some(Prop::F64(value.into())),
        ExpressionTerm::DoubleLiteral(value) => Some(Prop::F64(value.into())),
        ExpressionTerm::DateTimeLiteral(value) => {
            let lexical = value.to_string();
            if value.timezone().is_some() {
                DateTime::parse_from_rfc3339(&lexical)
                    .ok()
                    .map(|dt| Prop::DTime(dt.with_timezone(&Utc)))
            } else {
                NaiveDateTime::parse_from_str(&lexical, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(Prop::NDTime)
            }
        }
        #[allow(unreachable_patterns)]
        _ => None,
    }
}
