//! The bijections between Raphtory names and RDF terms: one for node names, one for layer
//! names.
//!
//! [`term_of`] maps every node name to an RDF term and [`name_of`] is its checked inverse:
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
//! [`layer_term`] and [`layer_name_of`] do the same for layer names, except that the layer of a
//! predicate IRI is named by the IRI with `%`, `/`, `:`, `?` and `*` percent-escaped (`%25`,
//! `%2F`, `%3A`, `%3F` and `%2A`), so `<http://ex/knows>` is the layer `http%3A%2F%2Fex%2Fknows`.
//! Layer names must be safe as file names on every platform, because the storage may use them
//! in folder names. A layer name starting with `"` or `_:` follows the literal and blank-node
//! rules above, and any other layer name that is not such an escaped IRI becomes
//! `<raphtory:pct_encode(n)>`, like a node name. So the layer of `<raphtory:rest>` is
//! `pct_decode(rest)`, which may contain characters not allowed in file names; writes reject
//! those.
//!
//! RDF 1.2 terms are never the term of a name, so the mappings do not depend on whether
//! oxigraph was built with RDF 1.2. `name_of(term_of(n).as_ref()) == Some(n)` and
//! `layer_name_of(layer_term(n).as_ref()) == Some(n)` for every `n`, and the inverses return
//! `Some` only for terms their mapping can produce.
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use oxigraph::model::{
    vocab::rdf, BlankNode, Literal, LiteralRef, NamedNode, NamedNodeRef, Term, TermRef,
};
use percent_encoding::{
    percent_decode_str, utf8_percent_encode, AsciiSet, PercentEncode, NON_ALPHANUMERIC,
};
use raphtory_api::core::entities::properties::prop::Prop;
use spareval::ExpressionTerm;
use std::{borrow::Cow, str::FromStr};

/// The IRI prefix used for Raphtory names that are not themselves RDF terms.
///
/// For example the node `Alice Smith` is `<raphtory:Alice%20Smith>` and the default layer is
/// `<raphtory:_default>`.
pub const RESERVED_NS: &str = "raphtory:";

/// Maps a node name to its RDF term. Total and injective. Layer names use [`layer_term`].
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

/// Maps an RDF term back to the node name it stands for.
///
/// This is the checked inverse of [`term_of`]: it returns `Some(n)` only if
/// `term_of(&n) == t`. Non-canonical terms (for example `<raphtory:%61>`, lower-case
/// percent-escapes, a raw `:` after `raphtory:`, or terms built with `*_unchecked`
/// constructors that are not valid) give `None`.
pub fn name_of(t: TermRef<'_>) -> Option<String> {
    let candidate = match t {
        TermRef::NamedNode(n) => match n.as_str().strip_prefix(RESERVED_NS) {
            Some(rest) => pct_decode(rest)?,
            // `term_of` keeps exactly the valid IRIs outside `RESERVED_NS`
            None => return is_valid_iri(n).then(|| n.as_str().to_owned()),
        },
        TermRef::BlankNode(b) => format!("_:{}", b.as_str()),
        TermRef::Literal(l) => l.to_string(),
        #[allow(unreachable_patterns)]
        _ => return None, // RDF 1.2 triple terms
    };
    (term_of(&candidate).as_ref() == t).then_some(candidate)
}

/// Maps a layer name to its RDF term. Total and injective.
///
/// The layer of a predicate IRI is named by the IRI with `%`, `/`, `:`, `?` and `*` written
/// `%25`, `%2F`, `%3A`, `%3F` and `%2A`, so that layer names are valid file names. A name that
/// is not such an escaped IRI (it contains a raw `/`, `:`, `?` or `*` or another `%` sequence,
/// or does not unescape to an IRI outside `raphtory:`) is `<raphtory:pct_encode(name)>`, and
/// names starting with `"` or `_:` follow the literal and blank-node rules of [`term_of`].
///
/// # Example
/// ```
/// use raphtory::rdf::{layer_name_of, layer_term};
/// let knows = layer_term("http%3A%2F%2Fex%2Fknows");
/// assert_eq!(knows.to_string(), "<http://ex/knows>");
/// assert_eq!(layer_name_of(knows.as_ref()).as_deref(), Some("http%3A%2F%2Fex%2Fknows"));
/// assert_eq!(layer_term("_default").to_string(), "<raphtory:_default>");
/// ```
pub fn layer_term(name: &str) -> Term {
    if name.starts_with('"') || name.starts_with("_:") {
        return term_of(name);
    }
    if !name.starts_with(RESERVED_NS) {
        if let Some(iri) = unescape_layer(name) {
            if !iri.starts_with(RESERVED_NS) {
                if let Ok(iri) = NamedNode::new(iri) {
                    return iri.into();
                }
            }
        }
    }
    NamedNode::new_unchecked(format!("{RESERVED_NS}{}", pct_encode(name))).into()
}

/// Maps an RDF term back to the layer name it stands for.
///
/// This is the checked inverse of [`layer_term`]: it returns `Some(n)` only if
/// `layer_term(&n) == t`.
pub fn layer_name_of(t: TermRef<'_>) -> Option<String> {
    let candidate = match t {
        TermRef::NamedNode(n) => match n.as_str().strip_prefix(RESERVED_NS) {
            Some(rest) => pct_decode(rest)?,
            // `layer_term` unescapes the escaped name of a valid IRI back to it
            None => return is_valid_iri(n).then(|| escape_layer(n.as_str()).into_owned()),
        },
        TermRef::BlankNode(b) => format!("_:{}", b.as_str()),
        TermRef::Literal(l) => l.to_string(),
        #[allow(unreachable_patterns)]
        _ => return None, // RDF 1.2 triple terms
    };
    (layer_term(&candidate).as_ref() == t).then_some(candidate)
}

/// The layer name of an IRI that is not under [`RESERVED_NS`]: `%`, `/`, `:`, `?` and `*` as
/// `%25`, `%2F`, `%3A`, `%3F` and `%2A`.
pub(crate) fn escape_layer(iri: &str) -> Cow<'_, str> {
    if !iri.contains(LAYER_ESCAPED) {
        return Cow::Borrowed(iri);
    }
    let mut out = String::with_capacity(iri.len() + 16);
    for c in iri.chars() {
        match c {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            ':' => out.push_str("%3A"),
            '?' => out.push_str("%3F"),
            '*' => out.push_str("%2A"),
            c => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// The characters [`escape_layer`] escapes: the ones an IRI may contain that are not allowed
/// in Windows or Unix file names.
const LAYER_ESCAPED: [char; 5] = ['%', '/', ':', '?', '*'];

/// Inverse of [`escape_layer`]: `None` if `name` has a raw `/`, `:`, `?` or `*`, or a `%` not
/// starting `%25`, `%2F`, `%3A`, `%3F` or `%2A`.
fn unescape_layer(name: &str) -> Option<Cow<'_, str>> {
    if name.contains(['/', ':', '?', '*']) {
        return None;
    }
    if !name.contains('%') {
        return Some(Cow::Borrowed(name));
    }
    let mut parts = name.split('%');
    let mut out = String::with_capacity(name.len());
    out.push_str(parts.next()?);
    for part in parts {
        let c = match part.get(..2)? {
            "25" => '%',
            "2F" => '/',
            "3A" => ':',
            "3F" => '?',
            "2A" => '*',
            _ => return None,
        };
        out.push(c);
        out.push_str(&part[2..]);
    }
    Some(Cow::Owned(out))
}

/// The predicate IRI of the layer named `name`, or `None` if its term ([`layer_term`]) is not
/// an IRI (a layer named like a literal or a blank node gives generalized triples, which RDF
/// exports and SHACL leave out).
pub(crate) fn layer_predicate(name: &str) -> Option<NamedNode> {
    match layer_term(name) {
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

/// Whether `iri` is valid, as [`NamedNode::new`] checks it.
fn is_valid_iri(iri: NamedNodeRef<'_>) -> bool {
    NamedNodeRef::new(iri.as_str()).is_ok()
}

/// The bytes [`pct_encode`] escapes: all but `[A-Za-z0-9-._~]`.
const PCT_ENCODED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encodes every byte outside `[A-Za-z0-9-._~]` as upper-case `%XX`.
fn pct_encode(s: &str) -> PercentEncode<'_> {
    utf8_percent_encode(s, PCT_ENCODED)
}

/// Decodes `%XX` escapes, or `None` if the result is not UTF-8. It also decodes encodings
/// [`pct_encode`] does not give: [`name_of`] and [`layer_name_of`] reject those by encoding the
/// name again.
fn pct_decode(s: &str) -> Option<String> {
    percent_decode_str(s)
        .decode_utf8()
        .ok()
        .map(Cow::into_owned)
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
pub(crate) fn literal_to_prop(literal: &Literal) -> Option<Prop> {
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
