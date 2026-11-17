//! The bijections between Raphtory names and RDF terms, for node names and for layer names.
use oxigraph::model::{
    vocab::rdf, BlankNode, Literal, LiteralRef, NamedNode, NamedNodeRef, Term, TermRef,
};
use percent_encoding::{
    percent_decode_str, utf8_percent_encode, AsciiSet, PercentEncode, NON_ALPHANUMERIC,
};
use std::{borrow::Cow, str::FromStr};

/// The IRI prefix of names that are not RDF terms: `Alice Smith` is `<raphtory:Alice%20Smith>`.
pub const RESERVED_NS: &str = "raphtory:";

/// Maps a node name to its RDF term, injectively.
/// Absolute IRIs, `_:label` blank nodes and canonical N-Triples literals map to themselves.
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
    }
    iri_of(name).into()
}

/// The checked inverse of [`term_of`]: `Some(n)` only if `term_of(&n) == t`.
pub fn name_of(t: TermRef<'_>) -> Option<String> {
    let candidate = match t {
        TermRef::NamedNode(n) => match n.as_str().strip_prefix(RESERVED_NS) {
            Some(rest) => pct_decode(rest)?,
            // `term_of` keeps exactly the valid IRIs outside `RESERVED_NS`
            None => return is_valid_iri(n).then(|| n.as_str().to_owned()),
        },
        TermRef::BlankNode(b) => format!("_:{}", b.as_str()),
        TermRef::Literal(l) => l.to_string(),
        _ => return None, // RDF 1.2 triple terms
    };
    (term_of(&candidate).as_ref() == t).then_some(candidate)
}

/// The Raphtory name of a term: its node name ([`name_of`]), layer name or N-Triples form.
pub fn term_name(term: TermRef<'_>) -> String {
    name_of(term)
        .or_else(|| layer_name_of(term))
        .unwrap_or_else(|| term.to_string())
}

pub(crate) fn iri_of(name: &str) -> NamedNode {
    if !name.starts_with(RESERVED_NS) {
        if let Ok(iri) = NamedNode::new(name) {
            return iri;
        }
    }
    NamedNode::new_unchecked(format!("{RESERVED_NS}{}", pct_encode(name)))
}

pub(crate) fn layer_name_of(t: TermRef<'_>) -> Option<String> {
    let TermRef::NamedNode(n) = t else {
        return None;
    };
    match n.as_str().strip_prefix(RESERVED_NS) {
        Some(rest) => pct_decode(rest).filter(|name| iri_of(name).as_ref() == n),
        // `iri_of` keeps exactly the valid IRIs outside `RESERVED_NS`
        None => is_valid_iri(n).then(|| n.as_str().to_owned()),
    }
}

pub(crate) fn is_directional(literal: LiteralRef<'_>) -> bool {
    literal.language().is_some() && literal.datatype() != rdf::LANG_STRING
}

pub(crate) fn is_rdf11_term(t: TermRef<'_>) -> bool {
    match t {
        TermRef::NamedNode(_) | TermRef::BlankNode(_) => true,
        TermRef::Literal(literal) => !is_directional(literal),
        _ => false, // RDF 1.2 triple terms
    }
}

// `*_unchecked` constructors can build invalid IRIs
fn is_valid_iri(iri: NamedNodeRef<'_>) -> bool {
    NamedNodeRef::new(iri.as_str()).is_ok()
}

const PCT_ENCODED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn pct_encode(s: &str) -> PercentEncode<'_> {
    utf8_percent_encode(s, PCT_ENCODED)
}

// accepts encodings `pct_encode` never gives: callers re-encode to reject them
fn pct_decode(s: &str) -> Option<String> {
    percent_decode_str(s)
        .decode_utf8()
        .ok()
        .map(Cow::into_owned)
}
