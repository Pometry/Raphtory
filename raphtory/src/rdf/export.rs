//! Serialising the triples visible in a view.
use crate::{
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    rdf::{
        mapping::{layer_predicate, term_of},
        scan::EdgeScan,
        RdfError, RdfExportStats,
    },
};
use oxigraph::{
    io::{RdfFormat, RdfSerializer, WriterQuadSerializer},
    model::{
        vocab::rdf, BlankNode, BlankNodeRef, NamedNode, NamedNodeRef, NamedOrBlankNode,
        NamedOrBlankNodeRef, Term, TermRef, TripleRef,
    },
};
use raphtory_api::core::entities::{LayerId, VID};
use rustc_hash::FxHashMap;
use std::io::{self, BufWriter, Write};

/// Writes every `(edge, layer)` pair visible in `view.valid()` as one triple, grouped by
/// subject. Generalized triples are skipped and counted in [`RdfExportStats::skipped`].
pub(crate) fn write_rdf<G: StaticGraphViewOps + IntoDynamic, W: Write>(
    view: &G,
    writer: W,
    serializer: RdfSerializer,
) -> Result<RdfExportStats, GraphError> {
    let view = view.clone().into_dynamic();
    let mut sink = TripleSink::new(serializer, writer);
    let mut predicates: FxHashMap<LayerId, Option<NamedNode>> = FxHashMap::default();
    // The scan is grouped by subject, so cache the last subject term.
    let mut subject: Option<(VID, Option<NamedOrBlankNode>)> = None;
    for (s, l, o) in EdgeScan::all_by_nodes(view.clone(), None).only_valid() {
        if subject.as_ref().is_none_or(|(v, _)| *v != s) {
            let term = NamedOrBlankNode::try_from(term_of(&view.node_name(s))).ok();
            subject = Some((s, term));
        }
        let subject_term = subject.as_ref().and_then(|(_, t)| t.as_ref());
        let predicate = predicates
            .entry(l)
            .or_insert_with(|| layer_iri(&view, l))
            .as_ref();
        match (subject_term, predicate) {
            (Some(subject_term), Some(predicate)) => {
                let object = term_of(&view.node_name(o));
                sink.push(subject_term.as_ref(), predicate.as_ref(), object.as_ref())?;
            }
            _ => sink.skip(),
        }
    }
    Ok(sink.finish()?)
}

/// Writes triples with an [`RdfSerializer`], counting them. Shared by [`write_rdf`] and
/// CONSTRUCT/DESCRIBE results of `sparql_to_writer`.
///
/// Other formats write every triple as given. RDF/XML:
/// - skips triples whose predicate cannot be a property element ([`xml_predicate_safe`]),
/// - holds back an `rdf:type` triple whose object cannot name an element until another triple
///   of the run is written, and skips it if there is none ([`xml_element_safe`]),
/// - skips triples whose object literal XML cannot hold unchanged ([`xml_text_safe`]),
/// - renames blank nodes whose label is not an XML name ([`xml_node_id`]).
///
/// A run is consecutive triples with the same subject; callers must push all triples of a
/// subject consecutively. Output is flushed by [`finish`](Self::finish).
pub(crate) struct TripleSink<W: Write> {
    out: WriterQuadSerializer<BufWriter<W>>,
    rdf_xml: bool,
    stats: RdfExportStats,
    /// RDF/XML only: whether a predicate IRI can be a property element, memoised.
    predicates: FxHashMap<String, bool>,
    /// RDF/XML only: the subject of the current run, as given and as written.
    subject: Option<(NamedOrBlankNode, NamedOrBlankNode)>,
    /// RDF/XML only: whether a triple of the current run was written.
    subject_written: bool,
    /// RDF/XML only: the objects of the `rdf:type` triples of the current run held back until
    /// a triple of the run is written.
    held_types: Vec<Term>,
}

impl<W: Write> TripleSink<W> {
    pub(crate) fn new(serializer: RdfSerializer, writer: W) -> Self {
        Self {
            rdf_xml: serializer.format() == RdfFormat::RdfXml,
            out: serializer.for_writer(BufWriter::new(writer)),
            stats: RdfExportStats::default(),
            predicates: FxHashMap::default(),
            subject: None,
            subject_written: false,
            held_types: Vec::new(),
        }
    }

    /// Writes the triple `subject predicate object`, or skips it (see [`TripleSink`]).
    pub(crate) fn push(
        &mut self,
        subject: NamedOrBlankNodeRef<'_>,
        predicate: NamedNodeRef<'_>,
        object: TermRef<'_>,
    ) -> io::Result<()> {
        if !self.rdf_xml {
            self.out
                .serialize_triple(TripleRef::new(subject, predicate, object))?;
            self.stats.triples += 1;
            return Ok(());
        }
        if self
            .subject
            .as_ref()
            .is_none_or(|(given, _)| given.as_ref() != subject)
        {
            self.end_run();
            let written = match subject {
                NamedOrBlankNodeRef::BlankNode(node) => xml_node_id(node).into(),
                subject => subject.into_owned(),
            };
            self.subject = Some((subject.into_owned(), written));
        }
        if !self.predicate_safe(predicate) || !xml_text_safe(object) {
            self.stats.skipped += 1;
            return Ok(());
        }
        let object = match object {
            TermRef::BlankNode(node) => xml_node_id(node).into(),
            object => object.into_owned(),
        };
        if !self.subject_written && !xml_element_safe(predicate, object.as_ref()) {
            self.held_types.push(object);
            return Ok(());
        }
        let Some((_, subject)) = &self.subject else {
            unreachable!("the run was started above")
        };
        self.out
            .serialize_triple(TripleRef::new(subject, predicate, &object))?;
        self.stats.triples += 1;
        self.subject_written = true;
        for object in self.held_types.drain(..) {
            self.out
                .serialize_triple(TripleRef::new(subject, rdf::TYPE, &object))?;
            self.stats.triples += 1;
        }
        Ok(())
    }

    /// Counts a triple that cannot be written at all (a generalized triple).
    pub(crate) fn skip(&mut self) {
        self.stats.skipped += 1;
    }

    /// Ends the document, flushes the output and returns the counts.
    pub(crate) fn finish(mut self) -> io::Result<RdfExportStats> {
        self.end_run();
        self.out.finish()?.flush()?;
        Ok(self.stats)
    }

    /// RDF/XML: ends the current run, skipping the `rdf:type` triples still held back.
    fn end_run(&mut self) {
        self.stats.skipped += self.held_types.len();
        self.held_types.clear();
        self.subject_written = false;
    }

    /// RDF/XML: whether `predicate` can be a property element ([`xml_predicate_safe`]),
    /// memoised.
    fn predicate_safe(&mut self, predicate: NamedNodeRef<'_>) -> bool {
        if let Some(&known) = self.predicates.get(predicate.as_str()) {
            return known;
        }
        let known = xml_predicate_safe(predicate.as_str());
        self.predicates.insert(predicate.as_str().to_owned(), known);
        known
    }
}

/// The predicate IRI of a layer, or `None` if the layer's term is not an IRI. Resolved lazily
/// by id, so layers created during the export are handled (a lookup by id takes no lock that a
/// writer creating a layer waits for).
fn layer_iri(view: &DynamicGraph, layer: LayerId) -> Option<NamedNode> {
    layer_predicate(&view.get_layer_name(layer))
}

/// The RDF/XML syntax terms, which the RDF/XML serializer refuses as predicates (with an I/O
/// error once it has started the subject's element, so they must be skipped beforehand).
const RDF_XML_SYNTAX_TERMS: [&str; 9] = [
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Description",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#li",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#RDF",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#ID",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#about",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#parseType",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#resource",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#nodeID",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#datatype",
];

/// The namespace of `xmlns` attributes, which no element can be in.
const XMLNS_NAMESPACE: &str = "http://www.w3.org/2000/xmlns/";

/// Splits `iri` into a namespace and a local name as the RDF/XML serializer does to write it as
/// an element name: after the last character that cannot be in an XML name, or is a `:`, and
/// then before the first character that can start one. `None` if the local name would be empty.
fn split_xml_name(iri: &str) -> Option<(&str, &str)> {
    let base = iri.rfind(|c| !is_ncname_char(c))?;
    let start = base + iri[base..].find(is_ncname_start_char)?;
    Some(iri.split_at(start))
}

/// Whether the RDF/XML serializer can write `iri` as an element name: it needs a local name
/// ([`split_xml_name`]) and must not be in the `xmlns` namespace.
fn xml_element_name_safe(iri: &str) -> bool {
    split_xml_name(iri).is_some_and(|(namespace, _)| namespace != XMLNS_NAMESPACE)
}

/// Whether the RDF/XML serializer can write `predicate` as a property element.
fn xml_predicate_safe(predicate: &str) -> bool {
    !RDF_XML_SYNTAX_TERMS.contains(&predicate) && xml_element_name_safe(predicate)
}

/// Whether the RDF/XML serializer can write `predicate object` as the first triple of a
/// subject: a first `rdf:type` triple names the subject's element after its object (a syntax
/// term object is fine: the serializer writes `rdf:Description` instead).
fn xml_element_safe(predicate: NamedNodeRef<'_>, object: TermRef<'_>) -> bool {
    match object {
        TermRef::NamedNode(object) if predicate == rdf::TYPE => {
            xml_element_name_safe(object.as_str())
        }
        _ => true,
    }
}

/// Whether XML parsers read `term` back unchanged from RDF/XML or SPARQL Results XML: XML 1.0
/// cannot hold most C0 controls, U+FFFE or U+FFFF, and parsers turn a carriage return into a
/// line feed. RDF 1.2 triple terms are never safe.
pub(crate) fn xml_text_safe(term: TermRef<'_>) -> bool {
    match term {
        TermRef::Literal(literal) => literal.value().chars().all(|c| {
            matches!(c,
                '\t' | '\n' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..)
        }),
        TermRef::NamedNode(_) | TermRef::BlankNode(_) => true,
        #[allow(unreachable_patterns)]
        _ => false, // RDF 1.2 triple terms
    }
}

/// The label RDF/XML writes for a blank node as an `rdf:nodeID`, which must be an NCName.
///
/// Labels starting with a digit get an `x` prefix (`1` -> `x1`); so do labels of `x`s followed
/// by such a label (`x1` -> `xx1`), which keeps the mapping injective.
fn xml_node_id(node: BlankNodeRef<'_>) -> BlankNode {
    let label = node.as_str();
    let rest = label.trim_start_matches('x');
    if rest.starts_with(|c| !is_ncname_start_char(c)) {
        BlankNode::new_unchecked(format!("x{label}"))
    } else {
        node.into_owned()
    }
}

/// An XML `NameStartChar` other than `:`, so a character that can start an NCName.
fn is_ncname_start_char(c: char) -> bool {
    matches!(c,
        'A'..='Z'
        | '_'
        | 'a'..='z'
        | '\u{C0}'..='\u{D6}'
        | '\u{D8}'..='\u{F6}'
        | '\u{F8}'..='\u{2FF}'
        | '\u{370}'..='\u{37D}'
        | '\u{37F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}'
        | '\u{2070}'..='\u{218F}'
        | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

/// An XML `NameChar` other than `:`, so a character of an NCName.
fn is_ncname_char(c: char) -> bool {
    is_ncname_start_char(c)
        || matches!(c, '-' | '.' | '0'..='9' | '\u{B7}' | '\u{300}'..='\u{36F}' | '\u{203F}'..='\u{2040}')
}

/// Why `name` cannot be a prefix name in `format`, or `None` if it can.
fn invalid_prefix_name(format: RdfFormat, name: &str) -> Option<&'static str> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return None; // the empty prefix (`:local` in Turtle, the default namespace in RDF/XML)
    };
    match format {
        // PN_PREFIX: a letter, then letters, digits, `_`, `-`, `.` (and a few combining
        // characters), not ending with `.`
        RdfFormat::Turtle | RdfFormat::TriG | RdfFormat::N3 => (first == '_'
            || !is_ncname_start_char(first)
            || !chars.all(is_ncname_char)
            || name.ends_with('.'))
        .then_some(
            "it must be empty, or start with a letter, contain only letters, digits, '_', '-' \
             and '.', and not end with '.'",
        ),
        // an NCName that does not redefine the reserved `xml` and `xmlns` prefixes
        RdfFormat::RdfXml => {
            if !is_ncname_start_char(first) || !chars.all(is_ncname_char) {
                Some(
                    "it must be empty, or start with a letter or '_' and contain only letters, \
                     digits, '_', '-' and '.'",
                )
            } else if name == "xml" || name == "xmlns" {
                Some("'xml' and 'xmlns' are reserved")
            } else {
                None
            }
        }
        // these formats have no prefixes
        _ => None,
    }
}

/// Builds an [`RdfSerializer`] for `format` with the given prefixes (the serializer chooses the
/// order in which they are written; a repeated name keeps its last IRI).
///
/// Unlike [`RdfSerializer::with_prefix`], this also checks that each prefix name can be
/// written in `format`, failing with [`RdfError::InvalidPrefix`] on the first invalid one.
/// Formats without prefixes (N-Triples, N-Quads, JSON-LD) ignore them.
///
/// # Example
/// ```
/// use raphtory::{
///     prelude::*,
///     rdf::{serializer_with_prefixes, RdfError, RdfFormat},
/// };
///
/// let g = Graph::new();
/// g.add_edge(1, "http://ex/a", "http://ex/b", NO_PROPS, Some("http://ex/p"))
///     .unwrap();
/// let mut out = Vec::new();
/// let serializer = serializer_with_prefixes(RdfFormat::Turtle, [("ex", "http://ex/")]).unwrap();
/// g.to_rdf(&mut out, serializer).unwrap();
/// assert_eq!(
///     String::from_utf8(out).unwrap(),
///     "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\n"
/// );
///
/// assert!(matches!(
///     serializer_with_prefixes(RdfFormat::Turtle, [("a b", "http://ex/")]),
///     Err(RdfError::InvalidPrefix { .. })
/// ));
/// ```
pub fn serializer_with_prefixes(
    format: RdfFormat,
    prefixes: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> Result<RdfSerializer, RdfError> {
    let mut serializer = RdfSerializer::from_format(format);
    for (name, iri) in prefixes {
        let name = name.into();
        if let Some(reason) = invalid_prefix_name(format, &name) {
            return Err(RdfError::InvalidPrefix {
                name,
                format,
                reason,
            });
        }
        serializer = serializer.with_prefix(name, iri)?;
    }
    Ok(serializer)
}
