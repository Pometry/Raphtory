//! Serialising the triples visible in a view.
use crate::{
    mapping::{edge_predicate, term_of},
    RdfError, RdfFormat, WriteStats,
};
use oxigraph::{
    io::{RdfFormat as OxRdfFormat, RdfSerializer, WriterQuadSerializer},
    model::{
        vocab::rdf, BlankNode, BlankNodeRef, NamedNode, NamedNodeRef, NamedOrBlankNode,
        NamedOrBlankNodeRef, Term, TermRef, TripleRef,
    },
};
use raphtory::{db::api::view::DynamicGraph, prelude::*};
use raphtory_api::core::entities::LayerId;
use rustc_hash::FxHashMap;
use std::io::{self, BufWriter, Write};

/// Writes every `(edge, layer)` pair visible in `view` as one triple, grouped by
/// subject. Generalized triples are skipped and counted in [`WriteStats::skipped`].
pub(crate) fn write_rdf<W: Write>(
    view: DynamicGraph,
    writer: W,
    format: RdfFormat,
) -> Result<WriteStats, RdfError> {
    let mut sink = TripleSink::new(RdfSerializer::from_format(format.to_oxigraph()), writer);
    let mut predicates: FxHashMap<Option<LayerId>, Option<NamedNode>> = FxHashMap::default();
    for node in view.nodes() {
        let mut subject = None;
        for edge in node.out_edges().explode_layers() {
            let subject = subject
                .get_or_insert_with(|| NamedOrBlankNode::try_from(term_of(&node.name())).ok());
            let predicate = predicates
                .entry(edge.edge.layer())
                .or_insert_with(|| edge_predicate(&edge));
            match (subject, predicate) {
                (Some(subject), Some(predicate)) => {
                    let object = term_of(&edge.dst().name());
                    sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
                }
                _ => sink.skip(),
            }
        }
    }
    Ok(sink.finish()?)
}

/// Writes triples with an [`RdfSerializer`], counting them. Shared by [`write_rdf`], the
/// CONSTRUCT/DESCRIBE results of `sparql_to_writer` and `ShaclReport::write`.
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
    stats: WriteStats,
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
            rdf_xml: serializer.format() == OxRdfFormat::RdfXml,
            out: serializer.for_writer(BufWriter::new(writer)),
            stats: WriteStats::default(),
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
            self.stats.written += 1;
            return Ok(());
        }
        if self
            .subject
            .as_ref()
            .is_none_or(|(given, _)| given.as_ref() != subject)
        {
            self.end_run();
            let written = match subject {
                NamedOrBlankNodeRef::BlankNode(node) => {
                    xml_node_id(node).map_or_else(|| subject.into_owned(), Into::into)
                }
                subject => subject.into_owned(),
            };
            self.subject = Some((subject.into_owned(), written));
        }
        if !xml_predicate_safe(predicate.as_str()) || !xml_text_safe(object) {
            self.stats.skipped += 1;
            return Ok(());
        }
        let renamed = match object {
            TermRef::BlankNode(node) => xml_node_id(node).map(Term::from),
            _ => None,
        };
        let object = renamed.as_ref().map_or(object, Term::as_ref);
        if !self.subject_written && !xml_element_safe(predicate, object) {
            self.held_types.push(object.into_owned());
            return Ok(());
        }
        let Some((_, subject)) = &self.subject else {
            unreachable!("the run was started above")
        };
        self.out
            .serialize_triple(TripleRef::new(subject, predicate, object))?;
        self.stats.written += 1;
        self.subject_written = true;
        for object in self.held_types.drain(..) {
            self.out
                .serialize_triple(TripleRef::new(subject, rdf::TYPE, &object))?;
            self.stats.written += 1;
        }
        Ok(())
    }

    /// Counts a triple that cannot be written at all (a generalized triple).
    pub(crate) fn skip(&mut self) {
        self.stats.skipped += 1;
    }

    /// Ends the document, flushes the output and returns the counts.
    pub(crate) fn finish(mut self) -> io::Result<WriteStats> {
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
        _ => false, // RDF 1.2 triple terms
    }
}

/// The blank node RDF/XML writes in place of `node` as an `rdf:nodeID`, which must be an
/// NCName, or `None` if `node`'s label is written as is.
///
/// Labels starting with a digit get an `x` prefix (`1` -> `x1`); so do labels of `x`s followed
/// by such a label (`x1` -> `xx1`), which keeps the mapping injective.
fn xml_node_id(node: BlankNodeRef<'_>) -> Option<BlankNode> {
    let label = node.as_str();
    let rest = label.trim_start_matches('x');
    rest.starts_with(|c| !is_ncname_start_char(c))
        .then(|| BlankNode::new_unchecked(format!("x{label}")))
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
