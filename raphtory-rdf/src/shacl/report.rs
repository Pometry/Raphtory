use crate::{mapping::term_name, query::on_stack, RdfError, RdfFormat};
use oxigraph::{
    io::RdfSerializer,
    model::{BlankNode, NamedNode, NamedOrBlankNode, Term, TripleRef},
};
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{term::Object, SHACLPath},
    rdf_impl::OxigraphInMemory,
};
use rustc_hash::FxHashMap;
use shacl::validator::report::{ValidationReport, ValidationResult};
use std::{io::Write, mem};

/// One `sh:ValidationResult`, with its terms as Raphtory names ([`term_name`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
#[non_exhaustive]
pub struct ShaclResult {
    pub focus_node: String,
    pub constraint_component: String,
    /// The predicate's name for a single predicate, otherwise the path in rudof's syntax.
    pub path: Option<String>,
    pub value: Option<String>,
    pub source_shape: Option<String>,
    pub severity: String,
    pub messages: Vec<String>,
}

/// The `sh:ValidationReport` of [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
/// Its paths nest as deep as the shapes, so it is read and dropped on a stack sized like theirs.
#[derive(Debug)]
pub struct ShaclReport(pub(crate) ValidationReport, pub(super) usize);

impl Drop for ShaclReport {
    fn drop(&mut self) {
        let report = mem::take(&mut self.0);
        let _ = on_stack("dropping a SHACL report", self.1, || {
            drop(report);
            Ok(())
        });
    }
}

impl ShaclReport {
    /// `sh:conforms`: whether there are no results, of any severity.
    pub fn conforms(&self) -> bool {
        self.0.conforms()
    }

    /// The results (`sh:result`), sorted.
    pub fn results(&self) -> Vec<ShaclResult> {
        on_stack("the SHACL report", self.1, || {
            let name = |object: &Object| term_name(Term::from(object.clone()).as_ref());
            let mut results: Vec<_> = self
                .0
                .results()
                .iter()
                .map(|result| {
                    let mut messages: Vec<String> = result
                        .message()
                        .iter()
                        .map(|(_, message)| message.clone())
                        .collect();
                    messages.sort();
                    ShaclResult {
                        focus_node: name(result.focus_node()),
                        constraint_component: name(result.constraint_component()),
                        path: result.path().map(|path| match path {
                            SHACLPath::Predicate { pred } => {
                                term_name(NamedNode::from(pred.clone()).as_ref().into())
                            }
                            path => path.to_string(),
                        }),
                        value: result.value().map(name),
                        source_shape: result.source().map(name),
                        severity: IriS::from(result.severity()).to_string(),
                        messages,
                    }
                })
                .collect();
            results.sort();
            Ok(results)
        })
        .unwrap_or_else(|error| panic!("{error}"))
    }

    /// Writes the report as an `sh:ValidationReport` graph and returns the number of triples.
    pub fn write<W: Write>(&self, writer: W, format: RdfFormat) -> Result<usize, RdfError> {
        // rudof labels its own blank nodes 1, 2, .. in hex, which can be data or shapes labels too
        let mark = |object: &Object| match object {
            Object::BlankNode(id) => Object::BlankNode(format!("r{id}")),
            object => object.clone(),
        };
        let graph = on_stack("the SHACL report", self.1, || {
            let results = self.0.results().iter().map(|result| {
                ValidationResult::new(
                    mark(result.focus_node()),
                    mark(result.constraint_component()),
                    result.severity().clone(),
                )
                .with_path(result.path().cloned())
                .with_value(result.value().map(mark))
                .with_source(result.source().map(mark))
                .with_message(result.message().clone())
            });
            let mut graph = OxigraphInMemory::new();
            ValidationReport::new()
                .with_conforms(self.conforms())
                .with_results(results.collect())
                .to_rdf(&mut graph)
                .map_err(RdfError::shacl)?;
            Ok(graph)
        })?;
        let mut own = FxHashMap::default();
        let mut unmark = |node: BlankNode| match node.as_str().strip_prefix('r') {
            Some(id) => BlankNode::new_unchecked(id),
            None => own.entry(node).or_insert_with(BlankNode::default).clone(),
        };
        let mut serializer = RdfSerializer::from_format(format.to_oxigraph()).for_writer(writer);
        for quad in graph.quads() {
            let subject = match quad.subject {
                NamedOrBlankNode::BlankNode(node) => unmark(node).into(),
                subject => subject,
            };
            let object = match quad.object {
                Term::BlankNode(node) => unmark(node).into(),
                object => object,
            };
            serializer.serialize_triple(TripleRef::new(&subject, &quad.predicate, &object))?;
        }
        serializer.finish()?;
        Ok(graph.len())
    }

    /// The report as a document, as [`write`](Self::write) writes it.
    pub fn to_rdf_string(&self, format: RdfFormat) -> Result<String, RdfError> {
        let mut document = Vec::new();
        self.write(&mut document, format)?;
        Ok(String::from_utf8(document)?)
    }
}
