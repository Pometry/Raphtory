//! SHACL validation of graph views: the triples [`to_rdf`](crate::RdfView::to_rdf) writes are
//! copied into memory and validated by the SHACL Core engine of rudof's `shacl` crate.
pub(crate) mod limits;

pub use limits::ShaclLimit;

use crate::{
    export::for_each_triple,
    import::open_document,
    mapping::{layer_name_of, term_name},
    RdfError, RdfFormat,
};
use limits::{check_structure, run_guarded};
use oxigraph::{
    io::{RdfParser, RdfSerializer},
    model::{
        vocab::{rdf, rdfs},
        BlankNode, Graph, NamedNode, NamedOrBlankNode, Term, TermRef, Triple, TripleRef,
    },
};
use raphtory::{
    db::api::view::{DynamicGraph, IntoDynamic},
    prelude::LayerOps,
};
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{term::Object, vocabs::ShaclVocab, SHACLPath},
    rdf_impl::OxigraphInMemory,
};
use rustc_hash::FxHashMap;
use shacl::{
    error::IRError,
    ir::{dg::ShapeRecursionKind, IRSchema},
    rdf::ShaclParser,
    validator::{
        processor::{GraphValidation, ShaclProcessor},
        report::{ValidationReport, ValidationResult},
        store::Graph as DataGraph,
        ShaclConfig, ShaclValidationMode,
    },
};
use std::{
    io::{Read, Write},
    path::Path,
};

/// Options for [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
#[derive(Clone, Debug, Default)]
pub struct ShaclOptions {
    reject_recursive_shapes: bool,
}

impl ShaclOptions {
    /// Fail with [`ShaclLimit::RecursiveShapes`] rather than validate with recursive shapes.
    /// The validator recurses once per node along them, so a long chain aborts the process.
    pub fn with_reject_recursive_shapes(mut self, reject: bool) -> Self {
        self.reject_recursive_shapes = reject;
        self
    }
}

/// A parsed and compiled SHACL shapes graph, which can validate many views from many threads.
#[derive(Clone, Debug)]
pub struct ShaclShapes {
    schema: IRSchema,
    recursive: bool,
    layers: Option<Vec<String>>,
}

/// One `sh:ValidationResult`, with its terms as Raphtory names ([`term_name`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
#[derive(Debug, Clone)]
pub struct ShaclReport(pub(crate) ValidationReport);

impl ShaclShapes {
    /// Parses and compiles a shapes graph read in `format`, or Turtle if it is `None`.
    pub fn parse(
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<Self, RdfError> {
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
        if let Some(base_iri) = base_iri {
            parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
        }
        let graph = parser
            .for_reader(data)
            .map(|quad| quad.map(Triple::from))
            .collect::<Result<Graph, _>>()
            .map_err(RdfError::from)?;
        Self::compile(graph)
    }

    /// [`parse`](Self::parse) of a file, in `format` or the one its extension names.
    pub fn parse_path(
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<Self, RdfError> {
        let (file, format) = open_document(path.as_ref(), format.into())?;
        Self::parse(file, format, base_iri)
    }

    /// Compiles a shapes graph from its triples.
    pub fn from_triples<'a>(
        triples: impl IntoIterator<Item = TripleRef<'a>>,
    ) -> Result<Self, RdfError> {
        Self::compile(triples.into_iter().collect())
    }

    fn compile(graph: Graph) -> Result<Self, RdfError> {
        check_structure(&graph)?;
        // shapes only read the predicates they name, except closed shapes, which read them all
        let closed = graph
            .iter()
            .any(|triple| triple.predicate.as_str() == ShaclVocab::SH_CLOSED);
        let layers = (!closed).then(|| {
            let named = graph.iter().flat_map(|triple| {
                [
                    TermRef::from(triple.subject),
                    triple.predicate.into(),
                    triple.object,
                ]
            });
            let classes = [rdf::TYPE.into(), rdfs::SUB_CLASS_OF.into()];
            let mut layers: Vec<String> = named
                .chain(classes)
                .filter(TermRef::is_named_node)
                .filter_map(layer_name_of)
                .collect();
            layers.sort();
            layers.dedup();
            layers
        });
        let (schema, recursive) = run_guarded("the SHACL shapes compiler", || {
            let mut store = OxigraphInMemory::new();
            for triple in graph.iter() {
                store
                    .add_triple_ref(triple.subject, triple.predicate, triple.object)
                    .map_err(RdfError::shacl)?;
            }
            let ast = ShaclParser::new(store)
                .parse()
                .map_err(|error| RdfError::shacl(IRError::from(error)))?;
            let schema = IRSchema::try_from(ast).map_err(RdfError::shacl)?;
            let recursive = schema
                .recursion_kinds()
                .values()
                .any(|kind| *kind != ShapeRecursionKind::NonRecursive);
            Ok((schema, recursive))
        })?;
        Ok(Self {
            schema,
            recursive,
            layers,
        })
    }

    pub(crate) fn validate(
        &self,
        view: DynamicGraph,
        options: &ShaclOptions,
    ) -> Result<ShaclReport, RdfError> {
        if self.recursive && options.reject_recursive_shapes {
            return Err(RdfError::ShaclLimit(ShaclLimit::RecursiveShapes));
        }
        let view = match &self.layers {
            Some(layers) => view.valid_layers(layers).into_dynamic(),
            None => view,
        };
        let mut data = OxigraphInMemory::new();
        for_each_triple(&view, |triple| match triple {
            Some(triple) => data
                .add_triple_ref(triple.subject, triple.predicate, triple.object)
                .map_err(RdfError::shacl),
            None => Ok(()),
        })?;
        run_guarded("the SHACL validator", || {
            GraphValidation::from(DataGraph::from(data))
                .validate(
                    &self.schema,
                    &ShaclValidationMode::Native,
                    &ShaclConfig::default(),
                )
                .map(ShaclReport)
                .map_err(RdfError::shacl)
        })
    }
}

impl ShaclReport {
    /// `sh:conforms`: whether there are no results, of any severity.
    pub fn conforms(&self) -> bool {
        self.0.conforms()
    }

    /// The results (`sh:result`), sorted.
    pub fn results(&self) -> Vec<ShaclResult> {
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
                    severity: term_name(
                        NamedNode::from(IriS::from(result.severity()))
                            .as_ref()
                            .into(),
                    ),
                    messages,
                }
            })
            .collect();
        results.sort();
        results
    }

    /// Writes the report as an `sh:ValidationReport` graph and returns the number of triples.
    pub fn write<W: Write>(&self, writer: W, format: RdfFormat) -> Result<usize, RdfError> {
        // rudof labels its own blank nodes 1, 2, .. in hex, which can be data or shapes labels too
        let mark = |object: &Object| match object {
            Object::BlankNode(id) => Object::BlankNode(format!("r{id}")),
            object => object.clone(),
        };
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
