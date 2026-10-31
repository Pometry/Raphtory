use super::{nesting::nesting, store::Store, ShaclReport};
use crate::{graph::open_document, query::on_stack, RdfError, RdfFormat};
use oxigraph::{
    io::RdfParser,
    model::{Graph, Triple, TripleRef},
};
use prefixmap::PrefixMap;
use raphtory::db::api::view::DynamicGraph;
use rudof_rdf::rdf_impl::OxigraphInMemory;
use shacl::{
    error::IRError,
    ir::{dg::ShapeRecursionKind, IRSchema},
    rdf::ShaclParser,
    validator::{processor::ShaclProcessor, ShaclConfig, ShaclValidationMode},
};
use std::{io::Read, mem, path::Path};

/// A shapes graph the validator does not accept (see [`RdfError::ShaclLimit`]).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclLimit {
    #[error("recursive SHACL shapes (a shape that refers to itself) are not accepted")]
    RecursiveShapes,
    #[error("a SHACL path that contains itself is not accepted")]
    CyclicPath,
    #[error(
        "a SHACL path that expands to more steps than the shapes graph has triples is not accepted"
    )]
    PathTooLarge,
}

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
#[derive(Debug)]
pub struct ShaclShapes {
    schema: IRSchema,
    recursive: bool,
    // sizes the stack of all rudof work, which recurses along the shapes graph's nesting
    pub(crate) depth: usize,
}

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
        let depth = nesting(&graph)?;
        let (schema, recursive) = on_stack("the SHACL shapes compiler", depth, || {
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
            depth,
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
        let report = on_stack("the SHACL validator", self.depth, || {
            Store::new(view)
                .validate(
                    &self.schema,
                    &ShaclValidationMode::Native,
                    &ShaclConfig::default(),
                )
                .map_err(RdfError::shacl)
        })?;
        Ok(ShaclReport(report, self.depth))
    }
}

// rudof's schemas and reports nest as deep as the shapes, and so does dropping them
impl Drop for ShaclShapes {
    fn drop(&mut self) {
        let schema = mem::replace(&mut self.schema, IRSchema::new(PrefixMap::new()));
        let _ = on_stack("dropping SHACL shapes", self.depth, || {
            drop(schema);
            Ok(())
        });
    }
}
