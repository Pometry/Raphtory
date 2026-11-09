use super::{store::Store, ShaclReport};
use crate::{graph::open_document, RdfError, RdfFormat};
use oxigraph::{
    io::RdfParser,
    model::{Graph, Triple, TripleRef},
};
use raphtory::db::api::view::DynamicGraph;
use rudof_rdf::rdf_impl::OxigraphInMemory;
use shacl::{
    error::{IRError, ValidationError},
    ir::{dg::ShapeRecursionKind, IRSchema},
    rdf::ShaclParser,
    validator::{processor::ShaclProcessor, ShaclConfig, ShaclValidationMode},
};
use std::{
    io::Read,
    path::Path,
    time::{Duration, Instant},
};

/// Shapes or a validation that [`ShaclOptions`] does not allow (see [`RdfError::ShaclLimit`]).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclLimit {
    #[error("recursive SHACL shapes (a shape that refers to itself) are not accepted")]
    RecursiveShapes,
    #[error("the shapes are longer than {max} bytes")]
    ShapesTooLong { max: usize },
    #[error("the validation holds more than {max} results")]
    TooManyResults { max: usize },
    #[error("the validation timed out after {timeout:?}")]
    TimedOut { timeout: Duration },
}

/// Options for [`ShaclShapes::parse`] and [`RdfView::validate_shacl`](crate::RdfView::validate_shacl);
/// the default has no limits and accepts recursive shapes.
#[derive(Clone, Debug, Default)]
pub struct ShaclOptions {
    reject_recursive_shapes: bool,
    max_shapes_length: Option<usize>,
    max_results: Option<usize>,
    timeout: Option<Duration>,
}

impl ShaclOptions {
    /// Fail with [`ShaclLimit::RecursiveShapes`] rather than validate with recursive shapes.
    /// They are followed through the data, so validation memory grows with the longest chain they reach.
    pub fn with_reject_recursive_shapes(mut self, reject: bool) -> Self {
        self.reject_recursive_shapes = reject;
        self
    }

    /// The longest shapes document, in bytes; a longer one fails before it is parsed.
    pub fn with_max_shapes_length(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_shapes_length = max.into();
        self
    }

    /// The most results of a whole validation, split evenly across the shapes with targets.
    pub fn with_max_results(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_results = max.into();
        self
    }

    /// How long a validation may run; past it each read of the graph fails.
    pub fn with_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }
}

/// A parsed and compiled SHACL shapes graph, which can validate many views from many threads.
#[derive(Debug)]
pub struct ShaclShapes {
    schema: IRSchema,
    recursive: bool,
}

impl ShaclShapes {
    /// Parses and compiles a shapes graph read in `format`, or Turtle if it is `None`.
    pub fn parse(
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
        options: &ShaclOptions,
    ) -> Result<Self, RdfError> {
        let max = options.max_shapes_length;
        let mut document = Vec::new();
        data.take(max.map_or(u64::MAX, |max| (max as u64).saturating_add(1)))
            .read_to_end(&mut document)?;
        if let Some(max) = max.filter(|&max| document.len() > max) {
            return Err(RdfError::ShaclLimit(ShaclLimit::ShapesTooLong { max }));
        }
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
        if let Some(base_iri) = base_iri {
            parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
        }
        let graph = parser
            .for_reader(document.as_slice())
            .map(|quad| match Triple::from(quad?) {
                // interning a triple term in a Graph recurses and copies it once per nesting level
                triple if triple.object.is_triple() => Err(RdfError::Rdf12Term(format!(
                    "{} {} <<( ... )>>",
                    triple.subject, triple.predicate
                ))),
                triple => Ok(triple),
            })
            .collect::<Result<Graph, RdfError>>()?;
        Self::compile(graph)
    }

    /// [`parse`](Self::parse) of a file, in `format` or the one its extension names.
    pub fn parse_path(
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
        options: &ShaclOptions,
    ) -> Result<Self, RdfError> {
        let (file, format) = open_document(path.as_ref(), format.into())?;
        Self::parse(file, format, base_iri, options)
    }

    /// Compiles a shapes graph from its triples.
    pub fn from_triples<'a>(
        triples: impl IntoIterator<Item = TripleRef<'a>>,
    ) -> Result<Self, RdfError> {
        Self::compile(triples.into_iter().collect())
    }

    fn compile(graph: Graph) -> Result<Self, RdfError> {
        RdfError::caught("the SHACL shapes compiler", || {
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
            Ok(Self { schema, recursive })
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
        // rudof counts the results of each shape with targets, validating them in parallel
        let targeted = self.schema.iter_with_targets().count().max(1);
        let config = ShaclConfig::default()
            .with_max_results(options.max_results.map(|max| (max / targeted).max(1)));
        let mut store = Store::new(
            view,
            options.timeout.map(|timeout| (Instant::now(), timeout)),
        );
        let report = RdfError::caught("the SHACL validator", || {
            store
                .validate(&self.schema, &ShaclValidationMode::Native, &config)
                .map_err(|error| match (error, options.max_results) {
                    (ValidationError::TooManyResults(..), Some(max)) => {
                        RdfError::ShaclLimit(ShaclLimit::TooManyResults { max })
                    }
                    (error, _) => RdfError::shacl(error),
                })
        });
        // rudof drops the errors of target lookups, so a timed-out run can look conforming
        if let Some(timeout) = store.timed_out() {
            return Err(RdfError::ShaclLimit(ShaclLimit::TimedOut { timeout }));
        }
        Ok(ShaclReport(report?))
    }
}
