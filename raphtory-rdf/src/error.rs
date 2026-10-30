use crate::{shacl::ShaclLimit, QueryForm};
use oxigraph::{
    io::RdfParseError,
    model::{IriParseError, NamedNode, TryFromTermError},
    sparql::{QueryEvaluationError, SparqlSyntaxError},
};
use raphtory::errors::GraphError;
use raphtory_api::core::utils::time::ParseTimeError;
use std::{any::Any, error::Error, io, path::PathBuf, string::FromUtf8Error};

/// Errors raised by RDF import, export, SPARQL evaluation and SHACL validation.
/// Messages include the wrapped error, so `source()` does not return it again.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RdfError {
    /// A [`GraphError`], such as an I/O error or a time that does not parse.
    #[error(transparent)]
    Graph(#[from] GraphError),

    #[error("RDF parse error: {0}")]
    Parse(RdfParseError),

    #[error("SPARQL syntax error: {0}")]
    SparqlSyntax(SparqlSyntaxError),

    #[error("SPARQL evaluation error: {0}")]
    SparqlEvaluation(QueryEvaluationError),

    /// A query longer than [`with_max_query_length`](crate::SparqlOptions::with_max_query_length).
    #[error("SPARQL query too long: {length} bytes, the limit is {max} bytes")]
    QueryTooLong { length: usize, max: usize },

    /// Results larger than [`with_max_result_bytes`](crate::SparqlOptions::with_max_result_bytes).
    /// The writer holds at most that many bytes of them.
    #[error("SPARQL results larger than {max} bytes")]
    ResultsTooLarge { max: usize },

    /// More triple patterns than
    /// [`with_max_triple_patterns`](crate::SparqlOptions::with_max_triple_patterns) allows.
    #[error(
        "SPARQL query too complex: {count} triple patterns, the limit is {max} (each triple \
         pattern, property path step, BIND and (... AS ?v) of SELECT or GROUP BY counts one, \
         and each VALUES block one plus one per variable and per 100 rows)"
    )]
    TooManyPatterns { count: usize, max: usize },

    #[error("invalid IRI: {0}")]
    Iri(IriParseError),

    #[error("invalid RDF triple: {0}")]
    Triple(TryFromTermError),

    /// [`RdfView::from_view`](crate::RdfView::from_view) of an event graph view.
    #[error("RDF views need a view of a persistent graph")]
    NotPersistent,

    #[error(
        "{0} cannot be stored: IRIs under <raphtory:> must canonically encode a node or layer name"
    )]
    NonCanonicalTerm(String),

    /// An RDF 1.2 term to store; Raphtory stores RDF 1.1 triples only.
    #[error(
        "{0} cannot be stored: RDF 1.2 triple terms (including annotations and reifiers) and \
         directional language-tagged strings are not supported"
    )]
    Rdf12Term(String),

    #[error("unknown RDF format '{0}'")]
    UnknownFormat(String),

    /// A file without a format whose extension names none.
    #[error("cannot tell the RDF format of '{}' from its extension: give the format", .0.display())]
    NoPathFormat(PathBuf),

    /// An IRI under [`ASOF_NS`](crate::ASOF_NS) whose time does not parse.
    /// A query gets it wrapped in [`SparqlEvaluation`](Self::SparqlEvaluation).
    #[error("invalid time graph {iri}: expected epoch milliseconds or a date-time: {error}")]
    InvalidTimeGraph {
        iri: NamedNode,
        error: ParseTimeError,
    },

    /// A name that is neither a SPARQL results format nor an RDF format.
    #[error(
        "unknown SPARQL results format '{0}': expected json, xml, csv, tsv or an RDF format such \
         as nt or ttl"
    )]
    UnknownSparqlFormat(String),

    /// A [`SparqlFormat`](crate::SparqlFormat) without a format for the form of the query.
    #[error(
        "{} results cannot be written as {format}; use {}",
        if .form.is_graph() { "CONSTRUCT and DESCRIBE" } else { .form.name() },
        if .form.is_graph() {
            "an RDF format such as nt, ttl, jsonld or rdf"
        } else {
            "a SPARQL results format: json, xml, csv or tsv"
        }
    )]
    WrongResultsFormat { form: QueryForm, format: String },

    #[error("RDF output is not UTF-8: {0}")]
    NotUtf8(FromUtf8Error),

    /// A shapes graph that does not compile, or a validation that failed.
    #[error("SHACL error: {0}")]
    Shacl(Box<dyn Error + Send + Sync>),

    /// A shapes graph with a path rudof cannot expand, or recursive shapes the options reject.
    #[error("SHACL error: {0}")]
    ShaclLimit(ShaclLimit),

    /// A panic of SPARQL evaluation or SHACL validation, with its message.
    #[error("{0}")]
    Panicked(String),
}

impl RdfError {
    pub(crate) fn shacl(error: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::Shacl(error.into())
    }

    pub(crate) fn panicked(what: &str, panic: Box<dyn Any + Send>) -> Self {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic");
        Self::Panicked(format!("{what} panicked: {message}"))
    }
}

macro_rules! rdf_error_from {
    ($($variant:ident($err:ty)),* $(,)?) => {
        $(
            impl From<$err> for RdfError {
                fn from(error: $err) -> Self {
                    RdfError::$variant(error.into())
                }
            }
        )*
    };
}

rdf_error_from!(
    Parse(RdfParseError),
    SparqlSyntax(SparqlSyntaxError),
    SparqlEvaluation(QueryEvaluationError),
    Iri(IriParseError),
    Triple(TryFromTermError),
    NotUtf8(FromUtf8Error),
    Graph(io::Error),
    Graph(ParseTimeError),
);
