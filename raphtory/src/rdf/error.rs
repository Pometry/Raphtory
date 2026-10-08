use crate::errors::GraphError;
use oxigraph::{
    io::{RdfFormat, RdfParseError},
    model::IriParseError,
    sparql::{QueryEvaluationError, SparqlSyntaxError},
};
use std::time::Duration;

/// Errors raised by RDF import, export and SPARQL evaluation.
///
/// They reach callers wrapped in [`GraphError::Rdf`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RdfError {
    #[error("RDF parse error: {0}")]
    Parse(#[from] RdfParseError),

    #[error("SPARQL syntax error: {0}")]
    SparqlSyntax(#[from] SparqlSyntaxError),

    /// A query whose brackets `(`, `{` and `[` nest deeper than
    /// [`MAX_SPARQL_NESTING`](crate::rdf::MAX_SPARQL_NESTING); rejected before parsing.
    #[error(
        "SPARQL syntax error: brackets nest more than {max} deep at line {line}, column {column}"
    )]
    SparqlTooDeep {
        /// The deepest nesting allowed.
        max: usize,
        /// The line of the bracket that nests too deep, from 1.
        line: usize,
        /// The column of the bracket that nests too deep, in characters from 1.
        column: usize,
    },

    #[error("SPARQL evaluation error: {0}")]
    SparqlEvaluation(#[from] QueryEvaluationError),

    /// A query with more triple patterns than
    /// [`SparqlOptions::max_triple_patterns`](crate::rdf::SparqlOptions::max_triple_patterns)
    /// allows; rejected after parsing, before planning.
    #[error(
        "SPARQL query too complex: {count} triple patterns, the limit is {max} (each triple \
         pattern, property path step, BIND and (... AS ?v) of SELECT or GROUP BY counts one, \
         and each VALUES block one plus one per variable and per 100 rows)"
    )]
    TooManyPatterns {
        /// The number of triple patterns of the query, as counted for the limit.
        count: usize,
        /// The most triple patterns allowed.
        max: usize,
    },

    /// A query that ran longer than
    /// [`SparqlOptions::timeout`](crate::rdf::SparqlOptions::timeout) and was stopped.
    #[error("SPARQL query timed out: it ran longer than its time limit of {timeout:?}")]
    Timeout {
        /// The time limit of the query.
        timeout: Duration,
    },

    /// A query stopped because its
    /// [`SparqlOptions::cancellation_token`](crate::rdf::SparqlOptions::cancellation_token)
    /// was cancelled.
    #[error("SPARQL query cancelled")]
    Cancelled,

    #[error("invalid IRI: {0}")]
    Iri(#[from] IriParseError),

    #[error("RDF import needs a graph with string node ids; this graph uses u64 ids")]
    NonStringIds,

    #[error(
        "{0} cannot be stored: IRIs under <raphtory:> must canonically encode a node or layer name"
    )]
    NonCanonicalTerm(String),

    /// An RDF 1.2 term to store: a triple term (including annotations and reifiers) or a
    /// directional language-tagged string such as `"hi"@en--ltr`. Raphtory stores RDF 1.1
    /// triples only. Builds without oxigraph's RDF 1.2 support fail earlier with
    /// [`Parse`](Self::Parse), except JSON-LD, whose parser drops `@direction` and stores a
    /// plain language-tagged string.
    #[error(
        "{0} cannot be stored: RDF 1.2 triple terms (including annotations and reifiers) and \
         directional language-tagged strings are not supported"
    )]
    Rdf12Term(String),

    #[error("unknown RDF format '{0}'")]
    UnknownFormat(String),

    /// A prefix name that the serializer of `format` would write as is, giving a document that
    /// does not parse (see [`serializer_with_prefixes`](crate::rdf::serializer_with_prefixes)).
    #[error("invalid prefix name '{name}' for {format}: {reason}")]
    InvalidPrefix {
        name: String,
        format: RdfFormat,
        reason: &'static str,
    },

    /// An IRI under [`ASOF_NS`](crate::rdf::ASOF_NS) whose time does not parse. A query that
    /// uses one fails with [`SparqlEvaluation`](Self::SparqlEvaluation) wrapping this error in
    /// [`QueryEvaluationError::Dataset`].
    #[error("invalid time graph <{iri}>: {reason}")]
    InvalidTimeGraph { iri: String, reason: String },

    /// A name that is neither a SPARQL results format nor an RDF format (see
    /// [`SparqlFormat::parse`](crate::rdf::SparqlFormat::parse)).
    #[error(
        "unknown SPARQL results format '{0}': expected json, xml, csv, tsv or an RDF format such \
         as nt or ttl"
    )]
    UnknownSparqlFormat(String),

    /// A [`SparqlFormat`](crate::rdf::SparqlFormat) without a format for the form of the query
    /// (SELECT/ASK need a results format, CONSTRUCT/DESCRIBE an RDF format). Raised before
    /// evaluation, so nothing is written.
    #[error("{form} results cannot be written as {format}; use {expected}")]
    WrongResultsFormat {
        /// The query form, such as `SELECT`.
        form: &'static str,
        /// The format that was given.
        format: String,
        /// The kind of format the query form needs.
        expected: &'static str,
    },

    /// A literal of a SELECT result with control characters SPARQL Results XML cannot hold
    /// unchanged.
    #[error(
        "{0} cannot be written in SPARQL Results XML, which cannot hold its control characters \
         unchanged; use JSON"
    )]
    XmlUnsafeLiteral(String),

    /// An RDF 1.2 triple term in a SELECT result, which Raphtory does not write in SPARQL
    /// Results XML.
    #[error(
        "{0} is an RDF 1.2 triple term, which Raphtory does not write in SPARQL Results XML; \
         use JSON, CSV or TSV"
    )]
    XmlTripleTerm(String),

    /// A SHACL shapes graph that does not compile, or a validation that failed (see
    /// [`ShaclShapes`](crate::rdf::shacl::ShaclShapes)).
    #[cfg(feature = "shacl")]
    #[error("SHACL error: {0}")]
    Shacl(String),

    /// A SHACL report with a literal RDF/XML cannot hold unchanged (see
    /// [`ShaclReport::write`](crate::rdf::shacl::ShaclReport::write)); raised before anything
    /// is written.
    #[cfg(feature = "shacl")]
    #[error(
        "{0} cannot be written in an RDF/XML SHACL report, which cannot hold its control \
         characters unchanged; use Turtle, N-Triples or JSON-LD"
    )]
    XmlUnsafeReport(String),

    /// A SHACL shapes graph that uses a feature the validator does not support (see the
    /// [`shacl`](crate::rdf::shacl) module); rejected when the shapes are compiled.
    #[cfg(feature = "shacl")]
    #[error("unsupported SHACL feature: {0}")]
    ShaclUnsupported(String),
}

macro_rules! rdf_into_graph_error {
    ($($err:ty),* $(,)?) => {
        $(
            impl From<$err> for GraphError {
                fn from(error: $err) -> Self {
                    GraphError::Rdf(error.into())
                }
            }
        )*
    };
}

rdf_into_graph_error!(
    RdfParseError,
    SparqlSyntaxError,
    QueryEvaluationError,
    IriParseError
);
