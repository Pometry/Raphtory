use crate::{
    shacl::{ShaclFeature, ShaclLimit},
    QueryForm,
};
use oxigraph::{
    io::RdfParseError,
    model::{IriParseError, NamedNode, Term, TryFromTermError},
    sparql::{QueryEvaluationError, SparqlSyntaxError},
};
use raphtory::errors::GraphError;
use raphtory_api::core::utils::time::ParseTimeError;
use std::{any::Any, error::Error, io, path::PathBuf, string::FromUtf8Error};

/// Errors raised by RDF import, export, SPARQL evaluation and SHACL validation.
///
/// Each message includes the error it wraps, which `source()` does not return again, so an
/// error chain prints it once.
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

    /// A query whose brackets `(`, `{` and `[` nest deeper than
    /// [`MAX_SPARQL_NESTING`](crate::MAX_SPARQL_NESTING); rejected before parsing.
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
    SparqlEvaluation(QueryEvaluationError),

    /// A query longer than [`SparqlOptions::with_max_query_length`] allows; rejected before
    /// parsing.
    ///
    /// [`SparqlOptions::with_max_query_length`]: crate::SparqlOptions::with_max_query_length
    #[error("SPARQL query too long: {length} bytes, the limit is {max} bytes")]
    QueryTooLong {
        /// The length of the query, in bytes.
        length: usize,
        /// The longest query allowed, in bytes.
        max: usize,
    },

    /// Results larger than [`SparqlOptions::with_max_result_bytes`] allows; the writer holds
    /// at most that many bytes of them.
    ///
    /// [`SparqlOptions::with_max_result_bytes`]: crate::SparqlOptions::with_max_result_bytes
    #[error("SPARQL results larger than {max} bytes")]
    ResultsTooLarge {
        /// The most bytes of results allowed.
        max: usize,
    },

    /// A query with more triple patterns than [`SparqlOptions::with_max_triple_patterns`]
    /// allows; rejected after parsing, before planning.
    ///
    /// [`SparqlOptions::with_max_triple_patterns`]: crate::SparqlOptions::with_max_triple_patterns
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

    #[error("invalid IRI: {0}")]
    Iri(IriParseError),

    #[error("invalid RDF triple: {0}")]
    Triple(TryFromTermError),

    /// [`RdfView::from_view`](crate::RdfView::from_view) of a view of an event graph.
    #[error("RDF views need a view of a persistent graph")]
    NotPersistent,

    #[error(
        "{0} cannot be stored: IRIs under <raphtory:> must canonically encode a node or layer name"
    )]
    NonCanonicalTerm(String),

    /// A predicate whose layer name is not safe as a file name (see [`crate`]).
    #[error(
        "{0} cannot be stored: its layer name would be blank or contain a control character or \
         one of / \\ : * ? \" < > |, which layer names must not contain"
    )]
    UnsafeLayerName(String),

    /// An RDF 1.2 term to store: a triple term (including annotations and reifiers) or a
    /// directional language-tagged string such as `"hi"@en--ltr`. Raphtory stores RDF 1.1
    /// triples only.
    #[error(
        "{0} cannot be stored: RDF 1.2 triple terms (including annotations and reifiers) and \
         directional language-tagged strings are not supported"
    )]
    Rdf12Term(String),

    #[error("unknown RDF format '{0}'")]
    UnknownFormat(String),

    /// A file without a format whose extension names none (see
    /// [`RdfFormat::from_path`](crate::RdfFormat::from_path)).
    #[error("cannot tell the RDF format of '{}' from its extension: give the format", .0.display())]
    NoPathFormat(PathBuf),

    /// An IRI under [`ASOF_NS`](crate::ASOF_NS) whose time does not parse. A query that
    /// uses one fails with [`SparqlEvaluation`](Self::SparqlEvaluation) wrapping this error in
    /// [`QueryEvaluationError::Dataset`].
    #[error("invalid time graph {iri}: expected epoch milliseconds or a date-time: {error}")]
    InvalidTimeGraph {
        iri: NamedNode,
        error: ParseTimeError,
    },

    /// A name that is neither a SPARQL results format nor an RDF format (see
    /// [`SparqlFormat::parse`](crate::SparqlFormat::parse)).
    #[error(
        "unknown SPARQL results format '{0}': expected json, xml, csv, tsv or an RDF format such \
         as nt or ttl"
    )]
    UnknownSparqlFormat(String),

    /// A [`SparqlFormat`](crate::SparqlFormat) without a format for the form of the query
    /// (SELECT/ASK need a results format, CONSTRUCT/DESCRIBE an RDF format). Raised before
    /// evaluation, so nothing is written.
    #[error(
        "{} results cannot be written as {format}; use {}",
        if .form.is_graph() { "CONSTRUCT and DESCRIBE" } else { .form.name() },
        if .form.is_graph() {
            "an RDF format such as nt, ttl, jsonld or rdf"
        } else {
            "a SPARQL results format: json, xml, csv or tsv"
        }
    )]
    WrongResultsFormat {
        /// The form of the query.
        form: QueryForm,
        /// The format that was given.
        format: String,
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

    /// CONSTRUCT or DESCRIBE results with triples RDF/XML cannot write (those
    /// [`RdfView::to_rdf`](crate::RdfView::to_rdf) skips in RDF/XML); raised before
    /// anything is written.
    #[error(
        "RDF/XML cannot hold {skipped} of the {triples} triples of the results (such as triples \
         whose predicate does not end in an XML name); use another RDF format"
    )]
    RdfXmlSkipped {
        /// The number of triples RDF/XML cannot write.
        skipped: usize,
        /// The number of triples of the results.
        triples: usize,
    },

    #[error("RDF output is not UTF-8: {0}")]
    NotUtf8(FromUtf8Error),

    /// A SHACL shapes graph that does not compile, or a validation that failed (see
    /// [`ShaclShapes`](crate::shacl::ShaclShapes)).
    #[error("SHACL error: {0}")]
    Shacl(Box<dyn Error + Send + Sync>),

    /// A SHACL report with a literal RDF/XML cannot hold unchanged (see
    /// [`ShaclReport::write`](crate::shacl::ShaclReport::write)); raised before anything
    /// is written.
    #[error(
        "{0} cannot be written in an RDF/XML SHACL report, which cannot hold its control \
         characters unchanged; use Turtle, N-Triples or JSON-LD"
    )]
    XmlUnsafeReport(Term),

    /// A SHACL shapes graph that uses a feature the validator does not support (see the
    /// [`shacl`](crate::shacl) module); rejected when the shapes are compiled.
    #[error("unsupported SHACL feature {term}: {feature}")]
    ShaclUnsupported {
        /// The term of the shapes graph that uses it.
        term: NamedNode,
        feature: ShaclFeature,
    },

    /// A SHACL shapes graph the validator would read past one of its limits, overflowing its
    /// stack or its memory; rejected when the shapes are compiled, or for recursive shapes
    /// before a validation that rejects them.
    #[error("SHACL error: {0}")]
    ShaclLimit(ShaclLimit),

    /// A panic of SPARQL evaluation or SHACL validation, with its message.
    #[error("{0}")]
    Panicked(String),
}

impl RdfError {
    /// A [`Shacl`](Self::Shacl) error from an error or a message.
    pub(crate) fn shacl(error: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::Shacl(error.into())
    }

    /// A [`Panicked`](Self::Panicked) error of `what` from the payload of its panic.
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
