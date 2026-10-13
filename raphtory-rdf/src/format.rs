//! The RDF and SPARQL results formats of the RDF APIs.
use crate::RdfError;
use oxigraph::{
    io::{JsonLdProfileSet, RdfFormat as OxRdfFormat},
    sparql::results::QueryResultsFormat as OxQueryResultsFormat,
};
use std::{fmt, path::Path};

/// An RDF serialization format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RdfFormat {
    /// [N3](https://w3c.github.io/N3/spec/)
    N3,
    /// [N-Quads](https://www.w3.org/TR/n-quads/)
    NQuads,
    /// [N-Triples](https://www.w3.org/TR/n-triples/)
    NTriples,
    /// [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
    RdfXml,
    /// [TriG](https://www.w3.org/TR/trig/)
    TriG,
    /// [Turtle](https://www.w3.org/TR/turtle/)
    Turtle,
    /// [JSON-LD](https://www.w3.org/TR/json-ld/)
    JsonLd,
}

impl RdfFormat {
    /// Parses a format from a file extension, a name or a media type, such as `"ttl"`, `".ttl"`,
    /// `"turtle"`, `"text/turtle"`, `"nt"`, `"n-triples"`, `"nq"`, `"trig"`, `"rdf"`, `"xml"`,
    /// `"jsonld"` or `"n3"` (case-insensitive). Fails with [`RdfError::UnknownFormat`].
    pub fn parse(s: &str) -> Result<Self, RdfError> {
        let t = s.trim();
        let t = t.strip_prefix('.').unwrap_or(t);
        Self::from_extension(t)
            .or_else(|| Self::from_media_type(t))
            .or_else(|| Self::from_media_type(&format!("application/{t}")))
            .ok_or_else(|| RdfError::UnknownFormat(s.to_owned()))
    }

    /// The format named by the extension of a file, read as [`parse`](Self::parse) reads a
    /// name. Fails with [`RdfError::NoPathFormat`] if the path has no extension or one that
    /// names no format.
    pub fn from_path(path: &Path) -> Result<Self, RdfError> {
        path.extension()
            .and_then(|extension| Self::parse(extension.to_str()?).ok())
            .ok_or_else(|| RdfError::NoPathFormat(path.to_owned()))
    }

    /// The format of a file extension such as `ttl`, `nt`, `rdf` or `jsonld`
    /// (case-insensitive), or `None` if it names none.
    pub fn from_extension(extension: &str) -> Option<Self> {
        OxRdfFormat::from_extension(extension).and_then(Self::from_oxigraph)
    }

    /// The format of a media type such as `text/turtle` or
    /// `application/n-triples; charset=utf-8` (case-insensitive), or `None` if it names none.
    pub fn from_media_type(media_type: &str) -> Option<Self> {
        // oxigraph panics on a parameter whose value is a lone `"` (it strips the quotes of
        // `profile="…"` by slicing)
        let lone_quote = media_type.split(';').skip(1).any(|parameter| {
            parameter
                .split_once('=')
                .is_some_and(|(_, value)| value.trim() == "\"")
        });
        if lone_quote {
            return None;
        }
        OxRdfFormat::from_media_type(media_type).and_then(Self::from_oxigraph)
    }

    /// The IANA media type, such as `text/turtle`.
    pub fn media_type(self) -> &'static str {
        self.to_oxigraph().media_type()
    }

    /// The name of the format, such as `Turtle`.
    pub fn name(self) -> &'static str {
        self.to_oxigraph().name()
    }

    pub(crate) fn to_oxigraph(self) -> OxRdfFormat {
        match self {
            Self::N3 => OxRdfFormat::N3,
            Self::NQuads => OxRdfFormat::NQuads,
            Self::NTriples => OxRdfFormat::NTriples,
            Self::RdfXml => OxRdfFormat::RdfXml,
            Self::TriG => OxRdfFormat::TriG,
            Self::Turtle => OxRdfFormat::Turtle,
            Self::JsonLd => OxRdfFormat::JsonLd {
                profile: JsonLdProfileSet::empty(),
            },
        }
    }

    fn from_oxigraph(format: OxRdfFormat) -> Option<Self> {
        match format {
            OxRdfFormat::N3 => Some(Self::N3),
            OxRdfFormat::NQuads => Some(Self::NQuads),
            OxRdfFormat::NTriples => Some(Self::NTriples),
            OxRdfFormat::RdfXml => Some(Self::RdfXml),
            OxRdfFormat::TriG => Some(Self::TriG),
            OxRdfFormat::Turtle => Some(Self::Turtle),
            OxRdfFormat::JsonLd { .. } => Some(Self::JsonLd),
            _ => None,
        }
    }
}

impl fmt::Display for RdfFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A SPARQL query results format, for the results of `SELECT` and `ASK` queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QueryResultsFormat {
    /// [SPARQL 1.1 Query Results CSV](https://www.w3.org/TR/sparql11-results-csv-tsv/)
    Csv,
    /// [SPARQL 1.1 Query Results JSON](https://www.w3.org/TR/sparql11-results-json/)
    Json,
    /// [SPARQL 1.1 Query Results TSV](https://www.w3.org/TR/sparql11-results-csv-tsv/)
    Tsv,
    /// [SPARQL Query Results XML](https://www.w3.org/TR/rdf-sparql-XMLres/)
    Xml,
}

impl QueryResultsFormat {
    /// The format of a file extension such as `srj`, `srx`, `csv` or `tsv`
    /// (case-insensitive), or `None` if it names none.
    pub fn from_extension(extension: &str) -> Option<Self> {
        OxQueryResultsFormat::from_extension(extension).and_then(Self::from_oxigraph)
    }

    /// The format of a media type such as `application/sparql-results+json` or `text/csv`
    /// (case-insensitive), or `None` if it names none.
    pub fn from_media_type(media_type: &str) -> Option<Self> {
        OxQueryResultsFormat::from_media_type(media_type).and_then(Self::from_oxigraph)
    }

    /// The IANA media type, such as `application/sparql-results+json`.
    pub fn media_type(self) -> &'static str {
        self.to_oxigraph().media_type()
    }

    /// The name of the format, such as `SPARQL Results in JSON`.
    pub fn name(self) -> &'static str {
        self.to_oxigraph().name()
    }

    pub(crate) fn to_oxigraph(self) -> OxQueryResultsFormat {
        match self {
            Self::Csv => OxQueryResultsFormat::Csv,
            Self::Json => OxQueryResultsFormat::Json,
            Self::Tsv => OxQueryResultsFormat::Tsv,
            Self::Xml => OxQueryResultsFormat::Xml,
        }
    }

    fn from_oxigraph(format: OxQueryResultsFormat) -> Option<Self> {
        match format {
            OxQueryResultsFormat::Csv => Some(Self::Csv),
            OxQueryResultsFormat::Json => Some(Self::Json),
            OxQueryResultsFormat::Tsv => Some(Self::Tsv),
            OxQueryResultsFormat::Xml => Some(Self::Xml),
            _ => None,
        }
    }
}
