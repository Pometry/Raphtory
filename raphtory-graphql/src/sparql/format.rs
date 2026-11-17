use dynamic_graphql::Enum;
use raphtory_rdf::{QueryForm, RdfError, RdfFormat, SparqlFormat};

/// A format of SPARQL results: a SPARQL results format for SELECT and ASK, an RDF format for
/// CONSTRUCT and DESCRIBE. As the format of a SHACL shapes graph or report, an RDF format.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[graphql(name = "SparqlFormat")]
pub enum GqlSparqlFormat {
    /// SPARQL 1.1 Query Results JSON; JSON-LD for CONSTRUCT and DESCRIBE.
    Json,
    /// SPARQL Query Results XML; RDF/XML for CONSTRUCT and DESCRIBE.
    Xml,
    /// SPARQL 1.1 Query Results CSV.
    Csv,
    /// SPARQL 1.1 Query Results TSV.
    Tsv,
    /// Turtle.
    Turtle,
    /// N-Triples.
    NTriples,
    /// N-Quads.
    NQuads,
    /// TriG.
    Trig,
    /// RDF/XML.
    RdfXml,
    /// JSON-LD.
    JsonLd,
    /// N3.
    N3,
}

impl GqlSparqlFormat {
    const ALL: [Self; 11] = [
        Self::Json,
        Self::Xml,
        Self::Csv,
        Self::Tsv,
        Self::Turtle,
        Self::NTriples,
        Self::NQuads,
        Self::Trig,
        Self::RdfXml,
        Self::JsonLd,
        Self::N3,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Xml => "xml",
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Turtle => "ttl",
            Self::NTriples => "nt",
            Self::NQuads => "nq",
            Self::Trig => "trig",
            Self::RdfXml => "rdf",
            Self::JsonLd => "jsonld",
            Self::N3 => "n3",
        }
    }

    pub(crate) fn sparql_format(self) -> Result<SparqlFormat, RdfError> {
        SparqlFormat::parse(self.name())
    }

    pub(crate) fn rdf_format(self) -> Result<RdfFormat, RdfError> {
        RdfFormat::parse(self.name())
    }

    pub(crate) fn naming(form: QueryForm, format: SparqlFormat) -> Option<Self> {
        Self::ALL.into_iter().find(|name| {
            name.sparql_format().is_ok_and(|named| {
                if form.is_graph() {
                    named.graph == format.graph
                } else {
                    named.results == format.results
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::GqlSparqlFormat;
    use raphtory_rdf::{QueryForm, QueryResultsFormat, RdfFormat, SparqlFormat};

    #[test]
    fn every_format_has_a_value() {
        let named = |form, format: SparqlFormat| {
            GqlSparqlFormat::naming(form, format)
                .unwrap()
                .sparql_format()
                .unwrap()
        };
        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Csv,
            QueryResultsFormat::Tsv,
        ] {
            assert_eq!(
                named(QueryForm::Select, format.into()).results,
                Some(format)
            );
        }
        for format in [
            RdfFormat::Turtle,
            RdfFormat::NTriples,
            RdfFormat::NQuads,
            RdfFormat::TriG,
            RdfFormat::RdfXml,
            RdfFormat::JsonLd,
            RdfFormat::N3,
        ] {
            assert_eq!(
                named(QueryForm::Construct, format.into()).graph,
                Some(format)
            );
        }
    }
}
