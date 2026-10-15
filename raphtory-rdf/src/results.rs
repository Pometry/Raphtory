//! Serialising SPARQL results.
use crate::{
    limits::{check_query_length, Capped},
    query::{dedup_triples, execute, on_query_stack, parse, QueryForm, SparqlOptions},
    QueryResultsFormat, RdfError, RdfFormat, WriteStats,
};
use oxigraph::{
    io::RdfSerializer,
    sparql::{results::QueryResultsSerializer, QueryResults, QueryTripleIter},
};
use raphtory::db::api::view::DynamicGraph;
use std::io::{BufWriter, Write};

/// The output formats of SPARQL results: one for `SELECT`/`ASK`, one for `CONSTRUCT`/`DESCRIBE`.
/// A query whose slot is `None` fails before it is evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SparqlFormat {
    pub results: Option<QueryResultsFormat>,
    pub graph: Option<RdfFormat>,
}

impl Default for SparqlFormat {
    /// SPARQL Results JSON and N-Triples.
    fn default() -> Self {
        Self {
            results: Some(QueryResultsFormat::Json),
            graph: Some(RdfFormat::NTriples),
        }
    }
}

impl From<QueryResultsFormat> for SparqlFormat {
    fn from(format: QueryResultsFormat) -> Self {
        Self {
            results: Some(format),
            graph: None,
        }
    }
}

impl From<RdfFormat> for SparqlFormat {
    fn from(format: RdfFormat) -> Self {
        Self {
            results: None,
            graph: Some(format),
        }
    }
}

impl SparqlFormat {
    /// Parses a file extension, name or media type into the format it means for each slot.
    /// `json` is SPARQL Results JSON and JSON-LD; `xml` is SPARQL Results XML and RDF/XML.
    pub fn parse(s: &str) -> Result<Self, RdfError> {
        let t = s.trim();
        let t = t.strip_prefix('.').unwrap_or(t);
        let results = QueryResultsFormat::from_extension(t)
            .or_else(|| QueryResultsFormat::from_media_type(t))
            .or_else(|| QueryResultsFormat::from_media_type(&format!("application/{t}")));
        let graph = RdfFormat::parse(s).ok();
        if results.is_none() && graph.is_none() {
            return Err(RdfError::UnknownSparqlFormat(s.to_owned()));
        }
        Ok(Self { results, graph })
    }

    /// The format for `form`, and its media type, of the first `Accept` range that names one.
    /// `media_ranges` come preferred first; a specific range of lower quality overrides a wildcard.
    pub fn from_media_types<'a>(
        form: QueryForm,
        media_ranges: impl IntoIterator<Item = (&'a str, f32)>,
    ) -> Option<(Self, String)> {
        // the preferred first
        const RESULTS: [(QueryResultsFormat, &str); 7] = [
            (QueryResultsFormat::Json, "application/sparql-results+json"),
            (QueryResultsFormat::Json, "application/json"),
            (QueryResultsFormat::Xml, "application/sparql-results+xml"),
            (QueryResultsFormat::Xml, "application/xml"),
            (QueryResultsFormat::Xml, "text/xml"),
            (QueryResultsFormat::Csv, "text/csv"),
            (QueryResultsFormat::Tsv, "text/tab-separated-values"),
        ];
        const GRAPH: [(RdfFormat, &str); 11] = [
            (RdfFormat::Turtle, "text/turtle"),
            (RdfFormat::Turtle, "application/x-turtle"),
            (RdfFormat::Turtle, "application/turtle"),
            (RdfFormat::NTriples, "application/n-triples"),
            (RdfFormat::RdfXml, "application/rdf+xml"),
            (RdfFormat::RdfXml, "application/xml"),
            (RdfFormat::RdfXml, "text/xml"),
            (RdfFormat::JsonLd, "application/ld+json"),
            (RdfFormat::JsonLd, "application/json"),
            (RdfFormat::NQuads, "application/n-quads"),
            (RdfFormat::TriG, "application/trig"),
        ];
        let ranges: Vec<(String, f32)> = media_ranges
            .into_iter()
            .map(|(range, q)| {
                let essence = range.split(';').next().unwrap_or_default().trim();
                (essence.to_ascii_lowercase(), q)
            })
            .collect();
        ranges
            .iter()
            .filter(|(_, q)| *q > 0.0)
            .find_map(|(range, q)| {
                let Some(kind) = wildcard(range) else {
                    let format = if form.is_graph() {
                        RdfFormat::from_media_type(range).map(Self::from)
                    } else {
                        QueryResultsFormat::from_media_type(range).map(Self::from)
                    };
                    return format.map(|format| (format, range.as_str()));
                };
                // a more specific range of lower quality decides for the media types it names
                let served = |media_type: &str| {
                    (kind == "*/" || media_type.starts_with(kind))
                        && !ranges.iter().any(|(other, other_q)| {
                            other_q < q
                                && (other == media_type
                                    || (kind == "*/"
                                        && wildcard(other).is_some_and(|other| {
                                            other != "*/" && media_type.starts_with(other)
                                        })))
                        })
                };
                if form.is_graph() {
                    let (format, media_type) = GRAPH.into_iter().find(|(_, m)| served(m))?;
                    Some((Self::from(format), media_type))
                } else {
                    let (format, media_type) = RESULTS.into_iter().find(|(_, m)| served(m))?;
                    Some((Self::from(format), media_type))
                }
            })
            .map(|(format, media_type)| {
                // the format's own media type keeps its parameters, such as `charset=utf-8`
                let own = format
                    .results
                    .map(QueryResultsFormat::media_type)
                    .or(format.graph.map(RdfFormat::media_type))
                    .filter(|own| own.split(';').next() == Some(media_type));
                (format, own.unwrap_or(media_type).to_owned())
            })
    }

    fn check(&self, form: QueryForm) -> Result<(), RdfError> {
        if form.is_graph() {
            self.graph_format(form).map(|_| ())
        } else {
            self.results_format(form).map(|_| ())
        }
    }

    fn results_format(&self, form: QueryForm) -> Result<QueryResultsFormat, RdfError> {
        self.results.ok_or_else(|| self.wrong_format(form))
    }

    fn graph_format(&self, form: QueryForm) -> Result<RdfFormat, RdfError> {
        self.graph.ok_or_else(|| self.wrong_format(form))
    }

    fn wrong_format(&self, form: QueryForm) -> RdfError {
        let format = match (self.results, self.graph) {
            (Some(results), _) => results.name(),
            (None, Some(graph)) => graph.name(),
            (None, None) => "no format",
        };
        RdfError::WrongResultsFormat {
            form,
            format: format.to_owned(),
        }
    }
}

// `type/` for `type/*`, `*/` for `*/*`
fn wildcard(range: &str) -> Option<&str> {
    range.strip_suffix('*').filter(|kind| kind.ends_with('/'))
}

pub(crate) fn sparql_to_writer<W: Write + Send>(
    view: &DynamicGraph,
    query: &str,
    writer: W,
    format: SparqlFormat,
    options: &SparqlOptions,
) -> Result<WriteStats, RdfError> {
    check_query_length(query, options.max_query_length)?;
    on_query_stack(query, || {
        let query = parse(query)?;
        format.check(QueryForm::of(&query))?;
        execute(view, query, options, |results| {
            let results = match results {
                QueryResults::Graph(triples) => {
                    QueryResults::Graph(QueryTripleIter::new(dedup_triples(triples)))
                }
                results => results,
            };
            let mut out = Capped::new(writer, options.max_result_bytes);
            let written = write_query_results(results, &mut out, &format);
            out.check(written)
        })
    })
}

fn write_query_results<W: Write>(
    results: QueryResults<'_>,
    writer: W,
    format: &SparqlFormat,
) -> Result<WriteStats, RdfError> {
    let mut stats = WriteStats::default();
    match results {
        QueryResults::Solutions(solutions) => {
            let results_format = format.results_format(QueryForm::Select)?;
            let mut out = BufWriter::new(writer);
            let mut serializer = QueryResultsSerializer::from_format(results_format.to_oxigraph())
                .serialize_solutions_to_writer(&mut out, solutions.variables().to_vec())?;
            for solution in solutions {
                serializer.serialize(&solution.map_err(RdfError::from)?)?;
                stats.written += 1;
            }
            serializer.finish()?;
            out.flush()?;
        }
        QueryResults::Boolean(value) => {
            let results_format = format.results_format(QueryForm::Ask)?;
            let mut out = BufWriter::new(writer);
            QueryResultsSerializer::from_format(results_format.to_oxigraph())
                .serialize_boolean_to_writer(&mut out, value)?;
            out.flush()?;
        }
        QueryResults::Graph(triples) => {
            let graph_format = format.graph_format(QueryForm::Construct)?;
            let mut out = RdfSerializer::from_format(graph_format.to_oxigraph())
                .for_writer(BufWriter::new(writer));
            for triple in triples {
                out.serialize_triple(&triple.map_err(RdfError::from)?)?;
                stats.written += 1;
            }
            out.finish()?.flush()?;
        }
    }
    Ok(stats)
}
