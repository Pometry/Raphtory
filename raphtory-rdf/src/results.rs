//! Serialising SPARQL results: SPARQL results formats (JSON, XML, CSV, TSV) for `SELECT` and
//! `ASK`, RDF formats for `CONSTRUCT` and `DESCRIBE`.
use crate::{
    export::{xml_text_safe, TripleSink},
    limits::{check_query_length, Capped},
    query::{dedup_triples, execute, on_query_stack, parse, QueryForm, SparqlOptions},
    QueryResultsFormat, RdfError, RdfFormat, WriteStats,
};
use indexmap::{IndexMap, IndexSet};
use oxigraph::{
    io::RdfSerializer,
    model::{NamedNode, NamedOrBlankNode, Term, TermRef, Triple},
    sparql::{
        results::QueryResultsSerializer, QueryEvaluationError, QueryResults, QueryTripleIter,
    },
};
use raphtory::db::api::view::DynamicGraph;
use rustc_hash::FxBuildHasher;
use std::io::{BufWriter, Write};

/// The output formats of SPARQL results: one for the results of `SELECT` and `ASK` queries
/// and one for the triples of `CONSTRUCT` and `DESCRIBE` queries.
///
/// Writing the results of a query whose slot is `None` fails with
/// [`RdfError::WrongResultsFormat`] before anything is evaluated or written. Convert from a
/// [`QueryResultsFormat`] or an [`RdfFormat`] to fill one slot, or use [`parse`](Self::parse)
/// to fill both from one name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SparqlFormat {
    /// The format of `SELECT` and `ASK` results.
    pub results: Option<QueryResultsFormat>,
    /// The format of `CONSTRUCT` and `DESCRIBE` results.
    pub graph: Option<RdfFormat>,
}

impl Default for SparqlFormat {
    /// SPARQL 1.1 Query Results JSON for `SELECT` and `ASK`, N-Triples for `CONSTRUCT` and
    /// `DESCRIBE`.
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
    /// Parses a format from a file extension, a name or a media type (case-insensitive, with an
    /// optional leading `.`). Each slot gets the format the name means for it, so one name can
    /// stand for two formats:
    ///
    /// | Name | `SELECT` and `ASK` | `CONSTRUCT` and `DESCRIBE` |
    /// |---|---|---|
    /// | `json`, `application/json` | SPARQL Results JSON | JSON-LD |
    /// | `xml`, `text/xml` | SPARQL Results XML | RDF/XML |
    /// | `txt`, `text/plain` | SPARQL Results CSV | N-Triples |
    /// | `csv`, `tsv`, `srj`, `srx`, `application/sparql-results+json`, ... | that format | none |
    /// | `nt`, `ttl`, `turtle`, `rdf`, `jsonld`, `text/turtle`, ... | none | that format |
    ///
    /// An RDF format can be named by its file extension, name or media type. A name that is
    /// neither gives [`RdfError::UnknownSparqlFormat`].
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

    /// The format of the results of a query of `form` for the first of `media_ranges` (the
    /// ranges of an `Accept` header with their quality, the preferred first) that names one,
    /// with the media type it is served as, or `None` if none does: a SPARQL results format for
    /// `SELECT` and `ASK`, an RDF format for `CONSTRUCT` and `DESCRIBE`, as
    /// [`QueryResultsFormat::from_media_type`] and [`RdfFormat::from_media_type`] read them.
    /// Only the slot for `form` is set.
    ///
    /// `*/*` and `type/*` stand for the first of these formats served for a media type in the
    /// range: JSON, XML, CSV, TSV for `SELECT` and `ASK`; Turtle, N-Triples, RDF/XML, JSON-LD,
    /// N-Quads, TriG for `CONSTRUCT` and `DESCRIBE`. A range of quality 0 is never chosen, and
    /// a media type that a more specific range gives a lower quality is skipped, so
    /// `text/xml;q=0, text/csv;q=0, text/*` gives TSV for `SELECT`.
    ///
    /// # Example
    /// ```
    /// use raphtory_rdf::{QueryForm, QueryResultsFormat, RdfFormat, SparqlFormat};
    ///
    /// let accepted = [("text/html", 1.0), ("text/csv", 1.0), ("*/*", 0.5)];
    /// let (select, media_type) =
    ///     SparqlFormat::from_media_types(QueryForm::Select, accepted).unwrap();
    /// assert_eq!(select.results, Some(QueryResultsFormat::Csv));
    /// assert_eq!(media_type, "text/csv; charset=utf-8");
    /// let (construct, media_type) =
    ///     SparqlFormat::from_media_types(QueryForm::Construct, accepted).unwrap();
    /// assert_eq!(construct.graph, Some(RdfFormat::Turtle));
    /// assert_eq!(media_type, "text/turtle");
    /// ```
    pub fn from_media_types<'a>(
        form: QueryForm,
        media_ranges: impl IntoIterator<Item = (&'a str, f32)>,
    ) -> Option<(Self, String)> {
        // each format with the media types it is served for, the preferred first
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

    /// Checks that this has a format for the results of `form`.
    fn check(&self, form: QueryForm) -> Result<(), RdfError> {
        if form.is_graph() {
            self.graph_format(form).map(|_| ())
        } else {
            self.results_format(form).map(|_| ())
        }
    }

    /// The format of `SELECT` and `ASK` results.
    fn results_format(&self, form: QueryForm) -> Result<QueryResultsFormat, RdfError> {
        self.results.ok_or_else(|| self.wrong_format(form))
    }

    /// The format of `CONSTRUCT` and `DESCRIBE` results.
    fn graph_format(&self, form: QueryForm) -> Result<RdfFormat, RdfError> {
        self.graph.ok_or_else(|| self.wrong_format(form))
    }

    /// The error for results of `form`, which this has no format for.
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

/// `type/` for the media range `type/*`, `*/` for `*/*`, `None` for a media type.
fn wildcard(range: &str) -> Option<&str> {
    range.strip_suffix('*').filter(|kind| kind.ends_with('/'))
}

/// Body of [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer).
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
                // RDF/XML groups the triples by subject into sets, which drops the duplicates
                QueryResults::Graph(triples) if format.graph != Some(RdfFormat::RdfXml) => {
                    QueryResults::Graph(QueryTripleIter::new(dedup_triples(triples)))
                }
                results => results,
            };
            let mut out = Capped::new(writer, options.max_result_bytes);
            let written = write_query_results(results, &mut out, &format, options.max_result_bytes);
            out.check(written)
        })
    })
}

/// Writes query results in `format`, at most `max_bytes` of them.
fn write_query_results<W: Write>(
    results: QueryResults<'_>,
    mut writer: W,
    format: &SparqlFormat,
    max_bytes: Option<usize>,
) -> Result<WriteStats, RdfError> {
    let mut stats = WriteStats::default();
    match results {
        QueryResults::Solutions(solutions) => {
            let results_format = format.results_format(QueryForm::Select)?;
            let mut out = BufWriter::new(writer);
            let mut serializer = QueryResultsSerializer::from_format(results_format.to_oxigraph())
                .serialize_solutions_to_writer(&mut out, solutions.variables().to_vec())?;
            for solution in solutions {
                let solution = solution.map_err(RdfError::from)?;
                if results_format == QueryResultsFormat::Xml {
                    // Fail rather than write XML that parsers reject or alter.
                    if let Some((_, value)) =
                        solution.iter().find(|(_, v)| !xml_text_safe(v.as_ref()))
                    {
                        return Err(match value.as_ref() {
                            TermRef::Literal(_) => RdfError::XmlUnsafeLiteral(value.to_string()),
                            // RDF 1.2 triple terms (see `xml_text_safe`)
                            _ => RdfError::XmlTripleTerm(value.to_string()),
                        });
                    }
                }
                serializer.serialize(&solution)?;
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
            let serializer = RdfSerializer::from_format(graph_format.to_oxigraph());
            stats = if graph_format == RdfFormat::RdfXml {
                // `TripleSink` needs the triples of a subject to be consecutive.
                let subjects = group_by_subject(triples).map_err(RdfError::from)?;
                let mut document = Capped::new(Vec::new(), max_bytes);
                let built = (|| {
                    let mut sink = TripleSink::new(serializer, &mut document);
                    for (subject, properties) in &subjects {
                        for (predicate, object) in properties {
                            sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
                        }
                    }
                    sink.finish()
                })();
                let stats = document.check(built.map_err(RdfError::from))?;
                if stats.skipped > 0 {
                    return Err(RdfError::RdfXmlSkipped {
                        skipped: stats.skipped,
                        triples: stats.written + stats.skipped,
                    });
                }
                writer.write_all(&document.into_inner())?;
                writer.flush()?;
                stats
            } else {
                let mut sink = TripleSink::new(serializer, writer);
                for triple in triples {
                    let triple = triple.map_err(RdfError::from)?;
                    sink.push(
                        triple.subject.as_ref(),
                        triple.predicate.as_ref(),
                        triple.object.as_ref(),
                    )?;
                }
                sink.finish()?
            };
        }
    }
    Ok(stats)
}

/// The predicate-object pairs of each subject: the subjects in the order of their first triple,
/// the pairs of a subject in the order they were first built.
type BySubject =
    IndexMap<NamedOrBlankNode, IndexSet<(NamedNode, Term), FxBuildHasher>, FxBuildHasher>;

/// Groups the triples of a CONSTRUCT or DESCRIBE result by subject, without duplicates, failing
/// on the first evaluation error.
fn group_by_subject(triples: QueryTripleIter<'_>) -> Result<BySubject, QueryEvaluationError> {
    let mut subjects = BySubject::default();
    for triple in triples {
        let Triple {
            subject,
            predicate,
            object,
        } = triple?;
        subjects
            .entry(subject)
            .or_default()
            .insert((predicate, object));
    }
    Ok(subjects)
}
