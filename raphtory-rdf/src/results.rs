//! Serialising SPARQL results: SPARQL results formats (JSON, XML, CSV, TSV) for `SELECT` and
//! `ASK`, RDF formats for `CONSTRUCT` and `DESCRIBE`.
use crate::{
    export::{xml_text_safe, TripleSink},
    parse_rdf_format,
    query::{
        dedup_triples, execute, on_query_stack, parse, QueryForm, SparqlOptions, SparqlResults,
    },
    QueryResultsFormat, RdfError, RdfFormat, WriteStats,
};
use indexmap::{IndexMap, IndexSet};
use oxigraph::{
    io::RdfSerializer,
    model::{NamedNode, NamedOrBlankNode, Term, TermRef, Triple},
    sparql::{
        results::QueryResultsSerializer, QueryEvaluationError, QueryResults, QuerySolutionIter,
        QueryTripleIter,
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
        let graph = parse_rdf_format(s).ok();
        if results.is_none() && graph.is_none() {
            return Err(RdfError::UnknownSparqlFormat(s.to_owned()));
        }
        Ok(Self { results, graph })
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

impl SparqlResults {
    /// Writes the results in `format`: a SPARQL results format for `Solutions` and `Boolean`,
    /// an RDF format for `Graph`. Byte-identical to
    /// [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer) for the same query.
    ///
    /// # Example
    /// ```
    /// use raphtory::prelude::*;
    /// use raphtory_rdf::{QueryResultsFormat, RdfGraph, SparqlOptions};
    ///
    /// let g = Graph::new();
    /// g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
    /// let query = "SELECT ?who { raphtory:Alice raphtory:knows ?who }";
    /// let results = RdfGraph::from(g).sparql(query, &SparqlOptions::default()).unwrap();
    /// let mut csv = Vec::new();
    /// results.write(&mut csv, QueryResultsFormat::Csv).unwrap();
    /// assert_eq!(String::from_utf8(csv).unwrap(), "who\r\nraphtory:Bob\r\n");
    /// ```
    pub fn write<W: Write>(
        &self,
        writer: W,
        format: impl Into<SparqlFormat>,
    ) -> Result<WriteStats, RdfError> {
        let format = format.into();
        let results = match self {
            Self::Solutions { variables, rows } => {
                QueryResults::Solutions(QuerySolutionIter::from_tuples(
                    variables.clone().into(),
                    rows.iter().cloned().map(Ok),
                ))
            }
            Self::Boolean(value) => QueryResults::Boolean(*value),
            Self::Graph(triples) => {
                QueryResults::Graph(QueryTripleIter::new(triples.iter().cloned().map(Ok)))
            }
        };
        write_query_results(results, writer, &format)
    }
}

/// Body of [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer).
pub(crate) fn sparql_to_writer<W: Write + Send>(
    view: &DynamicGraph,
    query: &str,
    writer: W,
    format: SparqlFormat,
    options: &SparqlOptions,
) -> Result<WriteStats, RdfError> {
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
            write_query_results(results, writer, &format)
        })
    })
}

/// Writes query results; shared by [`SparqlResults::write`] and [`sparql_to_writer`].
fn write_query_results<W: Write>(
    results: QueryResults<'_>,
    mut writer: W,
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
                let solution = solution.map_err(RdfError::from)?;
                if results_format == QueryResultsFormat::Xml {
                    // Fail rather than write XML that parsers reject or alter.
                    if let Some((_, value)) =
                        solution.iter().find(|(_, v)| !xml_text_safe(v.as_ref()))
                    {
                        return Err(match value.as_ref() {
                            TermRef::NamedNode(_) | TermRef::BlankNode(_) | TermRef::Literal(_) => {
                                RdfError::XmlUnsafeLiteral(value.to_string())
                            }
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
                let mut document = Vec::new();
                let mut sink = TripleSink::new(serializer, &mut document);
                for (subject, properties) in &subjects {
                    for (predicate, object) in properties {
                        sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
                    }
                }
                let stats = sink.finish()?;
                if stats.skipped > 0 {
                    return Err(RdfError::RdfXmlSkipped {
                        skipped: stats.skipped,
                        triples: stats.written + stats.skipped,
                    });
                }
                writer.write_all(&document)?;
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
