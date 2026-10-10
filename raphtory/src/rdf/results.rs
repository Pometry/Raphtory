//! Serialising SPARQL results: SPARQL results formats (JSON, XML, CSV, TSV) for `SELECT` and
//! `ASK`, RDF formats for `CONSTRUCT` and `DESCRIBE`.
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    rdf::{
        export::{serializer_with_prefixes, xml_text_safe, TripleSink},
        parse_rdf_format,
        query::{
            dedup_triples, execute, on_query_stack, parse, QueryForm, SparqlOptions, SparqlResults,
        },
        RdfError, WriteStats,
    },
};
use indexmap::{IndexMap, IndexSet};
use oxigraph::{
    io::{RdfFormat, RdfSerializer},
    model::{NamedNode, NamedOrBlankNode, Term, TermRef, Triple},
    sparql::{
        results::{QueryResultsFormat, QueryResultsSerializer},
        QueryEvaluationError, QueryResults, QuerySolutionIter, QueryTripleIter,
    },
};
use rustc_hash::FxBuildHasher;
use std::io::{BufWriter, Write};

/// The output formats of SPARQL results: one for the results of `SELECT` and `ASK` queries
/// and one for the triples of `CONSTRUCT` and `DESCRIBE` queries.
///
/// Writing the results of a query whose slot is `None` fails with
/// [`RdfError::WrongResultsFormat`] before anything is evaluated or written. Convert from a
/// [`QueryResultsFormat`] or an [`RdfFormat`] to fill one slot, or use [`parse`](Self::parse)
/// to fill both from one name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparqlFormat {
    /// The format of `SELECT` and `ASK` results.
    pub results: Option<QueryResultsFormat>,
    /// The format of `CONSTRUCT` and `DESCRIBE` results.
    pub graph: Option<RdfFormat>,
    /// The prefix names and IRIs that `graph` abbreviates IRIs with, as in
    /// [`RdfView::to_rdf`](crate::rdf::RdfView::to_rdf); a name the format cannot write gives
    /// [`RdfError::InvalidPrefix`] before the query is evaluated.
    pub prefixes: Vec<(String, String)>,
}

impl Default for SparqlFormat {
    /// SPARQL 1.1 Query Results JSON for `SELECT` and `ASK`, N-Triples for `CONSTRUCT` and
    /// `DESCRIBE`.
    fn default() -> Self {
        Self {
            results: Some(QueryResultsFormat::Json),
            graph: Some(RdfFormat::NTriples),
            prefixes: Vec::new(),
        }
    }
}

impl From<QueryResultsFormat> for SparqlFormat {
    fn from(format: QueryResultsFormat) -> Self {
        Self {
            results: Some(format),
            graph: None,
            prefixes: Vec::new(),
        }
    }
}

impl From<RdfFormat> for SparqlFormat {
    fn from(format: RdfFormat) -> Self {
        Self {
            results: None,
            graph: Some(format),
            prefixes: Vec::new(),
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
    /// The RDF formats are the ones of [`parse_rdf_format`]. A name that is neither gives
    /// [`RdfError::UnknownSparqlFormat`].
    pub fn parse(s: &str) -> Result<Self, RdfError> {
        let t = s.trim();
        let t = t.strip_prefix('.').unwrap_or(t);
        // `QueryResultsFormat::from_media_type` cannot panic on quotes; `parse_rdf_format`
        // guards the input that makes `RdfFormat` panic.
        let results = QueryResultsFormat::from_extension(t)
            .or_else(|| QueryResultsFormat::from_media_type(t))
            .or_else(|| QueryResultsFormat::from_media_type(&format!("application/{t}")));
        let graph = parse_rdf_format(s).ok();
        if results.is_none() && graph.is_none() {
            return Err(RdfError::UnknownSparqlFormat(s.to_owned()));
        }
        Ok(Self {
            results,
            graph,
            prefixes: Vec::new(),
        })
    }

    /// The format with `prefixes` (see [`prefixes`](Self::prefixes)).
    pub fn with_prefixes(
        self,
        prefixes: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let prefixes = prefixes
            .into_iter()
            .map(|(name, iri)| (name.into(), iri.into()))
            .collect();
        Self { prefixes, ..self }
    }

    /// Checks that this has a format for the results of `form`.
    fn check(&self, form: Form) -> Result<(), RdfError> {
        if form.is_graph() {
            self.graph_serializer(form).map(|_| ())
        } else {
            self.results_format(form).map(|_| ())
        }
    }

    /// The format of `SELECT` and `ASK` results.
    fn results_format(&self, form: Form) -> Result<QueryResultsFormat, RdfError> {
        self.results.ok_or_else(|| self.wrong_format(form))
    }

    /// The serializer of `CONSTRUCT` and `DESCRIBE` results.
    fn graph_serializer(&self, form: Form) -> Result<RdfSerializer, RdfError> {
        let format = self.graph.ok_or_else(|| self.wrong_format(form))?;
        let prefixes = self
            .prefixes
            .iter()
            .map(|(name, iri)| (name.as_str(), iri.as_str()));
        serializer_with_prefixes(format, prefixes)
    }

    /// The error for results of `form`, which this has no format for.
    fn wrong_format(&self, form: Form) -> RdfError {
        let format = match (&self.results, &self.graph) {
            (Some(results), _) => results.name().to_owned(),
            // a JSON-LD format with a profile would be named "Streaming JSON-LD"
            (None, Some(graph)) => match graph {
                RdfFormat::JsonLd { .. } => "JSON-LD".to_owned(),
                format => format.name().to_owned(),
            },
            (None, None) => "no format".to_owned(),
        };
        let expected = if form.is_graph() {
            "an RDF format such as nt, ttl, jsonld or rdf"
        } else {
            "a SPARQL results format: json, xml, csv or tsv"
        };
        RdfError::WrongResultsFormat {
            form: form.name(),
            format,
            expected,
        }
    }
}

/// The form of results, which decides the slot of a [`SparqlFormat`] they need.
#[derive(Clone, Copy)]
enum Form {
    Query(QueryForm),
    /// The triples of a `CONSTRUCT` or a `DESCRIBE` query, once collected.
    Graph,
}

impl Form {
    fn of_results(results: &SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { .. } => Self::Query(QueryForm::Select),
            SparqlResults::Boolean(_) => Self::Query(QueryForm::Ask),
            SparqlResults::Graph(_) => Self::Graph,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Query(form) => form.name(),
            Self::Graph => "CONSTRUCT and DESCRIBE",
        }
    }

    fn is_graph(self) -> bool {
        match self {
            Self::Query(form) => form.is_graph(),
            Self::Graph => true,
        }
    }
}

impl SparqlResults {
    /// Writes the results in `format`: a SPARQL results format for `Solutions` and `Boolean`,
    /// an RDF format for `Graph`. Byte-identical to
    /// [`RdfView::sparql_to_writer`](crate::rdf::RdfView::sparql_to_writer) for the same query.
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     prelude::*,
    ///     rdf::QueryResultsFormat,
    /// };
    ///
    /// let g = Graph::new();
    /// g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
    /// let results = RdfGraph::from(g).sparql("SELECT ?who { raphtory:Alice raphtory:knows ?who }").unwrap();
    /// let mut csv = Vec::new();
    /// results.write(&mut csv, QueryResultsFormat::Csv).unwrap();
    /// assert_eq!(String::from_utf8(csv).unwrap(), "who\r\nraphtory:Bob\r\n");
    /// ```
    pub fn write<W: Write>(
        &self,
        writer: W,
        format: impl Into<SparqlFormat>,
    ) -> Result<WriteStats, GraphError> {
        let format = format.into();
        format.check(Form::of_results(self))?;
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

/// Body of [`RdfView::sparql_to_writer_with`](crate::rdf::RdfView::sparql_to_writer_with).
pub(crate) fn sparql_to_writer<G: StaticGraphViewOps + IntoDynamic, W: Write + Send>(
    view: &G,
    query: &str,
    writer: W,
    format: SparqlFormat,
    options: &SparqlOptions,
) -> Result<WriteStats, GraphError> {
    on_query_stack(query, || {
        let query = parse(query)?;
        format.check(Form::Query(QueryForm::of(&query)))?;
        execute(view, query, options, |results| {
            write_query_results(results, writer, &format)
        })
    })
}

/// Writes query results; shared by [`SparqlResults::write`] and [`sparql_to_writer`].
fn write_query_results<W: Write>(
    results: QueryResults<'_>,
    writer: W,
    format: &SparqlFormat,
) -> Result<WriteStats, GraphError> {
    let mut stats = WriteStats::default();
    match results {
        QueryResults::Solutions(solutions) => {
            let results_format = format.results_format(Form::Query(QueryForm::Select))?;
            let mut out = BufWriter::new(writer);
            let mut serializer = QueryResultsSerializer::from_format(results_format)
                .serialize_solutions_to_writer(&mut out, solutions.variables().to_vec())?;
            for solution in solutions {
                let solution = solution?;
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
                            #[allow(unreachable_patterns)]
                            _ => RdfError::XmlTripleTerm(value.to_string()),
                        }
                        .into());
                    }
                }
                serializer.serialize(&solution)?;
                stats.written += 1;
            }
            serializer.finish()?;
            out.flush()?;
        }
        QueryResults::Boolean(value) => {
            let results_format = format.results_format(Form::Query(QueryForm::Ask))?;
            let mut out = BufWriter::new(writer);
            QueryResultsSerializer::from_format(results_format)
                .serialize_boolean_to_writer(&mut out, value)?;
            out.flush()?;
        }
        QueryResults::Graph(triples) => {
            let serializer = format.graph_serializer(Form::Graph)?;
            stats = if serializer.format() == RdfFormat::RdfXml {
                // `TripleSink` needs the triples of a subject to be consecutive.
                let subjects = group_by_subject(triples)?;
                let mut sink = TripleSink::new(serializer, writer);
                for (subject, properties) in &subjects {
                    for (predicate, object) in properties {
                        sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
                    }
                }
                sink.finish()?
            } else {
                let mut sink = TripleSink::new(serializer, writer);
                for triple in dedup_triples(triples) {
                    let triple = triple?;
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
