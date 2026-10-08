//! Python bindings for RDF import and export and SPARQL queries (feature `rdf`).
//!
//! Adds `sparql` and `to_rdf` to every `GraphView` (so to `Graph`, `PersistentGraph` and all
//! their views) and `load_rdf` / `retract_rdf` to `Graph` and `PersistentGraph`. RDF terms cross
//! into Python as the Raphtory names they stand for (see [`name_of`]), so a value returned by
//! `sparql` can be passed straight to `g.node(..)`.
use crate::{
    errors::GraphError,
    python::graph::{
        graph::PyGraph, graph_with_deletions::PyPersistentGraph, views::graph_view::PyGraphView,
    },
    rdf::{
        literal_to_prop,
        model::{Term, TermRef},
        name_of, parse_rdf_format, serializer_with_prefixes, sparql_stack_size, timeout_from_secs,
        RdfError, RdfFormat, RdfMutationOps, RdfViewOps, SparqlFormat, SparqlOptions,
        SparqlResults,
    },
};
use chrono::Datelike;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyString},
    IntoPyObjectExt,
};
use raphtory_api::{core::entities::properties::prop::Prop, python::timeindex::EventTimeComponent};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

/// An RDF document passed from Python: its content or the path of a file.
pub(crate) enum RdfSource<'a> {
    Bytes(&'a [u8]),
    Path(PathBuf),
}

impl<'a> RdfSource<'a> {
    /// The source `source` stands for, or a `TypeError` if it is neither bytes nor a path, or
    /// is a str that looks like a document rather than a path.
    pub(crate) fn extract(source: &'a Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(bytes) = source.cast::<PyBytes>() {
            // `bytes` objects are immutable and `source` keeps this one alive, so the slice
            // stays valid while the GIL is released.
            Ok(Self::Bytes(bytes.as_bytes()))
        } else if let Ok(path) = source.extract::<PathBuf>() {
            if source.is_instance_of::<PyString>()
                && looks_like_document(&path.to_string_lossy())
                && !path.exists()
            {
                return Err(PyTypeError::new_err(
                    "RDF source is a str, which is read as a file path, but it looks like an RDF \
                     document and no such file exists; pass the document as bytes (e.g. \
                     doc.encode())",
                ));
            }
            Ok(Self::Path(path))
        } else {
            let type_name = source
                .get_type()
                .name()
                .map(|name| name.to_string())
                .unwrap_or_default();
            Err(PyTypeError::new_err(format!(
                "RDF source must be bytes (the document) or a str or PathLike (a file path), not {type_name}"
            )))
        }
    }

    /// The format to read: `format` if given, else the extension of a path (an unknown one is
    /// an error), else Turtle (which also reads N-Triples), for bytes and for a path without an
    /// extension, like the N-Triples that `to_rdf` writes to such a path.
    pub(crate) fn format(&self, format: Option<&str>) -> Result<RdfFormat, RdfError> {
        match (format, self) {
            (Some(format), _) => parse_rdf_format(format),
            (None, Self::Bytes(_)) => Ok(RdfFormat::Turtle),
            (None, Self::Path(path)) => path_format(path).unwrap_or(Ok(RdfFormat::Turtle)),
        }
    }

    /// Runs `read` on the document: the bytes, or the file opened for reading.
    pub(crate) fn read<R>(
        self,
        read: impl FnOnce(&mut dyn Read) -> Result<R, GraphError>,
    ) -> Result<R, GraphError> {
        match self {
            Self::Bytes(mut bytes) => read(&mut bytes),
            Self::Path(path) => {
                if path.is_dir() {
                    return Err(GraphError::IOErrorMsg(format!(
                        "cannot open '{}': it is a directory",
                        path.display()
                    )));
                }
                let mut file = File::open(&path).map_err(|error| {
                    GraphError::IOErrorMsg(format!("cannot open '{}': {error}", path.display()))
                })?;
                read(&mut file)
            }
        }
    }
}

/// The RDF format named by the extension of `path`, if it has one (`doc.` and `.nt` have none).
fn path_format(path: &Path) -> Option<Result<RdfFormat, RdfError>> {
    path.extension()
        .filter(|extension| !extension.is_empty())
        .map(|extension| parse_rdf_format(&extension.to_string_lossy()))
}

/// Whether a file path given as a `str` looks like an RDF document passed by mistake.
fn looks_like_document(path: &str) -> bool {
    let path = path.trim();
    path.contains('\n')
        || path.starts_with('@')
        || path.starts_with('<')
        || (path.contains(char::is_whitespace) && path.ends_with('.'))
}

/// Body of `load_rdf` (`retract == false`) and `retract_rdf` on both graph classes. Parsing and
/// writing run without the GIL. A `source` that is neither bytes nor a path raises the
/// `TypeError` of [`RdfSource::extract`].
fn read_document<G: RdfMutationOps + Sync>(
    graph: &G,
    time: EventTimeComponent,
    source: &Bound<'_, PyAny>,
    format: Option<&str>,
    base_iri: Option<&str>,
    retract: bool,
) -> PyResult<usize> {
    let document = RdfSource::extract(source)?;
    let format = document.format(format).map_err(GraphError::from)?;
    let read = |data: &mut dyn Read| {
        if retract {
            graph.retract_rdf(time, data, format, base_iri)
        } else {
            graph.load_rdf(time, data, format, base_iri)
        }
    };
    Ok(source.py().detach(|| document.read(read))?)
}

/// SPARQL results converted to `Send` Raphtory values, ready to become Python objects.
enum PySparqlResults {
    Rows {
        variables: Vec<String>,
        rows: Vec<Vec<Option<Prop>>>,
    },
    Boolean(bool),
    Triples(Vec<(String, String, Prop)>),
}

/// Runs `f`, which runs `query`, on a thread with the stack the query needs (see
/// [`sparql_stack_size`]): a long query could overflow the stack of the calling thread, which
/// would abort the Python process.
fn on_sparql_stack<R: Send>(
    query: &str,
    f: impl FnOnce() -> Result<R, GraphError> + Send,
) -> Result<R, GraphError> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("raphtory-sparql".to_owned())
            .stack_size(sparql_stack_size(query.len()))
            .spawn_scoped(scope, f)
            .map_err(|error| {
                GraphError::IOErrorMsg(format!(
                    "cannot start a thread for the SPARQL query: {error}"
                ))
            })?
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// The Raphtory name of a term, or its N-Triples form if it has none (only possible for terms
/// built by the query, such as a time graph IRI).
pub(crate) fn term_name(term: TermRef<'_>) -> String {
    name_of(term).unwrap_or_else(|| term.to_string())
}

/// The Python value of a term: its name, or the literal's value if `decode_literals` is set and
/// the literal's datatype is supported (and the value fits the Python type).
fn term_value(term: &Term, decode_literals: bool) -> Prop {
    if decode_literals {
        if let Term::Literal(literal) = term {
            if let Some(value) = literal_to_prop(literal).filter(fits_python) {
                return value;
            }
        }
    }
    Prop::str(term_name(term.as_ref()))
}

/// Whether a decoded literal can be converted to Python: a `datetime` only holds the years 1 to
/// 9999 (after the conversion to UTC for a date-time with a timezone).
fn fits_python(value: &Prop) -> bool {
    const PYTHON_YEARS: std::ops::RangeInclusive<i32> = 1..=9999;
    match value {
        Prop::DTime(value) => PYTHON_YEARS.contains(&value.year()),
        Prop::NDTime(value) => PYTHON_YEARS.contains(&value.year()),
        _ => true,
    }
}

impl PySparqlResults {
    fn new(results: SparqlResults, decode_literals: bool) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => Self::Rows {
                variables: variables.iter().map(|v| v.as_str().to_owned()).collect(),
                rows: rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| value.as_ref().map(|t| term_value(t, decode_literals)))
                            .collect()
                    })
                    .collect(),
            },
            SparqlResults::Boolean(value) => Self::Boolean(value),
            SparqlResults::Graph(triples) => Self::Triples(
                triples
                    .iter()
                    .map(|triple| {
                        (
                            term_name(triple.subject.as_ref().into()),
                            term_name(triple.predicate.as_ref().into()),
                            term_value(&triple.object, decode_literals),
                        )
                    })
                    .collect(),
            ),
        }
    }

    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Rows { variables, rows } => {
                let list = PyList::empty(py);
                for row in rows {
                    let dict = PyDict::new(py);
                    for (variable, value) in variables.iter().zip(row) {
                        dict.set_item(variable, value)?;
                    }
                    list.append(dict)?;
                }
                Ok(list.into_any().unbind())
            }
            Self::Boolean(value) => value.into_py_any(py),
            Self::Triples(triples) => triples.into_py_any(py),
        }
    }
}

#[pymethods]
impl PyGraphView {
    /// Runs a SPARQL 1.1 query on the RDF triples of this view.
    ///
    /// Every edge is one triple per layer, `src layer dst`, where node and layer names are RDF terms:
    /// an absolute IRI is kept as written, `_:label` is a blank node, a name in N-Triples literal form
    /// (e.g. `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`) is that literal, and any other name is
    /// the IRI `raphtory:` followed by the percent-encoded name (the `raphtory:` prefix is
    /// pre-registered, so the node `Alice` is `raphtory:Alice` and the default layer is
    /// `raphtory:_default`).
    ///
    /// The query sees the triples visible in `valid()`: on a PersistentGraph, the triples whose latest
    /// event in the view is an assertion (so `g.snapshot_at(t).sparql(..)` queries the state as of
    /// `t`); on a Graph, every triple asserted at least once in the view. Inside a query,
    /// `GRAPH <raphtory:asof:T> { .. }` matches its patterns against `snapshot_at(T)`, where `T` is
    /// epoch milliseconds or a date-time such as `2024-01-01`.
    ///
    /// Queries can also ask since when a triple has held. `raphtory:validFrom(s, p, o)` returns the time
    /// since which a triple visible in this view has held, and `raphtory:validTo(s, p, o)` the time it
    /// stopped holding, as `xsd:dateTime` values in UTC (a datetime with `decode_literals`);
    /// `raphtory:validFromTime` and `raphtory:validToTime` return the same times as integers (Raphtory
    /// times). An optional fourth argument is the reference time: a time graph such as
    /// `raphtory:asof:2024-01-01` (or the `?g` of `GRAPH ?g`, in a `BIND` after the `GRAPH` pattern), an
    /// integer or an `xsd:dateTime`. Without it they answer for the present of the view, so `validTo` is
    /// unbound. They are unbound for a triple the view (as of the reference time) does not show, and for
    /// arguments they do not accept. On a Graph a triple holds from its first assertion on; use
    /// `persistent_graph()` for intervals that end at retractions.
    ///
    /// `SERVICE` calls (SPARQL federated queries) are not supported: a query that calls a service
    /// raises a GraphError, and no query ever makes a network request.
    ///
    /// Every value is returned as the Raphtory name of its term (a str), so it can be passed to
    /// `node()` or `layer()`. The exceptions are IRIs under `raphtory:` that do not encode a name,
    /// such as the time graphs `<raphtory:asof:T>`: they are returned in N-Triples form, with angle
    /// brackets.
    ///
    /// With `format`, the results are instead returned serialized, as one str: for SELECT and ASK in
    /// a SPARQL results format ("json", "xml", "csv" or "tsv"), for CONSTRUCT and DESCRIBE in an RDF
    /// format (such as "nt", "ttl", "jsonld" or "rdf"). A name means what it means for the form of
    /// the query: "json" is SPARQL Results JSON for SELECT and ASK and JSON-LD for CONSTRUCT and
    /// DESCRIBE, "xml" is SPARQL Results XML or RDF/XML. Serialized results hold RDF terms, as
    /// `to_rdf()` writes them (the node `Alice` is `raphtory:Alice`), not Raphtory names. CSV loses
    /// the kind, datatype and language of values; SPARQL Results XML cannot hold a literal with a
    /// control character other than tab and line feed (including a carriage return), which raises an
    /// error; and RDF/XML skips the triples `to_rdf()` skips.
    ///
    /// By default a query runs until it is done. To run queries you do not control, bound them with
    /// `timeout`, which stops the query once it has run that long, and `max_triple_patterns`, which
    /// rejects a query that is too large before it is planned (planning cannot be interrupted by
    /// `timeout`). Each triple pattern counts one, including those of collections such as
    /// `(1 2 3)`; a property path counts one per predicate it names, a `BIND` or an expression
    /// `(... AS ?v)` of `SELECT` or `GROUP BY` one, and a `VALUES` block one plus one per variable
    /// and per 100 rows.
    ///
    /// Arguments:
    ///     query (str): The SPARQL query.
    ///     decode_literals (bool): If True, literals are returned as Python values (str, bool, int, float, Decimal or datetime) where their datatype is supported and the value fits the Python type (a datetime from year 1 to 9999), instead of their names. Cannot be combined with `format`. Defaults to False.
    ///     format (str, optional): If given, the results are returned as a document in this format, as a file extension, name or media type (e.g. "json", "xml", "csv", "tsv", "application/sparql-results+json" for SELECT and ASK; "nt", "ttl", "jsonld", "rdf", "text/turtle" for CONSTRUCT and DESCRIBE). Defaults to None.
    ///     timeout (float, optional): If given, the query is stopped with a GraphError once it has run this many seconds; if not, there is no time limit. Defaults to None.
    ///     max_triple_patterns (int, optional): If given, a query with more triple patterns raises a GraphError before it runs; if not, there is no limit. Defaults to None.
    ///
    /// Returns:
    ///     str | list[dict[str, Any]] | bool | list[tuple[str, str, Any]]: If `format` is given, the serialized results. Otherwise, for SELECT, one dict per solution mapping every variable (in the order of the SELECT clause, or sorted by name for `SELECT *`) to its value (None if unbound); for ASK, a bool; for CONSTRUCT and DESCRIBE, one (subject, predicate, object) tuple per triple.
    ///
    /// Raises:
    ///     GraphError: If the query does not parse or its evaluation fails (for example because it calls an unknown function), if `format` is unknown or does not fit the form of the query, if SPARQL Results XML cannot hold a value, if the query has more than `max_triple_patterns` triple patterns, or if it runs longer than `timeout`.
    ///     ValueError: If both `decode_literals` and `format` are given, or if `timeout` is negative or not finite.
    #[pyo3(signature = (query, decode_literals = false, format = None, timeout = None, max_triple_patterns = None))]
    fn sparql(
        &self,
        py: Python<'_>,
        query: &str,
        decode_literals: bool,
        format: Option<&str>,
        timeout: Option<f64>,
        max_triple_patterns: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let timeout = timeout
            .map(|seconds| {
                timeout_from_secs(seconds).ok_or_else(|| {
                    PyValueError::new_err("timeout must be a finite number of seconds, at least 0")
                })
            })
            .transpose()?;
        let options = SparqlOptions::default()
            .with_timeout(timeout)
            .with_max_triple_patterns(max_triple_patterns);
        let graph = self.graph.clone();
        let Some(format) = format else {
            let results = py.detach(move || {
                on_sparql_stack(query, || {
                    Ok(PySparqlResults::new(
                        graph.sparql_with(query, &options)?,
                        decode_literals,
                    ))
                })
            })?;
            return results.into_python(py);
        };
        if decode_literals {
            return Err(PyValueError::new_err(
                "decode_literals cannot be combined with format: serialized results hold RDF \
                 terms, not Python values",
            ));
        }
        let format = SparqlFormat::parse(format).map_err(GraphError::from)?;
        // the document is built in memory and thrown away if the query fails
        let document = py.detach(move || {
            on_sparql_stack(query, || {
                let mut document = Vec::new();
                graph.sparql_to_writer_with(query, &mut document, format, &options)?;
                String::from_utf8(document).map_err(|error| {
                    GraphError::IOErrorMsg(format!("SPARQL output is not UTF-8: {error}"))
                })
            })
        })?;
        document.into_py_any(py)
    }

    /// Exports the RDF triples of this view, the same triples `sparql()` sees.
    ///
    /// Every edge visible in `valid()` is written as one triple per layer, `src layer dst`, with
    /// node and layer names mapped to RDF terms as described in `sparql()`. Edges that are not valid
    /// RDF (from a node named by a literal, or in a layer named by a literal or a blank node) are
    /// skipped. RDF/XML also skips triples it cannot represent: those whose predicate IRI does not
    /// end in an XML name, is in the `xmlns` namespace or is an RDF/XML syntax term such as `rdf:li`,
    /// an `rdf:type` triple whose object IRI cannot name an element when it is its subject's only
    /// triple, and literals with a control character other than tab and line feed; a blank node label
    /// starting with a digit is written with an `x` in front (`_:1` is `x1`). Only the state of the
    /// view is exported, not its history.
    ///
    /// Arguments:
    ///     path (str | PathLike, optional): The file to write. If not given, the document is returned as a string instead. Defaults to None.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (e.g. "nt", "ttl", "turtle", "trig", "rdf"). If not given, it is taken from the extension of `path` (an unknown extension raises an error), and is N-Triples if there is no `path` or it has no extension. Defaults to None.
    ///     prefixes (dict[str, str], optional): Prefix names mapped to IRIs, used by formats that support them (Turtle, TriG, RDF/XML). A name must be empty or a valid prefix name of the format, e.g. a letter followed by letters, digits, '_', '-' or '.' (not ending with '.'). Defaults to None.
    ///
    /// Returns:
    ///     Optional[str]: The document if `path` is not given, otherwise None.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, a prefix name or IRI is invalid, or writing fails.
    #[pyo3(signature = (path = None, format = None, prefixes = None))]
    fn to_rdf(
        &self,
        py: Python<'_>,
        path: Option<PathBuf>,
        format: Option<&str>,
        prefixes: Option<HashMap<String, String>>,
    ) -> Result<Option<String>, GraphError> {
        let format = match (format, &path) {
            (Some(format), _) => parse_rdf_format(format)?,
            (None, Some(path)) => path_format(path).unwrap_or(Ok(RdfFormat::NTriples))?,
            (None, None) => RdfFormat::NTriples,
        };
        let mut prefixes: Vec<_> = prefixes.into_iter().flatten().collect();
        prefixes.sort();
        let serializer = serializer_with_prefixes(format, prefixes)?;
        let graph = self.graph.clone();
        py.detach(move || match path {
            Some(path) => {
                let mut file = File::create(&path).map_err(|error| {
                    GraphError::IOErrorMsg(format!("cannot create '{}': {error}", path.display()))
                })?;
                graph.to_rdf(&mut file, serializer)?;
                file.flush()?;
                Ok(None)
            }
            None => {
                let mut document = Vec::new();
                graph.to_rdf(&mut document, serializer)?;
                let document = String::from_utf8(document).map_err(|error| {
                    GraphError::IOErrorMsg(format!("RDF output is not UTF-8: {error}"))
                })?;
                Ok(Some(document))
            }
        })
    }
}

#[pymethods]
impl PyGraph {
    /// Asserts every triple of an RDF document at `time`.
    ///
    /// Each triple `s p o` is added as an edge from `s` to `o` in the layer `p`. Node and layer names
    /// are RDF terms: IRIs as written, blank nodes as `_:label` (renamed to fresh labels on every
    /// load) and literals in their N-Triples form, e.g. `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`,
    /// so identical literals share a node. Named graphs are rejected. The load is not atomic: triples
    /// read before an error stay in the graph.
    ///
    /// Each triple is written as one edge event, in document order, so a load can run while other
    /// threads query or write the graph.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are asserted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (e.g. "ttl", "nt", "turtle", "text/turtle", "trig", "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If `source` is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (time, source, format = None, base_iri = None))]
    fn load_rdf(
        &self,
        time: EventTimeComponent,
        source: &Bound<'_, PyAny>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<usize> {
        read_document(&self.graph, time, source, format, base_iri, false)
    }

    /// Retracts every triple of an RDF document at `time`.
    ///
    /// Each triple `s p o` is deleted from the layer `p` of the edge from `s` to `o`, with names
    /// mapped as in `load_rdf()`. A retraction is recorded even if the triple was never asserted.
    /// On a Graph, retractions are only seen through `persistent_graph()`. Blank-node labels are used
    /// as written, so they must be the stored labels (as returned by `to_rdf()` or `sparql()`).
    /// Each triple is written as one edge event, in document order.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are retracted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (e.g. "ttl", "nt", "turtle", "text/turtle", "trig", "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If `source` is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (time, source, format = None, base_iri = None))]
    fn retract_rdf(
        &self,
        time: EventTimeComponent,
        source: &Bound<'_, PyAny>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<usize> {
        read_document(&self.graph, time, source, format, base_iri, true)
    }
}

#[pymethods]
impl PyPersistentGraph {
    /// Asserts every triple of an RDF document at `time`.
    ///
    /// Each triple `s p o` is added as an edge from `s` to `o` in the layer `p`. Node and layer names
    /// are RDF terms: IRIs as written, blank nodes as `_:label` (renamed to fresh labels on every
    /// load) and literals in their N-Triples form, e.g. `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`,
    /// so identical literals share a node. Named graphs are rejected. The load is not atomic: triples
    /// read before an error stay in the graph.
    ///
    /// Each triple is written as one edge event, in document order, so a load can run while other
    /// threads query or write the graph.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are asserted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (e.g. "ttl", "nt", "turtle", "text/turtle", "trig", "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If `source` is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (time, source, format = None, base_iri = None))]
    fn load_rdf(
        &self,
        time: EventTimeComponent,
        source: &Bound<'_, PyAny>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<usize> {
        read_document(&self.graph, time, source, format, base_iri, false)
    }

    /// Retracts every triple of an RDF document at `time`.
    ///
    /// Each triple `s p o` is deleted from the layer `p` of the edge from `s` to `o`, with names
    /// mapped as in `load_rdf()`, so from `time` on it is no longer visible (until it is asserted
    /// again). A retraction is recorded even if the triple was never asserted. Blank-node labels are
    /// used as written, so they must be the stored labels (as returned by `to_rdf()` or `sparql()`).
    /// Each triple is written as one edge event, in document order.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are retracted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (e.g. "ttl", "nt", "turtle", "text/turtle", "trig", "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If `source` is neither bytes nor a path, or is a str that looks like a document rather than a path.
    #[pyo3(signature = (time, source, format = None, base_iri = None))]
    fn retract_rdf(
        &self,
        time: EventTimeComponent,
        source: &Bound<'_, PyAny>,
        format: Option<&str>,
        base_iri: Option<&str>,
    ) -> PyResult<usize> {
        read_document(&self.graph, time, source, format, base_iri, true)
    }
}
