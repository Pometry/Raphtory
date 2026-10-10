//! Python bindings for RDF (feature `rdf`): the `RdfGraph` and `RdfView` classes.
//!
//! RDF terms cross to and from Python as the Raphtory node names they stand for (see
//! [`name_of`] and [`term_of`]), predicates included, so a value returned by `sparql` can be
//! passed straight to `rg.graph.node(..)` or `add_triple`.
use crate::{
    db::{
        api::view::{internal::MaterializedGraph, DynamicGraph, IntoDynamic, StaticGraphViewOps},
        graph::views::{node_subgraph::NodeSubgraph, valid_graph::ValidGraph},
    },
    errors::GraphError,
    prelude::*,
    python::utils::PyNodeRef,
    rdf::{
        literal_to_prop,
        model::{Term, TermRef, Triple},
        name_of, parse_rdf_format, serializer_with_prefixes, term_of, timeout_from_secs, RdfError,
        RdfFormat, SparqlFormat, SparqlOptions, SparqlResults,
    },
};
use chrono::Datelike;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyString, PyType},
    IntoPyObjectExt,
};
use raphtory_api::{core::storage::arc_str::ArcStr, python::timeindex::EventTimeComponent};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
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

/// Body of `load_rdf` (`retract == false`) and `retract_rdf`. Parsing and writing run without
/// the GIL. A `source` that is neither bytes nor a path raises the
/// `TypeError` of [`RdfSource::extract`].
fn read_document(
    graph: &RdfGraph,
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

/// A timeout in seconds as a Duration ([`timeout_from_secs`]).
fn timeout_of(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    seconds
        .map(|seconds| {
            timeout_from_secs(seconds).map_err(|_| {
                PyValueError::new_err("timeout must be a finite number of seconds, at least 0")
            })
        })
        .transpose()
}

/// The triple of the terms of three node names ([`term_of`]).
fn triple_of(subject: &str, predicate: &str, object: &str) -> Result<Triple, GraphError> {
    Ok(
        Triple::from_terms(term_of(subject), term_of(predicate), term_of(object))
            .map_err(RdfError::from)?,
    )
}

/// A view of an RdfGraph: the RDF triples of a persistent graph view, one per edge and layer
/// visible in it.
///
/// Every view method (window, at, latest, before, after, layers, filter, valid, subgraph, ...)
/// returns an RdfView, and sparql, to_rdf and the SHACL methods see exactly the triples of the
/// view: under valid() the triples valid at the end of the view (on the whole graph, the current
/// state), under at(t) the state as of t, under a window every triple that held in it, and on the
/// whole graph every triple ever asserted or retracted.
#[pyclass(name = "RdfView", frozen, subclass, module = "raphtory")]
pub struct PyRdfView {
    pub(crate) view: RdfView,
}

impl_timeops!(PyRdfView, view, RdfView, "RdfView");
impl_filter_ops!(PyRdfView<RdfView>, view, "RdfView");
impl_layerops!(PyRdfView, view, RdfView, "RdfView");

impl<'py, G: StaticGraphViewOps + IntoDynamic> IntoPyObject<'py> for RdfView<G> {
    type Target = PyRdfView;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            PyRdfView {
                view: self.into_dynamic(),
            },
        )
    }
}

#[pymethods]
impl PyRdfView {
    /// The same view as a property graph, for algorithms and for its nodes and edges.
    ///
    /// Returns:
    ///     GraphView: The view.
    #[getter]
    fn graph(&self) -> DynamicGraph {
        self.view.graph().clone()
    }

    /// Return a view of the triples valid at the end of this view.
    ///
    /// On the whole graph that is the current state. The order of views matters: x.valid() is
    /// the state at the end of x, and valid().x is the current state further restricted to x.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn valid(&self) -> RdfView<ValidGraph<DynamicGraph>> {
        self.view.valid()
    }

    /// Return a view of the triples between the given nodes.
    ///
    /// Arguments:
    ///     nodes (list[NodeInput]): The nodes to keep.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn subgraph(&self, nodes: Vec<PyNodeRef>) -> RdfView<NodeSubgraph<DynamicGraph>> {
        self.view.subgraph(nodes)
    }

    /// Return a view of the triples between nodes of the given node types.
    ///
    /// Arguments:
    ///     node_types (list[str]): The node types to keep.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn subgraph_node_types(&self, node_types: Vec<ArcStr>) -> RdfView {
        self.view.subgraph_node_types(node_types).into_dynamic()
    }

    /// Return a view without the triples of the given nodes.
    ///
    /// Arguments:
    ///     nodes (list[NodeInput]): The nodes to exclude.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn exclude_nodes(&self, nodes: Vec<PyNodeRef>) -> RdfView<NodeSubgraph<DynamicGraph>> {
        self.view.exclude_nodes(nodes)
    }

    /// Runs a SPARQL 1.1 query on the RDF triples of this view.
    ///
    /// Every edge is one triple per layer, src layer dst, where node names are RDF terms: an
    /// absolute IRI is kept as written, _:label is a blank node, a name in N-Triples literal form
    /// (such as "42"^^<http://www.w3.org/2001/XMLSchema#integer>) is that literal, and any other
    /// name is the IRI raphtory: followed by the percent-encoded name (the raphtory: prefix is
    /// pre-registered, so the node Alice is raphtory:Alice). Layer names must be safe as file
    /// names, so the layer of a predicate IRI is the IRI with %, /, :, ? and * percent-escaped as
    /// %25, %2F, %3A, %3F and %2A (the predicate <http://ex/knows> is the layer
    /// http%3A%2F%2Fex%2Fknows), and any other layer name is raphtory: followed by the
    /// percent-encoded name (the default layer is raphtory:_default).
    ///
    /// The query sees exactly the triples of this view: rg.valid().sparql(..) queries the
    /// current state and rg.at(t).sparql(..) the state as of t. Inside a query,
    /// GRAPH <raphtory:asof:T> { .. } matches its patterns against snapshot_at(T) of this view,
    /// where T is epoch milliseconds or a date-time such as 2024-01-01.
    ///
    /// raphtory:validFrom(s, p, o) returns the time since which a triple visible in this view
    /// has held, and raphtory:validTo(s, p, o) the time it stopped holding, as xsd:dateTime
    /// values in UTC (a datetime with decode_literals); raphtory:validFromTime and
    /// raphtory:validToTime return the same times as integers (Raphtory times). An optional
    /// fourth argument is the reference time: a time graph such as raphtory:asof:2024-01-01 (or
    /// the ?g of GRAPH ?g, in a BIND after the GRAPH pattern), an integer or an xsd:dateTime.
    /// Without it they answer for the end of the view, so validTo is unbound for a triple that
    /// still holds. They are unbound for a triple the view (as of the reference time) does not
    /// show, and for arguments they do not accept.
    ///
    /// SERVICE calls (SPARQL federated queries) are not supported: a query that calls a service
    /// raises a GraphError, and no query ever makes a network request.
    ///
    /// Every value is returned as the node name of its term (a str), so it can be passed to
    /// graph.node(); an IRI predicate of a CONSTRUCT or DESCRIBE triple is returned as its IRI.
    /// IRIs under raphtory: that do not encode a name, such as the time graphs
    /// <raphtory:asof:T>, are returned in N-Triples form, with angle brackets.
    ///
    /// With format, the results are instead returned serialized, as one str: for SELECT and ASK
    /// in a SPARQL results format ("json", "xml", "csv" or "tsv"), for CONSTRUCT and DESCRIBE in
    /// an RDF format (such as "nt", "ttl", "jsonld" or "rdf"). "json" is SPARQL Results JSON for
    /// SELECT and ASK and JSON-LD for CONSTRUCT and DESCRIBE, and "xml" is SPARQL Results XML or
    /// RDF/XML. Serialized results hold RDF terms, as to_rdf() writes them, not Raphtory names.
    ///
    /// By default a query runs until it is done. To run queries you do not control, bound them
    /// with timeout, which stops the query once it has run that long, and max_triple_patterns,
    /// which rejects a query that is too large before it is planned (planning cannot be
    /// interrupted by timeout). Each triple pattern counts one, a property path one per
    /// predicate it names, a BIND or a SELECT or GROUP BY expression one, and a VALUES block one
    /// plus one per variable and per 100 rows.
    ///
    /// Arguments:
    ///     query (str): The SPARQL query.
    ///     decode_literals (bool): If True, literals are returned as Python values (str, bool, int, float, Decimal or datetime) where their datatype is supported and the value fits the Python type (a datetime from year 1 to 9999), instead of their names. Cannot be combined with format. Defaults to False.
    ///     format (str, optional): If given, the results are returned as a document in this format, as a file extension, name or media type (such as "json", "xml", "csv", "tsv" or "application/sparql-results+json" for SELECT and ASK, and "nt", "ttl", "jsonld", "rdf" or "text/turtle" for CONSTRUCT and DESCRIBE). Defaults to None.
    ///     timeout (float, optional): If given, the query is stopped with a GraphError once it has run this many seconds. Defaults to None.
    ///     max_triple_patterns (int, optional): If given, a query with more triple patterns raises a GraphError before it runs. Defaults to None.
    ///
    /// Returns:
    ///     list[dict[str, Any]] | bool | list[tuple[str, str, Any]] | str: For SELECT, one dict per solution mapping every variable (in the order of the SELECT clause, or sorted by name for SELECT *) to its value (None if unbound); for ASK, a bool; for CONSTRUCT and DESCRIBE, one (subject, predicate, object) tuple per triple; with format, the serialized results.
    ///
    /// Raises:
    ///     GraphError: If the query does not parse or its evaluation fails, if format is unknown or does not fit the form of the query, if SPARQL Results XML cannot hold a value, if the query has more than max_triple_patterns triple patterns, or if it runs longer than timeout.
    ///     ValueError: If both decode_literals and format are given, or if timeout is negative or not finite.
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
        let options = SparqlOptions::default()
            .with_timeout(timeout_of(timeout)?)
            .with_max_triple_patterns(max_triple_patterns);
        let Some(format) = format else {
            let results = py.detach(|| {
                self.view
                    .sparql_with(query, &options)
                    .map(|results| PySparqlResults::new(results, decode_literals))
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
        let document = py.detach(|| {
            let mut document = Vec::new();
            self.view
                .sparql_to_writer_with(query, &mut document, format, &options)?;
            String::from_utf8(document).map_err(|error| {
                GraphError::IOErrorMsg(format!("SPARQL output is not UTF-8: {error}"))
            })
        })?;
        document.into_py_any(py)
    }

    /// Exports the RDF triples of this view, the same triples sparql() sees.
    ///
    /// Every edge visible in this view is written as one triple per layer, src layer dst, with
    /// node and layer names mapped to RDF terms as described in sparql(). Edges that are not
    /// valid RDF (from a node named by a literal, or in a layer named by a literal or a blank
    /// node) are skipped. RDF/XML also skips triples it cannot represent: those whose predicate
    /// IRI does not end in an XML name, is in the xmlns namespace or is an RDF/XML syntax term
    /// such as rdf:li, an rdf:type triple whose object IRI cannot name an element when it is its
    /// subject's only triple, and literals with a control character other than tab and line
    /// feed; a blank node label starting with a digit is written with an x in front (_:1 is x1).
    /// Only the state of the view is exported, not its history.
    ///
    /// Arguments:
    ///     path (str | PathLike, optional): The file to write. If not given, the document is returned as a string instead. Defaults to None.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (such as "nt", "ttl", "turtle", "trig" or "rdf"). If not given, it is taken from the extension of path (an unknown extension raises an error), and is N-Triples if there is no path or it has no extension. Defaults to None.
    ///     prefixes (dict[str, str], optional): Prefix names mapped to IRIs, used by formats that support them (Turtle, TriG, RDF/XML). A name must be empty or a valid prefix name of the format, such as a letter followed by letters, digits, '_', '-' or '.' (not ending with '.'). Defaults to None.
    ///
    /// Returns:
    ///     Optional[str]: The document if path is not given, otherwise None.
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
        let mut prefixes: Vec<(&str, &str)> = prefixes
            .iter()
            .flatten()
            .map(|(name, iri)| (name.as_str(), iri.as_str()))
            .collect();
        prefixes.sort();
        py.detach(|| match path {
            Some(path) => {
                // before the file is truncated
                serializer_with_prefixes(format, prefixes.iter().copied()).map(drop)?;
                let mut file = File::create(&path).map_err(|error| {
                    GraphError::IOErrorMsg(format!("cannot create '{}': {error}", path.display()))
                })?;
                self.view.to_rdf(&mut file, format, &prefixes)?;
                file.flush()?;
                Ok(None)
            }
            None => {
                let mut document = Vec::new();
                self.view.to_rdf(&mut document, format, &prefixes)?;
                let document = String::from_utf8(document).map_err(|error| {
                    GraphError::IOErrorMsg(format!("RDF output is not UTF-8: {error}"))
                })?;
                Ok(Some(document))
            }
        })
    }
}

/// A graph viewed as RDF, with persistent semantics: the place to load, retract and query
/// triples.
///
/// Each triple s p o is an edge from s to o in the layer of p, and a retraction is a deletion.
/// Wrapping a Graph shares its storage, so nothing is copied and writes through either handle
/// are seen by both; retractions are seen by the RdfGraph and ignored by event views of the same
/// graph. The whole graph shows every triple ever asserted or retracted: valid() is the current
/// state and at(t) the state as of t.
///
/// Arguments:
///     graph (Graph | PersistentGraph, optional): The graph to view as RDF. If not given, a new empty graph is created. Defaults to None.
#[pyclass(name = "RdfGraph", extends = PyRdfView, frozen, module = "raphtory")]
pub struct PyRdfGraph {
    graph: RdfGraph,
}

impl PyRdfGraph {
    fn initializer(graph: RdfGraph) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyRdfView {
            view: graph.clone().into_dynamic(),
        })
        .add_subclass(Self { graph })
    }
}

#[pymethods]
impl PyRdfGraph {
    #[new]
    #[pyo3(signature = (graph = None))]
    fn py_new(graph: Option<MaterializedGraph>) -> PyClassInitializer<Self> {
        Self::initializer(graph.map_or_else(RdfGraph::new, RdfGraph::from))
    }

    /// Opens the graph stored at path, event or persistent, as an RdfGraph.
    ///
    /// Arguments:
    ///     path (str | PathLike): The path of the graph folder.
    ///
    /// Returns:
    ///     RdfGraph: The graph.
    #[staticmethod]
    fn load(py: Python<'_>, path: PathBuf) -> PyResult<Py<Self>> {
        Py::new(py, Self::initializer(RdfGraph::load(&path)?))
    }

    /// The graph as a property graph, sharing its storage.
    ///
    /// Returns:
    ///     PersistentGraph: The graph.
    #[getter]
    fn graph(&self) -> PersistentGraph {
        self.graph.graph().clone()
    }

    /// Asserts every triple of an RDF document at time.
    ///
    /// Each triple s p o is added as an edge from s to o in the layer of p. Node names are RDF
    /// terms: IRIs as written, blank nodes as _:label (renamed to fresh labels on every load) and
    /// literals in their N-Triples form, such as "42"^^<http://www.w3.org/2001/XMLSchema#integer>,
    /// so identical literals share a node. The layer of a predicate IRI not under raphtory: is
    /// the IRI with %, /, :, ? and * percent-escaped as %25, %2F, %3A, %3F and %2A, so that layer
    /// names are safe as file names: the layer of <http://ex/knows> is http%3A%2F%2Fex%2Fknows.
    /// The layer of <raphtory:x> is x percent-decoded, and a predicate whose layer would be blank
    /// or contain a control character or any of / \ : * ? " < > | raises an error. Named graphs
    /// are rejected. Each triple is written as one edge event, in document order, so a load can
    /// run while other threads query or write the graph; it is not atomic: triples read before
    /// an error stay in the graph.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are asserted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (such as "ttl", "nt", "turtle", "text/turtle", "trig" or "rdf"). If not given, it is taken from the extension of a path (an unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes and for a path without an extension. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If source is neither bytes nor a path, or is a str that looks like a document rather than a path.
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

    /// Retracts every triple of an RDF document at time.
    ///
    /// Each triple s p o is deleted from the layer of p of the edge from s to o, with names
    /// mapped as in load_rdf(), so from time on it does not hold (until it is asserted again). A
    /// retraction is recorded even if the triple was never asserted. Blank-node labels are used
    /// as written, so they must be the stored labels (as returned by to_rdf() or sparql()). Each
    /// triple is written as one edge event, in document order.
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triples are retracted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as in load_rdf(). Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the format is unknown, the document does not parse, the graph uses integer node ids, or the operation fails.
    ///     TypeError: If source is neither bytes nor a path, or is a str that looks like a document rather than a path.
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

    /// Asserts one triple at time.
    ///
    /// The terms are given as the node names sparql() returns, so its values can be passed
    /// straight back: an absolute IRI as written (http://ex/alice), _:label for a blank node
    /// (used as written), a literal in its N-Triples form ("Bob" or
    /// "42"^^<http://www.w3.org/2001/XMLSchema#integer>), and any other name as raphtory: followed
    /// by the percent-encoded name (Alice is raphtory:Alice).
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triple is asserted.
    ///     subject (str): The subject, such as http://ex/alice or _:b0.
    ///     predicate (str): The predicate, such as http://ex/knows.
    ///     object (str): The object, such as http://ex/bob or "Bob".
    ///
    /// Returns:
    ///     None:
    ///
    /// Raises:
    ///     GraphError: If a term cannot be in its position (such as a literal subject), or the triple cannot be stored.
    fn add_triple(
        &self,
        time: EventTimeComponent,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Result<(), GraphError> {
        self.graph
            .add_triple(time, &triple_of(subject, predicate, object)?)
    }

    /// Retracts one triple at time.
    ///
    /// The terms are given as the node names sparql() returns, so its values can be passed
    /// straight back: an absolute IRI as written (http://ex/alice), _:label for a blank node
    /// (used as written), a literal in its N-Triples form ("Bob" or
    /// "42"^^<http://www.w3.org/2001/XMLSchema#integer>), and any other name as raphtory: followed
    /// by the percent-encoded name (Alice is raphtory:Alice).
    ///
    /// Arguments:
    ///     time (TimeInput): The time at which the triple is retracted.
    ///     subject (str): The subject, such as http://ex/alice or _:b0.
    ///     predicate (str): The predicate, such as http://ex/knows.
    ///     object (str): The object, such as http://ex/bob or "Bob".
    ///
    /// Returns:
    ///     None:
    ///
    /// Raises:
    ///     GraphError: If a term cannot be in its position (such as a literal subject), or the triple cannot be stored.
    fn retract_triple(
        &self,
        time: EventTimeComponent,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Result<(), GraphError> {
        self.graph
            .retract_triple(time, &triple_of(subject, predicate, object)?)
    }

    fn __reduce__<'py>(slf: &Bound<'py, Self>) -> (Bound<'py, PyType>, (PersistentGraph,)) {
        (slf.get_type(), (slf.get().graph.graph().clone(),))
    }
}
