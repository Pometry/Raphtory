//! Python bindings for RDF: the `RdfGraph` and `RdfView` classes.
//!
//! RDF terms cross to and from Python as the Raphtory node names they stand for (see
//! [`term_name`] and [`term_of`]), predicates included, so a value returned by `sparql` can be
//! passed straight to `rg.graph.node(..)` or `add_triple`.
use crate::{
    term_name, term_of, RdfError, RdfFormat, RdfGraph, RdfView, SparqlFormat, SparqlOptions,
    SparqlResults, Triple,
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pybacked::PyBackedBytes,
    types::{PyDict, PyList, PyString},
    IntoPyObjectExt,
};
use raphtory::{
    db::{
        api::{
            state::ops::filter::NodeTypeFilterOp,
            view::{
                internal::MaterializedGraph, DynamicGraph, IntoDynHop, IntoDynamic,
                StaticGraphViewOps,
            },
        },
        graph::views::{
            cached_view::CachedView, filter::node_filtered_graph::NodeFilteredGraph,
            node_subgraph::NodeSubgraph, valid_graph::ValidGraph,
        },
    },
    impl_filter_ops, impl_layerops, impl_timeops,
    prelude::*,
    python::{types::iterable::FromIterable, utils::PyNodeRef},
};
use raphtory_api::{
    core::{
        storage::arc_str::ArcStr,
        utils::time::{InputTime, TryIntoInputTime},
    },
    python::{error::adapt_err_value, timeindex::EventTimeComponent},
};
use std::path::PathBuf;

impl From<RdfError> for PyErr {
    fn from(err: RdfError) -> Self {
        match err {
            RdfError::Graph(err) => err.into(),
            RdfError::SparqlSyntax(_)
            | RdfError::SparqlTooDeep { .. }
            | RdfError::Parse(_)
            | RdfError::UnknownFormat(_)
            | RdfError::NoPathFormat(_)
            | RdfError::UnknownSparqlFormat(_)
            | RdfError::WrongResultsFormat { .. } => PyValueError::new_err(err.to_string()),
            err => adapt_err_value(&err),
        }
    }
}

#[derive(FromPyObject)]
pub(crate) enum RdfSource {
    Bytes(PyBackedBytes),
    Path(PathBuf),
}

enum PySparqlResults {
    Rows {
        variables: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    Boolean(bool),
    Triples(Vec<(String, String, String)>),
}

impl PySparqlResults {
    fn new(results: SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => Self::Rows {
                variables: variables.iter().map(|v| v.as_str().to_owned()).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|v| v.map(|t| term_name(t.as_ref())))
                            .collect()
                    })
                    .collect(),
            },
            SparqlResults::Boolean(value) => Self::Boolean(value),
            SparqlResults::Graph(triples) => Self::Triples(
                triples
                    .into_iter()
                    .map(|triple| {
                        (
                            term_name(triple.subject.as_ref().into()),
                            term_name(triple.predicate.as_ref().into()),
                            term_name(triple.object.as_ref()),
                        )
                    })
                    .collect(),
            ),
        }
    }

    fn into_python(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Rows { variables, rows } => {
                let keys: Vec<_> = variables.iter().map(|v| PyString::intern(py, v)).collect();
                let list = PyList::empty(py);
                for row in rows {
                    let dict = PyDict::new(py);
                    for (key, value) in keys.iter().zip(row) {
                        dict.set_item(key, value)?;
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

fn triple_of(subject: &str, predicate: &str, object: &str) -> Result<Triple, RdfError> {
    Ok(Triple::from_terms(
        term_of(subject),
        term_of(predicate),
        term_of(object),
    )?)
}

fn input_time(
    timestamp: EventTimeComponent,
    event_id: Option<usize>,
) -> Result<InputTime, RdfError> {
    Ok(match event_id {
        None => timestamp.try_into_input_time()?,
        Some(event_id) => (timestamp, event_id).try_into_input_time()?,
    })
}

/// A view of an RdfGraph: the RDF triples of a persistent graph view, one per edge and layer
/// visible in it.
///
/// Every view method (window, at, latest, before, after, layers, filter, valid, subgraph, ...)
/// returns an RdfView, and sparql, to_rdf and validate_shacl see exactly the triples of the
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
                view: self.into_dyn_hop(),
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

    /// Return a view of only the triples still valid at the end of this view.
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
    fn subgraph(&self, nodes: FromIterable<PyNodeRef>) -> RdfView<NodeSubgraph<DynamicGraph>> {
        self.view.subgraph(nodes)
    }

    /// Return a view of the triples between nodes of the given node types.
    ///
    /// Arguments:
    ///     node_types (list[str]): The node types to keep.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn subgraph_node_types(
        &self,
        node_types: FromIterable<ArcStr>,
    ) -> RdfView<NodeFilteredGraph<DynamicGraph, NodeTypeFilterOp>> {
        self.view.subgraph_node_types(node_types)
    }

    /// Return a view without the triples of the given nodes.
    ///
    /// Arguments:
    ///     nodes (list[NodeInput]): The nodes to exclude.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn exclude_nodes(&self, nodes: FromIterable<PyNodeRef>) -> RdfView<NodeSubgraph<DynamicGraph>> {
        self.view.exclude_nodes(nodes)
    }

    /// Return a view with the nodes and edges visible in this view precomputed.
    ///
    /// Returns:
    ///     RdfView: The view.
    fn cache_view(&self) -> RdfView<CachedView<DynamicGraph>> {
        self.view.cache_view()
    }

    /// Runs a SPARQL 1.1 query on the RDF triples of this view.
    ///
    /// The query sees exactly the triples of this view: rg.valid().sparql(..) queries the
    /// current state and rg.at(t).sparql(..) the state as of t. Inside a query,
    /// GRAPH <raphtory:asof:T> { .. } matches its patterns against before(T + 1).valid() of this
    /// view, where T is epoch milliseconds or a date-time such as 2024-01-01. On rg.valid() a
    /// time graph holds only the triples that held at T and still hold at the end of the view,
    /// so compare times on a view without valid().
    ///
    /// raphtory:validFrom(s, p, o) returns when the latest run of a triple visible in this view
    /// started (or the start of the view, if it already held there), and raphtory:validTo(s, p, o)
    /// the retraction that ended that run (unbound if it still holds at the end of the view), as
    /// xsd:dateTime values in UTC; raphtory:validFromTime and
    /// raphtory:validToTime return the same times as integers (Raphtory times). An optional
    /// fourth argument, a time graph such as raphtory:asof:2024-01-01 (or the ?g of GRAPH ?g, in
    /// a BIND after the GRAPH pattern), makes them answer in that time graph instead. They are
    /// unbound for a triple they do not see, and for arguments they do not accept.
    ///
    /// SERVICE calls (SPARQL federated queries) are not supported: a query that calls a service
    /// raises a GraphError, and no query ever makes a network request.
    ///
    /// Every value, predicates included, is returned as the name of its term (a str), so a node
    /// can be passed to graph.node().
    ///
    /// With format, the results are instead returned serialized, as one str: for SELECT and ASK
    /// in a SPARQL results format ("json", "xml", "csv" or "tsv"), for CONSTRUCT and DESCRIBE in
    /// an RDF format (such as "nt", "ttl", "jsonld" or "rdf"). "json" is SPARQL Results JSON for
    /// SELECT and ASK and JSON-LD for CONSTRUCT and DESCRIBE, and "xml" is SPARQL Results XML or
    /// RDF/XML. Serialized results hold RDF terms, as to_rdf() writes them, not Raphtory names.
    ///
    /// Arguments:
    ///     query (str): The SPARQL query.
    ///     format (str, optional): If given, the results are returned as a document in this format, as a file extension, name or media type (such as "json", "xml", "csv", "tsv" or "application/sparql-results+json" for SELECT and ASK, and "nt", "ttl", "jsonld", "rdf" or "text/turtle" for CONSTRUCT and DESCRIBE). Defaults to None.
    ///
    /// Returns:
    ///     Union[list[dict[str, Optional[str]]], bool, list[tuple[str, str, str]], str]: For SELECT, one dict per solution mapping every variable (in the order of the SELECT clause, or sorted by name for SELECT *) to its value (None if unbound); for ASK, a bool; for CONSTRUCT and DESCRIBE, one (subject, predicate, object) tuple per triple; with format, the serialized results.
    ///
    /// Raises:
    ///     GraphError: If the evaluation of the query fails, or if SPARQL Results XML cannot hold a value or RDF/XML a triple (such as one whose predicate does not end in an XML name).
    ///     ValueError: If the query does not parse, or if format is unknown or does not fit the form of the query.
    #[pyo3(signature = (query, format = None))]
    fn sparql(&self, py: Python<'_>, query: &str, format: Option<&str>) -> PyResult<Py<PyAny>> {
        let options = SparqlOptions::default();
        let Some(format) = format else {
            let results =
                py.detach(|| self.view.sparql(query, &options).map(PySparqlResults::new))?;
            return results.into_python(py);
        };
        let format = SparqlFormat::parse(format)?;
        let document = py.detach(|| self.view.sparql_to_string(query, format, &options))?;
        document.into_py_any(py)
    }

    /// Exports the RDF triples of this view, the same triples sparql() sees.
    ///
    /// Every edge visible in this view is written as one triple per layer, src layer dst. Edges
    /// that are not valid RDF (from a node named by a literal, or in a layer named by a literal or
    /// a blank node) are skipped. RDF/XML also skips triples it cannot represent: those whose
    /// predicate IRI does not end in an XML name, is in the xmlns namespace or is an RDF/XML syntax
    /// term such as rdf:li, an rdf:type triple whose object IRI cannot name an element when it is
    /// its subject's only triple, and literals with a control character other than tab and line
    /// feed. Only the state of the view is exported, not its history.
    ///
    /// Arguments:
    ///     path (str | PathLike, optional): The file to write. If not given, the document is returned as a string instead. Defaults to None.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (such as "nt", "ttl", "turtle", "trig" or "rdf"). If not given, it is taken from the extension of path (an unknown extension raises an error), and is N-Triples if there is no path or it has no extension. Defaults to None.
    ///
    /// Returns:
    ///     Optional[str]: The document if path is not given, otherwise None.
    ///
    /// Raises:
    ///     GraphError: If writing fails.
    ///     ValueError: If the format is unknown.
    #[pyo3(signature = (path = None, format = None))]
    fn to_rdf(
        &self,
        py: Python<'_>,
        path: Option<PathBuf>,
        format: Option<&str>,
    ) -> Result<Option<String>, RdfError> {
        let format = format.map(RdfFormat::parse).transpose()?;
        py.detach(|| match path {
            Some(path) => {
                self.view.to_rdf_path(path, format)?;
                Ok(None)
            }
            None => Ok(Some(self.view.to_rdf_string(format)?)),
        })
    }
}

/// A graph viewed as RDF: the place to load, retract and query triples. It always uses
/// persistent semantics.
///
/// Each triple s p o is an edge from s to o in the layer of p, and a retraction is a deletion.
/// Wrapping a Graph shares its storage, so nothing is copied and writes through either handle
/// are seen by both. The whole graph shows every triple ever asserted or retracted: valid() is
/// the current state and at(t) the state as of t.
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
            view: graph.clone().into_dyn_hop(),
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

    /// The graph as a property graph, sharing its storage.
    ///
    /// Returns:
    ///     PersistentGraph: The graph.
    #[getter]
    fn graph(&self) -> PersistentGraph {
        self.graph.graph().clone()
    }

    /// Asserts every triple of an RDF document at timestamp.
    ///
    /// Each triple s p o is added as an edge from s to o in the layer of p. Blank nodes get fresh
    /// labels on every load. Named graphs are rejected. Each triple is written as one edge event,
    /// in document order, so a load can run while other threads query or write the graph; it is not
    /// atomic: triples read before an error stay in the graph.
    ///
    /// Arguments:
    ///     timestamp (TimeInput): The time at which the triples are asserted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as a file extension, name or media type (such as "ttl", "nt", "turtle", "text/turtle", "trig" or "rdf"). If not given, it is taken from the extension of a path (a missing or unknown extension raises an error), and is Turtle (which also reads N-Triples) for bytes. Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///     event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    ///     ValueError: If the format is unknown or the document does not parse.
    ///     TypeError: If source is neither bytes nor a path.
    #[pyo3(signature = (timestamp, source, format = None, base_iri = None, event_id = None))]
    fn load_rdf(
        &self,
        py: Python<'_>,
        timestamp: EventTimeComponent,
        source: RdfSource,
        format: Option<&str>,
        base_iri: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<usize, RdfError> {
        let time = input_time(timestamp, event_id)?;
        let format = format.map(RdfFormat::parse).transpose()?;
        py.detach(|| match source {
            RdfSource::Bytes(bytes) => self.graph.load_rdf(time, bytes.as_ref(), format, base_iri),
            RdfSource::Path(path) => self.graph.load_rdf_path(time, path, format, base_iri),
        })
    }

    /// Retracts every triple of an RDF document at timestamp.
    ///
    /// Each triple s p o is deleted from the layer of p of the edge from s to o, so from timestamp
    /// on it does not hold (until it is asserted again). A retraction is recorded even if the
    /// triple was never asserted. Blank-node labels are used as written, so they must be the stored
    /// labels (as returned by to_rdf() or sparql()). Each triple is written as one edge event, in
    /// document order. The retraction streams and is not atomic: triples before an error stay
    /// retracted.
    ///
    /// Arguments:
    ///     timestamp (TimeInput): The time at which the triples are retracted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, as in load_rdf(). Defaults to None.
    ///     base_iri (str, optional): The IRI against which relative IRIs are resolved. Defaults to None.
    ///     event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     int: The number of triples read.
    ///
    /// Raises:
    ///     GraphError: If the operation fails.
    ///     ValueError: If the format is unknown or the document does not parse.
    ///     TypeError: If source is neither bytes nor a path.
    #[pyo3(signature = (timestamp, source, format = None, base_iri = None, event_id = None))]
    fn retract_rdf(
        &self,
        py: Python<'_>,
        timestamp: EventTimeComponent,
        source: RdfSource,
        format: Option<&str>,
        base_iri: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<usize, RdfError> {
        let time = input_time(timestamp, event_id)?;
        let format = format.map(RdfFormat::parse).transpose()?;
        py.detach(|| match source {
            RdfSource::Bytes(bytes) => {
                self.graph
                    .retract_rdf(time, bytes.as_ref(), format, base_iri)
            }
            RdfSource::Path(path) => self.graph.retract_rdf_path(time, path, format, base_iri),
        })
    }

    /// Asserts one triple at timestamp.
    ///
    /// The terms are names, as sparql() returns them, so its values can be passed straight back.
    ///
    /// Arguments:
    ///     timestamp (TimeInput): The time at which the triple is asserted.
    ///     subject (str): The subject, such as http://ex/alice or _:b0.
    ///     predicate (str): The predicate, such as http://ex/knows.
    ///     object (str): The object, such as http://ex/bob or "Bob".
    ///     event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     None:
    ///
    /// Raises:
    ///     GraphError: If a term cannot be in its position (such as a literal subject), or the triple cannot be stored.
    #[pyo3(signature = (timestamp, subject, predicate, object, event_id = None))]
    fn add_triple(
        &self,
        timestamp: EventTimeComponent,
        subject: &str,
        predicate: &str,
        object: &str,
        event_id: Option<usize>,
    ) -> Result<(), RdfError> {
        self.graph.add_triple(
            input_time(timestamp, event_id)?,
            &triple_of(subject, predicate, object)?,
        )
    }

    /// Retracts one triple at timestamp.
    ///
    /// Terms are given as in add_triple().
    ///
    /// Arguments:
    ///     timestamp (TimeInput): The time at which the triple is retracted.
    ///     subject (str): The subject, such as http://ex/alice or _:b0.
    ///     predicate (str): The predicate, such as http://ex/knows.
    ///     object (str): The object, such as http://ex/bob or "Bob".
    ///     event_id (int, optional): The optional integer which will be used as an event id.
    ///
    /// Returns:
    ///     None:
    ///
    /// Raises:
    ///     GraphError: If a term cannot be in its position (such as a literal subject), or the triple cannot be stored.
    #[pyo3(signature = (timestamp, subject, predicate, object, event_id = None))]
    fn retract_triple(
        &self,
        timestamp: EventTimeComponent,
        subject: &str,
        predicate: &str,
        object: &str,
        event_id: Option<usize>,
    ) -> Result<(), RdfError> {
        self.graph.retract_triple(
            input_time(timestamp, event_id)?,
            &triple_of(subject, predicate, object)?,
        )
    }
}
