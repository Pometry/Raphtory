//! Python bindings for `RdfGraph` and `RdfView`. Terms cross to and from Python as the Raphtory
//! names they stand for ([`term_name`], [`term_of`], and layer names for predicates).
use crate::{
    mapping::iri_of, term_name, term_of, RdfError, RdfFormat, RdfGraph, RdfView, SparqlFormat,
    SparqlOptions, SparqlResults, Triple,
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
enum RdfSource {
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
        iri_of(predicate),
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

/// A view of an RdfGraph: one RDF triple per edge and layer visible in a persistent graph view.
///
/// Every view method returns an RdfView; sparql and to_rdf see its triples.
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
    /// The same view as a property graph.
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
    /// Values are returned as Raphtory names. In a query, GRAPH <raphtory:asof:T> { .. } reads
    /// this view as of T, and raphtory:validFrom/validTo(s, p, o) give a triple's latest run.
    ///
    /// Arguments:
    ///     query (str): The SPARQL query.
    ///     format (str, optional): If given, the results are returned serialised in this format, such as "json", "csv", "nt" or "ttl". Defaults to None.
    ///
    /// Returns:
    ///     Union[list[dict[str, Optional[str]]], bool, list[tuple[str, str, str]], str]: A dict per solution for SELECT, a bool for ASK, (subject, predicate, object) tuples for CONSTRUCT and DESCRIBE, or the document if format is given.
    ///
    /// Raises:
    ///     GraphError: If the evaluation of the query fails.
    ///     ValueError: If the query does not parse, or format is unknown or does not fit the query.
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

    /// Exports the RDF triples of this view, the ones sparql() sees.
    ///
    /// Triples whose subject is a literal are skipped.
    ///
    /// Arguments:
    ///     path (str | PathLike, optional): The file to write; if not given, the document is returned. Defaults to None.
    ///     format (str, optional): The RDF format, such as "nt", "ttl" or "rdf"; by default the one the extension of path names, else N-Triples. Defaults to None.
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

/// A graph viewed as RDF with persistent semantics: the place to load, retract and query triples.
///
/// Each triple s p o is an edge from s to o in the layer of p. Wrapping a Graph shares its storage.
///
/// Arguments:
///     graph (Graph | PersistentGraph, optional): The graph to view as RDF; a new empty graph if not given. Defaults to None.
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
    /// Blank nodes get fresh labels and named graphs are rejected. Not atomic.
    ///
    /// Arguments:
    ///     timestamp (TimeInput): The time at which the triples are asserted.
    ///     source (bytes | str | PathLike): The document as bytes, or the path of a file to read.
    ///     format (str, optional): The RDF format, such as "ttl", "nt" or "rdf"; by default the one a path's extension names, or Turtle for bytes. Defaults to None.
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
    /// Blank-node labels are used as written, so they must be the stored ones. Not atomic.
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
    /// Terms are names, as sparql() returns them.
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
