use crate::{
    db::api::view::{DynamicGraph, IntoDynamic, MaterializedGraph, StaticGraphViewOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        types::wrappers::document::PyDocument,
        utils::{block_on, execute_async_task, PyNodeRef},
    },
    vectors::{
        cache::VectorCache,
        custom::{serve_custom_embedding, EmbeddingFunction, EmbeddingServer},
        storage::OpenAIEmbeddings,
        template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
        vector_selection::DynamicVectorSelection,
        vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
        Document, DocumentEntity, Embedding,
    },
};

use itertools::Itertools;
use pyo3::{
    exceptions::{PyException, PyTypeError},
    prelude::*,
    types::{PyFunction, PyList},
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{path::PathBuf, sync::Arc};
use tokio::runtime::Runtime;

type DynamicVectorisedGraph = VectorisedGraph<DynamicGraph>;

#[pyclass(name = "OpenAIEmbeddings")]
#[derive(Clone)]
pub struct PyOpenAIEmbeddings {
    model: String,
    api_base: Option<String>,
    api_key_env: Option<String>,
    org_id: Option<String>,
    project_id: Option<String>,
}

#[pymethods]
impl PyOpenAIEmbeddings {
    #[new]
    #[pyo3(signature = (model="text-embedding-3-small", api_base=None, api_key_env=None, org_id=None, project_id=None))]
    fn new(
        model: &str,
        api_base: Option<String>,
        api_key_env: Option<String>,
        org_id: Option<String>,
        project_id: Option<String>,
    ) -> Self {
        Self {
            model: model.to_owned(),
            api_base,
            api_key_env,
            org_id,
            project_id,
        }
    }
}
impl From<PyOpenAIEmbeddings> for OpenAIEmbeddings {
    fn from(value: PyOpenAIEmbeddings) -> Self {
        Self {
            model: value.model.clone(),
            api_base: value.api_base.clone(),
            api_key_env: value.api_key_env.clone(),
            org_id: value.org_id.clone(),
            project_id: value.project_id.clone(),
        }
    }
}

impl EmbeddingFunction for Arc<Py<PyFunction>> {
    fn call(&self, text: &str) -> Vec<f32> {
        Python::with_gil(|py| {
            // TODO: remove unwraps?
            let any = self.call1(py, (text,)).unwrap();
            let list = any.downcast_bound::<PyList>(py).unwrap();
            list.iter().map(|value| value.extract().unwrap()).collect()
        })
    }
}

#[pyfunction]
pub fn embedding_server(address: String) -> EmbeddingServerDecorator {
    EmbeddingServerDecorator { address }
}

#[pyclass]
struct EmbeddingServerDecorator {
    address: String,
}

#[pymethods]
impl EmbeddingServerDecorator {
    fn __call__(&self, function: Py<PyFunction>) -> PyEmbeddingServer {
        PyEmbeddingServer {
            function: function.into(),
            address: self.address.clone(),
        }
    }
}

// struct RunningServer {
//     runtime: Runtime,
//     server: EmbeddingServer,
// }

#[pyclass(name = "EmbeddingServer")]
pub struct PyEmbeddingServer {
    function: Arc<Py<PyFunction>>,
    address: String,
    // running: Option<RunningServer>, // TODO: use all of these ideas for the GraphServer implementation
}
// TODO: ideally, I should allow users to provide this server object as embedding model, so the  fact it has an OpenAI  like API is transparent to the user

impl PyEmbeddingServer {
    fn create_running_server(&self) -> (Runtime, EmbeddingServer) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // let runtime = tokio::runtime::Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap();
        let execution =
            runtime.block_on(serve_custom_embedding(&self.address, self.function.clone()));
        (runtime, execution)
    }
}

#[pymethods]
impl PyEmbeddingServer {
    fn run(&self) {
        let (runtime, execution) = self.create_running_server();
        runtime.block_on(execution.wait());
    }

    fn start(&self) -> PyRunningEmbeddingServer {
        let (runtime, execution) = self.create_running_server();
        PyRunningEmbeddingServer {
            runtime,
            execution: Some(execution),
        }
    }
}

#[pyclass(name = "RunningEmbeddingServer")]
struct PyRunningEmbeddingServer {
    runtime: Runtime,
    execution: Option<EmbeddingServer>, // TODO: rename EmbeddingServer to ServerHandle?
}

#[pymethods]
impl PyRunningEmbeddingServer {
    fn stop(&mut self) -> PyResult<()> {
        if let Some(execution) = &mut self.execution {
            self.runtime.block_on(execution.stop());
            self.execution = None;
            Ok(())
        } else {
            Err(PyException::new_err("Embedding server was already stopped"))
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        // py: Python,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<()> {
        self.stop()
    }
}

pub type PyWindow = Option<(EventTime, EventTime)>;

pub fn translate_window(window: PyWindow) -> Option<(i64, i64)> {
    window.map(|(start, end)| (start.into_time().t(), end.into_time().t()))
}

#[derive(Clone)]
pub enum PyQuery {
    Raw(String),
    Computed(Embedding),
}

impl PyQuery {
    fn into_embedding<G: StaticGraphViewOps>(
        self,
        graph: &VectorisedGraph<G>,
    ) -> PyResult<Embedding> {
        match self {
            Self::Raw(query) => {
                let graph = graph.clone();
                let result = Ok(execute_async_task(move || async move {
                    graph.embed_text(query).await
                })?);
                result
            }
            Self::Computed(embedding) => Ok(embedding),
        }
    }
}

impl<'source> FromPyObject<'source> for PyQuery {
    fn extract_bound(query: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(text) = query.extract::<String>() {
            return Ok(PyQuery::Raw(text));
        }
        if let Ok(embedding) = query.extract::<Vec<f32>>() {
            return Ok(PyQuery::Computed(embedding.into()));
        }
        let message = format!("query '{query}' must be a str, or a list of float");
        Err(PyTypeError::new_err(message))
    }
}

impl<'py> IntoPyObject<'py> for Document<DynamicGraph> {
    type Target = PyDocument;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyDocument(self).into_pyobject(py)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> Document<G> {
    pub fn into_dynamic(self) -> Document<DynamicGraph> {
        let Document {
            entity,
            content,
            embedding,
        } = self;
        let entity = match entity {
            // TODO: define a common method node/edge.into_dynamic for NodeView, as this code is duplicated in model/graph/node.rs and model/graph/edge.rs
            DocumentEntity::Node(node) => DocumentEntity::Node(node.into_dynamic()),
            DocumentEntity::Edge(edge) => DocumentEntity::Edge(edge.into_dynamic()),
        };
        Document {
            entity,
            content,
            embedding,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> From<Document<G>> for PyDocument {
    fn from(value: Document<G>) -> Self {
        Self(value.into_dynamic())
    }
}

#[derive(FromPyObject)]
pub enum TemplateConfig {
    Bool(bool),
    String(String),
    // re-enable the code below to be able to customise the erro message
    // #[pyo3(transparent)]
    // CatchAll(Bound<'py, PyAny>), // This extraction never fails
}

impl TemplateConfig {
    pub fn get_template_or(self, default: &str) -> Option<String> {
        match self {
            Self::Bool(vectorise) => {
                if vectorise {
                    Some(default.to_owned())
                } else {
                    None
                }
            }
            Self::String(custom_template) => Some(custom_template),
        }
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Bool(false))
    }
}

#[pymethods]
impl PyGraphView {
    /// Create a VectorisedGraph from the current graph.
    ///
    /// Args:
    ///   embedding (Callable[[list], list]): Specify the embedding function used to vectorise documents into embeddings.
    ///   nodes (bool | str): Enable for nodes to be embedded, disable for nodes to not be embedded or specify a custom document property to use if a string is provided. Defaults to True.
    ///   edges (bool | str): Enable for edges to be embedded, disable for edges to not be embedded or specify a custom document property to use if a string is provided. Defaults to True.
    ///   cache (str, optional): Path used to store the cache of embeddings.
    ///   verbose (bool): Enable to print logs reporting progress. Defaults to False.
    ///
    /// Returns:
    ///   VectorisedGraph: A VectorisedGraph with all the documents and their embeddings, with an initial empty selection.
    #[pyo3(signature = (embedding, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true), cache = None, verbose = false))]
    fn vectorise(
        &self,
        embedding: PyOpenAIEmbeddings,
        nodes: TemplateConfig,
        edges: TemplateConfig,
        cache: Option<String>,
        verbose: bool,
    ) -> PyResult<DynamicVectorisedGraph> {
        let template = DocumentTemplate {
            node_template: nodes.get_template_or(DEFAULT_NODE_TEMPLATE),
            edge_template: edges.get_template_or(DEFAULT_EDGE_TEMPLATE),
        };
        let graph = self.graph.clone();
        execute_async_task(move || async move {
            let cache = if let Some(cache) = cache {
                VectorCache::on_disk(&PathBuf::from(cache)).await?
            } else {
                VectorCache::in_memory()
            };
            let model = cache.openai(embedding.into()).await?;
            Ok(graph.vectorise(model, template, None, verbose).await?)
        })
    }
}

#[pyclass(name = "VectorisedGraph", module = "raphtory.vectors", frozen)]
/// VectorisedGraph object that contains embedded documents that correspond to graph entities.
pub struct PyVectorisedGraph(DynamicVectorisedGraph);

impl From<DynamicVectorisedGraph> for PyVectorisedGraph {
    fn from(value: DynamicVectorisedGraph) -> Self {
        PyVectorisedGraph(value)
    }
}

impl From<VectorisedGraph<MaterializedGraph>> for PyVectorisedGraph {
    fn from(value: VectorisedGraph<MaterializedGraph>) -> Self {
        PyVectorisedGraph(value.into_dynamic())
    }
}

impl<'py> IntoPyObject<'py> for DynamicVectorisedGraph {
    type Target = PyVectorisedGraph;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyVectorisedGraph(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for DynamicVectorSelection {
    type Target = PyVectorSelection;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyVectorSelection(self).into_pyobject(py)
    }
}

/// A VectorisedGraph, containing a set of documents positioned in an embedding space. This object allows you to get a selection
/// of those documents using a query and similarity scores.
#[pymethods]
impl PyVectorisedGraph {
    /// Return an empty selection of entities.
    fn empty_selection(&self) -> DynamicVectorSelection {
        self.0.empty_selection()
    }

    /// Perform a similarity search between each entity's associated document and a specified `query`. Returns a number of entities up to a specified `limit` ranked in ascending order of distance.
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The maximum number of new entities in the result.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search.
    #[pyo3(signature = (query, limit, window=None))]
    pub fn entities_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = query.into_embedding(&self.0)?;
        let w = translate_window(window);
        let s = block_on(self.0.entities_by_similarity(&embedding, limit, w))?;
        Ok(s)
    }

    /// Perform a similarity search between each node's associated document and a specified `query`. Returns a number of nodes up to a specified `limit` ranked in ascending order of distance.
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The maximum number of new nodes in the result.
    ///   window (Tuple[int | str, int | str], optional): The window where documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search.
    #[pyo3(signature = (query, limit, window=None))]
    pub fn nodes_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = query.into_embedding(&self.0)?;
        let w = translate_window(window);
        Ok(block_on(self.0.nodes_by_similarity(&embedding, limit, w))?)
    }

    /// Perform a similarity search between each edge's associated document and a specified `query`. Returns a number of edges up to a specified `limit` ranked in ascending order of distance.
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The maximum number of new edges in the results.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search.
    #[pyo3(signature = (query, limit, window=None))]
    pub fn edges_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = query.into_embedding(&self.0)?;
        let w = translate_window(window);
        Ok(block_on(self.0.edges_by_similarity(&embedding, limit, w))?)
    }
}

#[pyclass(name = "VectorSelection", module = "raphtory.vectors")]
pub struct PyVectorSelection(DynamicVectorSelection);

/// A VectorisedGraph, containing a set of documents positioned in the graph space and a selection
/// over those documents
#[pymethods]
impl PyVectorSelection {
    /// Returns the nodes present in the current selection.
    ///
    /// Returns:
    ///     list[Node]: List of nodes in the current selection.
    fn nodes(&self) -> Vec<PyNode> {
        self.0
            .nodes()
            .into_iter()
            .map(|node| node.into())
            .collect_vec()
    }

    /// Returns the edges present in the current selection.
    ///
    /// Returns:
    ///     list[Edge]: List of edges in the current selection.
    fn edges(&self) -> Vec<PyEdge> {
        self.0
            .edges()
            .into_iter()
            .map(|edge| edge.into())
            .collect_vec()
    }

    /// Returns the documents present in the current selection.
    ///
    /// Returns:
    ///     list[Document]: List of documents in the current selection.
    fn get_documents(&self) -> PyResult<Vec<Document<DynamicGraph>>> {
        Ok(block_on(self.0.get_documents())?)
    }

    /// Returns the documents present in the current selection alongside their distances.
    ///
    /// Returns:
    ///     list[Tuple[Document, float]]: List of documents and distances.
    fn get_documents_with_distances(&self) -> PyResult<Vec<(Document<DynamicGraph>, f32)>> {
        Ok(block_on(self.0.get_documents_with_distances())?)
    }

    /// Add all the documents associated with the specified `nodes` to the current selection.
    ///
    /// Documents added by this call are assumed to have a distance of 0.
    ///
    /// Args:
    ///   nodes (list): List of the node ids or nodes to add.
    ///
    /// Returns:
    ///     None:
    fn add_nodes(mut self_: PyRefMut<'_, Self>, nodes: Vec<PyNodeRef>) {
        self_.0.add_nodes(nodes)
    }

    /// Add all the documents associated with the specified `edges` to the current selection.
    ///
    /// Documents added by this call are assumed to have a distance of 0.
    ///
    /// Args:
    ///   edges (list):  List of the edge ids or edges to add.
    ///
    /// Returns:
    ///     None:
    fn add_edges(mut self_: PyRefMut<'_, Self>, edges: Vec<(PyNodeRef, PyNodeRef)>) {
        self_.0.add_edges(edges)
    }

    /// Add all the documents in a specified `selection` to the current selection.
    ///
    /// Args:
    ///   selection (VectorSelection): Selection to be added.
    ///
    /// Returns:
    ///   VectorSelection: The combined selection.
    pub fn append(mut self_: PyRefMut<'_, Self>, selection: &Self) -> DynamicVectorSelection {
        self_.0.append(&selection.0).clone()
    }

    /// Add all the documents a specified number of `hops` away from the selection.
    ///
    /// Two documents A and B are considered to be 1 hop away from each other if they are on the same
    /// entity or if they are on the same node/edge pair. Provided that two nodes A and C are n
    /// hops away of each other if there is a document B such that A is n - 1 hops away of B and B
    /// is 1 hop away of C.
    ///
    /// Args:
    ///   hops (int): The number of hops to carry out the expansion.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (hops, window=None))]
    fn expand(mut self_: PyRefMut<'_, Self>, hops: usize, window: PyWindow) {
        self_.0.expand(hops, translate_window(window))
    }

    /// Add to the selection the `limit` adjacent entities closest to `query`
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///
    /// 1. All the entities 1 hop away of some of the entities included on the selection (and
    ///    not already selected) are marked as candidates.
    /// 2. Those candidates are added to the selection in ascending distance from `query`.
    ///
    /// This loops goes on until the number of new entities reaches a total of `limit`
    /// entities or until no more documents are available
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The number of documents to add.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_entities_by_similarity(
        mut slf: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = query.into_embedding(&slf.0.graph)?;
        let w = translate_window(window);
        block_on(slf.0.expand_entities_by_similarity(&embedding, limit, w))?;

        Ok(())
    }

    /// Add to the selection the `limit` adjacent nodes closest to `query`
    ///
    /// This function has the same behaviour as expand_entities_by_similarity but it only considers nodes.
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The maximum number of new nodes to add.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_nodes_by_similarity(
        mut slf: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = query.into_embedding(&slf.0.graph)?;
        let w = translate_window(window);
        block_on(slf.0.expand_nodes_by_similarity(&embedding, limit, w))?;
        Ok(())
    }

    /// Add to the selection the `limit` adjacent edges closest to `query`
    ///
    /// This function has the same behaviour as expand_entities_by_similarity but it only considers edges.
    ///
    /// Args:
    ///   query (str | list): The text or the embedding to calculate the distance from.
    ///   limit (int): The maximum number of new edges to add.
    ///   window (Tuple[int | str, int | str], optional): The window that documents need to belong to in order to be considered.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_edges_by_similarity(
        mut slf: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = query.into_embedding(&slf.0.graph)?;
        let w = translate_window(window);
        block_on(slf.0.expand_edges_by_similarity(&embedding, limit, w))?;
        Ok(())
    }
}
