use crate::{
    core::utils::{errors::GraphError, time::IntoTime},
    db::{
        api::view::{DynamicGraph, IntoDynamic, MaterializedGraph, StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        types::wrappers::document::PyDocument,
        utils::{execute_async_task, PyNodeRef, PyTime},
    },
    vectors::{
        template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
        vector_selection::DynamicVectorSelection,
        vectorisable::Vectorisable,
        vectorised_graph::{DynamicVectorisedGraph, VectorisedGraph},
        Document, DocumentEntity, Embedding, EmbeddingFunction, EmbeddingResult,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyFunction, PyList},
};

pub type PyWindow = Option<(PyTime, PyTime)>;

pub fn translate_window(window: PyWindow) -> Option<(i64, i64)> {
    window.map(|(start, end)| (start.into_time(), end.into_time()))
}

#[derive(Clone)]
pub enum PyQuery {
    Raw(String),
    Computed(Embedding),
}

impl PyQuery {
    async fn into_embedding<E: EmbeddingFunction + ?Sized>(
        self,
        embedding: &E,
    ) -> PyResult<Embedding> {
        match self {
            Self::Raw(query) => {
                let result = embedding.call(vec![query]).await;
                Ok(result.map_err(GraphError::from)?.remove(0))
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
            DocumentEntity::Node(node) => DocumentEntity::Node(NodeView {
                base_graph: node.base_graph.into_dynamic(),
                graph: node.graph.into_dynamic(),
                node: node.node,
            }),
            DocumentEntity::Edge(edge) => DocumentEntity::Edge(EdgeView {
                // TODO: same as for nodes
                base_graph: edge.base_graph.into_dynamic(),
                graph: edge.graph.into_dynamic(),
                edge: edge.edge,
            }),
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
    /// Create a VectorisedGraph from the current graph
    ///
    /// Args:
    ///   embedding (Callable[[list], list]): the embedding function to translate documents to embeddings
    ///   cache (str, optional): the file to be used as a cache to avoid calling the embedding function
    ///   overwrite_cache (bool): whether or not to overwrite the cache if there are new embeddings. Defaults to False.
    ///   nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///   edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///   verbose (bool): whether or not to print logs reporting the progress. Defaults to False.
    ///
    /// Returns:
    ///   VectorisedGraph: A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    #[pyo3(signature = (embedding, cache = None, overwrite_cache = false, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true), verbose = false))]
    fn vectorise(
        &self,
        embedding: Bound<PyFunction>,
        cache: Option<String>,
        overwrite_cache: bool,
        nodes: TemplateConfig,
        edges: TemplateConfig,
        verbose: bool,
    ) -> PyResult<DynamicVectorisedGraph> {
        let template = DocumentTemplate {
            node_template: nodes.get_template_or(DEFAULT_NODE_TEMPLATE),
            edge_template: edges.get_template_or(DEFAULT_EDGE_TEMPLATE),
        };
        let embedding = embedding.unbind();
        let cache = cache.map(|cache| cache.into()).into();
        let graph = self.graph.clone();
        execute_async_task(move || async move {
            Ok(graph
                .vectorise(
                    Box::new(embedding),
                    cache,
                    overwrite_cache,
                    template,
                    None,
                    verbose,
                )
                .await?)
        })
    }
}

#[pyclass(name = "VectorisedGraph", module = "raphtory.vectors", frozen)]
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

/// A vectorised graph, containing a set of documents positioned in the graph space and a selection
/// over those documents
#[pymethods]
impl PyVectorisedGraph {
    /// Save the embeddings present in this graph to `file` so they can be further used in a call to `vectorise`
    fn save_embeddings(&self, file: String) {
        self.0.save_embeddings(file.into());
    }

    /// Return an empty selection of documents
    fn empty_selection(&self) -> DynamicVectorSelection {
        self.0.empty_selection()
    }

    /// Search the top scoring entities according to `query` with no more than `limit` entities
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the maximum number of new entities to search
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search
    #[pyo3(signature = (query, limit, window=None))]
    pub fn entities_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = compute_embedding(&self.0, query)?;
        Ok(self
            .0
            .entities_by_similarity(&embedding, limit, translate_window(window)))
    }

    /// Search the top scoring nodes according to `query` with no more than `limit` nodes
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the maximum number of new nodes to search
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search
    #[pyo3(signature = (query, limit, window=None))]
    pub fn nodes_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = compute_embedding(&self.0, query)?;
        Ok(self
            .0
            .nodes_by_similarity(&embedding, limit, translate_window(window)))
    }

    /// Search the top scoring edges according to `query` with no more than `limit` edges
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the maximum number of new edges to search
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   VectorSelection: The vector selection resulting from the search
    #[pyo3(signature = (query, limit, window=None))]
    pub fn edges_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = compute_embedding(&self.0, query)?;
        Ok(self
            .0
            .edges_by_similarity(&embedding, limit, translate_window(window)))
    }
}

#[pyclass(name = "VectorSelection", module = "raphtory.vectors")]
pub struct PyVectorSelection(DynamicVectorSelection);

/// A vectorised graph, containing a set of documents positioned in the graph space and a selection
/// over those documents
#[pymethods]
impl PyVectorSelection {
    /// Return the nodes present in the current selection
    ///
    /// Returns:
    ///     list[Node]: list of nodes in the current selection
    fn nodes(&self) -> Vec<PyNode> {
        self.0
            .nodes()
            .into_iter()
            .map(|node| node.into())
            .collect_vec()
    }

    /// Return the edges present in the current selection
    ///
    /// Returns:
    ///     list[Edge]: list of edges in the current selection
    fn edges(&self) -> Vec<PyEdge> {
        self.0
            .edges()
            .into_iter()
            .map(|edge| edge.into())
            .collect_vec()
    }

    /// Return the documents present in the current selection
    ///
    /// Returns:
    ///     list[Document]: list of documents in the current selection
    fn get_documents(&self) -> Vec<Document<DynamicGraph>> {
        // TODO: review if I can simplify this
        self.0.get_documents()
    }

    /// Return the documents alongside their scores present in the current selection
    ///
    /// Returns:
    ///     list[Tuple[Document, float]]: list of documents and scores
    fn get_documents_with_scores(&self) -> Vec<(Document<DynamicGraph>, f32)> {
        self.0.get_documents_with_scores()
    }

    /// Add all the documents associated with the `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   nodes (list): a list of the node ids or nodes to add
    ///
    /// Returns:
    ///     None:
    fn add_nodes(mut self_: PyRefMut<'_, Self>, nodes: Vec<PyNodeRef>) {
        self_.0.add_nodes(nodes)
    }

    /// Add all the documents associated with the `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   edges (list):  a list of the edge ids or edges to add
    ///
    /// Returns:
    ///     None:
    fn add_edges(mut self_: PyRefMut<'_, Self>, edges: Vec<(PyNodeRef, PyNodeRef)>) {
        self_.0.add_edges(edges)
    }

    /// Add all the documents in `selection` to the current selection
    ///
    /// Args:
    ///   selection (VectorSelection): a selection to be added
    ///
    /// Returns:
    ///   VectorSelection: The selection with the new documents
    pub fn append(mut self_: PyRefMut<'_, Self>, selection: &Self) -> DynamicVectorSelection {
        self_.0.append(&selection.0).clone()
    }

    /// Add all the documents `hops` hops away to the selection
    ///
    /// Two documents A and B are considered to be 1 hop away of each other if they are on the same
    /// entity or if they are on the same node/edge pair. Provided that, two nodes A and C are n
    /// hops away of  each other if there is a document B such that A is n - 1 hops away of B and B
    /// is 1 hop away of C.
    ///
    /// Args:
    ///   hops (int): the number of hops to carry out the expansion
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (hops, window=None))]
    fn expand(mut self_: PyRefMut<'_, Self>, hops: usize, window: PyWindow) {
        self_.0.expand(hops, translate_window(window))
    }

    /// Add the top `limit` adjacent entities with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the entities 1 hop away of some of the entities included on the selection (and
    ///      not already selected) are marked as candidates.
    ///   2. Those candidates are added to the selection in descending order according to the
    ///      similarity score obtained against the `query`.
    ///
    /// This loops goes on until the number of new entities reaches a total of `limit`
    /// entities or until no more documents are available
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the number of documents to add
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_entities_by_similarity(
        mut self_: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = compute_embedding(&self_.0.graph, query)?;
        self_
            .0
            .expand_entities_by_similarity(&embedding, limit, translate_window(window));
        Ok(())
    }

    /// Add the top `limit` adjacent nodes with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers nodes.
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the maximum number of new nodes to add
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_nodes_by_similarity(
        mut self_: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = compute_embedding(&self_.0.graph, query)?;
        self_
            .0
            .expand_nodes_by_similarity(&embedding, limit, translate_window(window));
        Ok(())
    }

    /// Add the top `limit` adjacent edges with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers edges.
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to score against
    ///   limit (int): the maximum number of new edges to add
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_edges_by_similarity(
        mut self_: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = compute_embedding(&self_.0.graph, query)?;
        self_
            .0
            .expand_edges_by_similarity(&embedding, limit, translate_window(window));
        Ok(())
    }
}

pub fn compute_embedding<G: StaticGraphViewOps>(
    vectors: &VectorisedGraph<G>,
    query: PyQuery,
) -> PyResult<Embedding> {
    let embedding = vectors.embedding.clone();
    execute_async_task(move || async move { query.into_embedding(embedding.as_ref()).await })
}

impl EmbeddingFunction for Py<PyFunction> {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, EmbeddingResult<Vec<Embedding>>> {
        let embedding_function = Python::with_gil(|py| self.clone_ref(py));
        Box::pin(async move {
            Python::with_gil(|py| {
                let embedding_function = embedding_function.bind(py);
                let python_texts = PyList::new(py, texts)?;
                let result = embedding_function.call1((python_texts,))?;
                let embeddings = result.downcast::<PyList>().map_err(|_| {
                    PyTypeError::new_err(
                        "value returned by the embedding function was not a python list",
                    )
                })?;

                let embeddings: EmbeddingResult<Vec<_>> = embeddings
                    .iter()
                    .map(|embedding| {
                        let pylist = embedding.downcast::<PyList>().map_err(|_| {
                            PyTypeError::new_err("one of the values in the list returned by the embedding function was not a python list")
                        })?;
                        let embedding: EmbeddingResult<Embedding> = pylist
                            .iter()
                            .map(|element| Ok(element.extract::<f32>()?))
                            .collect();
                        Ok(embedding?)
                    })
                    .collect();
                Ok(embeddings?)
            })
        })
    }
}
