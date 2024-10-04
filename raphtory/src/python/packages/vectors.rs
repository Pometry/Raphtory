use crate::{
    core::{
        entities::nodes::node_ref::NodeRef,
        utils::{errors::GraphError, time::IntoTime},
        DocumentInput, Lifespan, Prop,
    },
    db::api::{
        properties::{internal::PropertiesOps, Properties},
        view::{MaterializedGraph, StaticGraphViewOps},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        types::wrappers::document::{PyDocument, PyEmbedding},
        utils::{execute_async_task, PyTime},
    },
    vectors::{
        template::DocumentTemplate,
        vector_selection::DynamicVectorSelection,
        vectorisable::Vectorisable,
        vectorised_graph::{DynamicVectorisedGraph, VectorisedGraph},
        Document, Embedding, EmbeddingFunction, EmbeddingResult,
    },
};
use chrono::DateTime;
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
    fn extract(query: &'source PyAny) -> PyResult<Self> {
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

fn format_time(millis: i64) -> String {
    if millis == 0 {
        "unknown time".to_owned()
    } else {
        match DateTime::from_timestamp_millis(millis) {
            Some(time) => time.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string(),
            None => "unknown time".to_owned(),
        }
    }
}

impl PyDocument {
    pub fn extract_rust_document(&self, py: Python) -> Result<Document, String> {
        if let (Some(entity), Some(embedding)) = (&self.entity, &self.embedding) {
            let node = entity.extract::<PyNode>(py);
            let edge = entity.extract::<PyEdge>(py);
            let graph = entity.extract::<PyGraphView>(py);
            if let Ok(node) = node {
                Ok(Document::Node {
                    name: node.name(),
                    content: self.content.clone(),
                    embedding: embedding.embedding(),
                    life: self.life,
                })
            } else if let Ok(edge) = edge {
                Ok(Document::Edge {
                    src: edge.edge.src().name(),
                    dst: edge.edge.dst().name(),
                    content: self.content.clone(),
                    embedding: embedding.embedding(),
                    life: self.life,
                })
            } else if let Ok(graph) = graph {
                Ok(Document::Graph {
                    name: graph
                        .graph
                        .properties()
                        .get("name")
                        .map(|prop| prop.to_string()),
                    content: self.content.clone(),
                    embedding: embedding.embedding(),
                    life: self.life,
                })
            } else {
                Err("document entity is not a node nor an edge nor a graph".to_owned())
            }
        } else {
            Err("Document entity and embedding have to be defined".to_owned())
        }
    }
}

pub fn into_py_document(
    document: Document,
    graph: &DynamicVectorisedGraph,
    py: Python,
) -> PyDocument {
    match document {
        Document::Graph {
            content,
            life,
            embedding,
            ..
        } => PyDocument {
            content,
            entity: Some(graph.source_graph.clone().into_py(py)),
            embedding: Some(PyEmbedding(embedding)),
            life,
        },
        Document::Node {
            name,
            content,
            embedding,
            life,
        } => {
            let node = graph.source_graph.node(name).unwrap();

            PyDocument {
                content,
                entity: Some(node.into_py(py)),
                embedding: Some(PyEmbedding(embedding)),
                life,
            }
        }
        Document::Edge {
            src,
            dst,
            content,
            embedding,
            life,
        } => {
            let edge = graph.source_graph.edge(src, dst).unwrap();

            PyDocument {
                content,
                entity: Some(edge.into_py(py)),
                embedding: Some(PyEmbedding(embedding)),
                life,
            }
        }
    }
}

/// This funtions ignores the time history of temporal props if their type is Document and they have a life different than Lifespan::Inherited
fn get_documents_from_props<P: PropertiesOps + Clone>(
    properties: Properties<P>,
    name: &str,
) -> Box<dyn Iterator<Item = DocumentInput>> {
    let prop = properties.temporal().get(name);

    match prop {
        Some(prop) => {
            let props = prop.into_iter();
            let docs = props
                .map(|(time, prop)| prop_to_docs(&prop, Lifespan::event(time)).collect_vec())
                .flatten();
            Box::new(docs)
        }
        None => match properties.get(name) {
            Some(prop) => Box::new(
                prop_to_docs(&prop, Lifespan::Inherited)
                    .collect_vec()
                    .into_iter(),
            ),
            _ => Box::new(std::iter::empty()),
        },
    }
}

impl Lifespan {
    fn overwrite_inherited(&self, default_lifespan: Lifespan) -> Self {
        match self {
            Lifespan::Inherited => default_lifespan,
            other => other.clone(),
        }
    }
}

fn prop_to_docs(
    prop: &Prop,
    default_lifespan: Lifespan,
) -> Box<dyn Iterator<Item = DocumentInput> + '_> {
    match prop {
        Prop::List(docs) => Box::new(
            docs.iter()
                .map(move |prop| prop_to_docs(prop, default_lifespan))
                .flatten(),
        ),
        Prop::Map(doc_map) => Box::new(
            doc_map
                .values()
                .map(move |prop| prop_to_docs(prop, default_lifespan))
                .flatten(),
        ),
        Prop::Document(document) => Box::new(std::iter::once(DocumentInput {
            life: document.life.overwrite_inherited(default_lifespan),
            ..document.clone()
        })),
        prop => Box::new(std::iter::once(DocumentInput {
            content: prop.to_string(),
            life: default_lifespan,
        })),
    }
}

#[pymethods]
impl PyGraphView {
    /// Create a VectorisedGraph from the current graph
    ///
    /// Args:
    ///   embedding (Callable[[list], list]): the embedding function to translate documents to embeddings
    ///   cache (str): the file to be used as a cache to avoid calling the embedding function (optional)
    ///   overwrite_cache (bool): whether or not to overwrite the cache if there are new embeddings (optional)
    ///   graph_template (str): the document template for the graphs (optional)
    ///   node_template (str): the document template for the nodes (optional)
    ///   edge_template (str): the document template for the edges (optional)
    ///   verbose (bool): whether or not to print logs reporting the progress
    ///
    /// Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    #[pyo3(signature = (embedding, cache = None, overwrite_cache = false, graph_template = None, node_template = None, edge_template = None, graph_name = None, verbose = false))]
    fn vectorise(
        &self,
        embedding: &PyFunction,
        cache: Option<String>,
        overwrite_cache: bool,
        graph_template: Option<String>,
        node_template: Option<String>,
        edge_template: Option<String>,
        graph_name: Option<String>,
        verbose: bool,
    ) -> PyResult<DynamicVectorisedGraph> {
        let embedding: Py<PyFunction> = embedding.into();
        let graph = self.graph.clone();
        let cache = cache.map(|cache| cache.into()).into();
        let template = DocumentTemplate {
            graph_template,
            node_template,
            edge_template,
        };
        execute_async_task(move || async move {
            Ok(graph
                .vectorise(
                    Box::new(embedding.clone()),
                    cache,
                    overwrite_cache,
                    template,
                    graph_name,
                    verbose,
                )
                .await?)
        })
    }
}

#[pyclass(name = "VectorisedGraph", frozen)]
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

impl IntoPy<PyObject> for DynamicVectorisedGraph {
    fn into_py(self, py: Python) -> PyObject {
        Py::new(py, PyVectorisedGraph(self)).unwrap().into_py(py)
    }
}

impl IntoPy<PyObject> for DynamicVectorSelection {
    fn into_py(self, py: Python) -> PyObject {
        Py::new(py, PyVectorSelection(self)).unwrap().into_py(py)
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

    /// Search the top scoring documents according to `query` with no more than `limit` documents
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of documents to search
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   The vector selection resulting from the search
    #[pyo3(signature = (query, limit, window=None))]
    pub fn documents_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<DynamicVectorSelection> {
        let embedding = compute_embedding(&self.0, query)?;
        Ok(self
            .0
            .documents_by_similarity(&embedding, limit, translate_window(window)))
    }

    /// Search the top scoring entities according to `query` with no more than `limit` entities
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new entities to search
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   The vector selection resulting from the search
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
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new nodes to search
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   The vector selection resulting from the search
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
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new edges to search
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   The vector selection resulting from the search
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

#[pyclass(name = "VectorSelection")]
pub struct PyVectorSelection(DynamicVectorSelection);

/// A vectorised graph, containing a set of documents positioned in the graph space and a selection
/// over those documents
#[pymethods]
impl PyVectorSelection {
    /// Return the nodes present in the current selection
    fn nodes(&self) -> Vec<PyNode> {
        self.0
            .nodes()
            .into_iter()
            .map(|node| node.into())
            .collect_vec()
    }

    /// Return the edges present in the current selection
    fn edges(&self) -> Vec<PyEdge> {
        self.0
            .edges()
            .into_iter()
            .map(|edge| edge.into())
            .collect_vec()
    }

    /// Return the documents present in the current selection
    fn get_documents(&self, py: Python) -> Vec<PyDocument> {
        // TODO: review if I can simplify this
        self.get_documents_with_scores(py)
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

    /// Return the documents alongside their scores present in the current selection
    fn get_documents_with_scores(&self, py: Python) -> Vec<(PyDocument, f32)> {
        let docs = self.0.get_documents_with_scores();
        docs.into_iter()
            .map(|(doc, score)| (into_py_document(doc, &self.0.graph, py), score))
            .collect()
    }

    /// Add all the documents associated with the `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   nodes (list): a list of the node ids or nodes to add
    fn add_nodes(mut self_: PyRefMut<'_, Self>, nodes: Vec<NodeRef>) {
        self_.0.add_nodes(nodes)
    }

    /// Add all the documents associated with the `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   edges (list):  a list of the edge ids or edges to add
    fn add_edges(mut self_: PyRefMut<'_, Self>, edges: Vec<(NodeRef, NodeRef)>) {
        self_.0.add_edges(edges)
    }

    /// Add all the documents in `selection` to the current selection
    ///
    /// Args:
    ///   selection: a selection to be added
    ///
    /// Returns:
    ///   The selection with the new documents
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
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    #[pyo3(signature = (hops, window=None))]
    fn expand(mut self_: PyRefMut<'_, Self>, hops: usize, window: PyWindow) {
        self_.0.expand(hops, translate_window(window))
    }

    /// Add the top `limit` adjacent documents with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the documents 1 hop away of some of the documents included on the selection (and
    /// not already selected) are marked as candidates.
    ///   2. Those candidates are added to the selection in descending order according to the
    /// similarity score obtained against the `query`.
    ///
    /// This loops goes on until the current selection reaches a total of `limit`  documents or
    /// until no more documents are available
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_documents_by_similarity(
        mut self_: PyRefMut<'_, Self>,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<()> {
        let embedding = compute_embedding(&self_.0.graph, query)?;
        self_
            .0
            .expand_documents_by_similarity(&embedding, limit, translate_window(window));
        Ok(())
    }

    /// Add the top `limit` adjacent entities with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the entities 1 hop away of some of the entities included on the selection (and
    /// not already selected) are marked as candidates.
    ///   2. Those candidates are added to the selection in descending order according to the
    /// similarity score obtained against the `query`.
    ///
    /// This loops goes on until the number of new entities reaches a total of `limit`
    /// entities or until no more documents are available
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
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
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new nodes to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
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
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new edges to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
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
        let embedding_function = self.clone();
        Box::pin(async move {
            Python::with_gil(|py| {
                let python_texts = PyList::new(py, texts);
                let result = embedding_function.call1(py, (python_texts,))?;
                let embeddings: &PyList = result.downcast(py).map_err(|_| {
                    PyTypeError::new_err(
                        "value returned by the embedding function was not a python list",
                    )
                })?;

                let embeddings: EmbeddingResult<Vec<_>> = embeddings
                    .iter()
                    .map(|embedding| {
                        let pylist: &PyList = embedding.downcast().map_err(|_| {
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
