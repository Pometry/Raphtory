use crate::{
    core::{entities::nodes::node_ref::NodeRef, utils::time::IntoTime},
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::{internal::DynamicGraph, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
    python::{
        graph::{edge::PyEdge, node::PyNode, views::graph_view::PyGraphView},
        utils::{execute_async_task, PyTime},
    },
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorisable::Vectorisable,
        vectorised_graph::DynamicVectorisedGraph,
        Document, DocumentInput, Embedding, EmbeddingFunction, Lifespan,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    exceptions::{PyException, PyTypeError},
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone)]
pub enum PyQuery {
    Raw(String),
    Computed(Embedding),
}

impl PyQuery {
    async fn into_embedding<E: EmbeddingFunction + ?Sized>(self, embedding: &E) -> Embedding {
        match self {
            Self::Raw(query) => embedding.call(vec![query]).await.remove(0),
            Self::Computed(embedding) => embedding,
        }
    }
}

impl<'source> FromPyObject<'source> for PyQuery {
    fn extract(query: &'source PyAny) -> PyResult<Self> {
        if let Ok(text) = query.extract::<String>() {
            return Ok(PyQuery::Raw(text));
        }
        if let Ok(embedding) = query.extract::<Embedding>() {
            return Ok(PyQuery::Computed(embedding));
        }
        let message = format!("query '{query}' must be a str, or a list of float");
        Err(PyTypeError::new_err(message))
    }
}

#[derive(Clone)]
#[pyclass(name = "GraphDocument", frozen, get_all)]
pub struct PyGraphDocument {
    content: String,
    entity: PyObject,
}

impl PyGraphDocument {
    pub fn extract_rust_document(&self, py: Python) -> PyResult<Document> {
        let node = self.entity.extract::<PyNode>(py);
        let edge = self.entity.extract::<PyEdge>(py);
        if let Ok(node) = node {
            Ok(Document::Node {
                name: node.name(),
                content: self.content.clone(),
            })
        } else if let Ok(edge) = edge {
            Ok(Document::Edge {
                src: edge.edge.src().name(),
                dst: edge.edge.dst().name(),
                content: self.content.clone(),
            })
        } else {
            Err(PyException::new_err(
                "document entity is not a node nor an edge",
            ))
        }
    }
}

#[pymethods]
impl PyGraphDocument {
    #[new]
    fn new(content: String, entity: PyObject) -> Self {
        Self { content, entity }
    }

    fn __repr__(&self, py: Python) -> String {
        let entity_repr = match self.entity.call_method0(py, "__repr__") {
            Ok(repr) => repr.extract::<String>(py).unwrap_or("None".to_owned()),
            Err(_) => "None".to_owned(),
        };
        let py_content = self.content.clone().into_py(py);
        let content_repr = match py_content.call_method0(py, "__repr__") {
            Ok(repr) => repr.extract::<String>(py).unwrap_or("''".to_owned()),
            Err(_) => "''".to_owned(),
        };
        format!(
            "GraphDocument(content={}, entity={})",
            content_repr, entity_repr
        )
    }
}

#[cfg(feature = "python")]
pub struct PyDocumentTemplate {
    node_document: Option<String>,
    edge_document: Option<String>,
    default_template: DefaultTemplate,
}

impl PyDocumentTemplate {
    pub fn new(node_document: Option<String>, edge_document: Option<String>) -> Self {
        Self {
            node_document,
            edge_document,
            default_template: DefaultTemplate,
        }
    }
}

impl<G: StaticGraphViewOps> DocumentTemplate<G> for PyDocumentTemplate {
    fn node(&self, node: &NodeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.node_document {
            Some(node_document) => get_documents_from_prop(node.properties(), node_document),
            None => self.default_template.node(node),
        }
    }

    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.edge_document {
            Some(edge_document) => get_documents_from_prop(edge.properties(), edge_document),
            None => self.default_template.edge(edge),
        }
    }
}

fn get_documents_from_prop<P: PropertiesOps + Clone + 'static>(
    properties: Properties<P>,
    name: &str,
) -> Box<dyn Iterator<Item = DocumentInput>> {
    let prop = properties.temporal().iter().find(|(key, _)| *key == name);

    match prop {
        Some((_, prop)) => {
            let iter = prop.iter().map(|(time, prop)| DocumentInput {
                content: prop.to_string(),
                life: Lifespan::Event { time },
            });
            Box::new(iter)
        }
        None => match properties.get(name) {
            Some(prop) => Box::new(std::iter::once(prop.to_string().into())),
            _ => Box::new(std::iter::empty()),
        },
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
    ///   node_document (str): the property name to be used as document for nodes (optional)
    ///   edge_document (str): the property name to be used as document for edges (optional)
    ///   verbose (bool): whether or not to print logs reporting the progress
    ///
    /// Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    #[pyo3(signature = (embedding, cache = None, overwrite_cache = false, node_document = None, edge_document = None, verbose = false))]
    fn vectorise(
        &self,
        embedding: &PyFunction,
        cache: Option<String>,
        overwrite_cache: bool,
        node_document: Option<String>,
        edge_document: Option<String>,
        verbose: bool,
    ) -> DynamicVectorisedGraph {
        let embedding: Py<PyFunction> = embedding.into();
        let graph = self.graph.clone();
        let cache = cache.map(PathBuf::from);
        let template = PyDocumentTemplate::new(node_document, edge_document);
        execute_async_task(move || async move {
            graph
                .vectorise_with_template(
                    Box::new(embedding.clone()),
                    cache,
                    overwrite_cache,
                    Arc::new(template) as Arc<dyn DocumentTemplate<DynamicGraph>>,
                    verbose,
                )
                .await
        })
    }
}

#[pyclass(name = "VectorisedGraph", frozen)]
pub struct PyVectorisedGraph(DynamicVectorisedGraph);

impl IntoPy<PyObject> for DynamicVectorisedGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, PyVectorisedGraph(self)).unwrap().into_py(py)
    }
}

type Window = Option<(PyTime, PyTime)>;

fn translate(window: Window) -> Option<(i64, i64)> {
    window.map(|(start, end)| (start.into_time(), end.into_time()))
}

/// A vectorised graph, containing a set of documents positioned in the graph space and a selection
/// over those documents
#[pymethods]
impl PyVectorisedGraph {
    /// Save the embeddings present in this graph to `file` so they can be further used in a call to `vectorise`
    fn save_embeddings(&self, file: String) {
        self.0.save_embeddings(file.into());
    }

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
    fn get_documents(&self, py: Python) -> Vec<PyGraphDocument> {
        self.get_documents_with_scores(py)
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

    /// Return the documents alongside their scores present in the current selection
    fn get_documents_with_scores(&self, py: Python) -> Vec<(PyGraphDocument, f32)> {
        let docs = self.0.get_documents_with_scores();

        docs.into_iter()
            .map(|(doc, score)| match doc {
                Document::Node { name, content } => {
                    let node = self.0.source_graph.node(name).unwrap();
                    (
                        PyGraphDocument {
                            content,
                            entity: node.into_py(py),
                        },
                        score,
                    )
                }
                Document::Edge { src, dst, content } => {
                    let edge = self.0.source_graph.edge(src, dst).unwrap();
                    (
                        PyGraphDocument {
                            content,
                            entity: edge.into_py(py),
                        },
                        score,
                    )
                }
            })
            .collect_vec()
    }

    /// Add all the documents from `nodes` and `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   nodes (list): a list of the node ids or nodes to add
    ///   edges (list):  a list of the edge ids or edges to add
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    fn append(
        &self,
        nodes: Vec<NodeRef>,
        edges: Vec<(NodeRef, NodeRef)>,
    ) -> DynamicVectorisedGraph {
        self.0.append(nodes, edges)
    }

    /// Add all the documents from `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   nodes (list): a list of the node ids or nodes to add
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    fn append_nodes(&self, nodes: Vec<NodeRef>) -> DynamicVectorisedGraph {
        self.append(nodes, vec![])
    }

    /// Add all the documents from `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a score of 0.
    ///
    /// Args:
    ///   edges (list):  a list of the edge ids or edges to add
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    fn append_edges(&self, edges: Vec<(NodeRef, NodeRef)>) -> DynamicVectorisedGraph {
        self.append(vec![], edges)
    }

    /// Add the top `limit` documents to the current selection using `query`
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new documents to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn append_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_by_similarity(&embedding, limit, translate(window))
    }

    /// Add the top `limit` node documents to the current selection using `query`
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new documents to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn append_nodes_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_nodes_by_similarity(&embedding, limit, translate(window))
    }

    /// Add the top `limit` edge documents to the current selection using `query`
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new documents to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn append_edges_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_edges_by_similarity(&embedding, limit, translate(window))
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
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (hops, window=None))]
    fn expand(&self, hops: usize, window: Window) -> DynamicVectorisedGraph {
        self.0.expand(hops, translate(window))
    }

    /// Add the top `limit` adjacent documents with higher score for `query` to the selection
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///   1. All the documents 1 hop away of some of the documents included on the selection (and
    /// not already selected) are marked as candidates.
    ///  2. Those candidates are added to the selection in descending order according to the
    /// similarity score obtained against the `query`.
    ///
    /// This loops goes on until the current selection reaches a total of `limit`  documents or
    /// until no more documents are available
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_by_similarity(&embedding, limit, translate(window))
    }

    /// Add the top `limit` adjacent node documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers nodes.
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new documents to add  
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_nodes_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_nodes_by_similarity(&embedding, limit, translate(window))
    }

    /// Add the top `limit` adjacent edge documents with higher score for `query` to the selection
    ///
    /// This function has the same behavior as expand_by_similarity but it only considers edges.
    ///
    /// Args:
    ///   query (str or list): the text or the embedding to score against
    ///   limit (int): the maximum number of new documents to add
    ///   window ((int | str, int | str)): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   A new vectorised graph containing the updated selection
    #[pyo3(signature = (query, limit, window=None))]
    fn expand_edges_by_similarity(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_edges_by_similarity(&embedding, limit, translate(window))
    }
}

fn compute_embedding(vectors: &DynamicVectorisedGraph, query: PyQuery) -> Embedding {
    let embedding = vectors.embedding.clone();
    execute_async_task(move || async move { query.into_embedding(embedding.as_ref()).await })
}

impl EmbeddingFunction for Py<PyFunction> {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>> {
        let embedding_function = self.clone();
        Box::pin(async move {
            Python::with_gil(|py| {
                let python_texts = PyList::new(py, texts);
                let result = embedding_function.call1(py, (python_texts,)).unwrap();
                let embeddings: &PyList = result.downcast(py).unwrap();

                embeddings
                    .iter()
                    .map(|embedding| {
                        let pylist: &PyList = embedding.downcast().unwrap();
                        pylist
                            .iter()
                            .map(|element| element.extract::<f32>().unwrap())
                            .collect_vec()
                    })
                    .collect_vec()
            })
        })
    }
}
