use crate::{
    core::{
        entities::nodes::node_ref::NodeRef, utils::time::IntoTime, DocumentInput, Lifespan, Prop,
    },
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
        types::wrappers::document::PyDocument,
        utils::{execute_async_task, PyTime},
    },
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        graph_entity::GraphEntity,
        vectorisable::Vectorisable,
        vectorised_graph::DynamicVectorisedGraph,
        Document, Embedding, EmbeddingFunction,
    },
};
use chrono::DateTime;
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    exceptions::{PyAttributeError, PyTypeError},
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{path::PathBuf, sync::Arc};

pub type PyWindow = Option<(PyTime, PyTime)>;

pub fn translate_py_window(window: PyWindow) -> Option<(i64, i64)> {
    window.map(|(start, end)| (start.into_time(), end.into_time()))
}

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

#[pyfunction(signature = (entity, filter_out = vec![], force_static = vec![]))]
pub fn generate_property_list(
    entity: &PyAny,
    // TODO: add time_format parameter with options: None (number) or str, to set some format like "%Y-%m-%d %H:%M:%S"
    filter_out: Vec<&str>,
    force_static: Vec<&str>,
) -> PyResult<String> {
    let node = entity.extract::<PyNode>().map(|node| node.node);
    let edge = entity.extract::<PyEdge>().map(|edge| edge.edge);

    if let Ok(node) = node {
        Ok(node.generate_property_list(&format_time, filter_out, force_static))
    } else if let Ok(edge) = edge {
        Ok(edge.generate_property_list(&format_time, filter_out, force_static))
    } else {
        Err(PyAttributeError::new_err(
            "First argument 'entity' has to be of type Node or Edge",
        ))
    }
}

impl PyDocument {
    pub fn extract_rust_document(&self, py: Python) -> Result<Document, String> {
        match &self.entity {
            None => Err("Document entity cannot be None".to_owned()),
            Some(entity) => {
                let node = entity.extract::<PyNode>(py);
                let edge = entity.extract::<PyEdge>(py);
                let graph = entity.extract::<PyGraphView>(py);
                if let Ok(node) = node {
                    Ok(Document::Node {
                        name: node.name(),
                        content: self.content.clone(),
                        life: self.life,
                    })
                } else if let Ok(edge) = edge {
                    Ok(Document::Edge {
                        src: edge.edge.src().name(),
                        dst: edge.edge.dst().name(),
                        content: self.content.clone(),
                        life: self.life,
                    })
                } else if let Ok(graph) = graph {
                    Ok(Document::Graph {
                        name: graph.graph.properties().get("name").unwrap().to_string(),
                        content: self.content.clone(),
                        life: self.life,
                    })
                } else {
                    Err("document entity is not a node nor an edge nor a graph".to_owned())
                }
            }
        }
    }
}

pub fn into_py_document(
    document: Document,
    graph: &DynamicVectorisedGraph,
    py: Python,
) -> PyDocument {
    match document {
        Document::Graph { content, life, .. } => PyDocument {
            content,
            entity: Some(graph.source_graph.clone().into_py(py)),
            life,
        },
        Document::Node {
            name,
            content,
            life,
        } => {
            let node = graph.source_graph.node(name).unwrap();

            PyDocument {
                content,
                entity: Some(node.into_py(py)),
                life,
            }
        }
        Document::Edge {
            src,
            dst,
            content,
            life,
        } => {
            let edge = graph.source_graph.edge(src, dst).unwrap();

            PyDocument {
                content,
                entity: Some(edge.into_py(py)),
                life,
            }
        }
    }
}

#[allow(dead_code)]
#[cfg(feature = "python")] // we dont need this, we already have the flag for the full module
#[allow(dead_code)]
pub struct PyDocumentTemplate {
    graph_document: Option<String>,
    node_document: Option<String>,
    edge_document: Option<String>,
    default_template: DefaultTemplate,
}

impl PyDocumentTemplate {
    pub fn new(
        graph_document: Option<String>,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> Self {
        Self {
            graph_document,
            node_document,
            edge_document,
            default_template: DefaultTemplate,
        }
    }
}

impl<G: StaticGraphViewOps> DocumentTemplate<G> for PyDocumentTemplate {
    // TODO: review, how can we let the users use the default template from graphql??
    fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.graph_document {
            Some(graph_document) => get_documents_from_props(graph.properties(), graph_document),
            None => Box::new(std::iter::empty()), // self.default_template.graph(graph),
        }
    }

    fn node(&self, node: &NodeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.node_document {
            Some(node_document) => get_documents_from_props(node.properties(), node_document),
            None => Box::new(std::iter::empty()), // self.default_template.node(node),
        }
    }

    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.edge_document {
            Some(edge_document) => get_documents_from_props(edge.properties(), edge_document),
            None => Box::new(std::iter::empty()), // self.default_template.edge(edge),
        }
    }
}

/// This funtions ignores the time history of temporal props if their type is Document and they have a life different than Lifespan::Inherited
fn get_documents_from_props<P: PropertiesOps + Clone + 'static>(
    properties: Properties<P>,
    name: &str,
) -> Box<dyn Iterator<Item = DocumentInput>> {
    let prop = properties.temporal().get(name);

    match prop {
        Some(prop) => {
            let props = prop.iter();
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
    ///   node_document (str): the property name to be used as document for nodes (optional)
    ///   edge_document (str): the property name to be used as document for edges (optional)
    ///   verbose (bool): whether or not to print logs reporting the progress
    ///
    /// Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    #[pyo3(signature = (embedding, cache = None, overwrite_cache = false, graph_document = None, node_document = None, edge_document = None, verbose = false))]
    fn vectorise(
        &self,
        embedding: &PyFunction,
        cache: Option<String>,
        overwrite_cache: bool,
        graph_document: Option<String>,
        node_document: Option<String>,
        edge_document: Option<String>,
        verbose: bool,
    ) -> DynamicVectorisedGraph {
        let embedding: Py<PyFunction> = embedding.into();
        let graph = self.graph.clone();
        let cache = cache.map(PathBuf::from);
        let template = PyDocumentTemplate::new(graph_document, node_document, edge_document);
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

impl From<DynamicVectorisedGraph> for PyVectorisedGraph {
    fn from(value: DynamicVectorisedGraph) -> Self {
        PyVectorisedGraph(value)
    }
}

impl IntoPy<PyObject> for DynamicVectorisedGraph {
    fn into_py(self, py: Python) -> PyObject {
        Py::new(py, PyVectorisedGraph(self)).unwrap().into_py(py)
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
            .map(|(doc, score)| (into_py_document(doc, &self.0, py), score))
            .collect()
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_by_similarity(&embedding, limit, translate_py_window(window))
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_nodes_by_similarity(&embedding, limit, translate_py_window(window))
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .append_edges_by_similarity(&embedding, limit, translate_py_window(window))
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
    fn expand(&self, hops: usize, window: PyWindow) -> DynamicVectorisedGraph {
        self.0.expand(hops, translate_py_window(window))
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_by_similarity(&embedding, limit, translate_py_window(window))
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_nodes_by_similarity(&embedding, limit, translate_py_window(window))
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
        window: PyWindow,
    ) -> DynamicVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_edges_by_similarity(&embedding, limit, translate_py_window(window))
    }
}

pub fn compute_embedding(vectors: &DynamicVectorisedGraph, query: PyQuery) -> Embedding {
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
