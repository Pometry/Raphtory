use crate::{
    core::{entities::vertices::vertex_ref::VertexRef, utils::time::IntoTime},
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::internal::DynamicGraph,
        },
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    python::{
        graph::{edge::PyEdge, vertex::PyVertex, views::graph_view::PyGraphView},
        utils::PyTime,
    },
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorisable::Vectorisable,
        vectorised_graph::VectorisedGraph,
        Document, DocumentInput, Embedding, EmbeddingFunction, Lifespan,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{future::Future, path::PathBuf, thread};

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

#[pyclass(name = "GraphDocument", frozen, get_all)]
pub struct PyGraphDocument {
    content: String,
    entity: PyObject,
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

struct PyDocumentTemplate {
    node_document: Option<String>,
    edge_document: Option<String>,
    default_template: DefaultTemplate,
}

impl PyDocumentTemplate {
    fn new(node_document: Option<String>, edge_document: Option<String>) -> Self {
        Self {
            node_document,
            edge_document,
            default_template: DefaultTemplate,
        }
    }
}

impl<G: GraphViewOps> DocumentTemplate<G> for PyDocumentTemplate {
    fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.node_document {
            Some(node_document) => get_documents_from_prop(vertex.properties(), node_document),
            None => self.default_template.node(vertex),
        }
    }

    fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
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
    let prop = properties
        .temporal()
        .iter()
        .find(|(key, _)| key.to_string() == name);

    match prop {
        Some((_, prop)) => {
            let iter = prop.iter().map(|(time, prop)| DocumentInput {
                content: prop.to_string(),
                life: Lifespan::Event { time },
            });
            Box::new(iter)
        }
        None => {
            let content = properties.get(name).unwrap().to_string();
            Box::new(std::iter::once(content.into()))
        }
    }
}

type InnerVectorisedGraph = VectorisedGraph<DynamicGraph, PyDocumentTemplate>;

#[pyclass(name = "VectorisedGraph", frozen)]
pub struct PyVectorisedGraph(InnerVectorisedGraph);

impl IntoPy<PyObject> for InnerVectorisedGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, PyVectorisedGraph(self)).unwrap().into_py(py)
    }
}

type Window = Option<(PyTime, PyTime)>;

fn translate(window: Window) -> Option<(i64, i64)> {
    window.map(|(start, end)| (start.into_time(), end.into_time()))
}

#[pymethods]
impl PyVectorisedGraph {
    #[staticmethod]
    fn build(
        graph: &PyGraphView,
        embedding: &PyFunction,
        cache: Option<String>,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyVectorisedGraph {
        // FIXME: we should be able to specify templates only for one type of entity: nodes/edges
        let embedding: Py<PyFunction> = embedding.into();
        let graph = graph.graph.clone();
        let cache = cache.map(|cache| PathBuf::from(cache));
        let template = PyDocumentTemplate::new(node_document, edge_document);
        spawn_async_task(move || async move {
            let vectorised_graph = graph
                .vectorise_with_template(Box::new(embedding.clone()), cache, template)
                .await;
            PyVectorisedGraph(vectorised_graph)
        })
    }

    fn nodes(&self) -> Vec<PyVertex> {
        self.0
            .nodes()
            .into_iter()
            .map(|vertex| vertex.into())
            .collect_vec()
    }

    fn edges(&self) -> Vec<PyEdge> {
        self.0
            .edges()
            .into_iter()
            .map(|edge| edge.into())
            .collect_vec()
    }

    fn get_documents(&self, py: Python) -> Vec<PyGraphDocument> {
        self.get_documents_with_scores(py)
            .into_iter()
            .map(|(doc, _)| doc)
            .collect_vec()
    }

    fn get_documents_with_scores(&self, py: Python) -> Vec<(PyGraphDocument, f32)> {
        let docs = self.0.get_documents_with_scores();

        docs.into_iter()
            .map(|(doc, score)| match doc {
                Document::Node { name, content } => {
                    let vertex = self.0.source_graph.vertex(name).unwrap();
                    (
                        PyGraphDocument {
                            content,
                            entity: vertex.into_py(py),
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

    #[pyo3(signature = (hops, window=None))]
    fn expand(&self, hops: usize, window: Window) -> InnerVectorisedGraph {
        self.0.expand(hops, translate(window))
    }

    #[pyo3(signature = (query, limit, window=None))]
    fn expand_with_search(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> InnerVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .expand_with_search(&embedding, limit, translate(window))
    }

    fn select(
        &self,
        nodes: Vec<VertexRef>,
        edges: Vec<(VertexRef, VertexRef)>,
    ) -> InnerVectorisedGraph {
        self.0.select(nodes, edges)
    }

    fn select_nodes(&self, nodes: Vec<VertexRef>) -> InnerVectorisedGraph {
        self.select(nodes, vec![])
    }

    fn select_edges(&self, edges: Vec<(VertexRef, VertexRef)>) -> InnerVectorisedGraph {
        self.select(vec![], edges)
    }

    #[pyo3(signature = (query, limit, window=None))]
    fn search_similar_entities(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> InnerVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .search_similar_entities(&embedding, limit, translate(window))
    }

    #[pyo3(signature = (query, limit, window=None))]
    fn search_similar_nodes(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> InnerVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .search_similar_nodes(&embedding, limit, translate(window))
    }

    #[pyo3(signature = (query, limit, window=None))]
    fn search_similar_edges(
        &self,
        query: PyQuery,
        limit: usize,
        window: Window,
    ) -> InnerVectorisedGraph {
        let embedding = compute_embedding(&self.0, query);
        self.0
            .search_similar_edges(&embedding, limit, translate(window))
    }
}

fn compute_embedding(vectors: &InnerVectorisedGraph, query: PyQuery) -> Embedding {
    let embedding = vectors.embedding.clone();
    spawn_async_task(move || async move { query.into_embedding(embedding.as_ref()).await })
}

// This function takes a function that returns a future instead of taking just a future because
// a task might return an unsendable future but what we can do is making a function returning that
// future which is sendable itself
fn spawn_async_task<T, F, O>(task: T) -> O
where
    T: FnOnce() -> F + Send + 'static,
    F: Future<Output = O> + 'static,
    O: Send + 'static,
{
    Python::with_gil(|py| {
        py.allow_threads(move || {
            thread::spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(task())
            })
            .join()
            .expect("error when waiting for async task to complete")
        })
    })
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
