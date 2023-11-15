use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
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
        types::repr::Repr,
        utils::PyTime,
    },
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorizable::Vectorizable,
        vectorized_graph::VectorizedGraph,
        vectorized_graph_selection::VectorizedGraphSelection,
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
use std::{future::Future, path::PathBuf, sync::Arc, thread};

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

type InnerVectorizedGraphSelection = VectorizedGraphSelection<DynamicGraph, PyDocumentTemplate>;

#[pyclass(name = "VectorizedGraphSelection", frozen)]
pub struct PyVectorizedGraphSelection(InnerVectorizedGraphSelection);

impl IntoPy<PyObject> for InnerVectorizedGraphSelection {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, PyVectorizedGraphSelection(self))
            .unwrap()
            .into_py(py)
    }
}

#[pymethods]
impl PyVectorizedGraphSelection {
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
                    let vertex = self.0.vectors.source_graph.vertex(name).unwrap();
                    (
                        PyGraphDocument {
                            content,
                            entity: vertex.into_py(py),
                        },
                        score,
                    )
                }
                Document::Edge { src, dst, content } => {
                    let edge = self.0.vectors.source_graph.edge(src, dst).unwrap();
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

    fn add_new_entities(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0.vectors, query);
        self.0.add_new_entities(&embedding, limit)
    }

    fn add_new_nodes(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0.vectors, query);
        self.0.add_new_nodes(&embedding, limit)
    }

    fn add_new_edges(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0.vectors, query);
        self.0.add_new_edges(&embedding, limit)
    }

    fn expand(&self, hops: usize) -> InnerVectorizedGraphSelection {
        self.0.expand(hops)
    }

    fn expand_with_search(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0.vectors, query);
        self.0.expand_with_search(&embedding, limit)
    }
}

type InnerVectorizedGraph = VectorizedGraph<DynamicGraph, PyDocumentTemplate>;

#[pyclass(name = "VectorizedGraph", frozen)]
pub struct PyVectorizedGraph(InnerVectorizedGraph);

impl IntoPy<PyObject> for InnerVectorizedGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, PyVectorizedGraph(self)).unwrap().into_py(py)
    }
}

#[pymethods]
impl PyVectorizedGraph {
    #[staticmethod]
    fn build_from_graph(
        graph: &PyGraphView,
        embedding: &PyFunction,
        cache: Option<String>,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyVectorizedGraph {
        // FIXME: we should be able to specify templates only for one type of entity: nodes/edges
        let embedding: Py<PyFunction> = embedding.into();
        let graph = graph.graph.clone();
        let cache = cache.map(|cache| PathBuf::from(cache));
        let template = PyDocumentTemplate::new(node_document, edge_document);
        spawn_async_task(move || async move {
            let vectorized_graph = graph
                .vectorize_with_template(Box::new(embedding.clone()), cache, template)
                .await;
            PyVectorizedGraph(vectorized_graph)
        })
    }

    #[pyo3(signature = (start=None, end=None))]
    fn window(&self, start: Option<PyTime>, end: Option<PyTime>) -> InnerVectorizedGraph {
        self.0.window(start, end)
    }

    fn empty_selection(&self) -> InnerVectorizedGraphSelection {
        self.0.empty_selection()
    }

    fn select(
        &self,
        nodes: Vec<VertexRef>,
        edges: Vec<(VertexRef, VertexRef)>,
    ) -> InnerVectorizedGraphSelection {
        self.0.select(nodes, edges)
    }

    fn select_nodes(&self, nodes: Vec<VertexRef>) -> InnerVectorizedGraphSelection {
        self.select(nodes, vec![])
    }

    fn select_edges(&self, edges: Vec<(VertexRef, VertexRef)>) -> InnerVectorizedGraphSelection {
        self.select(vec![], edges)
    }

    fn search_similar_entities(
        &self,
        query: PyQuery,
        limit: usize,
    ) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0, query);
        self.empty_selection().add_new_entities(&embedding, limit)
    }

    fn search_similar_nodes(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0, query);
        self.empty_selection().add_new_nodes(&embedding, limit)
    }

    fn search_similar_edges(&self, query: PyQuery, limit: usize) -> InnerVectorizedGraphSelection {
        let embedding = compute_embedding(&self.0, query);
        self.empty_selection().add_new_edges(&embedding, limit)
    }
}

fn compute_embedding(vectors: &InnerVectorizedGraph, query: PyQuery) -> Embedding {
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
