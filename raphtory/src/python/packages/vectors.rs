use crate::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    python::{graph::views::graph_view::PyGraphView, types::repr::Repr},
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorizable::Vectorizable,
        vectorized_graph::VectorizedGraph,
        Document, DocumentInput, Embedding, EmbeddingFunction,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{future::Future, path::PathBuf, sync::Arc, thread};

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
            Some(node_document) => {
                let prop = vertex.properties().get(node_document).unwrap();
                Box::new(std::iter::once(prop.to_string().into()))
            }
            None => self.default_template.node(vertex),
        }
    }

    fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.edge_document {
            Some(edge_document) => {
                let prop = edge.properties().get(edge_document).unwrap();
                Box::new(std::iter::once(prop.to_string().into()))
            }
            None => self.default_template.edge(edge),
        }
    }
}

#[pyclass(name = "VectorizedGraph", frozen)]
pub struct PyVectorizedGraph {
    vectors: Arc<VectorizedGraph<DynamicGraph, PyDocumentTemplate>>,
}

#[pymethods]
impl PyVectorizedGraph {
    #[new]
    fn new<'a>(
        py: Python<'a>,
        graph: &'a PyGraphView,
        embedding: &'a PyFunction,
        cache: &'a str,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyVectorizedGraph {
        // FIXME: we should be able to specify templates only for one type of entity: nodes/edges
        let embedding: Py<PyFunction> = embedding.into();
        let graph = graph.graph.clone();
        let cache = PathBuf::from(cache);
        let template = PyDocumentTemplate::new(node_document, edge_document);

        py.allow_threads(move || {
            spawn_async_task(async move {
                let vectorized_graph = graph
                    .vectorize_with_template(Box::new(embedding.clone()), &cache, template)
                    .await;
                PyVectorizedGraph {
                    vectors: Arc::new(vectorized_graph),
                }
            })
        })
    }

    fn search(
        &self,
        py: Python,
        query: String,
        init: usize,
        min_nodes: usize,
        min_edges: usize,
        limit: usize,
    ) -> Vec<PyGraphDocument> {
        let vectors = self.vectors.clone();
        let docs = py.allow_threads(move || {
            spawn_async_task(async move {
                vectors
                    .similarity_search(
                        query.as_str(),
                        init,
                        min_nodes,
                        min_edges,
                        limit,
                        None,
                        None,
                    )
                    .await
            })
        });

        docs.into_iter()
            .map(|doc| match doc {
                Document::Node { name, content } => {
                    let vertex = self.vectors.graph.vertex(name).unwrap();
                    PyGraphDocument {
                        content: content,
                        entity: vertex.into_py(py),
                    }
                }
                Document::Edge { src, dst, content } => {
                    let edge = self.vectors.graph.edge(src, dst).unwrap();
                    PyGraphDocument {
                        content: content,
                        entity: edge.into_py(py),
                    }
                }
            })
            .collect_vec()
    }
}

fn spawn_async_task<O: Send + 'static, F: Future<Output = O> + Send + 'static>(
    task: F,
) -> F::Output {
    thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task)
    })
    .join()
    .expect("error when waiting for async task to complete")
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
