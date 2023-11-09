use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::internal::{DynamicGraph, IntoDynamic},
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
        vectorized_graph::{VectorizedGraph, VectorizedGraphSelection},
        Document, DocumentInput, Embedding, EmbeddingFunction, Lifespan,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{
    future::Future,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

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
        .find(|(key, prop)| key.to_string() == name);

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

type InnerVectorizedGraphSelection =
    VectorizedGraphSelection<DynamicGraph, DynamicGraph, PyDocumentTemplate>;

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

    fn add_new_entities(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let selection = self.0.clone();
        spawn_async_task(
            move || async move { selection.add_new_entities(query.as_str(), limit).await },
        )
    }

    fn add_new_nodes(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let selection = self.0.clone();
        spawn_async_task(
            move || async move { selection.add_new_nodes(query.as_str(), limit).await },
        )
    }

    fn add_new_edges(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let selection = self.0.clone();
        spawn_async_task(
            move || async move { selection.add_new_edges(query.as_str(), limit).await },
        )
    }

    fn expand(&self, hops: usize) -> InnerVectorizedGraphSelection {
        self.0.expand(hops)
    }

    fn expand_with_search(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let selection = self.0.clone();
        spawn_async_task(move || async move {
            selection.expand_with_search(query.as_str(), limit).await
        })
    }
}

type InnerVectorizedGraph = VectorizedGraph<DynamicGraph, DynamicGraph, PyDocumentTemplate>;

#[pyclass(name = "VectorizedGraph", frozen)]
pub struct PyVectorizedGraph(InnerVectorizedGraph);

impl IntoPy<PyObject> for InnerVectorizedGraph {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Py::new(py, PyVectorizedGraph(self)).unwrap().into_py(py)
    }
}

#[pymethods]
impl PyVectorizedGraph {
    #[new]
    fn new(
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
    pub fn window(&self, start: Option<PyTime>, end: Option<PyTime>) -> InnerVectorizedGraph {
        let window = self.0.window(start, end);
        InnerVectorizedGraph::new(
            window.source_graph,
            window.template,
            window.embedding,
            window.node_documents,
            window.edge_documents,
            window.windowed_graph.into_dynamic(),
            window.window_start,
            window.window_end,
        )
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
        self.0.select_nodes(nodes)
    }

    fn select_edges(&self, edges: Vec<(VertexRef, VertexRef)>) -> InnerVectorizedGraphSelection {
        self.0.select_edges(edges)
    }

    fn search_similar_entities(
        &self,
        query: String,
        limit: usize,
    ) -> InnerVectorizedGraphSelection {
        let vectors = self.0.clone();
        spawn_async_task(move || async move {
            vectors.search_similar_entities(query.as_str(), limit).await
        })
    }

    fn search_similar_nodes(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let vectors = self.0.clone();
        spawn_async_task(move || async move {
            vectors.search_similar_nodes(query.as_str(), limit).await
        })
    }

    fn search_similar_edges(&self, query: String, limit: usize) -> InnerVectorizedGraphSelection {
        let vectors = self.0.clone();
        spawn_async_task(move || async move {
            vectors.search_similar_edges(query.as_str(), limit).await
        })
    }

    // fn search_with_scores(
    //     &self,
    //     py: Python,
    //     query: String,
    //     init: usize,
    //     min_nodes: usize,
    //     min_edges: usize,
    //     limit: usize,
    // ) -> Vec<(PyGraphDocument, f32)> {
    //     let vectors = self.vectors.clone();
    //     let start = self.start.clone();
    //     let end = self.end.clone();
    //     let docs = py.allow_threads(move || {
    //         spawn_async_task(move || async move {
    //             vectors
    //                 .similarity_search_with_scores(
    //                     query.as_str(),
    //                     init,
    //                     min_nodes,
    //                     min_edges,
    //                     limit,
    //                     start,
    //                     end,
    //                 )
    //                 .await
    //         })
    //     });
    //
    //     docs.into_iter()
    //         .map(|(doc, score)| match doc {
    //             Document::Node { name, content } => {
    //                 let vertex = self.vectors.source_graph.vertex(name).unwrap();
    //                 (
    //                     PyGraphDocument {
    //                         content,
    //                         entity: vertex.into_py(py),
    //                     },
    //                     score,
    //                 )
    //             }
    //             Document::Edge { src, dst, content } => {
    //                 let edge = self.vectors.source_graph.edge(src, dst).unwrap();
    //                 (
    //                     PyGraphDocument {
    //                         content,
    //                         entity: edge.into_py(py),
    //                     },
    //                     score,
    //                 )
    //             }
    //         })
    //         .collect_vec()
    // }
}

// This function takes a function that returns a future instead of just a future because vector
// APIs return unsendable futures but what we can do is making a function returning those futures
// which is sendable itself
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

// old version
// fn spawn_async_task<T, F, O>(task: T) -> O
// where
//     T: FnOnce() -> F + Send + 'static,
//     F: Future<Output = O> + 'static,
//     O: Send + 'static,
// {
//     thread::spawn(move || {
//         tokio::runtime::Builder::new_multi_thread()
//             .enable_all()
//             .build()
//             .unwrap()
//             .block_on(task())
//     })
//     .join()
//     .expect("error when waiting for async task to complete")
// }

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
