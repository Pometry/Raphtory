use crate::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, VertexViewOps},
    python::graph::views::graph_view::PyGraphView,
    vectors::{
        vectorizable::Vectorizable, vectorized_graph::VectorizedGraph, DocumentOps, Embedding,
        EmbeddingFunction,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{path::PathBuf, sync::Arc};

/// Graph view is a read-only version of a graph at a certain point in time.
#[pyclass(name = "VectorizedGraph", frozen)]
pub struct PyVectorizedGraph {
    vectors: Arc<VectorizedGraph<DynamicGraph>>,
}

#[pymethods]
impl PyVectorizedGraph {
    #[new]
    fn new(
        py: Python<'_>,
        graph: &PyGraphView,
        embedding: &PyFunction,
        cache: &str,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyResult<Self> {
        // FIXME: we should be able to specify templates only for one type of entity: nodes/edges

        let embedding: Py<PyFunction> = embedding.into();
        let graph = graph.graph.clone();
        let cache = PathBuf::from(cache);

        // FIXME: Maybe we should have two versions: a VectorizedGraph (sync) and AsyncVectorizedGraph, in both python and rust
        // this instead is just terrible
        pyo3_asyncio::tokio::run(py, async move {
            let vectorized_graph = match (node_document, edge_document) {
                (Some(node_document), Some(edge_document)) => {
                    let node_template = move |vertex: &VertexView<DynamicGraph>| {
                        vertex.properties().get(&node_document).unwrap().to_string()
                    };
                    let edge_template = move |edge: &EdgeView<DynamicGraph>| {
                        edge.properties().get(&edge_document).unwrap().to_string()
                    };
                    graph.vectorize_with_templates(
                        Box::new(embedding.clone()),
                        &cache,
                        node_template,
                        edge_template,
                    )
                }
                (None, None) => graph.vectorize(Box::new(embedding.clone()), &cache),
                _ => panic!("you need to specify both templates for now sadly"),
            };

            Ok(PyVectorizedGraph {
                vectors: Arc::new(vectorized_graph.await),
            })
        })
    }

    fn similarity_search(
        &self,
        py: Python<'_>,
        query: String,
        init: usize,
        min_nodes: usize,
        min_edges: usize,
        limit: usize,
    ) -> PyResult<Vec<String>> {
        let vectors = self.vectors.clone();
        pyo3_asyncio::tokio::run(py, async move {
            let docs = vectors
                .similarity_search(
                    query.as_str(),
                    init,
                    min_nodes,
                    min_edges,
                    limit,
                    None,
                    None,
                )
                .await;
            Ok(docs.into_iter().map(|doc| doc.into_content()).collect_vec())
        })
    }
}

impl EmbeddingFunction for Py<PyFunction> {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>> {
        // FIXME: return result and avoid unwraps!!

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
