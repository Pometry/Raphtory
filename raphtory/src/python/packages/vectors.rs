use crate::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    python::graph::views::graph_view::PyGraphView,
    vectors::{
        document_template::{DefaultTemplate, DocumentTemplate},
        vectorizable::Vectorizable,
        vectorized_graph::VectorizedGraph,
        DocumentInput, DocumentOps, Embedding, EmbeddingFunction,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use pyo3::{
    prelude::*,
    types::{PyFunction, PyList},
};
use std::{path::PathBuf, sync::Arc};

struct PyDefaultTemplate {
    node_document: Option<String>,
    edge_document: Option<String>,
    default_template: DefaultTemplate,
}

impl DocumentTemplate for PyDefaultTemplate {
    fn node<G: GraphViewOps>(
        &self,
        vertex: &VertexView<G>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.node_document {
            Some(node_document) => {
                let prop = vertex.properties().get(node_document).unwrap();
                Box::new(std::iter::once(prop.to_string().into()))
            }
            None => self.default_template.node(vertex),
        }
    }

    fn edge<G: GraphViewOps>(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        match &self.edge_document {
            Some(edge_document) => {
                let prop = edge.properties().get(edge_document).unwrap();
                Box::new(std::iter::once(prop.to_string().into()))
            }
            None => self.default_template.edge(edge),
        }
    }
}

/// Graph view is a read-only version of a graph at a certain point in time.
#[pyclass(name = "VectorizedGraph", frozen)]
pub struct PyVectorizedGraph {
    vectors: Arc<VectorizedGraph<DynamicGraph, PyDefaultTemplate>>,
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
            let template = PyDefaultTemplate {
                node_document,
                edge_document,
                default_template: DefaultTemplate,
            };

            let vectorized_graph =
                graph.vectorize_with_template(Box::new(embedding.clone()), &cache, template);

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
