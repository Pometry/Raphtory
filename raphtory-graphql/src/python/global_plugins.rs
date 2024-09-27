use crate::model::plugins::query_plugin::QueryPlugin;
use pyo3::{pyclass, pymethods, Python};
use raphtory::{
    python::{
        packages::vectors::{
            compute_embedding, into_py_document, translate_window, PyQuery, PyVectorisedGraph,
            PyWindow,
        },
        types::wrappers::document::PyDocument,
    },
    vectors::{vectorised_cluster::VectorisedCluster, Document},
};

/// A class for accessing graphs hosted in a Raphtory GraphQL server and running global search for
/// graph documents
#[pyclass(name = "GraphqlGraphs")]
pub struct PyGlobalPlugins(pub(crate) QueryPlugin);

#[pymethods]
impl PyGlobalPlugins {
    /// Return the top documents with the smallest cosine distance to `query`
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of documents to return
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A list of documents
    fn search_graph_documents(
        &self,
        py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> Vec<PyDocument> {
        self.search_graph_documents_with_scores(py, query, limit, window)
            .into_iter()
            .map(|(doc, _score)| doc)
            .collect()
    }

    /// Same as `search_graph_documents` but it also returns the scores alongside the documents
    fn search_graph_documents_with_scores(
        &self,
        py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> Vec<(PyDocument, f32)> {
        let window = translate_window(window);
        let graphs = self.0.vectorised_graphs.read();
        let cluster = VectorisedCluster::new(&graphs);
        let vectorised_graphs = self.0.vectorised_graphs.read();
        let graph_entry = vectorised_graphs.iter().next();
        let (_, first_graph) = graph_entry
            .expect("trying to search documents with no vectorised graphs on the server");
        let embedding = compute_embedding(first_graph, query);
        let documents = cluster.search_graph_documents_with_scores(&embedding, limit, window);
        documents.into_iter().map(|(doc, score)| {
            let graph = match &doc {
                Document::Graph { name, .. } => {
                    vectorised_graphs.get(name).unwrap()
                }
                _ => panic!("search_graph_documents_with_scores returned a document that is not from a graph"),
            };
            (into_py_document(doc, graph, py), score)
        }).collect()
    }

    /// Return the `VectorisedGraph` with name `name` or `None` if it doesn't exist
    fn get(&self, name: &str) -> Option<PyVectorisedGraph> {
        self.0
            .vectorised_graphs
            .read()
            .get(name)
            .map(|graph| graph.clone().into())
    }
}
