use crate::model::plugins::query_plugin::QueryPlugin;
use pyo3::{pyclass, pymethods, PyResult, Python};
use raphtory::{
    db::api::view::DynamicGraph,
    python::{
        packages::vectors::{
            compute_embedding, translate_window, PyQuery, PyVectorisedGraph, PyWindow,
        },
    },
    vectors::{vectorised_cluster::VectorisedCluster, Document},
};

/// A class for accessing graphs hosted in a Raphtory GraphQL server and running global search for
/// graph documents
#[pyclass(name = "GraphqlGraphs", module = "raphtory.graphql")]
pub struct PyGlobalPlugins(pub(crate) QueryPlugin);

#[pymethods]
impl PyGlobalPlugins {
    /// Return the top documents with the smallest cosine distance to `query`
    ///
    /// Arguments:
    ///   query (str): the text or the embedding to score against
    ///   limit (int): the maximum number of documents to return
    ///   window (Tuple[TimeInput, TimeInput], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   list[Document]: A list of documents
    fn search_graph_documents(
        &self,
        py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<Vec<Document<DynamicGraph>>> {
        let docs = self.search_graph_documents_with_scores(py, query, limit, window)?;
        Ok(docs.into_iter().map(|(doc, _)| doc).collect())
    }

    /// Same as `search_graph_documents` but it also returns the scores alongside the documents
    ///
    /// Arguments:
    ///   query (str): the text or the embedding to score against
    ///   limit (int): the maximum number of documents to return
    ///   window (Tuple[TimeInput, TimeInput], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///   list[Tuple[Document, float]]: A list of documents and their scores
    fn search_graph_documents_with_scores(
        &self,
        _py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> PyResult<Vec<(Document<DynamicGraph>, f32)>> {
        let window = translate_window(window);
        let graphs = &self.0.graphs;
        let cluster = VectorisedCluster::new(&graphs);
        let graph_entry = graphs.iter().next();
        let (_, first_graph) = graph_entry
            .expect("trying to search documents with no vectorised graphs on the server");
        let embedding = compute_embedding(first_graph, query)?;
        let documents = cluster.search_graph_documents_with_scores(&embedding, limit, window);
        Ok(documents
            .into_iter()
            .map(|(doc, score)| (doc.into_dynamic(), score))
            .collect())
    }

    /// Return the `VectorisedGraph` with name `name` or `None` if it doesn't exist
    ///
    /// Arguments:
    ///     name (str): the name of the graph
    /// Returns:
    ///     Optional[VectorisedGraph]: the graph if it exists
    fn get(&self, name: &str) -> Option<PyVectorisedGraph> {
        self.0.graphs.get(name).map(|graph| graph.clone().into())
    }
}
