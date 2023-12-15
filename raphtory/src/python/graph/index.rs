use crate::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, node::NodeView},
    },
    python::{graph::views::graph_view::PyGraphView, utils::errors::adapt_err_value},
    search::IndexedGraph,
};
use pyo3::prelude::*;

#[pymethods]
impl PyGraphView {
    /// Indexes all node and edge properties.
    /// Returns a GraphIndex which allows the user to search the edges and nodes of the graph via tantivity fuzzy matching queries.
    /// Note this is currently immutable and will not update if the graph changes. This is to be improved in a future release.
    ///
    /// Returns:
    ///    GraphIndex - Returns a GraphIndex
    fn index(&self) -> GraphIndex {
        GraphIndex::new(self.graph.clone())
    }
}

/// A searchable Index for a `Graph`. This allows for fuzzy and exact searches of nodes and edges.
/// This makes use of Tantivity internally to provide the search functionality.
/// To create a graph index, call `graph.index()` on any `Graph` object in python.
#[pyclass]
pub struct GraphIndex {
    graph: IndexedGraph<DynamicGraph>,
}

impl GraphIndex {
    pub(crate) fn new(g: DynamicGraph) -> Self {
        Self {
            graph: IndexedGraph::from(g),
        }
    }
}

#[pymethods]
impl GraphIndex {
    /// Searches for nodes which match the given query. This uses Tantivy's fuzzy search.
    /// If you would like to better understand the query syntax, please visit our documentation at https://docs.raphtory.com
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0 i.e. the first page of results.
    ///    prefix(bool):  If prefix is set to true, the fuzzy matching will be applied as a prefix search, meaning it matches terms that start with the query term. Defaults to false.
    ///    levenshtein_distance(int): The levenshtein_distance parameter defines the maximum edit distance allowed for fuzzy matching. It specifies the number of changes (insertions, deletions, or substitutions) required to match the query term. Defaults to 0 (exact matching).
    ///
    /// Returns:
    ///    A list of nodes which match the query. The list will be empty if no nodes match.
    #[pyo3(signature = (query, limit=25, offset=0, prefix=false, levenshtein_distance=0))]
    fn fuzzy_search_nodes(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<NodeView<DynamicGraph>>, PyErr> {
        self.graph
            .fuzzy_search_nodes(query, limit, offset, prefix, levenshtein_distance)
            .map_err(|e| adapt_err_value(&e))
    }

    /// Searches for edges which match the given query. This uses Tantivy's fuzzy search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0 i.e. the first page of results.
    ///    prefix(bool):  If prefix is set to true, the fuzzy matching will be applied as a prefix search, meaning it matches terms that start with the query term. Defaults to false.
    ///    levenshtein_distance(int): The levenshtein_distance parameter defines the maximum edit distance allowed for fuzzy matching. It specifies the number of changes (insertions, deletions, or substitutions) required to match the query term. Defaults to 0 (exact matching).
    ///
    /// Returns:
    ///    A list of edges which match the query. The list will be empty if no edges match the query.
    #[pyo3(signature = (query, limit=25, offset=0, prefix=false, levenshtein_distance=0))]
    fn fuzzy_search_edges(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<EdgeView<DynamicGraph>>, PyErr> {
        self.graph
            .fuzzy_search_edges(query, limit, offset, prefix, levenshtein_distance)
            .map_err(|e| adapt_err_value(&e))
    }

    /// Searches for nodes which match the given query. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0 i.e. the first page of results.
    ///
    /// Returns:
    ///    A list of nodes which match the query. The list will be empty if no nodes match.
    #[pyo3(signature = (query, limit=25, offset=0))]
    fn search_nodes(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<DynamicGraph>>, PyErr> {
        self.graph
            .search_nodes(query, limit, offset)
            .map_err(|e| adapt_err_value(&e))
    }

    /// Searches for edges which match the given query. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0 i.e. the first page of results.
    ///
    /// Returns:
    ///    A list of edges which match the query. The list will be empty if no edges match the query.
    #[pyo3(signature = (query, limit=25, offset=0))]
    fn search_edges(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<DynamicGraph>>, PyErr> {
        self.graph
            .search_edges(query, limit, offset)
            .map_err(|e| adapt_err_value(&e))
    }
}
