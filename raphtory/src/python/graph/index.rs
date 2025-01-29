use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::{graph::SearchableGraphOps, internal::DynamicGraph},
        graph::{edge::EdgeView, node::NodeView},
    },
    python::graph::views::graph_view::PyGraphView,
};
use pyo3::prelude::*;

#[pymethods]
impl PyGraphView {
    /// Indexes all node and edge properties.
    /// Returns a GraphIndex which allows the user to search the edges and nodes of the graph via tantivy fuzzy matching queries.
    /// Note this is currently immutable and will not update if the graph changes. This is to be improved in a future release.
    ///
    /// Returns:
    ///    GraphIndex - Returns a GraphIndex
    // fn index(&self) -> GraphIndex {
    //     GraphIndex::new(self.graph.clone())
    // }

    /// Searches for nodes which match the given query. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0.
    ///
    /// Returns:
    ///    list[Node]: A list of nodes which match the query. The list will be empty if no nodes match.
    #[pyo3(signature = (query, limit=25, offset=0))]
    fn search_nodes(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<DynamicGraph>>, GraphError> {
        self.graph.search_nodes(query, limit, offset)
    }

    /// Searches for edges which match the given query. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0.
    ///
    /// Returns:
    ///    list[Edge]: A list of edges which match the query. The list will be empty if no edges match the query.
    #[pyo3(signature = (query, limit=25, offset=0))]
    fn search_edges(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<DynamicGraph>>, GraphError> {
        self.graph.search_edges(query, limit, offset)
    }

    /// Searches for nodes which match the given query. This uses Tantivy's fuzzy search.
    /// If you would like to better understand the query syntax, please visit our documentation at https://docs.raphtory.com
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination.
    ///         Returns the first page of results by default.
    ///    prefix(bool):  If prefix is set to true, the fuzzy matching will be applied as a prefix search, meaning it matches terms that start with the query term. Defaults to False.
    ///    levenshtein_distance(int): The levenshtein_distance parameter defines the maximum edit distance allowed for fuzzy matching. It specifies the number of changes (insertions, deletions, or substitutions) required to match the query term. Defaults to 0.
    ///         The default corresponds to exact matching.
    ///
    /// Returns:
    ///    list[Node]: A list of nodes which match the query. The list will be empty if no nodes match.
    #[pyo3(signature = (query, limit=25, offset=0, prefix=false, levenshtein_distance=0))]
    fn fuzzy_search_nodes(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<NodeView<DynamicGraph>>, GraphError> {
        self.graph
            .fuzzy_search_nodes(query, limit, offset, prefix, levenshtein_distance)
    }

    /// Searches for edges which match the given query. This uses Tantivy's fuzzy search.
    ///
    /// Arguments:
    ///    query(str): The query to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Returns the first page of results by default.
    ///    prefix(bool):  If prefix is set to true, the fuzzy matching will be applied as a prefix search, meaning it matches terms that start with the query term. Defaults to False.
    ///    levenshtein_distance(int): The levenshtein_distance parameter defines the maximum edit distance allowed for fuzzy matching. It specifies the number of changes (insertions, deletions, or substitutions) required to match the query term. Defaults to 0.
    ///         The default value corresponds to exact matching.
    ///
    /// Returns:
    ///    list[Edge]: A list of edges which match the query. The list will be empty if no edges match the query.
    #[pyo3(signature = (query, limit=25, offset=0, prefix=false, levenshtein_distance=0))]
    fn fuzzy_search_edges(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<EdgeView<DynamicGraph>>, GraphError> {
        self.graph
            .fuzzy_search_edges(query, limit, offset, prefix, levenshtein_distance)
    }
}
