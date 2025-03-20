use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{DynamicGraph, InternalStorageOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    python::{graph::views::graph_view::PyGraphView, types::wrappers::filter_expr::PyFilterExpr},
};
use pyo3::prelude::*;

#[pymethods]
impl PyGraphView {
    /// Searches for nodes which match the given filter expression. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    filter: The filter expression to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0.
    ///
    /// Returns:
    ///    list[Node]: A list of nodes which match the filter expression. The list will be empty if no nodes match.
    #[pyo3(signature = (filter, limit=25, offset=0))]
    fn search_nodes(
        &self,
        filter: PyFilterExpr,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<NodeView<DynamicGraph>>, GraphError> {
        self.graph
            .searcher()?
            .search_nodes(&self.graph, filter.0.clone(), limit, offset)
    }

    /// Searches for edges which match the given filter expression. This uses Tantivy's exact search.
    ///
    /// Arguments:
    ///    filter: The filter expression to search for.
    ///    limit(int): The maximum number of results to return. Defaults to 25.
    ///    offset(int): The number of results to skip. This is useful for pagination. Defaults to 0.
    ///
    /// Returns:
    ///    list[Edge]: A list of edges which match the filter expression. The list will be empty if no edges match the query.
    #[pyo3(signature = (filter, limit=25, offset=0))]
    fn search_edges(
        &self,
        filter: PyFilterExpr,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<DynamicGraph>>, GraphError> {
        self.graph
            .searcher()?
            .search_edges(&self.graph, filter.0.clone(), limit, offset)
    }
}
