use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::{
            internal::{CoreGraphOps, DynamicGraph},
            IndexSpec, IndexSpecBuilder,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::SearchableGraphOps,
    python::{graph::views::graph_view::PyGraphView, types::wrappers::filter_expr::PyFilterExpr},
};
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::props::PropMapper;
use std::collections::HashSet;

#[pyclass(name = "IndexSpec", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyIndexSpec {
    pub(crate) graph: DynamicGraph,
    pub(crate) spec: IndexSpec,
}

#[pymethods]
impl PyIndexSpec {
    fn __repr__(&self) -> PyResult<String> {
        let repr = format!(
            "IndexSpec(\n  node_const_props=[{}],\n  node_temp_props=[{}],\n  edge_const_props=[{}],\n  edge_temp_props=[{}]\n)",
            self.prop_repr(&self.spec.node_const_props, self.node_const_meta()),
            self.prop_repr(&self.spec.node_temp_props, self.node_temp_meta()),
            self.prop_repr(&self.spec.edge_const_props, self.edge_const_meta()),
            self.prop_repr(&self.spec.edge_temp_props, self.edge_temp_meta()),
        );
        Ok(repr)
    }

    #[getter]
    fn node_const_props(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_const_props, self.node_const_meta())
    }

    #[getter]
    fn node_temp_props(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_temp_props, self.node_temp_meta())
    }

    #[getter]
    fn edge_const_props(&self) -> Vec<String> {
        self.prop_names(&self.spec.edge_const_props, self.edge_const_meta())
    }

    #[getter]
    fn edge_temp_props(&self) -> Vec<String> {
        self.prop_names(&self.spec.edge_temp_props, self.edge_temp_meta())
    }
}

impl PyIndexSpec {
    fn prop_names(&self, prop_ids: &HashSet<usize>, meta: &PropMapper) -> Vec<String> {
        let mut names: Vec<String> = prop_ids
            .iter()
            .map(|id| meta.get_name(*id).to_string())
            .collect();
        names.sort();
        names
    }

    fn prop_repr(&self, prop_ids: &HashSet<usize>, meta: &PropMapper) -> String {
        self.prop_names(prop_ids, meta)
            .into_iter()
            .map(|name| format!("('{}')", name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn node_const_meta(&self) -> &PropMapper {
        self.graph.node_meta().const_prop_meta()
    }

    fn node_temp_meta(&self) -> &PropMapper {
        self.graph.node_meta().temporal_prop_meta()
    }

    fn edge_const_meta(&self) -> &PropMapper {
        self.graph.edge_meta().const_prop_meta()
    }

    fn edge_temp_meta(&self) -> &PropMapper {
        self.graph.edge_meta().temporal_prop_meta()
    }
}

#[pyclass(name = "IndexSpecBuilder", module = "raphtory")]
#[derive(Clone)]
pub struct PyIndexSpecBuilder {
    builder: IndexSpecBuilder<DynamicGraph>,
}

#[pymethods]
impl PyIndexSpecBuilder {
    #[new]
    pub fn new(graph: PyGraphView) -> Self {
        Self {
            builder: IndexSpecBuilder::new(graph.graph.clone()),
        }
    }

    pub fn with_all_node_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_props(),
        })
    }

    pub fn with_all_const_node_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_const_node_props(),
        })
    }

    pub fn with_all_temp_node_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_temp_node_props(),
        })
    }

    pub fn with_const_node_props(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_const_node_props(props)?,
        })
    }

    pub fn with_temp_node_props(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_temp_node_props(props)?,
        })
    }

    pub fn with_all_edge_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_props(),
        })
    }

    pub fn with_all_edge_const_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_const_props(),
        })
    }

    pub fn with_all_temp_edge_props(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_temp_edge_props(),
        })
    }

    pub fn with_const_edge_props(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_const_edge_props(props)?,
        })
    }

    pub fn with_temp_edge_props(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_temp_edge_props(props)?,
        })
    }

    pub fn build(&self) -> PyIndexSpec {
        PyIndexSpec {
            graph: self.builder.graph.clone(),
            spec: self.builder.build(),
        }
    }
}

#[pymethods]
impl PyGraphView {
    /// Get index spec
    fn get_index_spec(&self) -> Result<PyIndexSpec, GraphError> {
        let spec = self.graph.get_index_spec()?;
        Ok(PyIndexSpec {
            graph: self.graph.clone(),
            spec,
        })
    }

    /// Create graph index
    fn create_index(&self) -> Result<(), GraphError> {
        self.graph.create_index()
    }

    /// Create graph index with the provided index spec.
    fn create_index_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph.create_index_with_spec(py_spec.spec.clone())
    }

    /// Creates a graph index in memory (RAM).
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    fn create_index_in_ram(&self) -> Result<(), GraphError> {
        self.graph.create_index_in_ram()
    }

    /// Creates a graph index in memory (RAM) with the provided index spec.
    ///
    /// This is primarily intended for use in tests and should not be used in production environments,
    /// as the index will not be persisted to disk.
    fn create_index_in_ram_with_spec(&self, py_spec: &PyIndexSpec) -> Result<(), GraphError> {
        self.graph
            .create_index_in_ram_with_spec(py_spec.spec.clone())
    }

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
        let filter = filter.try_as_node_filter()?;
        self.graph.search_nodes(filter, limit, offset)
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
        let filter = filter.try_as_edge_filter()?;
        self.graph.search_edges(filter, limit, offset)
    }
}
