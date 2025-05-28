use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::{internal::DynamicGraph, IndexSpec, IndexSpecBuilder},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::SearchableGraphOps,
    python::{graph::views::graph_view::PyGraphView, types::wrappers::filter_expr::PyFilterExpr},
};
use pyo3::prelude::*;
use raphtory_api::core::PropType;

#[pyclass(name = "IndexSpec", module = "raphtory", frozen)]
#[derive(Clone)]
pub struct PyIndexSpec {
    pub(crate) spec: IndexSpec,
}

#[pymethods]
impl PyIndexSpec {
    fn __repr__(&self) -> PyResult<String> {
        let fmt_props = |props: &Vec<(String, usize, PropType)>| {
            props
                .iter()
                .map(|(name, id, typ)| format!("('{}', {}, '{:?}')", name, id, typ))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let repr = format!(
            "IndexSpec(\n  node_const_props=[{}],\n  node_temp_props=[{}],\n  edge_const_props=[{}],\n  edge_temp_props=[{}]\n)",
            fmt_props(&self.spec.node_const_props),
            fmt_props(&self.spec.node_temp_props),
            fmt_props(&self.spec.edge_const_props),
            fmt_props(&self.spec.edge_temp_props),
        );

        Ok(repr)
    }

    #[getter]
    fn node_const_props(&self) -> Vec<(String, usize, String)> {
        self.spec
            .node_const_props
            .iter()
            .map(|(name, id, typ)| (name.clone(), *id, format!("{:?}", typ)))
            .collect()
    }

    #[getter]
    fn node_temp_props(&self) -> Vec<(String, usize, String)> {
        self.spec
            .node_temp_props
            .iter()
            .map(|(name, id, typ)| (name.clone(), *id, format!("{:?}", typ)))
            .collect()
    }

    #[getter]
    fn edge_const_props(&self) -> Vec<(String, usize, String)> {
        self.spec
            .edge_const_props
            .iter()
            .map(|(name, id, typ)| (name.clone(), *id, format!("{:?}", typ)))
            .collect()
    }

    #[getter]
    fn edge_temp_props(&self) -> Vec<(String, usize, String)> {
        self.spec
            .edge_temp_props
            .iter()
            .map(|(name, id, typ)| (name.clone(), *id, format!("{:?}", typ)))
            .collect()
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
            spec: self.builder.build(),
        }
    }
}

#[pymethods]
impl PyGraphView {
    /// Get index spec
    fn get_index_spec(&self) -> Result<PyIndexSpec, GraphError> {
        let spec = self.graph.get_index_spec()?;
        Ok(PyIndexSpec { spec })
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
