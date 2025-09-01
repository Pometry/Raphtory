use crate::{
    db::{
        api::view::{
            internal::{CoreGraphOps, DynamicGraph},
            IndexSpec, IndexSpecBuilder, IntoDynamic, MaterializedGraph,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphError,
    prelude::SearchableGraphOps,
    python::{filter::filter_expr::PyFilterExpr, graph::views::graph_view::PyGraphView},
};
use ahash::HashSet;
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::meta::PropMapper;

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
            "IndexSpec(\n  node_node_metadata=[{}],\n  node_properties=[{}],\n  edge_metadata=[{}],\n  edge_properties=[{}]\n)",
            self.prop_repr(&self.spec.node_metadata, self.node_const_meta()),
            self.prop_repr(&self.spec.node_properties, self.node_temp_meta()),
            self.prop_repr(&self.spec.edge_metadata, self.edge_const_meta()),
            self.prop_repr(&self.spec.edge_properties, self.edge_temp_meta()),
        );
        Ok(repr)
    }

    #[getter]
    fn node_metadata(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_metadata, self.node_const_meta())
    }

    #[getter]
    fn node_properties(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_properties, self.node_temp_meta())
    }

    #[getter]
    fn edge_metadata(&self) -> Vec<String> {
        self.prop_names(&self.spec.edge_metadata, self.edge_const_meta())
    }

    #[getter]
    fn edge_properties(&self) -> Vec<String> {
        self.prop_names(&self.spec.edge_properties, self.edge_temp_meta())
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
        self.graph.node_meta().metadata_mapper()
    }

    fn node_temp_meta(&self) -> &PropMapper {
        self.graph.node_meta().temporal_prop_mapper()
    }

    fn edge_const_meta(&self) -> &PropMapper {
        self.graph.edge_meta().metadata_mapper()
    }

    fn edge_temp_meta(&self) -> &PropMapper {
        self.graph.edge_meta().temporal_prop_mapper()
    }
}

#[pyclass(name = "IndexSpecBuilder", module = "raphtory")]
#[derive(Clone)]
pub struct PyIndexSpecBuilder {
    builder: IndexSpecBuilder<MaterializedGraph>,
}

#[pymethods]
impl PyIndexSpecBuilder {
    #[new]
    pub fn new(graph: MaterializedGraph) -> Self {
        Self {
            builder: IndexSpecBuilder::new(graph),
        }
    }

    pub fn with_all_node_properties_and_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_properties_and_metadata(),
        })
    }

    pub fn with_all_node_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_metadata(),
        })
    }

    pub fn with_all_node_properties(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_properties(),
        })
    }

    pub fn with_node_metadata(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_node_metadata(props)?,
        })
    }

    pub fn with_node_properties(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_node_properties(props)?,
        })
    }

    pub fn with_all_edge_properties_and_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_properties_and_metadata(),
        })
    }

    pub fn with_all_edge_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_metadata(),
        })
    }

    pub fn with_all_edge_properties(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_properties(),
        })
    }

    pub fn with_edge_metadata(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_edge_metadata(props)?,
        })
    }

    pub fn with_edge_properties(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_edge_properties(props)?,
        })
    }

    pub fn build(&self) -> PyIndexSpec {
        PyIndexSpec {
            graph: self.builder.graph.clone().into_dynamic(),
            spec: self.builder.clone().build(),
        }
    }
}

#[pymethods]
impl PyGraphView {
    /// Get index spec
    ///
    /// Return:
    ///     IndexSpec:
    fn get_index_spec(&self) -> Result<PyIndexSpec, GraphError> {
        let spec = self.graph.get_index_spec()?;
        Ok(PyIndexSpec {
            graph: self.graph.clone(),
            spec,
        })
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
    ) -> Result<Vec<NodeView<'static, DynamicGraph>>, GraphError> {
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
