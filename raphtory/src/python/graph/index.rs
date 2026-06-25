use crate::{
    db::api::view::{
        internal::{CoreGraphOps, DynamicGraph},
        IndexSpec, IndexSpecBuilder, IntoDynamic, MaterializedGraph,
    },
    errors::GraphError,
    prelude::SearchableGraphOps,
    python::graph::views::graph_view::PyGraphView,
};
use ahash::HashSet;
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::meta::PropMapper;

#[pyclass(name = "IndexSpec", module = "raphtory", frozen)]
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
    /// Get node metadata.
    ///
    /// Returns:
    ///     list[str]:
    fn node_metadata(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_metadata, self.node_const_meta())
    }

    #[getter]
    /// Get node properties.
    ///
    /// Returns:
    ///     list[str]:
    fn node_properties(&self) -> Vec<String> {
        self.prop_names(&self.spec.node_properties, self.node_temp_meta())
    }

    #[getter]
    /// Get edge metadata.
    ///
    /// Returns:
    ///     list[str]:
    fn edge_metadata(&self) -> Vec<String> {
        self.prop_names(&self.spec.edge_metadata, self.edge_const_meta())
    }

    #[getter]
    /// Get edge properties.
    ///
    /// Returns:
    ///     list[str]:
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

    /// Adds all node properties and metadata to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_node_properties_and_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_properties_and_metadata(),
        })
    }

    /// Adds all node metadata to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_node_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_metadata(),
        })
    }

    /// Adds all node properties to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_node_properties(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_node_properties(),
        })
    }

    /// Adds specified node metadata to the spec.
    ///
    /// Arguments:
    ///     props: list of metadata.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_node_metadata(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_node_metadata(props)?,
        })
    }

    /// Adds specified node properties to the spec.
    ///
    /// Arguments:
    ///     props: list of properties.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_node_properties(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_node_properties(props)?,
        })
    }

    /// Adds all edge properties and metadata to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_edge_properties_and_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_properties_and_metadata(),
        })
    }

    /// Adds all edge metadata to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_edge_metadata(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_metadata(),
        })
    }

    /// Adds all edge properties to the spec.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_all_edge_properties(&mut self) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_all_edge_properties(),
        })
    }

    /// Adds specified edge metadata to the spec.
    ///
    /// Arguments:
    ///     props: List of metadata.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_edge_metadata(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_edge_metadata(props)?,
        })
    }

    /// Adds specified edge properties to the spec.
    ///
    /// Arguments:
    ///     props: List of properties.
    ///
    /// Returns:
    ///     dict[str, Any]:
    pub fn with_edge_properties(&mut self, props: Vec<String>) -> PyResult<Self> {
        Ok(Self {
            builder: self.builder.clone().with_edge_properties(props)?,
        })
    }

    /// Return a spec
    ///
    /// Returns:
    ///     IndexSpec:
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
    /// Returns:
    ///     IndexSpec:
    fn get_index_spec(&self) -> Result<PyIndexSpec, GraphError> {
        let spec = self.graph.get_index_spec()?;
        Ok(PyIndexSpec {
            graph: self.graph.clone(),
            spec,
        })
    }
}
