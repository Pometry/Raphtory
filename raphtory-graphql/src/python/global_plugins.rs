use crate::model::plugins::query_plugin::QueryPlugin;
use pyo3::{pyclass, pymethods};
use raphtory::python::packages::vectors::PyVectorisedGraph;

/// A class for accessing graphs hosted in a Raphtory GraphQL server
#[pyclass(name = "GraphqlGraphs", module = "raphtory.graphql")]
pub struct PyGlobalPlugins(pub(crate) QueryPlugin);

#[pymethods]
impl PyGlobalPlugins {
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
