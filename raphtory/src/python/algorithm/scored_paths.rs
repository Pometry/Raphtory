use crate::{
    algorithms::pathing::scored_paths::{ScoredPath, ScoringMap},
    db::{api::view::DynamicGraph, graph::node::NodeView},
    prelude::NodeViewOps,
};
use pyo3::{prelude::*, Borrowed, BoundObject};
use pythonize::{depythonize, PythonizeError};
use raphtory_api::core::storage::arc_str::ArcStr;

/// Scoring rules for [top_scoring_paths][raphtory.algorithms.top_scoring_paths], given as a dict.
pub struct PyScoringMap(pub ScoringMap);

impl<'a, 'py> FromPyObject<'a, 'py> for PyScoringMap {
    type Error = PythonizeError;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        Ok(PyScoringMap(depythonize(&obj.into_bound())?))
    }
}

/// A path to the destination node, together with its total score.
#[pyclass(frozen, name = "ScoredPath", module = "raphtory.algorithms")]
pub struct PyScoredPath {
    score: f64,
    nodes: Vec<NodeView<'static, DynamicGraph>>,
    layers: Vec<ArcStr>,
}

impl PyScoredPath {
    pub(crate) fn new(graph: &DynamicGraph, path: ScoredPath) -> Self {
        Self {
            score: path.score,
            nodes: path
                .nodes
                .into_iter()
                .map(|node| NodeView::new_internal(graph.clone(), node))
                .collect(),
            layers: path.layers,
        }
    }
}

#[pymethods]
impl PyScoredPath {
    /// Sum of every node score and edge score along the path.
    ///
    /// Returns:
    ///     float: The total score of the path.
    #[getter]
    fn score(&self) -> f64 {
        self.score
    }

    /// The nodes on the path, in traversal order.
    ///
    /// Returns:
    ///     list[Node]: The path, starting at the start node and ending at the destination.
    #[getter]
    fn nodes(&self) -> Vec<NodeView<'static, DynamicGraph>> {
        self.nodes.clone()
    }

    /// The layer traversed at each hop. `layers[i]` connects `nodes[i]` to `nodes[i + 1]`.
    ///
    /// Returns:
    ///     list[str]: One layer name per hop.
    #[getter]
    fn layers(&self) -> Vec<ArcStr> {
        self.layers.clone()
    }

    /// The number of hops on the path.
    ///
    /// Returns:
    ///     int: The number of edges traversed.
    fn __len__(&self) -> usize {
        self.layers.len()
    }

    fn __repr__(&self) -> String {
        let mut path = String::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if let Some(layer) = i.checked_sub(1).and_then(|hop| self.layers.get(hop)) {
                path.push_str(&format!(" -[{layer}]-> "));
            }
            path.push_str(&node.name());
        }
        format!("ScoredPath(score={}, path={path})", self.score)
    }
}
