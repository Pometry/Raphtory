use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpointWrapper, EdgeFilter},
        node_filter::NodeFilter,
        EdgeFilterFactory, EdgeViewFilterOps, PropertyFilterFactory, ViewWrapOps,
    },
    python::{filter::node_expr::PyExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;
use crate::prelude::EdgeViewOps;

/// Entry point for filtering an edge endpoint (source or destination).
///
/// An `EdgeEndpoint` is obtained from `Edge.src()` or `Edge.dst()` and allows
/// you to filter on endpoint fields (id, name, type) as well as endpoint
/// properties and metadata.
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.dst().name().starts_with("user:")
///     Edge.src().property("country") == "UK"
#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
pub struct PyEdgeEndpoint(pub EdgeEndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    /// Selects the endpoint node ID field for filtering.
    fn id(&self) -> PyExpr {
        self.0.clone().id().into()
    }

    /// Selects the endpoint node name field for filtering.
    fn name(&self) -> PyExpr {
        self.0.clone().name().into()
    }

    /// Selects the endpoint node type field for filtering.
    fn node_type(&self) -> PyExpr {
        self.0.clone().node_type().into()
    }

    /// Filters an endpoint node property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyExpr {
        self.0.clone().property(name).into()
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        self.0.clone().metadata(name).into()
    }
}

/// Entry point for constructing edge filter expressions.
///
/// The `Edge` filter provides:
/// - endpoint filters via `src()` and `dst()`,
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.property("weight") > 0.5
///     Edge.window(0, 10).is_active()
///     Edge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
pub struct PyEdgeFilter(Arc<dyn EdgeFilterFactory>);

#[pymethods]
impl PyEdgeFilter {
    #[new]
    fn new() -> PyEdgeFilter {
        PyEdgeFilter(Arc::new(EdgeFilter))
    }

    /// Selects the edge **source endpoint** for filtering.
    fn src(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    /// Selects the edge **destination endpoint** for filtering.
    fn dst(&self) -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }

    /// Filters an edge property by name.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyExpr {
        self.0.property(name).into()
    }

    /// Filters an edge metadata field by name.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        self.0.metadata(name).into()
    }

    /// Restricts edge evaluation to the given time window.
    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeFilter {
        self.0.clone().window(start, end).into()
    }

    /// Restricts edge evaluation to a single point in time.
    fn at(&self, time: EventTime) -> PyEdgeFilter {
        self.0.clone().at(time).into()
    }

    /// Restricts edge evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyEdgeFilter {
        self.0.clone().after(time).into()
    }

    /// Restricts edge evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyEdgeFilter {
        self.0.clone().before(time).into()
    }

    /// Evaluates edge predicates against the latest available edge state.
    fn latest(&self) -> PyEdgeFilter {
        self.0.clone().latest().into()
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    fn snapshot_at(&self, time: EventTime) -> PyEdgeFilter {
        self.0.clone().snapshot_at(time).into()
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyEdgeFilter {
        self.0.clone().snapshot_latest().into()
    }

    /// Restricts evaluation to edges belonging to the given layer.
    fn layer(&self, layer: String) -> PyEdgeFilter {
        self.0.clone().layer(layer).into()
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyEdgeFilter {
        self.0.clone().layer(layers).into()
    }

    /// Matches edges that have at least one event in the current view.
    fn is_active(&self) -> PyEdgeFilter {
        self.0.is_active().into()
    }

    /// Matches edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyEdgeFilter {
        self.0.is_valid().into()
    }

    /// Matches edges that have been deleted.
    fn is_deleted(&self) -> PyEdgeFilter {
        self.0.is_deleted().into()
    }

    /// Matches edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyEdgeFilter {
        self.0.is_self_loop().into()
    }
}
