use crate::{
    db::graph::views::filter::model::{
        exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
        is_active_edge_filter::IsActiveEdge,
        is_deleted_filter::IsDeletedEdge,
        is_self_loop_filter::IsSelfLoopEdge,
        is_valid_filter::IsValidEdge,
        property_filter::PropertyRef,
        FilterTree,
    },
    python::{
        filter::{
            edge_expr::DynEdgeFilterFactory,
            filter_expr::PyFilterExpr,
            node_expr::{PyExpr, PyPropertyExpr},
            wire::{wrap_exploded_views, WireEntity, WireLhs, WireTarget, WireView},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
use std::sync::Arc;

/// Entry point for constructing **exploded edge** filter expressions.
///
/// An **exploded edge** represents an edge view where temporal events are treated
/// as individually addressable edge instances (i.e. "event-level" edges), rather
/// than a single aggregated edge across time.
///
/// This filter provides:
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over exploded edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     ExplodedEdge.property("weight") > 0.5
///     ExplodedEdge.window(0, 10).is_active()
///     ExplodedEdge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
pub struct PyExplodedEdgeFilter(Arc<dyn DynEdgeFilterFactory>, Vec<WireView>);

impl PyExplodedEdgeFilter {
    pub(crate) fn root() -> Self {
        PyExplodedEdgeFilter(Arc::new(ExplodedEdgeFilter), Vec::new())
    }

    fn wrap(&self, factory: Arc<dyn DynEdgeFilterFactory>, view: WireView) -> Self {
        let mut views = self.1.clone();
        views.push(view);
        PyExplodedEdgeFilter(factory, views)
    }

    fn lhs(&self, target: WireTarget) -> WireLhs {
        WireLhs {
            entity: WireEntity::ExplodedEdge,
            endpoint: None,
            target,
            ops: Vec::new(),
            views: self.1.clone(),
        }
    }
}

#[pymethods]
impl PyExplodedEdgeFilter {
    #[new]
    fn new() -> PyExplodedEdgeFilter {
        Self::root()
    }

    /// Filters an exploded edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    fn property(&self, name: String) -> PyPropertyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Property(name.clone())));
        PyPropertyExpr::new(self.0.dyn_property(name), Some(lhs))
    }

    /// Filters an exploded edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an exploded edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    fn metadata(&self, name: String) -> PyExpr {
        let lhs = self.lhs(WireTarget::Prop(PropertyRef::Metadata(name.clone())));
        PyExpr::new(self.0.dyn_metadata(name), Some(lhs))
    }

    /// Restricts exploded edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    fn window(&self, start: EventTime, end: EventTime) -> PyExplodedEdgeFilter {
        self.wrap(self.0.dyn_window(start, end), WireView::Window(start, end))
    }

    /// Restricts exploded edge evaluation to a single point in time.
    fn at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.wrap(
            self.0.dyn_at(time),
            WireView::Window(time, EventTime::end(time.t().saturating_add(1))),
        )
    }

    /// Restricts exploded edge evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.wrap(
            self.0.dyn_after(time),
            WireView::Window(
                EventTime::start(time.t().saturating_add(1)),
                EventTime::end(i64::MAX),
            ),
        )
    }

    /// Restricts exploded edge evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.wrap(
            self.0.dyn_before(time),
            WireView::Window(EventTime::start(i64::MIN), EventTime::end(time.t())),
        )
    }

    /// Evaluates exploded edge predicates against the latest available state.
    fn latest(&self) -> PyExplodedEdgeFilter {
        self.wrap(self.0.dyn_latest(), WireView::Latest)
    }

    /// Evaluates exploded edge predicates against a snapshot of the graph at a given time.
    fn snapshot_at(&self, time: EventTime) -> PyExplodedEdgeFilter {
        self.wrap(self.0.dyn_snapshot_at(time), WireView::SnapshotAt(time))
    }

    /// Evaluates exploded edge predicates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyExplodedEdgeFilter {
        self.wrap(self.0.dyn_snapshot_latest(), WireView::SnapshotLatest)
    }

    /// Restricts evaluation to exploded edges belonging to the given layer.
    fn layer(&self, layer: String) -> PyExplodedEdgeFilter {
        self.wrap(
            self.0.dyn_layer(vec![layer.clone()]),
            WireView::Layers(vec![layer]),
        )
    }

    /// Restricts evaluation to exploded edges belonging to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyExplodedEdgeFilter {
        let layers = layers.to_vec();
        self.wrap(self.0.dyn_layer(layers.clone()), WireView::Layers(layers))
    }

    /// Matches exploded edges that have at least one event in the current view.
    fn is_active(&self) -> PyFilterExpr {
        let tree = FilterTree::ExplodedEdge(wrap_exploded_views(
            CompositeExplodedEdgeFilter::IsActiveEdge(IsActiveEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_active(), Some(tree))
    }

    /// Matches exploded edges that are structurally valid in the current view.
    fn is_valid(&self) -> PyFilterExpr {
        let tree = FilterTree::ExplodedEdge(wrap_exploded_views(
            CompositeExplodedEdgeFilter::IsValidEdge(IsValidEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_valid(), Some(tree))
    }

    /// Matches exploded edges that have been deleted.
    fn is_deleted(&self) -> PyFilterExpr {
        let tree = FilterTree::ExplodedEdge(wrap_exploded_views(
            CompositeExplodedEdgeFilter::IsDeletedEdge(IsDeletedEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_deleted(), Some(tree))
    }

    /// Matches exploded edges that are self-loops (source == destination).
    fn is_self_loop(&self) -> PyFilterExpr {
        let tree = FilterTree::ExplodedEdge(wrap_exploded_views(
            CompositeExplodedEdgeFilter::IsSelfLoopEdge(IsSelfLoopEdge),
            &self.1,
        ));
        PyFilterExpr(self.0.dyn_is_self_loop(), Some(tree))
    }
}
