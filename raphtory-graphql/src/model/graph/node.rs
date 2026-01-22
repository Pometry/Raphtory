use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::{GqlEdgeFilter, GqlNodeFilter, NodeViewCollection},
        history::GqlHistory,
        nodes::GqlNodes,
        path_from_node::GqlPathFromNode,
        property::{GqlMetadata, GqlProperties},
        timeindex::{GqlEventTime, GqlTimeInput},
        windowset::GqlNodeWindowSet,
        GqlAlignmentUnit, WindowDuration,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    algorithms::components::{in_component, out_component},
    core::utils::time::TryIntoInterval,
    db::{
        api::{
            properties::dyn_props::DynProperties,
            view::{filter_ops::NodeSelect, Filter, *},
        },
        graph::{
            node::NodeView,
            views::filter::model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::NodeStateOps,
};
use raphtory_api::core::utils::time::IntoTime;

/// Raphtory graph node.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Node")]
pub struct GqlNode {
    pub(crate) vv: NodeView<'static, DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic> From<NodeView<'static, G>> for GqlNode {
    fn from(value: NodeView<'static, G>) -> Self {
        Self {
            vv: NodeView::new_internal(value.graph.into_dynamic(), value.node),
        }
    }
}

#[ResolvedObjectFields]
/// A collection of edges.
///
/// Collections can be filtered and used to create lists.
impl GqlNode {
    /// Returns the unique id of the node.
    async fn id(&self) -> String {
        self.vv.id().to_string()
    }

    /// Returns the name of the node.
    pub async fn name(&self) -> String {
        self.vv.name()
    }

    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Return a view of the node containing only the default layer.
    async fn default_layer(&self) -> GqlNode {
        self.vv.default_layer().into()
    }

    /// Return a view of node containing all layers specified.
    async fn layers(&self, names: Vec<String>) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.valid_layers(names).into()).await
    }

    /// Returns a collection containing nodes belonging to all layers except the excluded list of layers.
    async fn exclude_layers(&self, names: Vec<String>) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.exclude_valid_layers(names).into()).await
    }

    /// Returns a collection containing nodes belonging to the specified layer.
    async fn layer(&self, name: String) -> GqlNode {
        self.vv.valid_layers(name).into()
    }

    /// Returns a collection containing nodes belonging to all layers except the excluded layer.
    async fn exclude_layer(&self, name: String) -> GqlNode {
        self.vv.exclude_valid_layers(name).into()
    }

    /// Creates a WindowSet with the specified window size and optional step using a rolling window.
    ///
    /// A rolling window is a window that moves forward by step size at each iteration.
    ///
    /// alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
    /// If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
    /// e.g. "1 month and 1 day" will align at the start of the day.
    /// Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
    /// the start of the first window and/or after the end of the last window (i.e. not included in any window).
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlNodeWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.vv.rolling_aligned(window, step, unit.into())?
        } else {
            self.vv.rolling(window, step)?
        };
        Ok(GqlNodeWindowSet::new(ws))
    }

    /// Creates a WindowSet with the specified step size using an expanding window.
    ///
    /// An expanding window is a window that grows by step size at each iteration.
    ///
    /// alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
    /// If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
    /// e.g. "1 month and 1 day" will align at the start of the day.
    async fn expanding(
        &self,
        step: WindowDuration,
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlNodeWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.vv.expanding_aligned(step, unit.into())?
        } else {
            self.vv.expanding(step)?
        };
        Ok(GqlNodeWindowSet::new(ws))
    }

    /// Create a view of the node including all events between the specified start (inclusive) and end (exclusive).
    async fn window(&self, start: GqlTimeInput, end: GqlTimeInput) -> GqlNode {
        self.vv.window(start.into_time(), end.into_time()).into()
    }

    /// Create a view of the node including all events at a specified time.
    async fn at(&self, time: GqlTimeInput) -> GqlNode {
        self.vv.at(time.into_time()).into()
    }

    /// Create a view of the node including all events at the latest time.
    async fn latest(&self) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.latest().into()).await
    }

    /// Create a view of the node including all events that are valid at the specified time.
    async fn snapshot_at(&self, time: GqlTimeInput) -> GqlNode {
        self.vv.snapshot_at(time.into_time()).into()
    }

    /// Create a view of the node including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.snapshot_latest().into()).await
    }

    /// Create a view of the node including all events before specified end time (exclusive).
    async fn before(&self, time: GqlTimeInput) -> GqlNode {
        self.vv.before(time.into_time()).into()
    }

    /// Create a view of the node including all events after the specified start time (exclusive).
    async fn after(&self, time: GqlTimeInput) -> GqlNode {
        self.vv.after(time.into_time()).into()
    }

    /// Shrink a Window to a specified start and end time, if these are earlier and later than the current start and end respectively.
    async fn shrink_window(&self, start: GqlTimeInput, end: GqlTimeInput) -> Self {
        self.vv
            .shrink_window(start.into_time(), end.into_time())
            .into()
    }

    /// Set the start of the window to the larger of a specified start time and self.start().
    async fn shrink_start(&self, start: GqlTimeInput) -> Self {
        self.vv.shrink_start(start.into_time()).into()
    }

    /// Set the end of the window to the smaller of a specified end and self.end().
    async fn shrink_end(&self, end: GqlTimeInput) -> Self {
        self.vv.shrink_end(end.into_time()).into()
    }

    async fn apply_views(&self, views: Vec<NodeViewCollection>) -> Result<GqlNode, GraphError> {
        let mut return_view: GqlNode = self.vv.clone().into();
        for view in views {
            return_view = match view {
                NodeViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                NodeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                NodeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                NodeViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                NodeViewCollection::Layers(layers) => return_view.layers(layers).await,
                NodeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                NodeViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                NodeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                NodeViewCollection::At(at) => return_view.at(at).await,
                NodeViewCollection::Before(time) => return_view.before(time).await,
                NodeViewCollection::After(time) => return_view.after(time).await,
                NodeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                NodeViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                NodeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                NodeViewCollection::NodeFilter(filter) => return_view.filter(filter).await?,
            }
        }
        Ok(return_view)
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that the node exists.
    async fn earliest_time(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.earliest_time().into()).await
    }

    /// Returns the time of the first update made to the node.
    async fn first_update(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history().earliest_time().into()).await
    }

    /// Returns the latest time that the node exists.
    async fn latest_time(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.latest_time().into()).await
    }

    /// Returns the time of the last update made to the node.
    async fn last_update(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history().latest_time().into()).await
    }

    /// Gets the start time for the window. Errors if there is no window.
    async fn start(&self) -> GqlEventTime {
        self.vv.start().into()
    }

    /// Gets the end time for the window. Errors if there is no window.
    async fn end(&self) -> GqlEventTime {
        self.vv.end().into()
    }

    /// Returns a history object for the node, with time entries for node additions and changes made to node.
    async fn history(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history().into()).await
    }

    /// Get the number of edge events for this node.
    async fn edge_history_count(&self) -> usize {
        self.vv.edge_history_count()
    }

    /// Check if the node is active and it's history is not empty.
    async fn is_active(&self) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.is_active()).await
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    /// Returns the type of node.
    pub async fn node_type(&self) -> Option<String> {
        match self.vv.node_type() {
            None => None,
            str => str.map(|s| (*s).to_string()),
        }
    }

    /// Returns the properties of the node.
    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.vv.properties()).into()
    }

    /// Returns the metadata of the node.
    async fn metadata(&self) -> GqlMetadata {
        self.vv.metadata().into()
    }

    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////

    /// Returns the number of unique counter parties for this node.
    async fn degree(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.degree()).await
    }

    /// Returns the number edges with this node as the source.
    async fn out_degree(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.out_degree()).await
    }

    /// Returns the number edges with this node as the destination.
    async fn in_degree(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.in_degree()).await
    }

    async fn in_component(&self) -> GqlNodes {
        let self_clone = self.clone();
        blocking_compute(move || GqlNodes::new(in_component(self_clone.vv.clone()).nodes())).await
    }

    async fn out_component(&self) -> GqlNodes {
        let self_clone = self.clone();
        blocking_compute(move || GqlNodes::new(out_component(self_clone.vv.clone()).nodes())).await
    }

    /// Returns all connected edges.
    async fn edges(&self, select: Option<GqlEdgeFilter>) -> Result<GqlEdges, GraphError> {
        let base = self.vv.edges();
        if let Some(sel) = select {
            let ef: CompositeEdgeFilter = sel.try_into()?;
            let narrowed = blocking_compute(move || base.select(ef)).await?;
            return Ok(GqlEdges::new(narrowed));
        }
        Ok(GqlEdges::new(base))
    }

    /// Returns outgoing edges.
    async fn out_edges(&self, select: Option<GqlEdgeFilter>) -> Result<GqlEdges, GraphError> {
        let base = self.vv.out_edges();
        if let Some(sel) = select {
            let ef: CompositeEdgeFilter = sel.try_into()?;
            let narrowed = blocking_compute(move || base.select(ef)).await?;
            return Ok(GqlEdges::new(narrowed));
        }
        Ok(GqlEdges::new(base))
    }

    /// Returns incoming edges.
    async fn in_edges(&self, select: Option<GqlEdgeFilter>) -> Result<GqlEdges, GraphError> {
        let base = self.vv.in_edges();
        if let Some(sel) = select {
            let ef: CompositeEdgeFilter = sel.try_into()?;
            let narrowed = blocking_compute(move || base.select(ef)).await?;
            return Ok(GqlEdges::new(narrowed));
        }
        Ok(GqlEdges::new(base))
    }

    /// Returns neighbouring nodes.
    async fn neighbours<'a>(
        &self,
        select: Option<GqlNodeFilter>,
    ) -> Result<GqlPathFromNode, GraphError> {
        let base = self.vv.neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromNode::new(narrowed));
        }
        Ok(GqlPathFromNode::new(base))
    }

    /// Returns the number of neighbours that have at least one in-going edge to this node.
    async fn in_neighbours<'a>(
        &self,
        select: Option<GqlNodeFilter>,
    ) -> Result<GqlPathFromNode, GraphError> {
        let base = self.vv.in_neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromNode::new(narrowed));
        }
        Ok(GqlPathFromNode::new(base))
    }

    /// Returns the number of neighbours that have at least one out-going edge from this node.
    async fn out_neighbours(
        &self,
        select: Option<GqlNodeFilter>,
    ) -> Result<GqlPathFromNode, GraphError> {
        let base = self.vv.out_neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromNode::new(narrowed));
        }
        Ok(GqlPathFromNode::new(base))
    }

    async fn filter(&self, expr: GqlNodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = expr.try_into()?;
            let filtered = self_clone.vv.filter(filter)?;
            Ok(self_clone.update(filtered.into_dynamic()))
        })
        .await
    }
}

impl GqlNode {
    fn update<N: Into<NodeView<'static, DynamicGraph>>>(&self, node: N) -> Self {
        Self { vv: node.into() }
    }
}
