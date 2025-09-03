use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::EdgeViewCollection,
        node::GqlNode,
        property::{GqlMetadata, GqlProperties},
        windowset::GqlEdgeWindowSet,
        WindowDuration,
        WindowDuration::{Duration, Epoch},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::view::{DynamicGraph, EdgeViewOps, IntoDynamic, StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    errors::GraphError,
    prelude::{LayerOps, TimeOps},
};

/// Raphtory graph edge.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Edge")]
pub struct GqlEdge {
    pub(crate) ee: EdgeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<EdgeView<G, GH>> for GqlEdge
{
    fn from(value: EdgeView<G, GH>) -> Self {
        Self {
            ee: EdgeView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

impl GqlEdge {
    pub(crate) fn from_ref<
        G: StaticGraphViewOps + IntoDynamic,
        GH: StaticGraphViewOps + IntoDynamic,
    >(
        value: EdgeView<&G, &GH>,
    ) -> Self {
        value.cloned().into()
    }
}

#[ResolvedObjectFields]
impl GqlEdge {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Return a view of Edge containing only the default edge layer.
    async fn default_layer(&self) -> GqlEdge {
        self.ee.default_layer().into()
    }

    /// Returns a view of Edge containing all layers in the list of names.
    ///
    /// Errors if any of the layers do not exist.
    async fn layers(&self, names: Vec<String>) -> GqlEdge {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.valid_layers(names).into()).await
    }

    /// Returns a view of Edge containing all layers except the excluded list of names.
    ///
    /// Errors if any of the layers do not exist.
    async fn exclude_layers(&self, names: Vec<String>) -> GqlEdge {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.exclude_valid_layers(names).into()).await
    }

    /// Returns a view of Edge containing the specified layer.
    ///
    /// Errors if any of the layers do not exist.
    async fn layer(&self, name: String) -> GqlEdge {
        self.ee.valid_layers(name).into()
    }

    /// Returns a view of Edge containing all layers except the excluded layer specified.
    ///
    /// Errors if any of the layers do not exist.
    async fn exclude_layer(&self, name: String) -> GqlEdge {
        self.ee.exclude_valid_layers(name).into()
    }

    /// Creates a WindowSet with the given window duration and optional step using a rolling window.
    ///
    /// A rolling window is a window that moves forward by step size at each iteration.
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlEdgeWindowSet::new(
                        self.ee.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlEdgeWindowSet::new(
                    self.ee.rolling(window_duration, None)?,
                )),
            },
        }
    }

    /// Creates a WindowSet with the given step size using an expanding window.
    ///
    /// An expanding window is a window that grows by step size at each iteration.
    async fn expanding(&self, step: WindowDuration) -> Result<GqlEdgeWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlEdgeWindowSet::new(self.ee.expanding(step)?)),
            Epoch(step) => Ok(GqlEdgeWindowSet::new(self.ee.expanding(step)?)),
        }
    }

    /// Creates a view of the Edge including all events between the specified start (inclusive) and end (exclusive).
    ///
    /// For persistent graphs, any edge which exists at any point during the window will be included. You may want to restrict this to only edges that are present at the end of the window using the is_valid function.
    async fn window(&self, start: i64, end: i64) -> GqlEdge {
        self.ee.window(start, end).into()
    }

    /// Creates a view of the Edge including all events at a specified time.
    async fn at(&self, time: i64) -> GqlEdge {
        self.ee.at(time).into()
    }

    /// Returns a view of the edge at the latest time of the graph.
    async fn latest(&self) -> GqlEdge {
        self.ee.latest().into()
    }

    /// Creates a view of the Edge including all events that are valid at time.
    ///
    /// This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.
    async fn snapshot_at(&self, time: i64) -> GqlEdge {
        self.ee.snapshot_at(time).into()
    }

    /// Creates a view of the Edge including all events that are valid at the latest time.
    ///
    /// This is equivalent to a no-op for Graph and latest() for PersistentGraph.
    async fn snapshot_latest(&self) -> GqlEdge {
        self.ee.snapshot_latest().into()
    }

    /// Creates a view of the Edge including all events before a specified end (exclusive).
    async fn before(&self, time: i64) -> GqlEdge {
        self.ee.before(time).into()
    }

    /// Creates a view of the Edge including all events after a specified start (exclusive).
    async fn after(&self, time: i64) -> GqlEdge {
        self.ee.after(time).into()
    }

    /// Shrinks both the start and end of the window.
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.ee.shrink_window(start, end).into()
    }

    /// Set the start of the window.
    async fn shrink_start(&self, start: i64) -> Self {
        self.ee.shrink_start(start).into()
    }

    /// Set the end of the window.
    async fn shrink_end(&self, end: i64) -> Self {
        self.ee.shrink_end(end).into()
    }

    /// Takes a specified selection of views and applies them in given order.
    async fn apply_views(&self, views: Vec<EdgeViewCollection>) -> Result<GqlEdge, GraphError> {
        let mut return_view: GqlEdge = self.ee.clone().into();
        for view in views {
            return_view = match view {
                EdgeViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::Layers(layers) => return_view.layers(layers).await,

                EdgeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                EdgeViewCollection::Layer(layer) => return_view.layer(layer).await,

                EdgeViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,

                EdgeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                EdgeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                EdgeViewCollection::At(at) => return_view.at(at).await,
                EdgeViewCollection::Before(time) => return_view.before(time).await,
                EdgeViewCollection::After(time) => return_view.after(time).await,
                EdgeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                EdgeViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                EdgeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
            }
        }
        Ok(return_view)
    }

    /// Returns the earliest time of an edge.
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().first().cloned()).await
    }

    /// Returns the latest time of an edge.
    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().last().cloned()).await
    }

    /// Returns the time of an exploded edge. Errors on an unexploded edge.
    async fn time(&self) -> Result<i64, GraphError> {
        self.ee.time()
    }

    /// Returns the start time for rolling and expanding windows for this edge. Returns none if no window is applied.
    async fn start(&self) -> Option<i64> {
        self.ee.start()
    }

    /// Returns the end time of the window. Returns none if no window is applied.
    async fn end(&self) -> Option<i64> {
        self.ee.end()
    }

    /// Returns the source node of the edge.
    async fn src(&self) -> GqlNode {
        self.ee.src().into()
    }

    /// Returns the destination node of the edge.
    ///
    /// Returns:
    ///     GqlNode:
    async fn dst(&self) -> GqlNode {
        self.ee.dst().into()
    }

    /// Returns the node at the other end of the edge (same as dst() for out-edges and src() for in-edges).
    async fn nbr(&self) -> GqlNode {
        self.ee.nbr().into()
    }

    /// Returns the id of the edge.
    async fn id(&self) -> Vec<String> {
        let (src_name, dst_name) = self.ee.id();
        vec![src_name.to_string(), dst_name.to_string()]
    }

    /// Returns a view of the properties of the edge.
    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    /// Returns the metadata of an edge.
    async fn metadata(&self) -> GqlMetadata {
        self.ee.metadata().into()
    }

    /// Returns the names of the layers that have this edge as a member.
    async fn layer_names(&self) -> Vec<String> {
        self.ee
            .layer_names()
            .into_iter()
            .map(|x| x.into())
            .collect()
    }

    /// Returns the layer name of an exploded edge, errors on an edge.
    async fn layer_name(&self) -> Result<String, GraphError> {
        self.ee.layer_name().map(|x| x.into())
    }

    /// Returns an edge object for each update within the original edge.
    async fn explode(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode())
    }

    /// Returns an edge object for each layer within the original edge.
    ///
    /// Each new edge object contains only updates from the respective layers.
    async fn explode_layers(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode_layers())
    }

    /// Returns a list of timestamps of when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     List[int]:
    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history()).await
    }

    /// Returns a list of timestamps of when an edge is deleted.
    ///
    /// Returns:
    ///     List[int]:
    async fn deletions(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.deletions()).await
    }

    /// Checks if the edge is currently valid and exists at the current time.
    ///
    /// Returns: boolean
    async fn is_valid(&self) -> bool {
        self.ee.is_valid()
    }

    /// Checks if the edge is currently active and has at least one update within the current period.
    ///
    /// Returns: boolean
    async fn is_active(&self) -> bool {
        self.ee.is_active()
    }

    /// Checks if the edge is deleted at the current time.
    ///
    /// Returns: boolean
    async fn is_deleted(&self) -> bool {
        self.ee.is_deleted()
    }

    /// Returns true if the edge source and destination nodes are the same.
    ///
    /// Returns: boolean
    async fn is_self_loop(&self) -> bool {
        self.ee.is_self_loop()
    }
}
