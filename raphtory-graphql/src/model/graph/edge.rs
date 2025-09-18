use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::EdgeViewCollection,
        history::GqlHistory,
        node::GqlNode,
        property::{GqlMetadata, GqlProperties},
        timeindex::{GqlTimeIndexEntry, GqlTimeInput},
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
use raphtory_api::core::utils::time::TryIntoTime;

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
    async fn window(&self, start: GqlTimeInput, end: GqlTimeInput) -> Result<GqlEdge, GraphError> {
        Ok(self
            .ee
            .window(start.try_into_time()?, end.try_into_time()?)
            .into())
    }

    /// Creates a view of the Edge including all events at a specified time.
    async fn at(&self, time: GqlTimeInput) -> Result<GqlEdge, GraphError> {
        Ok(self.ee.at(time.try_into_time()?).into())
    }

    /// Returns a view of the edge at the latest time of the graph.
    async fn latest(&self) -> GqlEdge {
        self.ee.latest().into()
    }

    /// Creates a view of the Edge including all events that are valid at time.
    ///
    /// This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.
    async fn snapshot_at(&self, time: GqlTimeInput) -> Result<GqlEdge, GraphError> {
        Ok(self.ee.snapshot_at(time.try_into_time()?).into())
    }

    /// Creates a view of the Edge including all events that are valid at the latest time.
    ///
    /// This is equivalent to a no-op for Graph and latest() for PersistentGraph.
    async fn snapshot_latest(&self) -> GqlEdge {
        self.ee.snapshot_latest().into()
    }

    /// Creates a view of the Edge including all events before a specified end (exclusive).
    async fn before(&self, time: GqlTimeInput) -> Result<GqlEdge, GraphError> {
        Ok(self.ee.before(time.try_into_time()?).into())
    }

    /// Creates a view of the Edge including all events after a specified start (exclusive).
    async fn after(&self, time: GqlTimeInput) -> Result<GqlEdge, GraphError> {
        Ok(self.ee.after(time.try_into_time()?).into())
    }

    /// Shrinks both the start and end of the window.
    async fn shrink_window(
        &self,
        start: GqlTimeInput,
        end: GqlTimeInput,
    ) -> Result<Self, GraphError> {
        Ok(self
            .ee
            .shrink_window(start.try_into_time()?, end.try_into_time()?)
            .into())
    }

    /// Set the start of the window.
    async fn shrink_start(&self, start: GqlTimeInput) -> Result<Self, GraphError> {
        Ok(self.ee.shrink_start(start.try_into_time()?).into())
    }

    /// Set the end of the window.
    async fn shrink_end(&self, end: GqlTimeInput) -> Result<Self, GraphError> {
        Ok(self.ee.shrink_end(end.try_into_time()?).into())
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
                EdgeViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await?,
                EdgeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await?
                }
                EdgeViewCollection::At(at) => return_view.at(at).await?,
                EdgeViewCollection::Before(time) => return_view.before(time).await?,
                EdgeViewCollection::After(time) => return_view.after(time).await?,
                EdgeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await?
                }
                EdgeViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await?,
                EdgeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await?,
            }
        }
        Ok(return_view)
    }

    /// Returns the earliest time of an edge.
    async fn earliest_time(&self) -> Option<GqlTimeIndexEntry> {
        self.ee.earliest_time().map(|t| t.into())
    }

    async fn first_update(&self) -> Option<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().earliest_time().map(|t| t.into())).await
    }

    /// Returns the latest time of an edge.
    async fn latest_time(&self) -> Option<GqlTimeIndexEntry> {
        self.ee.latest_time().map(|t| t.into())
    }

    async fn last_update(&self) -> Option<GqlTimeIndexEntry> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().latest_time().map(|t| t.into())).await
    }

    /// Returns the time of an exploded edge. Errors on an unexploded edge.
    async fn time(&self) -> Result<GqlTimeIndexEntry, GraphError> {
        self.ee.time().map(|t| t.into())
    }

    /// Returns the start time for rolling and expanding windows for this edge. Returns none if no window is applied.
    async fn start(&self) -> Option<GqlTimeIndexEntry> {
        self.ee.start().map(|t| t.into())
    }

    /// Returns the end time of the window. Returns none if no window is applied.
    async fn end(&self) -> Option<GqlTimeIndexEntry> {
        self.ee.end().map(|t| t.into())
    }

    /// Returns the source node of the edge.
    async fn src(&self) -> GqlNode {
        self.ee.src().into()
    }

    /// Returns the destination node of the edge.
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

    /// Returns a History object with time entries for when an edge is added or change to an edge is made.
    async fn history(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().into()).await
    }

    /// Returns a history object with time entries for an edge's deletion times.
    async fn deletions(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.deletions().into()).await
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
