use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::{NodeFilter, NodeViewCollection},
        nodes::GqlNodes,
        path_from_node::GqlPathFromNode,
        property::{GqlMetadata, GqlProperties},
        windowset::GqlNodeWindowSet,
        WindowDuration,
        WindowDuration::{Duration, Epoch},
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    algorithms::components::{in_component, out_component},
    db::{
        api::{properties::dyn_props::DynProperties, view::*},
        graph::{node::NodeView, views::filter::model::node_filter::CompositeNodeFilter},
    },
    errors::GraphError,
    prelude::NodeStateOps,
};

/// Raphtory graph node.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Node")]
pub struct GqlNode {
    pub(crate) vv: NodeView<'static, DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<NodeView<'static, G, GH>> for GqlNode
{
    fn from(value: NodeView<'static, G, GH>) -> Self {
        Self {
            vv: NodeView::new_one_hop_filtered(
                value.base_graph.into_dynamic(),
                value.graph.into_dynamic(),
                value.node,
            ),
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
    /// Returns a collection of collections. This means that item in the window set is a collection of nodes.
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlNodeWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlNodeWindowSet::new(
                        self.vv.rolling(window_duration, Some(step_duration))?,
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlNodeWindowSet::new(
                    self.vv.rolling(window_duration, None)?,
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlNodeWindowSet::new(
                        self.vv.rolling(window_duration, Some(step_duration))?,
                    )),
                },
                None => Ok(GqlNodeWindowSet::new(
                    self.vv.rolling(window_duration, None)?,
                )),
            },
        }
    }

    /// Creates a WindowSet with the specified step size using an expanding window.
    async fn expanding(&self, step: WindowDuration) -> Result<GqlNodeWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlNodeWindowSet::new(self.vv.expanding(step)?)),
            Epoch(step) => Ok(GqlNodeWindowSet::new(self.vv.expanding(step)?)),
        }
    }

    /// Create a view of the node including all events between the specified start (inclusive) and end (exclusive).
    async fn window(&self, start: i64, end: i64) -> GqlNode {
        self.vv.window(start, end).into()
    }

    /// Create a view of the node including all events at a specified time.
    async fn at(&self, time: i64) -> GqlNode {
        self.vv.at(time).into()
    }

    /// Create a view of the node including all events at the latest time.
    async fn latest(&self) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.latest().into()).await
    }

    /// Create a view of the node including all events that are valid at the specified time.
    async fn snapshot_at(&self, time: i64) -> GqlNode {
        self.vv.snapshot_at(time).into()
    }

    /// Create a view of the node including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> GqlNode {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.snapshot_latest().into()).await
    }

    /// Create a view of the node including all events before specified end time (exclusive).
    async fn before(&self, time: i64) -> GqlNode {
        self.vv.before(time).into()
    }

    /// Create a view of the node including all events after the specified start time (exclusive).
    async fn after(&self, time: i64) -> GqlNode {
        self.vv.after(time).into()
    }

    /// Shrink a Window to a specified start and end time, if these are earlier and later than the current start and end respectively.
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.vv.shrink_window(start, end).into()
    }

    /// Set the start of the window to the larger of a specified start time and self.start().
    async fn shrink_start(&self, start: i64) -> Self {
        self.vv.shrink_start(start).into()
    }

    /// Set the end of the window to the smaller of a specified end and self.end().
    async fn shrink_end(&self, end: i64) -> Self {
        self.vv.shrink_end(end).into()
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
                NodeViewCollection::Layer(layer) => return_view.layer(layer).await,
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
                NodeViewCollection::NodeFilter(filter) => return_view.node_filter(filter).await?,
            }
        }
        Ok(return_view)
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that the node exists.
    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.earliest_time()).await
    }

    /// Returns the time of the first update made to the node.
    async fn first_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history().first().cloned()).await
    }

    /// Returns the latest time that the node exists.
    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.latest_time()).await
    }

    /// Returns the time of the last update made to the node.
    async fn last_update(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history().last().cloned()).await
    }

    /// Gets the start time for the window. Errors if there is no window.
    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    /// Gets the end time for the window. Errors if there is no window.
    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    /// Returns the history of a node, including node additions and changes made to node.
    async fn history(&self) -> Vec<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.vv.history()).await
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
    async fn edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.edges())
    }

    /// Returns outgoing edges.
    async fn out_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.out_edges())
    }

    /// Returns incoming edges.
    async fn in_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.in_edges())
    }

    /// Returns neighbouring nodes.
    async fn neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.neighbours())
    }

    /// Returns the number of neighbours that have at least one in-going edge to this node.
    async fn in_neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.in_neighbours())
    }

    /// Returns the number of neighbours that have at least one out-going edge from this node.
    async fn out_neighbours(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.out_neighbours())
    }

    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = filter.try_into()?;
            let filtered_nodes_applied = self_clone.vv.filter_nodes(filter)?;
            Ok(self_clone.update(filtered_nodes_applied.into_dynamic()))
        })
        .await
    }
}

impl GqlNode {
    fn update<N: Into<NodeView<'static, DynamicGraph>>>(&self, node: N) -> Self {
        Self { vv: node.into() }
    }
}
