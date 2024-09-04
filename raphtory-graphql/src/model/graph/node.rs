use crate::model::graph::{
    edges::GqlEdges, path_from_node::GqlPathFromNode, property::GqlProperties,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::{properties::dyn_props::DynProperties, view::*},
    graph::node::NodeView,
};
use tracing::instrument;

#[derive(ResolvedObject)]
pub(crate) struct Node {
    pub(crate) vv: NodeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<NodeView<G, GH>> for Node
{
    fn from(value: NodeView<G, GH>) -> Self {
        Self {
            vv: NodeView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                node: value.node,
            },
        }
    }
}

#[ResolvedObjectFields]
impl Node {
    #[instrument(skip(self))]
    async fn id(&self) -> String {
        self.vv.id().to_string()
    }

    #[instrument(skip(self))]
    pub async fn name(&self) -> String {
        self.vv.name()
    }

    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////
    #[instrument(skip(self))]
    async fn layers(&self, names: Vec<String>) -> Node {
        self.vv.valid_layers(names).into()
    }

    #[instrument(skip(self))]
    async fn exclude_layers(&self, names: Vec<String>) -> Node {
        self.vv.exclude_valid_layers(names).into()
    }

    #[instrument(skip(self))]
    async fn layer(&self, name: String) -> Node {
        self.vv.valid_layers(name).into()
    }

    #[instrument(skip(self))]
    async fn exclude_layer(&self, name: String) -> Node {
        self.vv.exclude_valid_layers(name).into()
    }

    #[instrument(skip(self))]
    async fn window(&self, start: i64, end: i64) -> Node {
        self.vv.window(start, end).into()
    }

    #[instrument(skip(self))]
    async fn at(&self, time: i64) -> Node {
        self.vv.at(time).into()
    }

    #[instrument(skip(self))]
    async fn before(&self, time: i64) -> Node {
        self.vv.before(time).into()
    }

    #[instrument(skip(self))]
    async fn after(&self, time: i64) -> Node {
        self.vv.after(time).into()
    }

    #[instrument(skip(self))]
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.vv.shrink_window(start, end).into()
    }

    #[instrument(skip(self))]
    async fn shrink_start(&self, start: i64) -> Self {
        self.vv.shrink_start(start).into()
    }

    #[instrument(skip(self))]
    async fn shrink_end(&self, end: i64) -> Self {
        self.vv.shrink_end(end).into()
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////
    #[instrument(skip(self))]
    async fn earliest_time(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    #[instrument(skip(self))]
    async fn first_update(&self) -> Option<i64> {
        self.vv.history().first().cloned()
    }

    #[instrument(skip(self))]
    async fn latest_time(&self) -> Option<i64> {
        self.vv.latest_time()
    }

    #[instrument(skip(self))]
    async fn last_update(&self) -> Option<i64> {
        self.vv.history().last().cloned()
    }

    #[instrument(skip(self))]
    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    #[instrument(skip(self))]
    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    #[instrument(skip(self))]
    async fn history(&self) -> Vec<i64> {
        self.vv.history()
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////
    #[instrument(skip(self))]
    pub async fn node_type(&self) -> Option<String> {
        match self.vv.node_type() {
            None => None,
            str => str.map(|s| (*s).to_string()),
        }
    }

    #[instrument(skip(self))]
    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.vv.properties()).into()
    }

    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////
    /// Returns the number of edges connected to this node
    #[instrument(skip(self))]
    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    /// Returns the number edges with this node as the source
    #[instrument(skip(self))]
    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    /// Returns the number edges with this node as the destination
    #[instrument(skip(self))]
    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
    }

    #[instrument(skip(self))]
    async fn edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.edges())
    }

    #[instrument(skip(self))]
    async fn out_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.out_edges())
    }

    #[instrument(skip(self))]
    async fn in_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.in_edges())
    }

    #[instrument(skip(self))]
    async fn neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.neighbours())
    }

    #[instrument(skip(self))]
    async fn in_neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.in_neighbours())
    }

    #[instrument(skip(self))]
    async fn out_neighbours(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.out_neighbours())
    }
}
