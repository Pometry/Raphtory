use crate::model::graph::{
    edges::GqlEdges, path_from_node::GqlPathFromNode, property::GqlProperties,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    algorithms::components::{in_component, out_component},
    db::{
        api::{properties::dyn_props::DynProperties, view::*},
        graph::node::NodeView,
    },
};
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
    async fn id(&self) -> String {
        self.vv.id().to_string()
    }

    pub async fn name(&self) -> String {
        self.vv.name()
    }

    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn layers(&self, names: Vec<String>) -> Node {
        self.vv.valid_layers(names).into()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Node {
        self.vv.exclude_valid_layers(names).into()
    }

    async fn layer(&self, name: String) -> Node {
        self.vv.valid_layers(name).into()
    }

    async fn exclude_layer(&self, name: String) -> Node {
        self.vv.exclude_valid_layers(name).into()
    }

    async fn window(&self, start: i64, end: i64) -> Node {
        self.vv.window(start, end).into()
    }

    async fn at(&self, time: i64) -> Node {
        self.vv.at(time).into()
    }

    async fn latest(&self) -> Node {
        self.vv.latest().into()
    }

    async fn snapshot_at(&self, time: i64) -> Node {
        self.vv.snapshot_at(time).into()
    }

    async fn snapshot_latest(&self) -> Node {
        self.vv.snapshot_latest().into()
    }

    async fn before(&self, time: i64) -> Node {
        self.vv.before(time).into()
    }

    async fn after(&self, time: i64) -> Node {
        self.vv.after(time).into()
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.vv.shrink_window(start, end).into()
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.vv.shrink_start(start).into()
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.vv.shrink_end(end).into()
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn earliest_time(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn first_update(&self) -> Option<i64> {
        self.vv.history().first().cloned()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.vv.latest_time()
    }

    async fn last_update(&self) -> Option<i64> {
        self.vv.history().last().cloned()
    }

    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    async fn history(&self) -> Vec<i64> {
        self.vv.history()
    }

    async fn is_active(&self) -> bool {
        self.vv.is_active()
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    pub async fn node_type(&self) -> Option<String> {
        match self.vv.node_type() {
            None => None,
            str => str.map(|s| (*s).to_string()),
        }
    }

    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.vv.properties()).into()
    }

    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////
    /// Returns the number of edges connected to this node

    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    /// Returns the number edges with this node as the source

    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    /// Returns the number edges with this node as the destination

    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
    }

    async fn in_component(&self) -> Vec<Node> {
        in_component(self.vv.clone())
            .iter()
            .map(|n| n.clone().into())
            .collect()
    }

    async fn out_component(&self) -> Vec<Node> {
        out_component(self.vv.clone())
            .iter()
            .map(|n| n.clone().into())
            .collect()
    }

    async fn edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.edges())
    }

    async fn out_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.out_edges())
    }

    async fn in_edges(&self) -> GqlEdges {
        GqlEdges::new(self.vv.in_edges())
    }

    async fn neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.neighbours())
    }

    async fn in_neighbours<'a>(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.in_neighbours())
    }

    async fn out_neighbours(&self) -> GqlPathFromNode {
        GqlPathFromNode::new(self.vv.out_neighbours())
    }
}
