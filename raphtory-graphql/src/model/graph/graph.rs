use crate::model::{
    algorithm::Algorithms,
    filters::{edgefilter::EdgeFilter, nodefilter::NodeFilter},
    graph::{edge::Edge, node::Node, property::Property},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::db::api::view::{
    internal::{DynamicGraph, IntoDynamic},
    GraphViewOps, TimeOps, VertexViewOps,
};

#[derive(ResolvedObject)]
pub(crate) struct GraphMeta {
    name: String,
    graph: DynamicGraph,
}

impl GraphMeta {
    pub fn new(name: String, graph: DynamicGraph) -> Self {
        Self { name, graph }
    }
}

#[ResolvedObjectFields]
impl GraphMeta {
    async fn name(&self) -> String {
        self.name.clone()
    }

    async fn static_properties(&self) -> Vec<Property> {
        self.graph
            .static_properties()
            .into_iter()
            .map(|(k, v)| Property::new(k, v))
            .collect()
    }

    async fn node_names(&self) -> Vec<String> {
        self.graph
            .vertices()
            .into_iter()
            .map(|v| v.name())
            .collect_vec()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    graph: DynamicGraph,
}

impl<G: GraphViewOps + IntoDynamic> From<G> for GqlGraph {
    fn from(value: G) -> Self {
        Self {
            graph: value.into_dynamic(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    async fn window(&self, t_start: i64, t_end: i64) -> GqlGraph {
        let w = self.graph.window(t_start, t_end);
        w.into()
    }

    async fn static_properties(&self) -> Vec<Property> {
        self.graph
            .static_properties()
            .into_iter()
            .map(|(k, v)| Property::new(k, v))
            .collect()
    }

    async fn nodes(&self, filter: Option<NodeFilter>) -> Vec<Node> {
        match filter {
            Some(filter) => self
                .graph
                .vertices()
                .iter()
                .map(|vv| vv.into())
                .filter(|n| filter.matches(n))
                .collect(),
            None => self.graph.vertices().iter().map(|vv| vv.into()).collect(),
        }
    }

    async fn edges<'a>(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .graph
                .edges()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.graph.edges().into_iter().map(|ev| ev.into()).collect(),
        }
    }

    async fn node(&self, name: String) -> Option<Node> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| &vv.name() == &name)
            .map(|vv| vv.into())
    }

    async fn node_id(&self, id: u64) -> Option<Node> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| vv.id() == id)
            .map(|vv| vv.into())
    }

    async fn algorithms(&self) -> Algorithms {
        self.graph.clone().into()
    }
}
