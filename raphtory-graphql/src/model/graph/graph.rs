use crate::model::algorithm::Algorithms;
use crate::model::filters::nodefilter::NodeFilter;
use crate::model::graph::edge::Edge;
use crate::model::graph::node::Node;
use crate::model::wrappers::dynamic::{DynamicGraph, IntoDynamic};
use crate::model::graph::property::Property;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};

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

    async fn edges<'a>(&self) -> Vec<Edge> {
        self.graph.edges().into_iter().map(|ev| ev.into()).collect()
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
