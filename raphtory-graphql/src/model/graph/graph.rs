use std::sync::Arc;

use crate::model::algorithm::Algorithms;
use crate::model::filters::nodefilter::NodeFilter;
use crate::model::graph::edge::Edge;
use crate::model::graph::node::Node;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::{BoxableGraphView, WrappedGraph};
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use crate::model::algorithm::Algorithms;
use crate::model::graph::edge::Edge;
use crate::model::graph::node::Node;
use crate::model::filters::nodefilter::NodeFilter;
use crate::model::wrappers::dynamic::{DynamicGraph, IntoDynamic};


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
