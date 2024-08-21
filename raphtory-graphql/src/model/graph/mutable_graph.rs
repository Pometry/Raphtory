use crate::model::graph::{node::Node, property::GqlPropValue};
use dynamic_graphql::{InputObject, ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::{utils::errors::GraphError, PropType},
    db::{
        api::{mutation::CollectProperties, view::MaterializedGraph},
        graph::node::NodeView,
    },
    prelude::*,
    search::IndexedGraph,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use std::path::PathBuf;

#[derive(InputObject)]
pub struct GqlPropInput {
    key: String,
    value: GqlPropValue,
}

#[derive(ResolvedObject)]
pub struct GqlMutableGraph {
    path: PathBuf,
    graph: IndexedGraph<MaterializedGraph>,
}

impl GqlMutableGraph {
    pub(crate) fn new(path: impl Into<PathBuf>, graph: IndexedGraph<MaterializedGraph>) -> Self {
        Self {
            path: path.into(),
            graph,
        }
    }
}

#[ResolvedObjectFields]
impl GqlMutableGraph {
    async fn node(&self, name: String) -> Option<GqlMutableNode> {
        let node = self.graph.node(name)?;
        Some(GqlMutableNode { node })
    }

    async fn add_node(
        &self,
        name: String,
        time: i64,
        props: Vec<GqlPropInput>,
        node_type: Option<String>,
    ) -> Result<GqlMutableNode, GraphError> {
        let props = props.into_iter().map(|p| (p.key, p.value.0));
        let node = self.graph.add_node(time, name, props, node_type.as_str())?;
        self.graph.write_updates()?;
        Ok(GqlMutableNode { node })
    }
}

#[derive(ResolvedObject)]
pub struct GqlMutableNode {
    node: NodeView<IndexedGraph<MaterializedGraph>>,
}

#[ResolvedObjectFields]
impl GqlMutableNode {
    async fn name(&self) -> String {
        self.node.name()
    }
}
