use dynamic_graphql::{ExpandObject, ExpandObjectFields};
use raphtory::prelude::GraphViewOps;
use raphtory_graphql::model::{GqlAlgorithms, QueryRoot};

#[derive(ExpandObject)]
pub(crate) struct HelloQuery<'a>(&'a QueryRoot);

#[ExpandObjectFields]
impl<'a> HelloQuery<'a> {
    async fn hello_query(name: String) -> String {
        "Hello, ".to_owned() + name.as_str()
    }
}

#[derive(ExpandObject)]
pub(crate) struct FancyAlgorithm<'a>(&'a GqlAlgorithms);

#[ExpandObjectFields]
impl<'a> FancyAlgorithm<'a> {
    async fn fancy_node_count(&self) -> usize {
        self.0.run(|graph| graph.count_nodes()).await
    }
}
