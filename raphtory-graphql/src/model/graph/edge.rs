use crate::model::graph::graph::DynamicGraph;
use crate::model::graph::node::Node;
use crate::model::graph::property::Property;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::edge::EdgeView;
use raphtory::db::view_api::EdgeViewOps;

#[derive(ResolvedObject)]
pub(crate) struct Edge {
    ee: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for Edge {
    fn from(ee: EdgeView<DynamicGraph>) -> Self {
        Self { ee }
    }
}

#[ResolvedObjectFields]
impl Edge {
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn src(&self) -> Node {
        self.ee.src().into()
    }

    async fn dst(&self) -> Node {
        self.ee.dst().into()
    }

    async fn property(&self, name: String) -> Option<Property> {
        let prop = self.ee.property(&name, true)?;
        Some(Property::new(name, prop))
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
