use crate::model::graph::{edge::Edge, node::Node};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::db::{
    api::view::{DynamicGraph, WindowSet},
    graph::{edge::EdgeView, node::NodeView, nodes::Nodes},
};

// #[derive(ResolvedObject)]
// pub(crate) struct GqlNodeWindowSet {
//     pub(crate) ws: WindowSet<'static, NodeView<DynamicGraph, DynamicGraph>>,
// }
//
// impl GqlNodeWindowSet {
//     pub(crate) fn new(ws: WindowSet<'static, NodeView<DynamicGraph, DynamicGraph>>) -> Self {
//         Self {
//             ws
//         }
//     }
// }
// #[ResolvedObjectFields]
// impl GqlNodeWindowSet {
//     async fn count(&self) -> usize {
//         self.ws.clone().count()
//     }
//
//     async fn page(&self, limit: usize, offset: usize) -> Vec<Node> {
//         let start = offset * limit;
//         self.ws
//             .clone()
//             .skip(start)
//             .take(limit)
//             .map(|n| n.into())
//             .collect()
//     }
//
//     async fn list(&self) -> Vec<Node> {
//         self.ws.clone().map(|n| n.into()).collect()
//     }
// }

macro_rules! define_window_set_wrapper {
    ($struct_name:ident, $inner_type:ty) => {
        #[derive(ResolvedObject)]
        pub(crate) struct $struct_name {
            pub(crate) ws: $inner_type,
        }

        impl $struct_name {
            pub(crate) fn new(ws: $inner_type) -> Self {
                Self { ws }
            }
        }

        #[ResolvedObjectFields]
        impl $struct_name {
            async fn count(&self) -> usize {
                self.ws.clone().count()
            }

            async fn page(&self, limit: usize, offset: usize) -> Vec<Node> {
                let start = offset * limit;
                self.ws
                    .clone()
                    .skip(start)
                    .take(limit)
                    .map(|n| n.into())
                    .collect()
            }

            async fn list(&self) -> Vec<Node> {
                self.ws.clone().map(|n| n.into()).collect()
            }
        }
    };
}

define_window_set_wrapper!(
    GqlNodeWindowSet,
    WindowSet<'static, NodeView<DynamicGraph, DynamicGraph>>
);
// define_window_set_wrapper!(GqlNodesWindowSet, WindowSet<'static, Nodes<'static,DynamicGraph, DynamicGraph>>);

#[derive(ResolvedObject)]
pub(crate) struct GqlEdgeWindowSet {
    pub(crate) ws: WindowSet<'static, EdgeView<DynamicGraph, DynamicGraph>>,
}

impl GqlEdgeWindowSet {
    pub(crate) fn new(ws: WindowSet<'static, EdgeView<DynamicGraph, DynamicGraph>>) -> Self {
        Self { ws }
    }
}
#[ResolvedObjectFields]
impl GqlEdgeWindowSet {
    async fn count(&self) -> usize {
        self.ws.clone().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Edge> {
        let start = offset * limit;
        self.ws
            .clone()
            .skip(start)
            .take(limit)
            .map(|e| e.into())
            .collect()
    }

    async fn list(&self) -> Vec<Edge> {
        self.ws.clone().map(|e| e.into()).collect()
    }
}
