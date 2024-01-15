use crate::db::api::view::{
    internal::{DynamicGraph, Static},
    StaticGraphViewOps,
};

pub trait IntoDynamic: 'static {
    fn into_dynamic(self) -> DynamicGraph;
}

impl<G: StaticGraphViewOps + Static> IntoDynamic for G {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl IntoDynamic for DynamicGraph {
    fn into_dynamic(self) -> DynamicGraph {
        self
    }
}
