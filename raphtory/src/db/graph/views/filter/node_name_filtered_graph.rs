use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            state::NodeOp,
            view::internal::{
                GraphView, Immutable, InheritAllEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalNodeFilterOps, Static,
            },
        },
        graph::views::filter::model::Filter,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::entities::{LayerIds, VID},
    inherit::Base,
};
use raphtory_api::core::storage::arc_str::OptionAsStr;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{
        graph::GraphStorage,
        nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
    },
};
use raphtory_storage::core_ops::CoreGraphOps;

#[derive(Debug, Clone)]
pub struct NodeNameFilterOp {
    filter: Filter,
}

impl NodeNameFilterOp {
    pub(crate) fn new(filter: Filter) -> Self {
        Self { filter }
    }
}

impl NodeOp for NodeNameFilterOp {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        let node_ref = storage.core_node(node);
        self.filter.matches(node_ref.name().as_str())
    }
}