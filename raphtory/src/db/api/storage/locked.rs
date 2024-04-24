use crate::core::{
    entities::{edges::edge_store::EdgeStore, nodes::node_store::NodeStore, EID, VID},
    storage::ReadLockedStorage,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct LockedGraph {
    pub(crate) nodes: Arc<ReadLockedStorage<NodeStore, VID>>,
    pub(crate) edges: Arc<ReadLockedStorage<EdgeStore, EID>>,
}
