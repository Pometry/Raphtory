use crate::{
    arrow::graph::TemporalGraph,
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            nodes::node_store::NodeStore,
            LayerIds, EID, VID,
        },
        storage::{ArcEntry, ReadLockedStorage},
        Direction,
    },
    db::api::view::internal::{FilterOps, FilterState},
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use std::{iter, sync::Arc};

#[derive(Debug)]
pub struct LockedGraph {
    pub(crate) nodes: ReadLockedStorage<NodeStore, VID>,
    pub(crate) edges: ReadLockedStorage<EdgeStore, EID>,
}
