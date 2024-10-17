use crate::core::{
    entities::{graph::tgraph::TemporalGraph, nodes::node_store::NodeStore, VID},
    storage::{
        raw_edges::{LockedEdges, WriteLockedEdges},
        ReadLockedStorage, WriteLockedNodes,
    },
    utils::errors::GraphError,
};
use raphtory_api::core::{entities::GidRef, storage::dict_mapper::MaybeNew};
use std::sync::Arc;

