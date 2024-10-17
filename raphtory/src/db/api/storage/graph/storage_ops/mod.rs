use super::{
    edges::{edge_entry::EdgeStorageEntry, unlocked::UnlockedEdges},
    nodes::node_entry::NodeStorageEntry,
};
use crate::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef,
            graph::tgraph::TemporalGraph,
            nodes::node_ref::NodeRef,
            properties::{graph_meta::GraphMeta, props::Meta},
            LayerIds, EID, VID,
        },
        utils::errors::GraphError,
        Direction,
    },
    db::api::{
        storage::graph::{
            edges::{
                edge_ref::EdgeStorageRef,
                edge_storage_ops::EdgeStorageOps,
                edges::{EdgesStorage, EdgesStorageRef},
            },
            locked::{LockedGraph, WriteLockedGraph},
            nodes::{
                node_owned_entry::NodeOwnedEntry,
                node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
                nodes::NodesStorage,
                nodes_ref::NodesStorageEntry,
            },
            variants::filter_variants::FilterVariants,
        },
        view::internal::{CoreGraphOps, FilterOps, FilterState, NodeList},
    },
    prelude::{DeletionOps, GraphViewOps},
};
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{iter, sync::Arc};

#[cfg(feature = "storage")]
use crate::{
    db::api::storage::graph::variants::storage_variants::StorageVariants,
    disk_graph::{
        storage_interface::{
            edges::DiskEdges,
            edges_ref::DiskEdgesRef,
            node::{DiskNode, DiskOwnedNode},
            nodes::DiskNodesOwned,
            nodes_ref::DiskNodesRef,
        },
        DiskGraphStorage,
    },
};

pub mod additions;
pub mod const_props;
pub mod deletions;
pub mod edge_filter;
pub mod layer_ops;
pub mod list_ops;
pub mod materialize;
pub mod node_filter;
pub mod prop_add;
pub mod time_props;
pub mod time_semantics;

impl DeletionOps for GraphStorage {}

impl CoreGraphOps for GraphStorage {
    #[inline(always)]
    fn core_graph(&self) -> &GraphStorage {
        self
    }
}

