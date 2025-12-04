use crate::db::api::{
        storage::{graph, storage::Storage},
        view::internal::{Base, InternalStorageOps},
    };
use raphtory_api::core::{
    entities::{properties::prop::Prop, EID, GID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_core::entities::GidRef;
use raphtory_storage::{core_ops::CoreGraphOps, mutation::addition_ops::{EdgeWriteLock, InternalAdditionOps}};
use storage::{
    api::edges::EdgeSegmentOps,
    error::StorageError,
    wal::{GraphReplayer, TransactionID, LSN},
};
use storage::resolver::GIDResolverOps;

/// Wrapper struct for implementing `GraphReplayer` for a `Storage`.
/// This is needed to workaround Rust's orphan rule since both `GraphReplayer`
/// and `Storage` are foreign to this crate.
#[derive(Debug)]
pub struct ReplayGraph {
    storage: Storage,
}

impl ReplayGraph {
    pub fn new(graph: Storage) -> Self {
        Self { storage: graph }
    }
}

impl GraphReplayer for ReplayGraph {
    fn replay_add_edge<PN: AsRef<str>>(
        &self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src_name: GID,
        src_id: VID,
        dst_name: GID,
        dst_id: VID,
        eid: EID,
        layer_name: Option<&str>,
        layer_id: usize,
        props_with_status: Vec<MaybeNew<(PN, usize, Prop)>>,
    ) -> Result<(), StorageError> {
        // TODO: Check max lsn on disk to see if this record should be replayed.

        let storage = self.storage.get_storage()
            .ok_or_else(|| StorageError::GenericFailure("Storage not available during replay".to_string()))?;

        let temporal_graph = storage.core_graph().mutable().unwrap();

        // 1. Insert prop ids into edge meta.
        // No need to validate props again since they are already validated before
        // being logged to the WAL.
        let edge_meta = temporal_graph.edge_meta();
        let mut prop_ids = Vec::new();

        for prop in props_with_status.into_iter() {
            let (prop_name, prop_id, prop_value) = prop.inner();
            let prop_mapper = edge_meta.temporal_prop_mapper();

            prop_mapper.set_id_and_dtype(prop_name.as_ref(), prop_id, prop_value.dtype());
            prop_ids.push((prop_id, prop_value));
        }

        // 2. Insert node ids into resolver.
        temporal_graph.logical_to_physical.set(GidRef::from(&src_name), src_id)?;
        temporal_graph.logical_to_physical.set(GidRef::from(&dst_name), dst_id)?;

        // 3. Insert layer id into the layer meta of both edge and node.
        let node_meta = temporal_graph.node_meta();

        edge_meta.layer_meta().set_id(layer_name.unwrap_or("_default"), layer_id);
        node_meta.layer_meta().set_id(layer_name.unwrap_or("_default"), layer_id);

        // 4. Grab src, dst and edge segment locks and add the edge.
        let mut add_edge_op = temporal_graph.atomic_add_edge(src_id, dst_id, Some(eid), layer_id).unwrap();

        let edge_id = add_edge_op.internal_add_static_edge(src_id, dst_id);
        let edge_id_with_layer = edge_id.map(|eid| eid.with_layer(layer_id));

        add_edge_op.internal_add_edge(t, src_id, dst_id, edge_id_with_layer, prop_ids);

        Ok(())
    }
}
