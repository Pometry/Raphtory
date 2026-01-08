//! Implements WAL replay for a `WriteLockedGraph`.
//! Allows for fast replay by making use of one-time lock acquisition for
//! all the segments in the graph.

use storage::pages::resolve_pos;
use crate::{WriteLockedGraph};
use raphtory_api::core::{
    entities::{properties::prop::Prop, EID, GID, VID},
    storage::timeindex::TimeIndexEntry,
    entities::properties::meta::STATIC_GRAPH_LAYER_ID,
};
use raphtory_core::entities::GidRef;
use storage::{
    api::nodes::NodeSegmentOps,
    api::edges::EdgeSegmentOps,
    persist::strategy::PersistenceStrategy,
    NS, ES, GS,
    error::StorageError,
    wal::{GraphReplay, TransactionID, LSN},
};
use storage::resolver::GIDResolverOps;

impl<EXT> GraphReplay for WriteLockedGraph<'_, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
{
    fn replay_add_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: TimeIndexEntry,
        src_name: GID,
        src_id: VID,
        dst_name: GID,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: usize,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError> {
        let temporal_graph = self.graph();
        let node_max_page_len = temporal_graph.storage().nodes().max_page_len();
        let edge_max_page_len = temporal_graph.storage().edges().max_page_len();

        // 1. Insert prop ids into edge meta.
        // No need to validate props again since they are already validated before
        // being logged to the WAL.
        let edge_meta = temporal_graph.edge_meta();
        let mut prop_ids_and_values = Vec::new();

        for (prop_name, prop_id, prop_value) in props.into_iter() {
            let prop_mapper = edge_meta.temporal_prop_mapper();

            prop_mapper.set_id_and_dtype(prop_name, prop_id, prop_value.dtype());
            prop_ids_and_values.push((prop_id, prop_value));
        }

        // 2. Insert node ids into resolver.
        temporal_graph.logical_to_physical.set(GidRef::from(&src_name), src_id)?;
        temporal_graph.logical_to_physical.set(GidRef::from(&dst_name), dst_id)?;

        // 3. Insert layer id into the layer meta of both edge and node.
        let node_meta = temporal_graph.node_meta();

        edge_meta.layer_meta().set_id(layer_name.as_deref().unwrap_or("_default"), layer_id);
        node_meta.layer_meta().set_id(layer_name.as_deref().unwrap_or("_default"), layer_id);

        // 4. Grab src writer and add edge data.
        let (src_segment_id, src_pos) = resolve_pos(src_id, node_max_page_len);
        let num_nodes = src_id.index() + 1;
        self.resize_chunks_to_num_nodes(num_nodes); // Create enough segments.

        let segment = self.graph().storage().nodes().get_or_create_segment(src_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut src_writer = self.nodes.get_mut(src_segment_id).unwrap().writer();
            src_writer.store_node_id(src_pos, STATIC_GRAPH_LAYER_ID, GidRef::from(&src_name));

            let is_new_edge_static = src_writer.get_out_edge(src_pos, dst_id, STATIC_GRAPH_LAYER_ID).is_none();
            let is_new_edge_layer = src_writer.get_out_edge(src_pos, dst_id, layer_id).is_none();

            // Add the edge to the static graph if it doesn't already exist.
            if is_new_edge_static {
                src_writer.add_static_outbound_edge(src_pos, dst_id, eid);
            }

            // Add the edge to the layer if it doesn't already exist, else just record the timestamp.
            if is_new_edge_layer {
                src_writer.add_outbound_edge(Some(t), src_pos, dst_id, eid.with_layer(layer_id));
            } else {
                src_writer.update_timestamp(t, src_pos, eid.with_layer(layer_id));
            }

            src_writer.mut_segment.set_lsn(lsn);

            // Release the writer for mutable access to dst_writer.
            drop(src_writer);
        }

        // 5. Grab dst writer and add edge data.
        let (dst_segment_id, dst_pos) = resolve_pos(dst_id, node_max_page_len);
        let num_nodes = dst_id.index() + 1;
        self.resize_chunks_to_num_nodes(num_nodes);

        let segment = self.graph().storage().nodes().get_or_create_segment(dst_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut dst_writer = self.nodes.get_mut(dst_segment_id).unwrap().writer();
            dst_writer.store_node_id(dst_pos, STATIC_GRAPH_LAYER_ID, GidRef::from(&dst_name));

            let is_new_edge_static = dst_writer.get_inb_edge(dst_pos, src_id, STATIC_GRAPH_LAYER_ID).is_none();
            let is_new_edge_layer = dst_writer.get_inb_edge(dst_pos, src_id, layer_id).is_none();

            if is_new_edge_static {
                dst_writer.add_static_inbound_edge(dst_pos, src_id, eid);
            }

            if is_new_edge_layer {
                dst_writer.add_inbound_edge(Some(t), dst_pos, src_id, eid.with_layer(layer_id));
            } else {
                dst_writer.update_timestamp(t, dst_pos, eid.with_layer(layer_id));
            }

            dst_writer.mut_segment.set_lsn(lsn);

            drop(dst_writer);
        }

        // 6. Grab edge writer and add temporal props & metadata.
        let (edge_segment_id, edge_pos) = resolve_pos(eid, edge_max_page_len);
        let num_edges = eid.index() + 1;
        self.resize_chunks_to_num_edges(num_edges);

        let segment = self.graph().storage().edges().get_or_create_segment(edge_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut edge_writer = self.edges.get_mut(edge_segment_id).unwrap().writer();

            let is_new_edge_static = edge_writer.get_edge(STATIC_GRAPH_LAYER_ID, edge_pos).is_none();

            // Add edge into the static graph if it doesn't already exist.
            if is_new_edge_static {
                let already_counted = false;
                edge_writer.add_static_edge(Some(edge_pos), src_id, dst_id, already_counted);
            }

            // Add edge into the specified layer with timestamp and props.
            edge_writer.add_edge(t, edge_pos, src_id, dst_id, prop_ids_and_values, layer_id);

            edge_writer.writer.set_lsn(lsn);
        }

        Ok(())
    }
}
