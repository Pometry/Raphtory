//! Implements WAL replay for a `WriteLockedGraph`.
//! Allows for fast replay by making use of one-time lock acquisition for
//! all the segments in the graph.

use crate::WriteLockedGraph;
use raphtory_api::core::{
    entities::{
        properties::{meta::STATIC_GRAPH_LAYER_ID, prop::Prop},
        EID, GID, VID,
    },
    storage::timeindex::EventTime,
};
use storage::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    error::StorageError,
    pages::resolve_pos,
    persist::{config::ConfigOps, strategy::PersistenceStrategy},
    resolver::GIDResolverOps,
    wal::{GraphReplay, TransactionID, LSN},
    ES, GS, NS,
};

impl<EXT> GraphReplay for WriteLockedGraph<'_, EXT>
where
    EXT: PersistenceStrategy<NS = NS<EXT>, ES = ES<EXT>, GS = GS<EXT>>,
    NS<EXT>: NodeSegmentOps<Extension = EXT>,
    ES<EXT>: EdgeSegmentOps<Extension = EXT>,
    GS<EXT>: GraphPropSegmentOps<Extension = EXT>,
{
    fn replay_add_edge(
        &mut self,
        lsn: LSN,
        transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GID>,
        src_id: VID,
        dst_name: Option<GID>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: usize,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError> {
        let temporal_graph = self.graph();
        let node_max_page_len = temporal_graph.extension().config().max_node_page_len();
        let edge_max_page_len = temporal_graph.extension().config().max_edge_page_len();

        // 1. Insert prop ids into edge meta.
        // No need to validate props again since they are already validated before
        // being logged to the WAL.
        let edge_meta = temporal_graph.edge_meta();

        for (prop_name, prop_id, prop_value) in &props {
            let prop_mapper = edge_meta.temporal_prop_mapper();
            prop_mapper.set_id_and_dtype(prop_name.as_str(), *prop_id, prop_value.dtype());
        }

        // 2. Insert node ids into resolver.
        if let Some(src_name) = src_name.as_ref() {
            temporal_graph
                .logical_to_physical
                .set(src_name.as_ref(), src_id)?;
        }

        if let Some(dst_name) = dst_name.as_ref() {
            temporal_graph
                .logical_to_physical
                .set(dst_name.as_ref(), dst_id)?;
        }

        // 3. Insert layer id into the layer meta of both edge and node.
        let node_meta = temporal_graph.node_meta();

        edge_meta
            .layer_meta()
            .set_id(layer_name.as_deref().unwrap_or("_default"), layer_id);
        node_meta
            .layer_meta()
            .set_id(layer_name.as_deref().unwrap_or("_default"), layer_id);

        // 4. Grab src writer and add edge data.
        let (src_segment_id, src_pos) = resolve_pos(src_id, node_max_page_len);
        let resize_vid = VID::from(src_id.index() + 1);
        self.resize_chunks_to_vid(resize_vid); // Create enough segments.

        let segment = self
            .graph()
            .storage()
            .nodes()
            .get_or_create_segment(src_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut src_writer = self.nodes.get_mut(src_segment_id).unwrap().writer();

            // Increment the node counter for this segment if this is a new node.
            if !src_writer.has_node(src_pos, STATIC_GRAPH_LAYER_ID) {
                src_writer.increment_seg_num_nodes();
            }

            if let Some(src_name) = src_name {
                src_writer.store_node_id(src_pos, STATIC_GRAPH_LAYER_ID, src_name);
            }

            let is_new_edge_static = src_writer
                .get_out_edge(src_pos, dst_id, STATIC_GRAPH_LAYER_ID)
                .is_none();
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
        let resize_vid = VID::from(dst_id.index() + 1);
        self.resize_chunks_to_vid(resize_vid);

        let segment = self
            .graph()
            .storage()
            .nodes()
            .get_or_create_segment(dst_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut dst_writer = self.nodes.get_mut(dst_segment_id).unwrap().writer();

            // Increment the node counter for this segment if this is a new node.
            if !dst_writer.has_node(dst_pos, STATIC_GRAPH_LAYER_ID) {
                dst_writer.increment_seg_num_nodes();
            }

            if let Some(dst_name) = dst_name {
                dst_writer.store_node_id(dst_pos, STATIC_GRAPH_LAYER_ID, dst_name);
            }

            let is_new_edge_static = dst_writer
                .get_inb_edge(dst_pos, src_id, STATIC_GRAPH_LAYER_ID)
                .is_none();
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
        let resize_eid = EID::from(eid.index() + 1);
        self.resize_chunks_to_eid(resize_eid);

        let segment = self
            .graph()
            .storage()
            .edges()
            .get_or_create_segment(edge_segment_id);
        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let mut edge_writer = self.edges.get_mut(edge_segment_id).unwrap().writer();

            let is_new_edge_static = edge_writer
                .get_edge(STATIC_GRAPH_LAYER_ID, edge_pos)
                .is_none();

            // Add edge into the static graph if it doesn't already exist.
            if is_new_edge_static {
                let already_counted = false;
                edge_writer.add_static_edge(Some(edge_pos), src_id, dst_id, already_counted);
            }

            // Add edge into the specified layer with timestamp and props.
            edge_writer.add_edge(
                t,
                edge_pos,
                src_id,
                dst_id,
                props
                    .into_iter()
                    .map(|(_, prop_id, prop_value)| (prop_id, prop_value)),
                layer_id,
            );

            edge_writer.writer.set_lsn(lsn);
        }

        Ok(())
    }
}
