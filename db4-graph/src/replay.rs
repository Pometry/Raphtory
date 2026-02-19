//! Implements WAL replay for a `WriteLockedGraph`.
//! Allows for fast replay by making use of one-time lock acquisition for
//! all the segments in the graph.

use crate::WriteLockedGraph;
use raphtory_api::core::{
    entities::{
        properties::{
            meta::STATIC_GRAPH_LAYER_ID,
            prop::{unify_types, Prop},
        },
        EID, GID, VID,
    },
    storage::timeindex::EventTime,
};
use raphtory_api::core::entities::LayerId;
use storage::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    error::StorageError,
    persist::strategy::PersistenceStrategy,
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
        _transaction_id: TransactionID,
        t: EventTime,
        src_name: Option<GID>,
        src_id: VID,
        dst_name: Option<GID>,
        dst_id: VID,
        eid: EID,
        layer_name: Option<String>,
        layer_id: LayerId,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError> {
        // Insert node ids into resolver.
        if let Some(src_name) = src_name.as_ref() {
            self.graph()
                .logical_to_physical
                .set(src_name.as_ref(), src_id)?;
        }

        if let Some(dst_name) = dst_name.as_ref() {
            self.graph()
                .logical_to_physical
                .set(dst_name.as_ref(), dst_id)?;
        }

        // Grab src writer and add edge data.
        let (src_segment_id, src_pos) = self.graph().storage().nodes().resolve_pos(src_id);
        self.resize_segments_to_vid(src_id); // Create enough segments.

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

        // Grab dst writer and add edge data.
        let (dst_segment_id, dst_pos) = self.graph().storage().nodes().resolve_pos(dst_id);
        self.resize_segments_to_vid(dst_id);

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

        // Grab edge writer and add temporal props & metadata.
        let (edge_segment_id, edge_pos) = self.graph().storage().edges().resolve_pos(eid);
        self.resize_segments_to_eid(eid);

        let segment = self
            .graph()
            .storage()
            .edges()
            .get_or_create_segment(edge_segment_id);

        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let edge_meta = self.graph().edge_meta();

            // Insert prop ids into edge meta.
            for (prop_name, prop_id, prop_value) in &props {
                let prop_mapper = edge_meta.temporal_prop_mapper();

                match prop_mapper.get_dtype(*prop_id) {
                    None => {
                        prop_mapper.set_id_and_dtype(
                            prop_name.as_str(),
                            *prop_id,
                            prop_value.dtype(),
                        );
                    }
                    Some(old_dtype) => {
                        let dtype = prop_value.dtype();
                        let mut unified = false;
                        let new_dtype = unify_types(&old_dtype, &dtype, &mut unified)?;
                        if unified {
                            prop_mapper.set_dtype(*prop_id, new_dtype);
                        }
                    }
                }
            }

            // Insert layer id into the layer meta of both edge and node.
            let node_meta = self.graph().node_meta();

            edge_meta
                .layer_meta()
                .set_id(layer_name.as_deref().unwrap_or("_default"), layer_id.0);
            node_meta
                .layer_meta()
                .set_id(layer_name.as_deref().unwrap_or("_default"), layer_id.0);

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

    fn replay_add_node(
        &mut self,
        lsn: LSN,
        _transaction_id: TransactionID,
        t: EventTime,
        node_name: Option<GID>,
        node_id: VID,
        node_type_and_id: Option<(String, usize)>,
        props: Vec<(String, usize, Prop)>,
    ) -> Result<(), StorageError> {
        // Insert node id into resolver.
        if let Some(ref name) = node_name {
            self.graph()
                .logical_to_physical
                .set(name.as_ref(), node_id)?;
        }

        // Resolve segment and check LSN.
        let (segment_id, pos) = self.graph().storage().nodes().resolve_pos(node_id);
        self.resize_segments_to_vid(node_id);

        let segment = self
            .graph()
            .storage()
            .nodes()
            .get_or_create_segment(segment_id);

        let immut_lsn = segment.immut_lsn();

        // Replay this entry only if it doesn't exist in immut.
        if immut_lsn < lsn {
            let node_meta = self.graph().node_meta();

            for (prop_name, prop_id, prop_value) in &props {
                let prop_mapper = node_meta.temporal_prop_mapper();
                match prop_mapper.get_dtype(*prop_id) {
                    None => {
                        prop_mapper.set_id_and_dtype(
                            prop_name.as_str(),
                            *prop_id,
                            prop_value.dtype(),
                        );
                    }
                    Some(old_dtype) => {
                        let dtype = prop_value.dtype();
                        let mut unified = false;
                        let new_dtype = unify_types(&old_dtype, &dtype, &mut unified)?;
                        if unified {
                            prop_mapper.set_dtype(*prop_id, new_dtype);
                        }
                    }
                }
            }

            // Set node type metadata early to prevent issues with borrowing node_writer.
            if let Some((ref node_type, node_type_id)) = node_type_and_id {
                node_meta
                    .node_type_meta()
                    .set_id(node_type.as_str(), node_type_id);
            }

            let mut node_writer = self.nodes.get_mut(segment_id).unwrap().writer();

            if !node_writer.has_node(pos, STATIC_GRAPH_LAYER_ID) {
                node_writer.increment_seg_num_nodes();
            }

            if let Some(name) = node_name {
                node_writer.store_node_id(pos, STATIC_GRAPH_LAYER_ID, name);
            }

            if let Some((_, node_type_id)) = node_type_and_id {
                node_writer.store_node_type(pos, STATIC_GRAPH_LAYER_ID, node_type_id);
            }

            // Add the node with its timestamp and props.
            node_writer.add_props(
                t,
                pos,
                STATIC_GRAPH_LAYER_ID,
                props
                    .into_iter()
                    .map(|(_, prop_id, prop_value)| (prop_id, prop_value)),
            );

            node_writer.mut_segment.set_lsn(lsn);
        }

        Ok(())
    }
}
