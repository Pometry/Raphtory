use std::ops::DerefMut;

use super::{
    GraphStore, edge_page::writer::EdgeWriter, node_page::writer::WriterPair, resolve_pos,
};
use crate::{
    LocalPOS,
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    error::DBV4Error,
    pages::{NODE_ID_PROP_KEY, NODE_TYPE_PROP_KEY},
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{EID, ELID, GidRef, VID},
    storage::timeindex::AsTime,
};

pub struct WriteSession<
    'a,
    MNS: DerefMut<Target = MemNodeSegment> + 'a,
    MES: DerefMut<Target = MemEdgeSegment> + 'a,
    NS: NodeSegmentOps,
    ES: EdgeSegmentOps,
    EXT,
> {
    node_writers: WriterPair<'a, MNS, NS>,
    edge_writer: Option<EdgeWriter<'a, MES, ES>>,
    graph: &'a GraphStore<NS, ES, EXT>,
}

impl<
    'a,
    MNS: DerefMut<Target = MemNodeSegment> + 'a,
    MES: DerefMut<Target = MemEdgeSegment> + 'a,
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    EXT: Clone + Default + Send + Sync,
> WriteSession<'a, MNS, MES, NS, ES, EXT>
{
    pub fn new(
        node_writers: WriterPair<'a, MNS, NS>,
        edge_writer: Option<EdgeWriter<'a, MES, ES>>,
        graph: &'a GraphStore<NS, ES, EXT>,
    ) -> Self {
        Self {
            node_writers,
            edge_writer,
            graph,
        }
    }

    pub fn add_edge_into_layer<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        edge: MaybeNew<ELID>,
        lsn: u64,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        // assert!(layer > 0, "Edge must be in a layer greater than 0");

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        if let Some(writer) = self.edge_writer.as_mut() {
            let edge_max_page_len = writer.writer.get_or_create_layer(layer).max_page_len();
            let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);
            let exists = Some(!edge.is_new());

            writer.add_edge(t, Some(edge_pos), src, dst, props, layer, lsn, exists);
        } else {
            let mut writer = self.graph.edge_writer(e_id.edge);
            let edge_max_page_len = writer.writer.get_or_create_layer(layer).max_page_len();
            let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);
            let exists = Some(!edge.is_new());

            writer.add_edge(t, Some(edge_pos), src, dst, props, layer, lsn, exists);
        }

        let edge_id = edge.inner();

        if edge_id.layer() > 0 {
            if edge.is_new()
                || self
                    .node_writers
                    .get_mut_src()
                    .get_out_edge(src_pos, dst, edge_id.layer())
                    .is_none()
            {
                self.node_writers
                    .get_mut_src()
                    .add_outbound_edge(t, src_pos, dst, edge_id, lsn);
                self.node_writers
                    .get_mut_dst()
                    .add_inbound_edge(t, dst_pos, src, edge_id, lsn);
            }

            self.node_writers
                .get_mut_src()
                .update_timestamp(t, src_pos, e_id, lsn);
            self.node_writers
                .get_mut_dst()
                .update_timestamp(t, dst_pos, e_id, lsn);
        }
    }

    pub fn delete_edge_from_layer<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        edge: MaybeNew<ELID>,
        lsn: u64,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        // assert!(layer > 0, "Edge must be in a layer greater than 0");

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        if let Some(writer) = self.edge_writer.as_mut() {
            let edge_max_page_len = writer.writer.get_or_create_layer(layer).max_page_len();
            let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);
            let exists = Some(!edge.is_new());

            writer.delete_edge(t, Some(edge_pos), src, dst, layer, lsn, exists);
        } else {
            let mut writer = self.graph.edge_writer(e_id.edge);
            let edge_max_page_len = writer.writer.get_or_create_layer(layer).max_page_len();
            let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);
            let exists = Some(!edge.is_new());

            writer.delete_edge(t, Some(edge_pos), src, dst, layer, lsn, exists);
        }

        let edge_id = edge.inner();

        if edge_id.layer() > 0 {
            if edge.is_new()
                || self
                    .node_writers
                    .get_mut_src()
                    .get_out_edge(src_pos, dst, edge_id.layer())
                    .is_none()
            {
                self.node_writers
                    .get_mut_src()
                    .add_outbound_edge(t, src_pos, dst, edge_id, lsn);
                self.node_writers
                    .get_mut_dst()
                    .add_inbound_edge(t, dst_pos, src, edge_id, lsn);
            }

            self.node_writers
                .get_mut_src()
                .update_deletion_time(t, src_pos, e_id, lsn);
            self.node_writers
                .get_mut_dst()
                .update_deletion_time(t, dst_pos, e_id, lsn);
        }
    }

    pub fn add_static_edge(
        &mut self,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
    ) -> MaybeNew<EID> {
        let src = src.into();
        let dst = dst.into();
        let layer_id = 0; // static graph goes to layer 0

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        if let Some(e_id) = self
            .node_writers
            .get_mut_src()
            .get_out_edge(src_pos, dst, layer_id)
        {
            let mut edge_writer = self.graph.edge_writer(e_id);
            let (_, edge_pos) = self.graph.edges().resolve_pos(e_id);

            edge_writer.add_static_edge(Some(edge_pos), src, dst, layer_id, lsn, None);

            MaybeNew::Existing(e_id)
        } else {
            let mut edge_writer = self.graph.get_free_writer();
            let edge_id = edge_writer.add_static_edge(None, src, dst, layer_id, lsn, None);
            let edge_id =
                edge_id.as_eid(edge_writer.segment_id(), self.graph.edges().max_page_len());

            self.node_writers.get_mut_src().add_static_outbound_edge(
                src_pos,
                dst,
                edge_id.with_layer(layer_id),
                lsn,
            );
            self.node_writers.get_mut_dst().add_static_inbound_edge(
                dst_pos,
                src,
                edge_id.with_layer(layer_id),
                lsn,
            );

            MaybeNew::New(edge_id)
        }
    }

    pub fn internal_add_edge<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        let src = src.into();
        let dst = dst.into();

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        if let Some(e_id) = self
            .node_writers
            .get_mut_src()
            .get_out_edge(src_pos, dst, layer)
        {
            let mut edge_writer = self.graph.edge_writer(e_id);
            let (_, edge_pos) = self.graph.edges().resolve_pos(e_id);
            edge_writer.add_edge(t, Some(edge_pos), src, dst, props, layer, lsn, None);
            let e_id = e_id.with_layer(layer);

            self.node_writers
                .get_mut_src()
                .update_timestamp(t, src_pos, e_id, lsn);
            self.node_writers
                .get_mut_dst()
                .update_timestamp(t, dst_pos, e_id, lsn);

            MaybeNew::Existing(e_id)
        } else {
            if let Some(e_id) = self
                .node_writers
                .get_mut_src()
                .get_out_edge(src_pos, dst, layer)
            {
                let mut edge_writer = self.graph.edge_writer(e_id);
                let (_, edge_pos) = self.graph.edges().resolve_pos(e_id);
                let e_id = e_id.with_layer(layer);

                edge_writer.add_edge(t, Some(edge_pos), src, dst, props, layer, lsn, None);
                self.node_writers
                    .get_mut_src()
                    .update_timestamp(t, src_pos, e_id, lsn);
                self.node_writers
                    .get_mut_dst()
                    .update_timestamp(t, dst_pos, e_id, lsn);

                MaybeNew::Existing(e_id)
            } else {
                let mut edge_writer = self.graph.get_free_writer();
                let edge_id = edge_writer.add_edge(t, None, src, dst, props, layer, lsn, None);
                let edge_id =
                    edge_id.as_eid(edge_writer.segment_id(), self.graph.edges().max_page_len());
                let edge_id = edge_id.with_layer(layer);

                self.node_writers
                    .get_mut_src()
                    .add_outbound_edge(t, src_pos, dst, edge_id, lsn);
                self.node_writers
                    .get_mut_dst()
                    .add_inbound_edge(t, dst_pos, src, edge_id, lsn);

                MaybeNew::New(edge_id)
            }
        }
    }

    pub fn store_node_id_as_prop(
        &mut self,
        id: GidRef,
        vid: impl Into<VID>,
    ) -> Result<(), DBV4Error> {
        let (prop_val, prop_dtype) = match id {
            GidRef::U64(id) => (Prop::U64(id), PropType::U64),
            GidRef::Str(id) => (Prop::Str(id.into()), PropType::Str),
        };

        self.store_node_const_prop(NODE_ID_PROP_KEY, prop_val, prop_dtype, vid)
    }

    pub fn store_node_type_as_prop(
        &mut self,
        node_type: &str,
        vid: impl Into<VID>,
    ) -> Result<(), DBV4Error> {
        let (prop_val, prop_dtype) = (Prop::Str(node_type.into()), PropType::Str);

        self.store_node_const_prop(NODE_TYPE_PROP_KEY, prop_val, prop_dtype, vid)
    }

    fn store_node_const_prop(
        &mut self,
        prop_key: &str,
        prop_val: Prop,
        prop_dtype: PropType,
        vid: impl Into<VID>,
    ) -> Result<(), DBV4Error> {
        let layer = 0;
        let prop_id = self
            .graph
            .node_meta()
            .const_prop_meta()
            .get_and_validate(prop_key, prop_dtype)?
            .ok_or_else(|| {
                DBV4Error::GenericFailure(format!("{} const prop not found", prop_key))
            })?;

        let props = vec![(prop_id, prop_val)];
        let (_, local_pos) = self.graph.nodes().resolve_pos(vid);
        let lsn = 0; // TODO: lsn should be passed in

        self.node_writers
            .get_mut_src()
            .update_c_props(local_pos, layer, props, lsn);

        Ok(())
    }
}
