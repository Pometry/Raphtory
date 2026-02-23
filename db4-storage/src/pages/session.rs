use super::{
    GraphStore, edge_page::writer::EdgeWriter, node_page::writer::NodeWriters, resolve_pos,
};
use crate::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    persist::strategy::PersistenceStrategy,
    segments::{edge::segment::MemEdgeSegment, node::segment::MemNodeSegment},
    wal::LSN,
};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::properties::{
        meta::{NODE_ID_IDX, STATIC_GRAPH_LAYER_ID},
        prop::Prop,
    },
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{EID, ELID, GidRef, VID},
    storage::timeindex::AsTime,
};

pub struct EdgeWriteSession<
    'a,
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> {
    node_writers: NodeWriters<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS>,
    edge_writer: EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES>,
    graph: &'a GraphStore<NS, ES, GS, EXT>,
}

impl<
    'a,
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    GS: GraphPropSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS, ES = ES, GS = GS>,
> EdgeWriteSession<'a, NS, ES, GS, EXT>
{
    pub fn new(
        node_writers: NodeWriters<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS>,
        edge_writer: EdgeWriter<'a, RwLockWriteGuard<'a, MemEdgeSegment>, ES>,
        graph: &'a GraphStore<NS, ES, GS, EXT>,
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
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        // assert!(layer > 0, "Edge must be in a layer greater than 0");

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        let edge_max_page_len = self
            .edge_writer
            .writer
            .get_or_create_layer(layer)
            .max_page_len();
        let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);

        self.edge_writer
            .add_edge(t, edge_pos, src, dst, props, layer);

        let edge_id = edge.inner();

        if edge.is_new()
            || self
                .node_writers
                .get_mut_src()
                .get_out_edge(src_pos, dst, edge_id.layer())
                .is_none()
        {
            self.node_writers
                .get_mut_src()
                .add_outbound_edge(Some(t), src_pos, dst, edge_id);
            self.node_writers
                .get_mut_dst()
                .add_inbound_edge(Some(t), dst_pos, src, edge_id);
        }

        self.node_writers
            .get_mut_src()
            .update_timestamp(t, src_pos, e_id);
        self.node_writers
            .get_mut_dst()
            .update_timestamp(t, dst_pos, e_id);
    }

    pub fn delete_edge_from_layer<T: AsTime>(
        &mut self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        edge: MaybeNew<ELID>,
    ) {
        let src = src.into();
        let dst = dst.into();
        let e_id = edge.inner();
        let layer = e_id.layer();

        // assert!(layer > 0, "Edge must be in a layer greater than 0");

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        let edge_max_page_len = self
            .edge_writer
            .writer
            .get_or_create_layer(layer)
            .max_page_len();
        let (_, edge_pos) = resolve_pos(e_id.edge, edge_max_page_len);

        self.edge_writer.delete_edge(t, edge_pos, src, dst, layer);

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
                    .add_outbound_edge(Some(t), src_pos, dst, edge_id);
                self.node_writers
                    .get_mut_dst()
                    .add_inbound_edge(Some(t), dst_pos, src, edge_id);
            }

            self.node_writers
                .get_mut_src()
                .update_deletion_time(t, src_pos, e_id);
            self.node_writers
                .get_mut_dst()
                .update_deletion_time(t, dst_pos, e_id);
        }
    }

    pub fn add_static_edge(&mut self, src: impl Into<VID>, dst: impl Into<VID>) -> MaybeNew<EID> {
        let src = src.into();
        let dst = dst.into();

        let (_, src_pos) = self.graph.nodes().resolve_pos(src);
        let (_, dst_pos) = self.graph.nodes().resolve_pos(dst);

        let existing_eid =
            self.node_writers
                .get_mut_src()
                .get_out_edge(src_pos, dst, STATIC_GRAPH_LAYER_ID);

        // Edge already exists, so no need to add it again.
        if let Some(eid) = existing_eid {
            return MaybeNew::Existing(eid);
        }

        let edge_pos = None;
        let already_counted = false;
        let edge_pos = self
            .edge_writer
            .add_static_edge(edge_pos, src, dst, already_counted);
        let edge_id = edge_pos.as_eid(
            self.edge_writer.segment_id(),
            self.graph.edges().max_page_len(),
        );

        self.node_writers
            .get_mut_src()
            .add_static_outbound_edge(src_pos, dst, edge_id);
        self.node_writers
            .get_mut_dst()
            .add_static_inbound_edge(dst_pos, src, edge_id);

        MaybeNew::New(edge_id)
    }

    pub fn node_writers(
        &mut self,
    ) -> &mut NodeWriters<'a, RwLockWriteGuard<'a, MemNodeSegment>, NS> {
        &mut self.node_writers
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.node_writers.set_lsn(lsn);
        self.edge_writer.set_lsn(lsn);
    }
}
