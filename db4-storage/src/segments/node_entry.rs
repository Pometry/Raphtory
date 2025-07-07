use crate::{
    LocalPOS, NodeEdgeAdditions, NodePropAdditions, NodeTProps,
    api::nodes::{NodeEntryOps, NodeRefOps},
    gen_t_props::WithTProps,
    gen_ts::{EdgeAdditionCellsRef, LayerIter, PropAdditionCellsRef, WithTimeCells},
    segments::node::MemNodeSegment,
};
use raphtory_api::core::{
    Direction,
    entities::{
        EID, VID,
        properties::{meta::Meta, prop::Prop},
    },
};
use raphtory_core::{
    entities::{LayerIds, edges::edge_ref::EdgeRef, properties::tprop::TPropCell},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use std::{ops::Deref, sync::Arc};

use super::additions::MemAdditions;

pub struct MemNodeEntry<'a, MNS> {
    pos: LocalPOS,
    ns: MNS,
    __marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, MNS: Deref<Target = MemNodeSegment>> MemNodeEntry<'a, MNS> {
    pub fn new(pos: LocalPOS, ns: MNS) -> Self {
        Self {
            pos,
            ns,
            __marker: std::marker::PhantomData,
        }
    }
}

impl<'a, MNS: Deref<Target = MemNodeSegment> + Send + Sync + 'a> NodeEntryOps<'a>
    for MemNodeEntry<'a, MNS>
{
    type Ref<'b>
        = MemNodeRef<'b>
    where
        'a: 'b,
        MNS: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b,
    {
        MemNodeRef {
            pos: self.pos,
            ns: self.ns.deref(),
        }
    }
}
#[derive(Copy, Clone, Debug)]
pub struct MemNodeRef<'a> {
    pos: LocalPOS,
    ns: &'a MemNodeSegment,
}

impl<'a> MemNodeRef<'a> {
    pub fn new(pos: LocalPOS, ns: &'a MemNodeSegment) -> Self {
        Self { pos, ns }
    }
}

impl<'a> WithTimeCells<'a> for MemNodeRef<'a> {
    type TimeCell = MemAdditions<'a>;

    fn t_props_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        let t_cell = MemAdditions::Props(self.ns.as_ref()[layer_id].times_from_props(self.pos));
        std::iter::once(
            range
                .map(|(start, end)| t_cell.range(start..end))
                .unwrap_or_else(|| t_cell),
        )
    }

    fn additions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        let additions = MemAdditions::Edges(self.ns.as_ref()[layer_id].additions(self.pos));
        std::iter::once(
            range
                .map(|(start, end)| additions.range(start..end))
                .unwrap_or_else(|| additions),
        )
    }

    fn deletions_tc(
        self,
        layer_id: usize,
        range: Option<(TimeIndexEntry, TimeIndexEntry)>,
    ) -> impl Iterator<Item = Self::TimeCell> + 'a {
        let deletions = MemAdditions::Edges(self.ns.as_ref()[layer_id].deletions(self.pos));
        std::iter::once(
            range
                .map(|(start, end)| deletions.range(start..end))
                .unwrap_or_else(|| deletions),
        )
    }

    fn num_layers(&self) -> usize {
        self.ns.as_ref().len()
    }
}

impl<'a> WithTProps<'a> for MemNodeRef<'a> {
    type TProp = TPropCell<'a>;

    fn num_layers(&self) -> usize {
        self.ns.as_ref().len()
    }

    fn into_t_props(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + 'a {
        let node_pos = self.pos;
        self.ns.as_ref()[layer_id]
            .t_prop(node_pos, prop_id)
            .into_iter()
            .map(|t_prop| t_prop.into())
    }
}

impl<'a> NodeRefOps<'a> for MemNodeRef<'a> {
    type Additions = NodePropAdditions<'a>;
    type EdgeAdditions = NodeEdgeAdditions<'a>;
    type TProps = NodeTProps<'a>;

    fn node_meta(&self) -> &Arc<Meta> {
        self.ns.node_meta()
    }

    fn vid(&self) -> VID {
        self.ns.to_vid(self.pos)
    }

    fn out_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos, layer_id)
    }

    fn inb_edges(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos, layer_id)
    }

    fn out_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.out_edges(self.pos, layer_id)
    }

    fn inb_edges_sorted(self, layer_id: usize) -> impl Iterator<Item = (VID, EID)> + 'a {
        self.ns.inb_edges(self.pos, layer_id)
    }

    fn c_prop(self, layer_id: usize, prop_id: usize) -> Option<Prop> {
        self.ns.as_ref()[layer_id].c_prop(self.pos, prop_id)
    }

    fn c_prop_str(self, layer_id: usize, prop_id: usize) -> Option<&'a str> {
        self.ns.as_ref()[layer_id].c_prop_str(self.pos, prop_id)
    }

    fn node_additions<L: Into<LayerIter<'a>>>(self, layer_id: L) -> Self::Additions {
        NodePropAdditions::new_with_layer(PropAdditionCellsRef::new(self), layer_id)
    }

    fn edge_additions<L: Into<LayerIter<'a>>>(self, layer_id: L) -> Self::EdgeAdditions {
        NodeEdgeAdditions::new_additions_with_layer(EdgeAdditionCellsRef::new(self), layer_id)
    }

    fn degree(self, layers: &LayerIds, dir: Direction) -> usize {
        match layers {
            LayerIds::One(layer_id) => self.ns.degree(self.pos, *layer_id, dir),
            LayerIds::All => self.ns.degree(self.pos, 0, dir),
            LayerIds::None => 0,
            layers => self.edges_iter(layers, dir).count(),
        }
    }

    fn find_edge(&self, dst: VID, layers: &LayerIds) -> Option<EdgeRef> {
        let eid = match layers {
            LayerIds::One(layer_id) => self.ns.get_out_edge(self.pos, dst, *layer_id),
            LayerIds::All => self.ns.get_out_edge(self.pos, dst, 0),
            LayerIds::Multiple(layers) => layers
                .iter()
                .find_map(|layer_id| self.ns.get_out_edge(self.pos, dst, layer_id)),
            LayerIds::None => None,
        };

        let src_id = self.ns.to_vid(self.pos);
        eid.map(|eid| EdgeRef::new_outgoing(eid, src_id, dst))
    }

    fn temporal_prop_layer(self, layer_id: usize, prop_id: usize) -> Self::TProps {
        NodeTProps::new_with_layer(self, layer_id, prop_id)
    }

    fn internal_num_layers(&self) -> usize {
        self.ns.as_ref().len()
    }

    fn has_layer_inner(self, layer_id: usize) -> bool {
        self.ns
            .as_ref()
            .get(layer_id)
            .and_then(|seg| seg.items().get(self.pos.0))
            .map_or(false, |x| *x)
    }
}
