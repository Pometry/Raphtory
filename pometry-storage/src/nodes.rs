use super::{
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        list_array::ChunkedListArray,
    },
    graph_builder::inbound_adj_builder,
    timestamps::TimeStamps,
    RAError,
};
use crate::{
    arrow2::array::PrimitiveArray,
    chunked_array::chunked_offsets::ChunkedOffsets,
    prelude::{ArrayOps, BaseArrayOps},
};
use raphtory_api::core::entities::{EID, VID};
use rayon::prelude::*;
use std::{fmt::Debug, path::Path};
use tracing::instrument;

#[derive(Debug, Clone, Default)]
pub struct Nodes<A = Adj> {
    pub(crate) adj_out: A,
    pub(crate) adj_in: Adj,
    pub(crate) additions: ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>>,
}

#[derive(Debug, Clone, Default)]
pub struct Adj {
    pub(crate) neighbours: ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    pub(crate) edges: ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
}

#[derive(Debug, Clone, Default)]
pub struct AdjTotal {
    pub(crate) adj_neighbours:
        ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>>,
}

impl Nodes<AdjTotal> {
    pub fn new_out_only_total(
        offsets: ChunkedOffsets,
        dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    ) -> Self {
        let adj_out = ChunkedListArray::new_from_parts(dst_ids, offsets);
        let adj_out = AdjTotal {
            adj_neighbours: adj_out,
        };

        Self {
            adj_out,
            adj_in: Default::default(),
            additions: Default::default(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = VID> + '_ {
        (0..self.adj_out.adj_neighbours.len()).into_iter().map(VID)
    }

    pub fn out_neighbours_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> + '_ {
        self.adj_out
            .adj_neighbours
            .value(node_id.0)
            .into_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn find_edge(&self, src_id: VID, dst_id: VID) -> Option<EID> {
        let VID(src_id) = src_id;
        let VID(dst_id) = dst_id;
        let needle = dst_id as u64;
        let start = self.adj_out.adj_neighbours.offsets().start(src_id);
        let value = self.adj_out.adj_neighbours.value(src_id);
        value
            .binary_search_by(|v| v.cmp(&needle))
            .map(|index| EID(start + index))
            .ok()
    }
}

impl Nodes {
    #[instrument(level = "debug", skip(offsets, dst_ids, edge_ids))]
    pub fn new(
        graph_dir: impl AsRef<Path> + Debug,
        offsets: ChunkedOffsets,
        dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        edge_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    ) -> Result<Self, RAError> {
        let mut nodes = Self::new_only_outbound(offsets, dst_ids, edge_ids)?;

        let (adj_in_neighbours, adj_in_edges) = inbound_adj_builder::build_inbound_adj_index(
            graph_dir,
            &nodes.adj_out.neighbours,
            &nodes.adj_out.edges,
        )?;

        nodes.adj_in = Adj {
            edges: adj_in_edges,
            neighbours: adj_in_neighbours,
        };
        Ok(nodes)
    }

    pub fn new_only_outbound(
        offsets: ChunkedOffsets,
        dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        edge_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    ) -> Result<Self, RAError> {
        let adj_out = ChunkedListArray::new_from_parts(dst_ids, offsets.clone());
        let edge_ids = ChunkedListArray::new_from_parts(edge_ids, offsets);

        Ok(Self {
            adj_out: Adj {
                neighbours: adj_out,
                edges: edge_ids,
            },
            ..Default::default()
        })
    }

    pub fn new_with_inbound(
        adj_out_offsets: ChunkedOffsets,
        adj_out_dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        adj_out_edge_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        adj_in_offsets: ChunkedOffsets,
        adj_in_src_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
        adj_in_edge_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    ) -> Self {
        Self {
            adj_out: Adj {
                neighbours: ChunkedListArray::new_from_parts(
                    adj_out_dst_ids,
                    adj_out_offsets.clone(),
                ),
                edges: ChunkedListArray::new_from_parts(adj_out_edge_ids, adj_out_offsets),
            },
            adj_in: Adj {
                neighbours: ChunkedListArray::new_from_parts(
                    adj_in_src_ids,
                    adj_in_offsets.clone(),
                ),
                edges: ChunkedListArray::new_from_parts(adj_in_edge_ids, adj_in_offsets),
            },
            additions: ChunkedListArray::new_from_parts(
                ChunkedArray::empty(),
                ChunkedOffsets::default(),
            ),
        }
    }
}

impl Nodes {
    pub fn additions(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<i64>, NonNull>> {
        &self.additions
    }

    pub fn node(&self, vid: VID) -> Node {
        Node::new(vid, self)
    }

    pub fn len(&self) -> usize {
        self.adj_out.neighbours.len()
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = Node> + '_ {
        (0..self.len())
            .into_par_iter()
            .map(VID)
            .map(|v_id| self.node(v_id))
    }

    pub fn iter(&self) -> impl Iterator<Item = Node> + '_ {
        (0..self.len()).map(VID).map(|v_id| self.node(v_id))
    }

    pub fn out_adj_list(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = (EID, VID)> + ExactSizeIterator<Item = (EID, VID)> + '_
    {
        self.out_edges_iter(node_id)
            .zip(self.out_neighbours_iter(node_id))
    }

    pub fn out_adj_list_from(
        &self,
        node_id: VID,
        offset: usize,
    ) -> impl DoubleEndedIterator<Item = (EID, VID)> + ExactSizeIterator<Item = (EID, VID)> + '_
    {
        self.out_edges_iter_from(node_id, offset)
            .zip(self.out_neighbours_iter_from(node_id, offset))
    }

    pub fn in_adj_list(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = (EID, VID)> + ExactSizeIterator<Item = (EID, VID)> + '_
    {
        self.in_edges_iter(node_id)
            .zip(self.in_neighbours_iter(node_id))
    }

    pub fn out_edges_par(&self, node_id: VID) -> impl IndexedParallelIterator<Item = EID> {
        let (e_start, e_end) = self.outbound_neighbours().offsets().start_end(node_id.0);
        (e_start..e_end).into_par_iter().map(|e_id| e_id.into())
    }

    pub fn out_edges_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = EID> + ExactSizeIterator<Item = EID> + '_ {
        let outb = self.outbound_edges().value(node_id.0);
        outb.into_iter().map(|e_id| EID(e_id as usize))
    }

    pub fn out_edges_iter_from(
        &self,
        node_id: VID,
        offset: usize,
    ) -> impl DoubleEndedIterator<Item = EID> + ExactSizeIterator<Item = EID> + '_ {
        let (e_start, e_end) = self.outbound_neighbours().offsets().start_end(node_id.0);
        (e_start + offset..e_end).map(|e_id| e_id.into())
    }

    pub fn out_neighbours_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> + '_ {
        self.adj_out
            .neighbours
            .value(node_id.0)
            .into_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn out_neighbours_iter_from(
        &self,
        node_id: VID,
        offset: usize,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> + '_ {
        self.adj_out
            .neighbours
            .value(node_id.0)
            .sliced(offset..)
            .into_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn out_neighbours_par(
        &self,
        node_id: VID,
    ) -> impl IndexedParallelIterator<Item = VID> + '_ {
        self.adj_out
            .neighbours
            .value(node_id.0)
            .into_par_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn in_neighbours_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> + '_ {
        self.adj_in
            .neighbours
            .value(node_id.0)
            .into_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn into_in_neighbours_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> {
        self.adj_in
            .neighbours
            .clone()
            .into_value(node_id.0)
            .map(|v_id| VID(v_id as usize))
    }

    pub fn into_out_neighbours_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator<Item = VID> {
        self.adj_out
            .neighbours
            .clone()
            .into_value(node_id.0)
            .map(|v_id| VID(v_id as usize))
    }

    pub fn in_neighbours_par(&self, node_id: VID) -> impl IndexedParallelIterator<Item = VID> + '_ {
        self.adj_in
            .neighbours
            .value(node_id.0)
            .into_par_iter()
            .map(|v_id| VID(v_id as usize))
    }

    pub fn in_edges_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = EID> + ExactSizeIterator<Item = EID> + '_ {
        let inb = self.inbound_edges().value(node_id.0);
        inb.into_iter().map(|e_id| EID(e_id as usize))
    }

    pub fn into_in_edges_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = EID> + ExactSizeIterator<Item = EID> {
        self.adj_in
            .edges
            .clone()
            .into_value(node_id.0)
            .map(|e_id| EID(e_id as usize))
    }

    pub fn into_out_edges_iter(
        &self,
        node_id: VID,
    ) -> impl DoubleEndedIterator<Item = EID> + ExactSizeIterator<Item = EID> {
        self.adj_out
            .edges
            .clone()
            .into_value(node_id.0)
            .map(|e_id| EID(e_id as usize))
    }

    pub fn in_edges_par(&self, node_id: VID) -> impl IndexedParallelIterator<Item = EID> + '_ {
        let inb = self.inbound_edges().value(node_id.0);
        inb.into_par_iter().map(|e_id| EID(e_id as usize))
    }

    pub fn find_edge(&self, src_id: VID, dst_id: VID) -> Option<EID> {
        let VID(src_id) = src_id;
        let VID(dst_id) = dst_id;
        let needle = dst_id as u64;
        let start = self.outbound_neighbours().offsets().start(src_id);
        self.outbound_neighbours()
            .value(src_id)
            .binary_search_by(|v| v.cmp(&needle))
            .map(|index| {
                let eid_pos = start + index;
                EID(self.adj_out.edges.values().get(eid_pos) as usize)
            })
            .ok()
    }

    pub fn in_degree(&self, node_id: VID) -> usize {
        let VID(node_id) = node_id;
        let (start, end) = self.inbound_neighbours().offsets().start_end(node_id);
        end - start
    }

    pub fn out_degree(&self, node_id: VID) -> usize {
        let VID(node_id) = node_id;
        let (start, end) = self.outbound_neighbours().offsets().start_end(node_id);
        end - start
    }

    pub fn outbound_neighbours(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>> {
        &self.adj_out.neighbours
    }

    pub fn inbound_neighbours(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>> {
        &self.adj_in.neighbours
    }

    pub fn outbound_edges(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>> {
        &self.adj_out.edges
    }

    pub fn inbound_edges(
        &self,
    ) -> &ChunkedListArray<'static, ChunkedArray<PrimitiveArray<u64>, NonNull>> {
        &self.adj_in.edges
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Node<'a> {
    vid: VID,
    nodes: &'a Nodes,
}

impl<'a> Node<'a> {
    pub(crate) fn new(vid: VID, nodes: &'a Nodes) -> Node<'a> {
        Self { vid: vid, nodes }
    }

    pub fn vid(&self) -> VID {
        self.vid
    }

    pub fn out_degree(&self) -> usize {
        self.nodes.out_degree(self.vid)
    }

    pub fn in_degree(&self) -> usize {
        self.nodes.in_degree(self.vid)
    }
    pub fn timestamps(self) -> TimeStamps<'a, i64> {
        let ts = self.nodes.additions.value(self.vid.into());

        TimeStamps::new(ts, None)
    }

    pub fn earliest(&self) -> i64 {
        self.nodes.additions.value(self.vid.into()).get(0)
    }

    pub fn latest(&self) -> i64 {
        let ts = self.nodes.additions.value(self.vid.into());
        ts.get(ts.len() - 1)
    }

    pub fn out_neighbours_par(self) -> impl IndexedParallelIterator<Item = VID> + 'a {
        self.nodes.out_neighbours_par(self.vid)
    }

    pub fn out_edges_par(self) -> impl IndexedParallelIterator<Item = EID> + 'a {
        self.nodes.out_edges_par(self.vid)
    }

    pub fn out_neighbours(self) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator + 'a {
        self.nodes.out_neighbours_iter(self.vid)
    }

    pub fn in_neighbours(self) -> impl DoubleEndedIterator<Item = VID> + ExactSizeIterator + 'a {
        self.nodes.in_neighbours_iter(self.vid)
    }
}
