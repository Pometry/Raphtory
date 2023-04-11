use core::iter::Map;
use itertools::chain;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ops::Range;

use crate::adj::Adj;
use crate::props::Props;
use crate::tadjset::{AdjEdge, TAdjSet};
use crate::tgraph::EdgeRef;
use crate::{Direction, Prop};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EdgeLayer {
    layer_id: usize,
    next_edge_id: usize,

    // Vector of adjacency lists. It is populated lazyly, so avoid using [] accessor for reading
    pub(crate) adj_lists: Vec<Adj>,
    pub(crate) props: Props,
}

impl EdgeLayer {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            layer_id: id,
            // Edge ids refer to the position of properties inside self.props.temporal_props
            // and self.props.static_props. Besides, negative and positive indices are used
            // to denote remote and local edges, respectively. Therefore, index "0" can be used to
            // denote neither local nor remote edges, which simply breaks this symmetry.
            // Hence, the first id to be provided as edge id is 1
            next_edge_id: 1,
            adj_lists: Default::default(),
            props: Default::default(),
        }
    }
}

// INGESTION:
impl EdgeLayer {
    pub(crate) fn add_edge_with_props(
        &mut self,
        t: i64,
        src: u64,
        dst: u64,
        src_pid: usize,
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let required_len = std::cmp::max(src_pid, dst_pid) + 1;
        self.ensure_adj_lists_len(required_len);
        let src_edge_meta_id = self.link_outbound_edge(t, src_pid, dst_pid, false);
        let dst_edge_meta_id = self.link_inbound_edge(t, src_pid, dst_pid, false);

        if src_edge_meta_id != dst_edge_meta_id {
            panic!(
                "Failure on {src} -> {dst} at time: {t} {src_edge_meta_id} != {dst_edge_meta_id}"
            );
        }

        self.props.upsert_temporal_props(t, src_edge_meta_id, props);
        self.next_edge_id += 1; // FIXME: we have this in three different places, prone to errors!
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src: u64, // we are on the source shard
        dst: u64,
        src_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        self.ensure_adj_lists_len(src_pid + 1);
        let src_edge_meta_id = self.link_outbound_edge(t, src_pid, dst.try_into().unwrap(), true);
        self.props.upsert_temporal_props(t, src_edge_meta_id, props);
        self.next_edge_id += 1;
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst: u64, // we are on the destination shard
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        self.ensure_adj_lists_len(dst_pid + 1);
        let dst_edge_meta_id = self.link_inbound_edge(t, src.try_into().unwrap(), dst_pid, true);
        self.props.upsert_temporal_props(t, dst_edge_meta_id, props);
        self.next_edge_id += 1;
    }
}

// INGESTION HELPERS:
impl EdgeLayer {
    #[inline]
    fn ensure_adj_lists_len(&mut self, len: usize) {
        if self.adj_lists.len() < len {
            self.adj_lists.resize_with(len, Default::default);
        }
    }

    fn link_inbound_edge(
        &mut self,
        t: i64,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo => {
                let edge_id = self.next_edge_id;

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_into(src, t, edge);

                edge_id
            }
            Adj::List {
                into, remote_into, ..
            } => {
                let list = if remote_edge { remote_into } else { into };
                let edge_id: usize = list
                    .find(src)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.next_edge_id);

                list.push(t, src, AdjEdge::new(edge_id, !remote_edge)); // idempotent
                edge_id
            }
        }
    }

    fn link_outbound_edge(
        &mut self,
        t: i64,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo => {
                let edge_id = self.next_edge_id;

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_out(dst, t, edge);

                edge_id
            }
            Adj::List {
                out, remote_out, ..
            } => {
                let list = if remote_edge { remote_out } else { out };
                let edge_id: usize = list
                    .find(dst)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.next_edge_id);

                list.push(t, dst, AdjEdge::new(edge_id, !remote_edge));
                edge_id
            }
        }
    }
}

// SINGLE EDGE ACCESS:
impl EdgeLayer {
    // TODO reuse function to return edge
    pub(crate) fn has_local_edge(&self, src_pid: usize, dst_pid: usize) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { out, .. } => out.find(dst_pid).is_some(),
        }
    }

    pub(crate) fn has_local_edge_window(
        &self,
        src_pid: usize,
        dst_pid: usize,
        w: &Range<i64>,
    ) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { out, .. } => out.find_window(dst_pid, w).is_some(),
        }
    }

    pub(crate) fn has_remote_edge(&self, src_pid: usize, dst: u64) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { remote_out, .. } => remote_out.find(dst as usize).is_some(),
        }
    }

    pub(crate) fn has_remote_edge_window(&self, src_pid: usize, dst: u64, w: &Range<i64>) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { remote_out, .. } => remote_out.find_window(dst as usize, w).is_some(),
        }
    }

    // try to merge the next four functions together
    pub(crate) fn local_edge(
        &self,
        src: u64,
        dst: u64,
        src_pid: usize,
        dst_pid: usize,
    ) -> Option<EdgeRef> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => None,
            Adj::List { out, .. } => {
                let e = out.find(dst_pid)?;
                Some(EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst_pid,
                    time: None,
                    is_remote: false,
                })
            }
        }
    }

    pub(crate) fn local_edge_window(
        &self,
        src: u64,
        dst: u64,
        src_pid: usize,
        dst_pid: usize,
        w: &Range<i64>,
    ) -> Option<EdgeRef> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => None,
            Adj::List { out, .. } => {
                let e = out.find_window(dst_pid, w)?;
                Some(EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst_pid,
                    time: None,
                    is_remote: false,
                })
            }
        }
    }

    pub(crate) fn remote_edge(&self, src: u64, dst: u64, src_pid: usize) -> Option<EdgeRef> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => None,
            Adj::List { remote_out, .. } => {
                let e = remote_out.find(dst as usize)?;
                Some(EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst as usize,
                    time: None,
                    is_remote: true,
                })
            }
        }
    }

    pub(crate) fn remote_edge_window(
        &self,
        src: u64,
        dst: u64,
        src_pid: usize,
        w: &Range<i64>,
    ) -> Option<EdgeRef> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => None,
            Adj::List { remote_out, .. } => {
                let e = remote_out.find_window(dst as usize, w)?;
                Some(EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst as usize,
                    time: None,
                    is_remote: true,
                })
            }
        }
    }
}

// AGGREGATED ACCESS:
impl EdgeLayer {
    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists.iter().map(|adj| adj.out_edges_len()).sum()
    }

    pub(crate) fn out_edges_len_window(&self, v_pid: usize, w: &Range<i64>) -> usize {
        self.adj_lists
            .get(v_pid)
            .unwrap_or(&Adj::Solo)
            .out_len_window(w)
    }
}

// MULTIPLE EDGE ACCES:
impl EdgeLayer {
    pub(crate) fn edges_iter<'a>(
        &'a self,
        vertex_id: u64,
        vertex_pid: usize,
        d: Direction,
        global_ids: &'a Vec<u64>,
    ) -> Box<dyn Iterator<Item = (usize, EdgeRef)> + Send + '_> {
        let builder = EdgeRefBuilder::new(self.layer_id, vertex_id, vertex_pid, global_ids);
        match self.adj_lists.get(vertex_pid).unwrap_or(&Adj::Solo) {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => {
                    let iter = chain!(out.iter(), remote_out.iter())
                        .map(move |(dst, e)| (*dst, builder.out_edge(*dst, e)));
                    Box::new(iter)
                }
                Direction::IN => {
                    let iter = chain!(into.iter(), remote_into.iter())
                        .map(move |(dst, e)| (*dst, builder.in_edge(*dst, e)));
                    Box::new(iter)
                }
                Direction::BOTH => {
                    let out_mapper = move |(dst, e): (&usize, AdjEdge)| {
                        (*dst, builder.clone().out_edge(*dst, e))
                    };
                    let in_mapper =
                        move |(dst, e): (&usize, AdjEdge)| (*dst, builder.in_edge(*dst, e));

                    let remote_out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_out.iter().map(out_mapper));
                    let remote_into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_into.iter().map(in_mapper));
                    let remote = vec![remote_out, remote_into]
                        .into_iter()
                        .kmerge_by(|(left, _), (right, _)| left < right);

                    let out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(out.iter().map(out_mapper));
                    let into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(into.iter().map(in_mapper));
                    let local = vec![out, into]
                        .into_iter()
                        .kmerge_by(|(left, _), (right, _)| left < right);

                    Box::new(chain!(local, remote))
                }
            },
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn edges_iter_window<'a>(
        &'a self,
        vertex_id: u64,
        vertex_pid: usize,
        r: &Range<i64>,
        d: Direction,
        global_ids: &'a Vec<u64>,
    ) -> Box<dyn Iterator<Item = (usize, EdgeRef)> + Send + '_> {
        let builder = EdgeRefBuilder::new(self.layer_id, vertex_id, vertex_pid, global_ids);
        match self.adj_lists.get(vertex_pid).unwrap_or(&Adj::Solo) {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => {
                    let iter = chain!(out.iter_window(r), remote_out.iter_window(r))
                        .map(move |(dst, e)| (dst, builder.out_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::IN => {
                    let iter = chain!(into.iter_window(r), remote_into.iter_window(r))
                        .map(move |(dst, e)| (dst, builder.in_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::BOTH => {
                    let out_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.out_edge(dst, e));
                    let in_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.in_edge(dst, e));

                    let remote_out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_out.iter_window(r).map(out_mapper));
                    let remote_into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_into.iter_window(r).map(in_mapper));
                    let remote = vec![remote_out, remote_into]
                        .into_iter()
                        .kmerge_by(|(left, _), (right, _)| left < right);

                    let out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(out.iter_window(r).map(out_mapper));
                    let into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(into.iter_window(r).map(in_mapper));
                    let local = vec![out, into]
                        .into_iter()
                        .kmerge_by(|(left, _), (right, _)| left < right);

                    Box::new(chain!(local, remote))
                }
            },
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn edges_iter_window_t<'a>(
        // TODO: change back to private if appropriate
        &'a self,
        v_id: u64,
        v_pid: usize,
        w: &Range<i64>,
        d: Direction,
        global_ids: &'a Vec<u64>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        let builder = EdgeRefBuilder::new(self.layer_id, v_id, v_pid, global_ids);
        match self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo) {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => Box::new(
                    chain!(out.iter_window_t(w), remote_out.iter_window_t(w))
                        .map(move |(dst, t, e)| builder.out_edge_t(dst, e, t)),
                ),
                Direction::IN => Box::new(
                    chain!(into.iter_window_t(w), remote_into.iter_window_t(w))
                        .map(move |(dst, t, e)| builder.in_edge_t(dst, e, t)),
                ),
                Direction::BOTH => Box::new(chain!(
                    self.edges_iter_window_t(v_id, v_pid, w, Direction::IN, global_ids),
                    self.edges_iter_window_t(v_id, v_pid, w, Direction::OUT, global_ids),
                )),
            },
            _ => Box::new(std::iter::empty()),
        }
    }
}

#[derive(Copy, Clone)]
struct EdgeRefBuilder<'a> {
    layer_id: usize,
    v_ref: u64,
    v_ref_pid: usize,
    logical_ids: &'a Vec<u64>,
}

impl<'a> EdgeRefBuilder<'a> {
    fn new(layer_id: usize, v_ref: u64, v_ref_pid: usize, logical_ids: &'a Vec<u64>) -> Self {
        Self {
            layer_id,
            v_ref,
            v_ref_pid,
            logical_ids,
        }
    }
    fn out_edge(&self, vertex: usize, e: AdjEdge) -> EdgeRef {
        EdgeRef {
            layer_id: self.layer_id,
            edge_id: e.edge_id(),
            src_g_id: self.v_ref,
            dst_g_id: self.v_g_id(vertex, e),
            src_id: self.v_ref_pid,
            dst_id: vertex,
            time: None,
            is_remote: !e.is_local(),
        }
    }
    fn out_edge_t(&self, vertex: usize, e: AdjEdge, t: i64) -> EdgeRef {
        EdgeRef {
            layer_id: self.layer_id,
            edge_id: e.edge_id(),
            src_g_id: self.v_ref,
            dst_g_id: self.v_g_id(vertex, e),
            src_id: self.v_ref_pid,
            dst_id: vertex,
            time: Some(t),
            is_remote: !e.is_local(),
        }
    }
    fn in_edge(&self, vertex: usize, e: AdjEdge) -> EdgeRef {
        EdgeRef {
            layer_id: self.layer_id,
            edge_id: e.edge_id(),
            src_g_id: self.v_g_id(vertex, e),
            dst_g_id: self.v_ref,
            src_id: vertex,
            dst_id: self.v_ref_pid,
            time: None,
            is_remote: !e.is_local(),
        }
    }
    fn in_edge_t(&self, vertex: usize, e: AdjEdge, t: i64) -> EdgeRef {
        EdgeRef {
            layer_id: self.layer_id,
            edge_id: e.edge_id(),
            src_g_id: self.v_g_id(vertex, e),
            dst_g_id: self.v_ref,
            src_id: vertex,
            dst_id: self.v_ref_pid,
            time: Some(t),
            is_remote: !e.is_local(),
        }
    }
    fn v_g_id(&self, vertex_pid: usize, e: AdjEdge) -> u64 {
        if e.is_local() {
            self.logical_ids[vertex_pid]
        } else {
            vertex_pid as u64
        }
    }
}

#[cfg(test)]
mod edge_layer_tests {
    use super::*;

    #[test]
    fn return_valid_next_available_edge_id() {
        let mut layer = EdgeLayer::new(0);

        // 0th index is not a valid edge id because it can't be used to correctly denote
        // both local as well as remote edge id. Hence edge ids must always start with 1.
        assert_eq!(layer.next_edge_id, 1);
    }
}
