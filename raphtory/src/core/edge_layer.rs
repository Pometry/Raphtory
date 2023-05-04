use itertools::chain;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::iter;
use std::ops::Range;

use crate::core::adj::Adj;
use crate::core::props::Props;
use crate::core::tadjset::AdjEdge;
use crate::core::tgraph::{EdgeRef, TimeIndex, VertexRef};
use crate::core::{Direction, Prop};

use super::tadjset::TAdjSet;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EdgeLayer {
    layer_id: usize,
    next_edge_id: usize,
    timestamps: Vec<TimeIndex>,

    // Vector of adjacency lists. It is populated lazyly, so avoid using [] accessor for reading
    pub(crate) adj_lists: Vec<Adj>,
    pub(crate) props: Props,
}

impl EdgeLayer {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            layer_id: id,
            next_edge_id: 0,
            adj_lists: Default::default(),
            props: Default::default(),
            timestamps: Default::default(),
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
        let edge_meta = self.get_edge_and_update_time(src_pid, dst_pid, t, Direction::OUT, false);
        self.link_outbound_edge(edge_meta, src_pid, dst_pid);
        self.link_inbound_edge(edge_meta, src_pid, dst_pid);

        self.props
            .upsert_temporal_props(t, edge_meta.edge_id(), props);
    }

    #[allow(unused_variables)]
    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src: u64, // we are on the source shard
        dst: u64,
        src_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        self.ensure_adj_lists_len(src_pid + 1);
        let edge_meta = self.get_edge_and_update_time(
            src_pid,
            dst.try_into().expect("assuming 64-bit platform"),
            t,
            Direction::OUT,
            true,
        );
        self.link_outbound_edge(
            edge_meta,
            src_pid,
            dst.try_into().expect("assuming 64-bit platform"),
        );
        self.props
            .upsert_temporal_props(t, edge_meta.edge_id(), props);
    }

    #[allow(unused_variables)]
    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst: u64, // we are on the destination shard
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        self.ensure_adj_lists_len(dst_pid + 1);
        let edge_meta = self.get_edge_and_update_time(
            dst_pid,
            src.try_into().expect("assuming 64-bit platform"),
            t,
            Direction::IN,
            true,
        );
        self.link_inbound_edge(
            edge_meta,
            src.try_into().expect("assuming 64-bit platform"),
            dst_pid,
        );
        self.props
            .upsert_temporal_props(t, edge_meta.edge_id(), props);
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

    fn get_edge_and_update_time(
        &mut self,
        local_v: usize,
        other: usize,
        t: i64,
        dir: Direction,
        is_remote: bool,
    ) -> AdjEdge {
        let edge = self.adj_lists[local_v]
            .get_edge(other, dir, is_remote)
            .unwrap_or_else(|| {
                let edge = AdjEdge::new(self.next_edge_id, !is_remote);
                self.next_edge_id += 1;
                edge
            });
        match self.timestamps.get_mut(edge.edge_id()) {
            Some(ts) => {
                ts.insert(t);
            }
            None => self.timestamps.push(TimeIndex::one(t)),
        };
        edge
    }

    pub(crate) fn link_inbound_edge(
        &mut self,
        edge: AdjEdge,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
    ) {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo => {
                *entry = Adj::new_into(src, edge);
            }
            Adj::List {
                into, remote_into, ..
            } => {
                let list = if edge.is_remote() { remote_into } else { into };
                list.push(src, edge);
            }
        }
    }

    pub(crate) fn link_outbound_edge(
        &mut self,
        edge: AdjEdge,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
    ) {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo => {
                *entry = Adj::new_out(dst, edge);
            }
            Adj::List {
                out, remote_out, ..
            } => {
                let list = if edge.is_remote() { remote_out } else { out };
                list.push(dst, edge);
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
            Adj::List { out, .. } => out
                .find(dst_pid)
                .filter(|e| self.timestamps[e.edge_id()].active(w.clone()))
                .is_some(),
        }
    }

    pub(crate) fn has_remote_edge(&self, src_pid: usize, dst: u64) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { remote_out, .. } => remote_out
                .find(dst.try_into().expect("assuming 64-bit platform"))
                .is_some(),
        }
    }

    pub(crate) fn has_remote_edge_window(&self, src_pid: usize, dst: u64, w: &Range<i64>) -> bool {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => false,
            Adj::List { remote_out, .. } => remote_out
                .find(dst.try_into().expect("assuming 64-bit platform"))
                .filter(|e| self.timestamps[e.edge_id()].active(w.clone()))
                .is_some(),
        }
    }

    pub(crate) fn get_edge_history(
        &self,
        src_pid: usize,
        dst_pid: usize,
        local: bool,
        window: Option<Range<i64>>,
    ) -> Vec<i64> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => vec![],
            Adj::List {
                out, remote_out, ..
            } => {
                if local {
                    match window {
                        None => out.find(dst_pid).map_or(vec![], |e| {
                            self.timestamps[e.edge_id()].iter().copied().collect()
                        }),
                        Some(w) => out.find(dst_pid).map_or(vec![], |e| {
                            self.timestamps[e.edge_id()].range(w).copied().collect()
                        }),
                    }
                } else {
                    match window {
                        None => remote_out.find(dst_pid).map_or(vec![], |e| {
                            self.timestamps[e.edge_id()].iter().copied().collect()
                        }),
                        Some(w) => remote_out.find(dst_pid).map_or(vec![], |e| {
                            self.timestamps[e.edge_id()].range(w).copied().collect()
                        }),
                    }
                }
            }
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
            Adj::List { out, .. } => out
                .find(dst_pid)
                .filter(|e| self.timestamps[e.edge_id()].active(w.clone()))
                .map(|e| EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst_pid,
                    time: None,
                    is_remote: false,
                }),
        }
    }

    pub(crate) fn remote_edge(&self, src: u64, dst: u64, src_pid: usize) -> Option<EdgeRef> {
        match self.adj_lists.get(src_pid).unwrap_or(&Adj::Solo) {
            Adj::Solo => None,
            Adj::List { remote_out, .. } => {
                let e = remote_out.find(dst.try_into().expect("assuming 64-bit platform"))?;
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
            Adj::List { remote_out, .. } => remote_out
                .find(dst.try_into().expect("assuming 64-bit platform"))
                .filter(|e| self.timestamps[e.edge_id()].active(w.clone()))
                .map(|e| EdgeRef {
                    layer_id: self.layer_id,
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst as usize,
                    time: None,
                    is_remote: true,
                }),
        }
    }
}

// AGGREGATED ACCESS:
impl EdgeLayer {
    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists.iter().map(|adj| adj.out_edges_len()).sum()
    }

    pub(crate) fn out_edges_len_window(&self, w: &Range<i64>) -> usize {
        self.timestamps
            .iter()
            .enumerate()
            .filter_map(|(i, ts)| ts.active(w.clone()).then_some(i))
            .map(|i| match &self.adj_lists[i] {
                Adj::Solo => 0,
                Adj::List {
                    out, remote_out, ..
                } => {
                    out.len_window(&self.timestamps, w) + remote_out.len_window(&self.timestamps, w)
                }
            })
            .sum()
    }
}

// MULTIPLE EDGE ACCES:
impl EdgeLayer {
    pub fn local_vertex_neighbours(
        &self,
        v_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = usize> + Send + '_> {
        let adj = self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List { out, into, .. } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(out.vertices());
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(into.vertices());
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(
                        [out.vertices(), into.vertices()]
                            .into_iter()
                            .kmerge()
                            .dedup(),
                    );
                    iter
                }
            },
        }
    }

    pub fn remote_vertex_neighbours(
        &self,
        v_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = usize> + Send + '_> {
        let adj = self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List {
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(remote_out.vertices());
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(remote_into.vertices());
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(
                        [remote_out.vertices(), remote_into.vertices()]
                            .into_iter()
                            .kmerge()
                            .dedup(),
                    );
                    iter
                }
            },
        }
    }

    pub fn local_vertex_neighbours_window(
        &self,
        v_pid: usize,
        d: Direction,
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = usize> + Send + '_> {
        let adj = self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List { out, into, .. } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(out.vertices_window(&self.timestamps, window));
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(into.vertices_window(&self.timestamps, window));
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(
                        [
                            out.vertices_window(&self.timestamps, window),
                            into.vertices_window(&self.timestamps, window),
                        ]
                        .into_iter()
                        .kmerge()
                        .dedup(),
                    );
                    iter
                }
            },
        }
    }

    pub fn remote_vertex_neighbours_window(
        &self,
        v_pid: usize,
        d: Direction,
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = usize> + Send + '_> {
        let adj = self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List {
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(remote_out.vertices_window(&self.timestamps, window));
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> =
                        Box::new(remote_into.vertices_window(&self.timestamps, window));
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = usize> + Send + '_> = Box::new(
                        [
                            remote_out.vertices_window(&self.timestamps, window),
                            remote_into.vertices_window(&self.timestamps, window),
                        ]
                        .into_iter()
                        .kmerge()
                        .dedup(),
                    );
                    iter
                }
            },
        }
    }

    pub fn degree(&self, v_pid: usize, d: Direction) -> usize {
        match d {
            Direction::OUT => match &self.adj_lists[v_pid] {
                Adj::Solo => 0,
                Adj::List {
                    out, remote_out, ..
                } => out.len() + remote_out.len(),
            },
            Direction::IN => match &self.adj_lists[v_pid] {
                Adj::Solo => 0,
                Adj::List {
                    into, remote_into, ..
                } => into.len() + remote_into.len(),
            },
            Direction::BOTH => match &self.adj_lists[v_pid] {
                Adj::Solo => 0,
                Adj::List {
                    out,
                    remote_out,
                    into,
                    remote_into,
                } => {
                    [out.vertices(), into.vertices()]
                        .into_iter()
                        .kmerge()
                        .dedup()
                        .count()
                        + [remote_out.vertices(), remote_into.vertices()]
                            .into_iter()
                            .kmerge()
                            .dedup()
                            .count()
                }
            },
        }
    }

    pub fn degree_window(&self, v_pid: usize, d: Direction, window: &Range<i64>) -> usize {
        let adj = &self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo);
        match adj {
            Adj::Solo => 0,
            Adj::List {
                out,
                remote_out,
                into,
                remote_into,
            } => match d {
                Direction::OUT => {
                    out.len_window(&self.timestamps, window)
                        + remote_out.len_window(&self.timestamps, window)
                }
                Direction::IN => {
                    into.len_window(&self.timestamps, window)
                        + remote_into.len_window(&self.timestamps, window)
                }
                Direction::BOTH => {
                    [
                        out.vertices_window(&self.timestamps, window),
                        into.vertices_window(&self.timestamps, window),
                    ]
                    .into_iter()
                    .kmerge()
                    .dedup()
                    .count()
                        + [
                            remote_out.vertices_window(&self.timestamps, window),
                            remote_into.vertices_window(&self.timestamps, window),
                        ]
                        .into_iter()
                        .kmerge()
                        .dedup()
                        .count()
                }
            },
        }
    }

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
                        .map(move |(dst, e)| (dst, builder.out_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::IN => {
                    let iter = chain!(into.iter(), remote_into.iter())
                        .map(move |(dst, e)| (dst, builder.in_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::BOTH => {
                    let out_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.clone().out_edge(dst, e));
                    let in_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.in_edge(dst, e));

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
                    let iter = chain!(
                        out.iter_window(&self.timestamps, r),
                        remote_out.iter_window(&self.timestamps, r)
                    )
                    .map(move |(dst, e)| (dst, builder.out_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::IN => {
                    let iter = chain!(
                        into.iter_window(&self.timestamps, r),
                        remote_into.iter_window(&self.timestamps, r)
                    )
                    .map(move |(dst, e)| (dst, builder.in_edge(dst, e)));
                    Box::new(iter)
                }
                Direction::BOTH => {
                    let out_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.out_edge(dst, e));
                    let in_mapper =
                        move |(dst, e): (usize, AdjEdge)| (dst, builder.in_edge(dst, e));

                    let remote_out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_out.iter_window(&self.timestamps, r).map(out_mapper));
                    let remote_into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(remote_into.iter_window(&self.timestamps, r).map(in_mapper));
                    let remote = vec![remote_out, remote_into]
                        .into_iter()
                        .kmerge_by(|(left, _), (right, _)| left < right);

                    let out: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(out.iter_window(&self.timestamps, r).map(out_mapper));
                    let into: Box<dyn Iterator<Item = (usize, EdgeRef)> + Send> =
                        Box::new(into.iter_window(&self.timestamps, r).map(in_mapper));
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
        w: &'a Range<i64>,
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
                Direction::OUT => Box::new(chain!(out.iter(), remote_out.iter()).flat_map(
                    move |(dst, e)| {
                        self.timestamps[e.edge_id()]
                            .range(w.clone())
                            .map(move |t| builder.out_edge_t(dst, e, *t))
                    },
                )),
                Direction::IN => Box::new(chain!(into.iter(), remote_into.iter()).flat_map(
                    move |(dst, e)| {
                        self.timestamps[e.edge_id()]
                            .range(w.clone())
                            .map(move |t| builder.in_edge_t(dst, e, *t))
                    },
                )),
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
}
