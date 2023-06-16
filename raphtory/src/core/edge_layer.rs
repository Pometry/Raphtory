use itertools::chain;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::iter;
use std::ops::Range;

use crate::core::adj::Adj;
use crate::core::edge_ref::EdgeRef;
use crate::core::props::Props;
use crate::core::timeindex::{TimeIndex, TimeIndexOps};
use crate::core::{Direction, Prop};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum VID {
    Local(usize),
    Remote(u64),
}

impl From<u64> for VID {
    fn from(value: u64) -> Self {
        VID::Remote(value)
    }
}

impl From<usize> for VID {
    fn from(value: usize) -> Self {
        VID::Local(value)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EdgeLayer {
    layer_id: usize,
    shard_id: usize,
    local_timestamps: Vec<TimeIndex>,
    local_deletions: Vec<TimeIndex>,
    remote_out_timestamps: Vec<TimeIndex>,
    remote_out_deletions: Vec<TimeIndex>,
    remote_into_timestamps: Vec<TimeIndex>,
    remote_into_deletions: Vec<TimeIndex>,

    // Vector of adjacency lists. It is populated lazyly, so avoid using [] accessor for reading
    adj_lists: Vec<Adj>,
    local_props: Props,
    remote_out_props: Props,
    remote_into_props: Props,
}

impl EdgeLayer {
    pub(crate) fn new(layer_id: usize, shard_id: usize) -> Self {
        Self {
            layer_id,
            shard_id,
            adj_lists: Default::default(),
            local_props: Default::default(),
            remote_out_props: Default::default(),
            local_timestamps: Default::default(),
            local_deletions: Default::default(),
            remote_out_timestamps: Default::default(),
            remote_out_deletions: Default::default(),
            remote_into_timestamps: Default::default(),
            remote_into_deletions: Default::default(),
            remote_into_props: Default::default(),
        }
    }

    fn new_local_out_edge_ref(
        &self,
        src_pid: usize,
        dst_pid: usize,
        e_pid: usize,
        time: Option<i64>,
    ) -> EdgeRef {
        EdgeRef::LocalOut {
            e_pid,
            shard_id: self.shard_id,
            layer_id: self.layer_id,
            src_pid,
            dst_pid,
            time,
        }
    }

    fn new_local_into_edge_ref(
        &self,
        src_pid: usize,
        dst_pid: usize,
        e_pid: usize,
        time: Option<i64>,
    ) -> EdgeRef {
        EdgeRef::LocalInto {
            e_pid,
            shard_id: self.shard_id,
            layer_id: self.layer_id,
            src_pid,
            dst_pid,
            time,
        }
    }

    fn new_remote_out_edge_ref(
        &self,
        src_pid: usize,
        dst: u64,
        e_pid: usize,
        time: Option<i64>,
    ) -> EdgeRef {
        EdgeRef::RemoteOut {
            e_pid,
            shard_id: self.shard_id,
            layer_id: self.layer_id,
            src_pid,
            dst,
            time,
        }
    }

    fn new_remote_into_edge_ref(
        &self,
        src: u64,
        dst_pid: usize,
        e_pid: usize,
        time: Option<i64>,
    ) -> EdgeRef {
        EdgeRef::RemoteInto {
            e_pid,
            shard_id: self.shard_id,
            layer_id: self.layer_id,
            src,
            dst_pid,
            time,
        }
    }
}

// INGESTION:
impl EdgeLayer {
    pub(crate) fn add_edge_with_props(
        &mut self,
        t: i64,
        src_pid: usize,
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let required_len = std::cmp::max(src_pid, dst_pid) + 1;
        let dst = VID::Local(dst_pid);
        let src = VID::Local(src_pid);
        self.ensure_adj_lists_len(required_len);
        let (edge_meta, timestamps, _) = self.get_edge_and_timestamps(src_pid, dst, Direction::OUT);
        timestamps.insert(t);
        self.link_outbound_edge(edge_meta, src_pid, dst);
        self.link_inbound_edge(edge_meta, src, dst_pid);
        self.local_props.upsert_temporal_props(t, edge_meta, props);
    }

    pub(crate) fn delete_edge(&mut self, t: i64, src_pid: usize, dst_pid: usize) {
        let required_len = std::cmp::max(src_pid, dst_pid) + 1;
        let dst = VID::Local(dst_pid);
        let src = VID::Local(src_pid);
        self.ensure_adj_lists_len(required_len);
        let (edge_meta, _, deletions) = self.get_edge_and_timestamps(src_pid, dst, Direction::OUT);
        deletions.insert(t);
        self.link_outbound_edge(edge_meta, src_pid, src);
        self.link_inbound_edge(edge_meta, src, dst_pid);
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src_pid: usize,
        dst: u64,
        props: &Vec<(String, Prop)>,
    ) {
        self.ensure_adj_lists_len(src_pid + 1);
        let dst = VID::Remote(dst);
        let (edge_meta, timestamps, _) = self.get_edge_and_timestamps(src_pid, dst, Direction::OUT);
        timestamps.insert(t);
        self.link_outbound_edge(edge_meta, src_pid, dst);
        self.remote_out_props
            .upsert_temporal_props(t, edge_meta, props);
    }

    pub(crate) fn delete_edge_remote_out(&mut self, t: i64, src_pid: usize, dst: u64) {
        self.ensure_adj_lists_len(src_pid + 1);
        let dst = VID::Remote(dst);
        let (edge_meta, _, deletions) = self.get_edge_and_timestamps(src_pid, dst, Direction::OUT);
        deletions.insert(t);
        self.link_outbound_edge(edge_meta, src_pid, dst);
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let src = VID::Remote(src);
        self.ensure_adj_lists_len(dst_pid + 1);
        let (edge_meta, timestamps, _) = self.get_edge_and_timestamps(dst_pid, src, Direction::IN);
        timestamps.insert(t);
        self.link_inbound_edge(edge_meta, src, dst_pid);
        self.remote_into_props
            .upsert_temporal_props(t, edge_meta, props);
    }

    pub(crate) fn delete_edge_remote_into(&mut self, t: i64, src: u64, dst_pid: usize) {
        let src = VID::Remote(src);
        self.ensure_adj_lists_len(dst_pid + 1);
        let (edge_meta, _, deletions) = self.get_edge_and_timestamps(dst_pid, src, Direction::IN);
        deletions.insert(t);
        self.link_inbound_edge(edge_meta, src, dst_pid);
    }

    pub(crate) fn edge_props_mut(&mut self, edge: EdgeRef) -> &mut Props {
        match edge {
            EdgeRef::RemoteInto { .. } => &mut self.remote_into_props,
            EdgeRef::RemoteOut { .. } => &mut self.remote_out_props,
            _ => &mut self.local_props,
        }
    }

    pub(crate) fn edge_props(&self, edge: EdgeRef) -> &Props {
        match edge {
            EdgeRef::RemoteInto { .. } => &self.remote_into_props,
            EdgeRef::RemoteOut { .. } => &self.remote_out_props,
            _ => &self.local_props,
        }
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

    #[inline]
    fn get_adj(&self, v_pid: usize) -> &Adj {
        self.adj_lists.get(v_pid).unwrap_or(&Adj::Solo)
    }

    fn get_edge_and_timestamps(
        &mut self,
        local_v: usize,
        other: VID,
        dir: Direction,
    ) -> (usize, &mut TimeIndex, &mut TimeIndex) {
        let (timestamps, deletions) = match other {
            VID::Remote(_) => match dir {
                Direction::IN => (
                    &mut self.remote_into_timestamps,
                    &mut self.remote_into_deletions,
                ),
                Direction::OUT => (
                    &mut self.remote_out_timestamps,
                    &mut self.remote_out_deletions,
                ),
                Direction::BOTH => {
                    panic!("Internal get_edge function should not be called with `Direction::BOTH`")
                }
            },
            VID::Local(_) => (&mut self.local_timestamps, &mut self.local_deletions),
        };
        let edge = self.adj_lists[local_v]
            .get_edge(other, dir)
            .unwrap_or_else(|| {
                let edge = timestamps.len();
                timestamps.push(TimeIndex::default());
                deletions.push(TimeIndex::default());
                edge
            });
        (edge, &mut timestamps[edge], &mut deletions[edge])
    }

    pub(crate) fn link_inbound_edge(
        &mut self,
        edge: usize,
        src: VID, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
    ) {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo => {
                *entry = Adj::new_into(src, edge);
            }
            Adj::List {
                into, remote_into, ..
            } => match src {
                VID::Remote(v) => remote_into.push(v, edge),
                VID::Local(v) => into.push(v, edge),
            },
        }
    }

    pub(crate) fn link_outbound_edge(
        &mut self,
        edge: usize,
        src_pid: usize,
        dst: VID, // may or may not pe physical id depending on remote_edge flag
    ) {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo => {
                *entry = Adj::new_out(dst, edge);
            }
            Adj::List {
                out, remote_out, ..
            } => match dst {
                VID::Remote(v) => remote_out.push(v, edge),
                VID::Local(v) => out.push(v, edge),
            },
        }
    }
}

// SINGLE EDGE ACCESS:
impl EdgeLayer {
    pub(crate) fn edge(&self, src: VID, dst: VID, w: Option<Range<i64>>) -> Option<EdgeRef> {
        match src {
            VID::Local(src_pid) => {
                let adj = self.get_adj(src_pid);
                match adj {
                    Adj::Solo => None,
                    Adj::List {
                        out, remote_out, ..
                    } => match dst {
                        VID::Local(dst_pid) => {
                            let e = out.find(dst_pid).and_then(|e| match w {
                                Some(w) => self.local_timestamps[e].active(w).then_some(e),
                                None => Some(e),
                            })?;
                            Some(EdgeRef::LocalOut {
                                e_pid: e,
                                shard_id: self.shard_id,
                                layer_id: self.layer_id,
                                src_pid,
                                dst_pid,
                                time: None,
                            })
                        }
                        VID::Remote(dst) => {
                            let e = remote_out.find(dst).and_then(|e| match w {
                                Some(w) => self.remote_out_timestamps[e].active(w).then_some(e),
                                None => Some(e),
                            })?;
                            Some(EdgeRef::RemoteOut {
                                e_pid: e,
                                shard_id: self.shard_id,
                                layer_id: self.layer_id,
                                src_pid,
                                dst,
                                time: None,
                            })
                        }
                    },
                }
            }
            VID::Remote(src) => match dst {
                VID::Local(dst_pid) => {
                    let adj = self.get_adj(dst_pid);
                    match adj {
                        Adj::Solo => None,
                        Adj::List { remote_into, .. } => {
                            let e = remote_into.find(src).filter(|e| match w {
                                Some(w) => self.remote_into_timestamps[*e].active(w),
                                None => true,
                            })?;
                            Some(EdgeRef::RemoteInto {
                                e_pid: e,
                                shard_id: self.shard_id,
                                layer_id: self.layer_id,
                                src,
                                dst_pid,
                                time: None,
                            })
                        }
                    }
                }
                VID::Remote(_) => None,
            },
        }
    }

    pub(crate) fn has_edge(&self, src: VID, dst: VID, w: Option<Range<i64>>) -> bool {
        self.edge(src, dst, w).is_some()
    }

    pub(crate) fn edge_additions(&self, edge: EdgeRef) -> &TimeIndex {
        match edge {
            EdgeRef::RemoteInto { e_pid, .. } => &self.remote_into_timestamps[e_pid],
            EdgeRef::RemoteOut { e_pid, .. } => &self.remote_out_timestamps[e_pid],
            EdgeRef::LocalInto { e_pid, .. } => &self.local_timestamps[e_pid],
            EdgeRef::LocalOut { e_pid, .. } => &self.local_timestamps[e_pid],
        }
    }

    pub(crate) fn edge_deletions(&self, edge: EdgeRef) -> &TimeIndex {
        match edge {
            EdgeRef::RemoteInto { e_pid, .. } => &self.remote_into_deletions[e_pid],
            EdgeRef::RemoteOut { e_pid, .. } => &self.remote_out_deletions[e_pid],
            EdgeRef::LocalInto { e_pid, .. } => &self.local_deletions[e_pid],
            EdgeRef::LocalOut { e_pid, .. } => &self.local_deletions[e_pid],
        }
    }
}

// AGGREGATED ACCESS:
impl EdgeLayer {
    pub(crate) fn out_edges_len(&self) -> usize {
        self.local_timestamps.len() + self.remote_out_timestamps.len()
    }
}

// MULTIPLE EDGE ACCES:
impl EdgeLayer {
    pub fn vertex_neighbours(
        &self,
        v_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VID> + Send + '_> {
        let adj = self.get_adj(v_pid);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        out.vertices()
                            .map_into()
                            .chain(remote_out.vertices().map_into()),
                    );
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        into.vertices()
                            .map_into()
                            .chain(remote_into.vertices().map_into()),
                    );
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        out.vertices()
                            .merge(into.vertices())
                            .dedup()
                            .map_into()
                            .chain(
                                remote_out
                                    .vertices()
                                    .merge(remote_into.vertices())
                                    .dedup()
                                    .map_into(),
                            ),
                    );
                    iter
                }
            },
        }
    }

    pub fn vertex_neighbours_window(
        &self,
        v_pid: usize,
        d: Direction,
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = VID> + Send + '_> {
        let adj = self.get_adj(v_pid);
        match adj {
            Adj::Solo => {
                let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(iter::empty());
                iter
            }
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        out.vertices_window(&self.local_timestamps, window)
                            .map_into()
                            .chain(
                                remote_out
                                    .vertices_window(&self.remote_out_timestamps, window)
                                    .map_into(),
                            ),
                    );
                    iter
                }
                Direction::IN => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        into.vertices_window(&self.local_timestamps, window)
                            .map_into()
                            .chain(
                                remote_into
                                    .vertices_window(&self.remote_into_timestamps, window)
                                    .map_into(),
                            ),
                    );
                    iter
                }
                Direction::BOTH => {
                    let iter: Box<dyn Iterator<Item = VID> + Send + '_> = Box::new(
                        out.vertices_window(&self.local_timestamps, window)
                            .merge(into.vertices_window(&self.local_timestamps, window))
                            .dedup()
                            .map_into()
                            .chain(
                                remote_out
                                    .vertices_window(&self.remote_out_timestamps, window)
                                    .merge(
                                        remote_into
                                            .vertices_window(&self.remote_into_timestamps, window),
                                    )
                                    .dedup()
                                    .map_into(),
                            ),
                    );
                    iter
                }
            },
        }
    }

    pub fn degree(&self, v_pid: usize, d: Direction) -> usize {
        let adj = self.get_adj(v_pid);
        match adj {
            Adj::Solo => 0,
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => out.len() + remote_out.len(),
                Direction::IN => into.len() + remote_into.len(),
                Direction::BOTH => {
                    out.vertices().merge(into.vertices()).dedup().count()
                        + remote_out
                            .vertices()
                            .merge(remote_into.vertices())
                            .dedup()
                            .count()
                }
            },
        }
    }

    pub(crate) fn vertex_edges_iter(
        &self,
        v_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        match self.get_adj(v_pid) {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => Box::new(
                    out.iter()
                        .map(move |(dst_pid, e)| {
                            self.new_local_out_edge_ref(v_pid, dst_pid, e, None)
                        })
                        .chain(remote_out.iter().map(move |(dst, e)| {
                            self.new_remote_out_edge_ref(v_pid, dst, e, None)
                        })),
                ),
                Direction::IN => Box::new(
                    into.iter()
                        .map(move |(src_pid, e)| {
                            self.new_local_into_edge_ref(src_pid, v_pid, e, None)
                        })
                        .chain(remote_into.iter().map(move |(src, e)| {
                            self.new_remote_into_edge_ref(src, v_pid, e, None)
                        })),
                ),

                Direction::BOTH => {
                    let remote = remote_out
                        .iter()
                        .map(move |(dst, e)| {
                            (dst, self.new_remote_out_edge_ref(v_pid, dst, e, None))
                        })
                        .merge_by(
                            remote_into.iter().map(move |(src, e)| {
                                (src, self.new_remote_into_edge_ref(src, v_pid, e, None))
                            }),
                            |(left, _), (right, _)| left < right,
                        )
                        .map(|item| item.1);

                    let local = out
                        .iter()
                        .map(move |(dst_pid, e)| {
                            (
                                dst_pid,
                                self.new_local_out_edge_ref(v_pid, dst_pid, e, None),
                            )
                        })
                        .merge_by(
                            into.iter().map(move |(src_pid, e)| {
                                (
                                    src_pid,
                                    self.new_local_into_edge_ref(src_pid, v_pid, e, None),
                                )
                            }),
                            |(left, _), (right, _)| left < right,
                        )
                        .map(|item| item.1);
                    Box::new(chain!(local, remote))
                }
            },
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn vertex_edges_iter_window(
        &self,
        v_pid: usize,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        match self.get_adj(v_pid) {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match d {
                Direction::OUT => Box::new(chain!(
                    out.iter_window(&self.local_timestamps, r)
                        .map(move |(dst_pid, e)| self
                            .new_local_out_edge_ref(v_pid, dst_pid, e, None)),
                    remote_out
                        .iter_window(&self.remote_out_timestamps, r)
                        .map(move |(dst, e)| self.new_remote_out_edge_ref(v_pid, dst, e, None))
                )),
                Direction::IN => {
                    let iter = chain!(
                        into.iter_window(&self.local_timestamps, r)
                            .map(move |(src_pid, e)| self
                                .new_local_into_edge_ref(src_pid, v_pid, e, None)),
                        remote_into
                            .iter_window(&self.remote_into_timestamps, r)
                            .map(move |(src, e)| self.new_remote_into_edge_ref(src, v_pid, e, None))
                    );
                    Box::new(iter)
                }
                Direction::BOTH => Box::new(chain!(
                    out.iter_window(&self.local_timestamps, r)
                        .map(move |(dst_pid, e)| (
                            dst_pid,
                            self.new_local_out_edge_ref(v_pid, dst_pid, e, None)
                        ))
                        .merge_by(
                            into.iter_window(&self.local_timestamps, r)
                                .map(move |(src_pid, e)| (
                                    src_pid,
                                    self.new_local_into_edge_ref(src_pid, v_pid, e, None)
                                )),
                            |left, right| left.0 < right.0
                        )
                        .map(|item| item.1),
                    remote_out
                        .iter_window(&self.remote_out_timestamps, r)
                        .map(move |(dst, e)| (
                            dst,
                            self.new_remote_out_edge_ref(v_pid, dst, e, None)
                        ))
                        .merge_by(
                            remote_into
                                .iter_window(&self.remote_into_timestamps, r)
                                .map(move |(src, e)| (
                                    src,
                                    self.new_remote_into_edge_ref(src, v_pid, e, None)
                                )),
                            |left, right| left.0 < right.0
                        )
                        .map(|item| item.1)
                )),
            },
            _ => Box::new(std::iter::empty()),
        }
    }
}
