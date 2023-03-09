use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::adj::Adj;
use crate::props::Props;
use crate::Prop;
use crate::{bitset::BitSet, tadjset::AdjEdge, Direction};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TemporalGraph {
    // Maps global (logical) id to the local (physical) id which is an index to the adjacency list vector
    pub(crate) logical_to_physical: FxHashMap<u64, usize>,

    // Vector of adjacency lists
    pub(crate) adj_lists: Vec<Adj>,

    // Time index pointing at the index against adjacency lists.
    index: BTreeMap<i64, BitSet>,

    // Properties abstraction for both vertices and edges
    pub(crate) props: Props,

    //earliest time seen in this graph
    pub(crate) earliest_time:i64,

    //latest time seen in this graph
    pub(crate) latest_time:i64
}

impl Default for TemporalGraph {
    fn default() -> Self {
        Self {
            logical_to_physical: Default::default(),
            adj_lists: Default::default(),
            index: Default::default(),
            props: Default::default(),
            earliest_time: i64::MAX,
            latest_time: i64::MIN
        }
    }
}

impl TemporalGraph {
    pub(crate) fn len(&self) -> usize {
        self.logical_to_physical.len()
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists
            .iter()
            .map(|adj| adj.out_edges_len())
            .reduce(|s1, s2| s1 + s2)
            .unwrap_or(0)
    }

    pub(crate) fn has_edge(&self, src: u64, dst: u64) -> bool {
        let v_pid = self.logical_to_physical[&src];

        match &self.adj_lists[v_pid] {
            Adj::Solo(_) => false,
            Adj::List {
                out, remote_out, ..
            } => {
                if !self.has_vertex(dst) {
                    remote_out.find(dst as usize).is_some()
                } else {
                    let dst_pid = self.logical_to_physical[&dst];
                    out.find(dst_pid).is_some()
                }
            }
        }
    }

    pub(crate) fn has_edge_window(&self, src: u64, dst: u64, w: &Range<i64>) -> bool {
        // First check if v1 exists within the given window
        if self.has_vertex_window(src, w) {
            let src_pid = self.logical_to_physical[&src];
            match &self.adj_lists[src_pid] {
                Adj::Solo(_) => false,
                Adj::List {
                    out, remote_out, ..
                } => {
                    // Then check if v2 exists in the given window while sharing an edge with v1
                    if !self.has_vertex_window(dst, &w) {
                        remote_out.find_window(dst as usize, &w).is_some()
                    } else {
                        let dst_pid = self.logical_to_physical[&dst];
                        out.find_window(dst_pid, &w).is_some()
                    }
                }
            }
        } else {
            false
        }
    }

    pub(crate) fn has_vertex(&self, v: u64) -> bool {
        self.logical_to_physical.contains_key(&v)
    }

    pub(crate) fn has_vertex_window(&self, v: u64, w: &Range<i64>) -> bool {
        if let Some(v_id) = self.logical_to_physical.get(&v) {
            self.index.range(w.clone()).any(|(_, bs)| bs.contains(v_id))
        } else {
            false
        }
    }

    pub(crate) fn add_vertex(&mut self, t: i64, v: u64) {
        self.add_vertex_with_props(t, v, &vec![])
    }

    pub(crate) fn add_vertex_with_props(&mut self, t: i64, v: u64, props: &Vec<(String, Prop)>) {

        //Updating time - only needs to be here as every other adding function calls this one
        if self.earliest_time >t {
            self.earliest_time = t
        }
        if self.latest_time < t {
            self.latest_time = t
        }

        let index = match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.adj_lists.len();
                self.adj_lists.push(Adj::Solo(v));

                self.logical_to_physical.insert(v, physical_id);

                self.index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id);
                    })
                    .or_insert_with(|| BitSet::one(physical_id));
                physical_id
            }
            Some(pid) => {
                self.index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(*pid);
                    })
                    .or_insert_with(|| BitSet::one(*pid));
                *pid
            }
        };

        self.props.upsert_vertex_props(t, index, props);
    }

    pub fn add_edge(&mut self, t: i64, src: u64, dst: u64) {
        self.add_edge_with_props(t, src, dst, &vec![])
    }

    pub(crate) fn add_edge_with_props(
        &mut self,
        t: i64,
        src: u64,
        dst: u64,
        props: &Vec<(String, Prop)>,
    ) {
        // mark the times of the vertices at t
        self.add_vertex(t, src);
        self.add_vertex(t, dst);

        let src_pid = self.logical_to_physical[&src];
        let dst_pid = self.logical_to_physical[&dst];

        let src_edge_meta_id = self.link_outbound_edge(t, src, src_pid, dst_pid, false);
        let dst_edge_meta_id = self.link_inbound_edge(t, dst, src_pid, dst_pid, false);

        if src_edge_meta_id != dst_edge_meta_id {
            panic!(
                "Failure on {src} -> {dst} at time: {t} {src_edge_meta_id} != {dst_edge_meta_id}"
            );
        }

        self.props.upsert_edge_props(t, src_edge_meta_id, props)
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src: u64, // we are on the source shard
        dst: u64,
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(t, src);
        let src_pid = self.logical_to_physical[&src];
        let src_edge_meta_id =
            self.link_outbound_edge(t, src, src_pid, dst.try_into().unwrap(), true);

        self.props.upsert_edge_props(t, src_edge_meta_id, props)
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst: u64, // we are on the destination shard
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(t, dst);

        let dst_pid = self.logical_to_physical[&dst];

        let dst_edge_meta_id =
            self.link_inbound_edge(t, dst, src.try_into().unwrap(), dst_pid, true);

        self.props.upsert_edge_props(t, dst_edge_meta_id, props)
    }

    pub(crate) fn degree(&self, v: u64, d: Direction) -> usize {
        let v_pid = self.logical_to_physical[&v];

        match &self.adj_lists[v_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => out.len() + remote_out.len(),
                Direction::IN => into.len() + remote_into.len(),
                _ => {
                    vec![
                        out.iter(),
                        into.iter(),
                        remote_out.iter(),
                        remote_into.iter(),
                    ] // FIXME: there are better ways of doing this, all adj lists are sorted except for the HashMap
                    .into_iter()
                    .flatten()
                    .unique_by(|(v, _)| *v)
                    .count()
                }
            },
            _ => 0,
        }
    }

    pub fn degree_window(&self, v: u64, w: &Range<i64>, d: Direction) -> usize {
        let v_pid = self.logical_to_physical[&v];

        match &self.adj_lists[v_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => out.len_window(w) + remote_out.len_window(w),
                Direction::IN => into.len_window(w) + remote_into.len_window(w),
                _ => vec![
                    out.iter_window(w),
                    into.iter_window(w),
                    remote_out.iter_window(w),
                    remote_into.iter_window(w),
                ]
                .into_iter()
                .flatten()
                .unique_by(|(v, _)| *v)
                .count(),
            },
            _ => 0,
        }
    }

    pub(crate) fn vertex(&self, v: u64) -> Option<VertexView> {
        let pid = self.logical_to_physical.get(&v)?;
        Some(match self.adj_lists[*pid] {
            Adj::Solo(lid) => VertexView {
                g_id: lid,
                pid: Some(*pid),
            },
            Adj::List { logical, .. } => VertexView {
                g_id: logical,
                pid: Some(*pid),
            },
        })
    }

    pub(crate) fn vertex_window(&self, v: u64, w: &Range<i64>) -> Option<VertexView> {
        let pid = self.logical_to_physical.get(&v)?;
        let w = w.clone();
        let mut vs = self.index.range(w.clone()).flat_map(|(_, vs)| vs.iter());

        match vs.contains(&pid) {
            true => Some(match self.adj_lists[*pid] {
                Adj::Solo(lid) => VertexView {
                    g_id: lid,
                    pid: Some(*pid),
                },
                Adj::List { logical, .. } => VertexView {
                    g_id: logical,
                    pid: Some(*pid),
                },
            }),
            false => None,
        }
    }

    pub(crate) fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send + '_> {
        Box::new(self.adj_lists.iter().map(|adj| *adj.logical()))
    }

    pub(crate) fn vertex_ids_window(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = u64> + Send + '_> {
        Box::new(
            self.index
                .range(w.clone())
                .map(|(_, vs)| vs.iter())
                .kmerge()
                .dedup()
                .map(move |pid| match self.adj_lists[pid] {
                    Adj::Solo(lid) => lid,
                    Adj::List { logical, .. } => logical,
                }),
        )
    }

    pub(crate) fn vertices(&self) -> Box<dyn Iterator<Item = VertexView> + Send + '_> {
        Box::new(
            self.adj_lists
                .iter()
                .enumerate()
                .map(|(pid, v)| VertexView {
                    g_id: *v.logical(),
                    pid: Some(pid),
                }),
        )
    }

    pub fn vertices_window(
        &self,
        w: Range<i64>,
    ) -> Box<dyn Iterator<Item = VertexView> + Send + '_> {
        let unique_vids = self
            .index
            .range(w.clone())
            .map(|(_, vs)| vs.iter())
            .kmerge()
            .dedup();

        let vs = unique_vids.map(move |pid| match self.adj_lists[pid] {
            Adj::Solo(lid) => VertexView {
                g_id: lid,
                pid: Some(pid),
            },
            Adj::List { logical, .. } => VertexView {
                g_id: logical,
                pid: Some(pid),
            },
        });

        Box::new(vs)
    }

    pub(crate) fn edge(&self, src: u64, dst: u64) -> Option<EdgeView> {
        let src_pid = self.logical_to_physical.get(&src)?;

        match &self.adj_lists[*src_pid] {
            Adj::Solo(_) => None,
            Adj::List {
                out, remote_out, ..
            } => {
                if !self.has_vertex(dst) {
                    let e = remote_out.find(dst as usize)?;
                    Some(EdgeView {
                        edge_id: e.edge_id(),
                        src_g_id: src,
                        dst_g_id: dst,
                        src_id: *src_pid,
                        dst_id: dst as usize,
                        time: None,
                        is_remote: !e.is_local(),
                    })
                } else {
                    let dst_pid = self.logical_to_physical.get(&dst)?;
                    let e = out.find(*dst_pid)?;
                    Some(EdgeView {
                        edge_id: e.edge_id(),
                        src_g_id: src,
                        dst_g_id: dst,
                        src_id: *src_pid,
                        dst_id: *dst_pid,
                        time: None,
                        is_remote: !e.is_local(),
                    })
                }
            }
        }
    }

    pub(crate) fn edge_window(&self, src: u64, dst: u64, w: &Range<i64>) -> Option<EdgeView> {
        // First check if v1 exists within the given window
        if self.has_vertex_window(src, w) {
            let src_pid = self.logical_to_physical.get(&src)?;
            match &self.adj_lists[*src_pid] {
                Adj::Solo(_) => Option::<EdgeView>::None,
                Adj::List {
                    out, remote_out, ..
                } => {
                    // Then check if v2 exists in the given window while sharing an edge with v1
                    if !self.has_vertex_window(dst, &w) {
                        let e = remote_out.find_window(dst as usize, &w)?;
                        Some(EdgeView {
                            edge_id: e.edge_id(),
                            src_g_id: src,
                            dst_g_id: dst,
                            src_id: *src_pid,
                            dst_id: dst as usize,
                            time: None,
                            is_remote: !e.is_local(),
                        })
                    } else {
                        let dst_pid = self.logical_to_physical.get(&dst)?;
                        let e = out.find_window(*dst_pid, &w)?;
                        Some(EdgeView {
                            edge_id: e.edge_id(),
                            src_g_id: src,
                            dst_g_id: dst,
                            src_id: *src_pid,
                            dst_id: *dst_pid,
                            time: None,
                            is_remote: !e.is_local(),
                        })
                    }
                }
            }
        } else {
            Option::<EdgeView>::None
        }
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub(crate) fn vertex_edges(
        &self,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.edges_iter(v_pid, d).map(move |(dst, e)| EdgeView {
                edge_id: e.edge_id(),
                src_g_id: v,
                dst_g_id: self.v_g_id(*dst, e),
                src_id: v_pid,
                dst_id: *dst,
                time: None,
                is_remote: !e.is_local(),
            })),
            Direction::IN => Box::new(self.edges_iter(v_pid, d).map(move |(dst, e)| EdgeView {
                edge_id: e.edge_id(),
                src_g_id: self.v_g_id(*dst, e),
                dst_g_id: v,
                src_id: *dst,
                dst_id: v_pid,
                time: None,
                is_remote: !e.is_local(),
            })),
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges(v, Direction::IN),
                self.vertex_edges(v, Direction::OUT)
            )),
        }
    }

    pub(crate) fn vertex_edges_window(
        &self,
        v: u64,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => {
                Box::new(
                    self.edges_iter_window(v_pid, w, d)
                        .map(move |(dst, e)| EdgeView {
                            edge_id: e.edge_id(),
                            src_g_id: v,
                            dst_g_id: self.v_g_id(dst, e),
                            src_id: v_pid,
                            dst_id: dst,
                            time: None,
                            is_remote: !e.is_local(),
                        }),
                )
            }
            Direction::IN => {
                Box::new(
                    self.edges_iter_window(v_pid, w, d)
                        .map(move |(dst, e)| EdgeView {
                            edge_id: e.edge_id(),
                            src_g_id: self.v_g_id(dst, e),
                            dst_g_id: v,
                            src_id: dst,
                            dst_id: v_pid,
                            time: None,
                            is_remote: !e.is_local(),
                        }),
                )
            }
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges_window(v, w, Direction::IN),
                self.vertex_edges_window(v, w, Direction::OUT)
            )),
        }
    }

    pub(crate) fn vertex_edges_window_t(
        &self,
        v: u64,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send + '_> {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.edges_iter_window_t(v_pid, w, d).map(
                move |(dst, t, e)| EdgeView {
                    edge_id: e.edge_id(),
                    src_g_id: v,
                    dst_g_id: self.v_g_id(dst, e),
                    src_id: v_pid,
                    dst_id: dst,
                    time: Some(t),
                    is_remote: !e.is_local(),
                },
            )),
            Direction::IN => Box::new(self.edges_iter_window_t(v_pid, w, d).map(
                move |(dst, t, e)| EdgeView {
                    edge_id: e.edge_id(),
                    src_g_id: self.v_g_id(dst, e),
                    dst_g_id: v,
                    src_id: dst,
                    dst_id: v_pid,
                    time: Some(t),
                    is_remote: !e.is_local(),
                },
            )),
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges_window_t(v, w, Direction::IN),
                self.vertex_edges_window_t(v, w, Direction::OUT)
            )),
        }
    }

    pub(crate) fn neighbours(
        &self,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexView> + Send + '_>
    where
        Self: Sized,
    {
        let edges = self.vertex_edges(v, d);

        Box::new(edges.map(move |edge| {
            let EdgeView {
                src_id,
                dst_id,
                is_remote,
                ..
            } = edge;

            let src_g_id = edge.src_g_id;
            let dst_g_id = edge.dst_g_id;

            if v == src_g_id {
                if is_remote {
                    VertexView::new(dst_g_id, None)
                } else {
                    VertexView::new(dst_g_id, Some(dst_id))
                }
            } else {
                if is_remote {
                    VertexView::new(src_g_id, None)
                } else {
                    VertexView::new(src_g_id, Some(src_id))
                }
            }
        }))
    }

    pub(crate) fn neighbours_window(
        &self,
        v: u64,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexView> + Send + '_>
    where
        Self: Sized,
    {
        let edges = self.vertex_edges_window(v, w, d);

        Box::new(edges.map(move |edge| {
            let EdgeView {
                src_id,
                dst_id,
                is_remote,
                ..
            } = edge;

            let src_g_id = edge.src_g_id;
            let dst_g_id = edge.dst_g_id;

            if v == src_g_id {
                if is_remote {
                    VertexView::new(dst_g_id, None)
                } else {
                    VertexView::new(dst_g_id, Some(dst_id))
                }
            } else {
                if is_remote {
                    VertexView::new(src_g_id, None)
                } else {
                    VertexView::new(src_g_id, Some(src_id))
                }
            }
        }))
    }

    pub(crate) fn neighbours_ids(
        &self,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + '_>
    where
        Self: Sized,
    {
        Box::new(self.neighbours(v, d).map(|vv| vv.g_id))
    }

    pub(crate) fn neighbours_ids_window(
        &self,
        v: u64,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send + '_>
    where
        Self: Sized,
    {
        Box::new(self.neighbours_window(v, w, d).map(|vv| vv.g_id))
    }

    pub(crate) fn vertex_prop(
        &self,
        v: u64,
        name: &str,
    ) -> Option<Box<dyn Iterator<Item = (&i64, Prop)> + '_>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;
        let prop_id = self.props.prop_ids.get(name)?;
        Some(meta.iter(*prop_id))
    }

    pub(crate) fn vertex_prop_window(
        &self,
        v: u64,
        name: &str,
        w: &Range<i64>,
    ) -> Option<Box<dyn Iterator<Item = (&i64, Prop)> + '_>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;
        let prop_id = self.props.prop_ids.get(name)?;
        Some(meta.iter_window(*prop_id, w.clone()))
    }

    pub(crate) fn vertex_prop_vec(&self, v: u64, name: &str) -> Option<Vec<(i64, Prop)>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;
        let prop_id = self.props.prop_ids.get(name)?;
        Some(meta.iter(*prop_id).map(|(t, p)| (*t, p)).collect_vec())
    }

    pub(crate) fn vertex_prop_vec_window(
        &self,
        v: u64,
        name: &str,
        w: &Range<i64>,
    ) -> Option<Vec<(i64, Prop)>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;
        let prop_id = self.props.prop_ids.get(name)?;
        Some(
            meta.iter_window(*prop_id, w.clone())
                .map(|(t, p)| (*t, p))
                .collect_vec(),
        )
    }

    pub(crate) fn vertex_props(&self, v: u64) -> Option<HashMap<String, Vec<(i64, Prop)>>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;

        let mut hm: HashMap<String, Vec<(i64, Prop)>> = HashMap::new();

        self.props.prop_ids.iter().for_each(|(k, v)| {
            if hm.contains_key(k) {
                let vs = hm.get_mut(k).unwrap();
                vs.append(
                    &mut meta
                        .iter(*v)
                        .map(|(x, y)| (*x, y.into()))
                        .collect::<Vec<(i64, Prop)>>(),
                )
            } else {
                let value = meta
                    .iter(*v)
                    .map(|(x, y)| (*x, y.into()))
                    .collect::<Vec<(i64, Prop)>>();
                if !value.is_empty() {
                    // self.g.props.prop_ids returns all prop ids, including edge property ids
                    hm.insert(k.clone(), Vec::from(value));
                }
            }
        });

        Some(hm) // Don't return "None" if hm.is_empty for Some({}) gets translated as {} in python
    }

    pub(crate) fn vertex_props_window(
        &self,
        v: u64,
        w: &Range<i64>,
    ) -> Option<HashMap<String, Vec<(i64, Prop)>>> {
        let index = self.logical_to_physical.get(&v)?;
        let meta = self.props.vertex_meta.get(*index)?;

        let mut hm: HashMap<String, Vec<(i64, Prop)>> = HashMap::new();

        self.props.prop_ids.iter().for_each(|(k, v)| {
            if hm.contains_key(k) {
                let vs = hm.get_mut(k).unwrap();
                vs.append(
                    &mut meta
                        .iter_window(*v, w.clone())
                        .map(|(x, y)| (*x, y.into()))
                        .collect::<Vec<(i64, Prop)>>(),
                )
            } else {
                let value = meta
                    .iter_window(*v, w.clone())
                    .map(|(x, y)| (*x, y.into()))
                    .collect::<Vec<(i64, Prop)>>();
                if !value.is_empty() {
                    // self.g.props.prop_ids returns all prop ids, including edge property ids
                    hm.insert(k.clone(), Vec::from(value));
                }
            }
        });

        Some(hm) // Don't return "None" if hm.is_empty for Some({}) gets translated as {} in python
    }

    pub fn edge_prop(
        &self,
        e: usize,
        name: &str,
    ) -> Option<Box<dyn Iterator<Item = (&i64, Prop)> + '_>> {
        let prop_id = self.props.prop_ids.get(name)?;
        Some(self.props.edge_meta[e].iter(*prop_id))
    }

    pub fn edge_prop_window(
        &self,
        e: usize,
        name: &str,
        w: Range<i64>,
    ) -> Option<Box<dyn Iterator<Item = (&i64, Prop)> + '_>> {
        let prop_id = self.props.prop_ids.get(name)?;
        Some(self.props.edge_meta[e].iter_window(*prop_id, w))
    }

    pub fn edge_prop_vec(&self, e: usize, name: &str) -> Option<Vec<(i64, Prop)>> {
        let prop_id = self.props.prop_ids.get(name)?;
        Some(
            self.props.edge_meta[e]
                .iter(*prop_id)
                .map(|(t, p)| (*t, p))
                .collect_vec(),
        )
    }

    pub fn edge_prop_vec_window(
        &self,
        e: usize,
        name: &str,
        w: Range<i64>,
    ) -> Option<Vec<(i64, Prop)>> {
        let prop_id = self.props.prop_ids.get(name)?;
        Some(
            self.props.edge_meta[e]
                .iter_window(*prop_id, w)
                .map(|(t, p)| (*t, p))
                .collect_vec(),
        )
    }
}

impl TemporalGraph {
    fn link_inbound_edge(
        &mut self,
        t: i64,
        dst_gid: u64,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.props.get_next_available_edge_id();

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_into(dst_gid, src, t, edge);

                edge_id
            }
            Adj::List {
                into, remote_into, ..
            } => {
                let list = if remote_edge { remote_into } else { into };
                let edge_id: usize = list
                    .find(src)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.props.get_next_available_edge_id());

                list.push(t, src, AdjEdge::new(edge_id, !remote_edge)); // idempotent
                edge_id
            }
        }
    }

    fn link_outbound_edge(
        &mut self,
        t: i64,
        src_gid: u64,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.props.get_next_available_edge_id();

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_out(src_gid, dst, t, edge);

                edge_id
            }
            Adj::List {
                out, remote_out, ..
            } => {
                let list = if remote_edge { remote_out } else { out };
                let edge_id: usize = list
                    .find(dst)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.props.get_next_available_edge_id());

                list.push(t, dst, AdjEdge::new(edge_id, !remote_edge));
                edge_id
            }
        }
    }

    fn edges_iter(
        &self,
        vid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&usize, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(out.iter(), remote_out.iter())),
                    Direction::IN => Box::new(itertools::chain!(into.iter(), remote_into.iter())),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter(),
                        into.iter(),
                        remote_out.iter(),
                        remote_into.iter()
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn edges_iter_window(
        &self,
        vid: usize,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(
                        out.iter_window(r),
                        remote_out.iter_window(r)
                    )),
                    Direction::IN => Box::new(itertools::chain!(
                        into.iter_window(r),
                        remote_into.iter_window(r),
                    )),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter_window(r),
                        into.iter_window(r),
                        remote_out.iter_window(r),
                        remote_into.iter_window(r)
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn edges_iter_window_t(
        &self,
        vid: usize,
        window: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, i64, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(
                        out.iter_window_t(window),
                        remote_out.iter_window_t(window)
                    )),
                    Direction::IN => Box::new(itertools::chain!(
                        into.iter_window_t(window),
                        remote_into.iter_window_t(window),
                    )),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter_window_t(window),
                        into.iter_window_t(window),
                        remote_out.iter_window_t(window),
                        remote_into.iter_window_t(window)
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn v_g_id(&self, v_id: usize, e: AdjEdge) -> u64 {
        if e.is_local() {
            *self.adj_lists[v_id].logical()
        } else {
            v_id.try_into().unwrap()
        }
    }
}

// helps us track what are we iterating over
#[derive(Debug, PartialEq)]
pub struct VertexView {
    pub g_id: u64,
    // `pid` is optional because pid info is unavailable while creating remote vertex view locally.
    // For instance, when returning vertex neighbours
    pub pid: Option<usize>,
}

impl VertexView {
    pub fn new(g_id: u64, pid: Option<usize>) -> Self {
        Self { g_id, pid }
    }
}

#[derive(Debug, PartialEq)]
pub struct EdgeView {
    pub edge_id: usize,
    pub src_g_id: u64,
    pub dst_g_id: u64,
    // src_id and dst_id could be global or physical depending upon edge being remote or local respectively
    src_id: usize,
    dst_id: usize,
    pub time: Option<i64>,
    pub is_remote: bool,
}

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
mod graph_test {
    use std::{path::PathBuf, vec};

    use csv::StringRecord;

    use crate::utils;

    use super::*;

    #[test]
    fn testhm() {
        let map = std::collections::HashMap::from([("a", 1), ("b", 2), ("c", 3)]);

        for val in map.values() {
            println!("sk: {:?}", val);
        }
    }

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);

        assert!(g.has_vertex(9));
        assert!(g.has_vertex_window(9, &(1..15)));
        assert_eq!(g.vertices().map(|v| v.g_id).collect::<Vec<u64>>(), vec![9]);
        assert_eq!(g.props.vertex_meta.get(2), None);
    }

    #[test]
    fn add_vertices_with_1_property() {
        let mut g = TemporalGraph::default();

        let v_id = 1;
        let ts = 1;
        g.add_vertex_with_props(ts, v_id, &vec![("type".into(), Prop::Str("wallet".into()))]);

        assert!(g.has_vertex(v_id));
        assert!(g.has_vertex_window(v_id, &(1..15)));
        assert_eq!(
            g.vertices().map(|v| v.g_id).collect::<Vec<u64>>(),
            vec![v_id]
        );

        let res = g
            .vertices()
            .flat_map(|v| g.vertex_prop_vec(v.g_id, "type"))
            .flatten()
            .collect_vec();

        assert_eq!(res, vec![(1i64, Prop::Str("wallet".into()))]);
    }

    #[test]
    fn add_vertices_with_multiple_properties() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        let res = g
            .vertices()
            .flat_map(|v| {
                let type_ = g.vertex_prop_vec(v.g_id, "type");
                let active = g.vertex_prop_vec(v.g_id, "active");
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(1i64, Prop::Str("wallet".into())), (1i64, Prop::U32(0)),]
        );
    }

    #[test]
    fn add_vertices_with_1_property_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        g.add_vertex_with_props(
            2,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(1)),
            ],
        );

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res: Vec<(i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                let type_ = g.vertex_prop_vec_window(v.g_id, "type", &(2..3));
                let active = g.vertex_prop_vec_window(v.g_id, "active", &(2..3));
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(2i64, Prop::Str("wallet".into())), (2i64, Prop::U32(1)),]
        );
    }

    #[test]
    fn add_vertices_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        g.add_vertex_with_props(2, 1, &vec![("label".into(), Prop::I32(12345))]);

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("origin".into(), Prop::F32(0.1)),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res = g
            .vertices()
            .flat_map(|v| {
                let type_ = g.vertex_prop_vec_window(v.g_id, "type", &(1..2));
                let active = g.vertex_prop_vec_window(v.g_id, "active", &(2..5));
                let label = g.vertex_prop_vec_window(v.g_id, "label", &(2..5));
                let origin = g.vertex_prop_vec_window(v.g_id, "origin", &(2..5));
                type_
                    .zip(active)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(origin)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![
                (1i64, Prop::Str("wallet".into())),
                (3, Prop::U32(2)),
                (2, Prop::I32(12345)),
                (3, Prop::F32(0.1)),
            ]
        );
    }

    #[test]
    #[ignore = "Undecided on the semantics of the time window over vertices shoule be supported in Docbrown"]
    fn add_vertex_at_time_t1_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.has_vertex(9));
        assert!(g.has_vertex_window(9, &(1..15)));
        assert!(g.has_vertex_window(9, &(5..15))); // FIXME: this is wrong and we might need a different kind of window here
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        let actual: Vec<u64> = g.vertices_window(0..2).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.vertices_window(2..10).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.vertices_window(0..10).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        g.add_edge(3, 9, 1);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..2), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..4), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        assert_eq!(actual, vec![1]);

        // the inbound neighbours of 1 at time 0..4 are 9
        let actual: Vec<u64> = g
            .vertex_edges_window(1, &(0..4), Direction::IN)
            .map(|e| e.src_g_id)
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn has_edge() {
        let mut g = TemporalGraph::default();
        g.add_vertex(1, 8);
        g.add_vertex(1, 9);
        g.add_vertex(2, 10);
        g.add_vertex(2, 11);
        g.add_edge(3, 9, 8);
        g.add_edge(3, 8, 9);
        g.add_edge(3, 9, 11);

        assert_eq!(g.has_edge(8, 9), true);
        assert_eq!(g.has_edge(9, 8), true);
        assert_eq!(g.has_edge(9, 11), true);
        assert_eq!(g.has_edge(11, 9), false);
        assert_eq!(g.has_edge(10, 11), false);
        assert_eq!(g.has_edge(10, 9), false);
    }

    #[test]
    fn edge_exists_inside_window() {
        let mut g = TemporalGraph::default();
        g.add_vertex(1, 5);
        g.add_vertex(2, 7);
        g.add_edge(3, 5, 7);

        let actual: Vec<bool> = g
            .vertex_edges_window(5, &(0..4), Direction::OUT)
            .map(|e| g.has_edge(e.src_g_id, e.dst_g_id))
            .collect();

        assert_eq!(actual, vec![true]);
    }

    #[test]
    fn edge_does_not_exists_outside_window() {
        let mut g = TemporalGraph::default();
        g.add_vertex(5, 9);
        g.add_vertex(7, 10);
        g.add_edge(8, 9, 10);

        let actual: Vec<bool> = g
            .vertex_edges_window(9, &(0..4), Direction::OUT)
            .map(|e| g.has_edge(e.src_g_id, e.dst_g_id))
            .collect();

        //return empty as no edges in this window
        assert_eq!(actual, Vec::<bool>::new());
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        assert_eq!(actual, Vec::<u64>::new());

        g.add_edge(3, 9, 1);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..2), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..4), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .vertex_edges_window(1, &(0..4), Direction::IN)
            .map(|e| e.src_g_id)
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3_overwrite() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        assert_eq!(actual, Vec::<u64>::new());

        g.add_edge(3, 9, 1);
        g.add_edge(12, 9, 1); // add the same edge again at different time

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.g_id).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..2), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..4), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .vertex_edges_window(1, &(0..4), Direction::IN)
            .map(|e| e.src_g_id)
            .collect();
        assert_eq!(actual, vec![9]);

        let actual: Vec<u64> = g
            .vertex_edges_window(9, &(0..13), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect();
        assert_eq!(actual, vec![1]);

        // when we look for time we see both variants
        let actual: Vec<(i64, u64)> = g
            .vertex_edges_window_t(9, &(0..13), Direction::OUT)
            .map(|e| (e.time.unwrap(), e.dst_g_id))
            .collect();
        assert_eq!(actual, vec![(3, 1), (12, 1)]);

        let actual: Vec<(i64, u64)> = g
            .vertex_edges_window_t(1, &(0..13), Direction::IN)
            .map(|e| (e.time.unwrap(), e.src_g_id))
            .collect();
        assert_eq!(actual, vec![(3, 9), (12, 9)]);
    }

    #[test]
    fn add_edges_at_t1t2t3_check_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge(4, 11, 22);
        g.add_edge(5, 22, 33);
        g.add_edge(6, 11, 44);

        let actual = g.vertices_window(1..4).map(|v| v.g_id).collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33]);

        let actual = g.vertices_window(1..6).map(|v| v.g_id).collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33, 44]);

        let actual = g
            .vertex_edges_window(11, &(1..5), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);

        let actual = g
            .vertex_edges_window_t(11, &(1..5), Direction::OUT)
            .map(|e| (e.time.unwrap(), e.dst_g_id))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(4, 22)]);

        let actual = g
            .vertex_edges_window_t(44, &(1..17), Direction::IN)
            .map(|e| (e.time.unwrap(), e.src_g_id))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(6, 11)]);

        let actual = g
            .vertex_edges_window(44, &(1..6), Direction::IN)
            .map(|e| e.dst_g_id)
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        let actual = g
            .vertex_edges_window(44, &(1..7), Direction::IN)
            .map(|e| e.src_g_id)
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![11];
        assert_eq!(actual, expected);

        let actual = g
            .vertex_edges_window(44, &(9..100), Direction::IN)
            .map(|e| e.dst_g_id)
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected)
    }

    #[test]
    fn add_the_same_edge_multiple_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge(4, 11, 22);
        g.add_edge(4, 11, 22);

        let actual = g
            .vertex_edges_window(11, &(1..5), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);
    }

    #[test]
    fn add_edge_with_1_property() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::U32(12))]);

        let edge_weights = g
            .vertex_edges(11, Direction::OUT)
            .flat_map(|e| {
                g.edge_prop(e.edge_id, "weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12)])
    }

    #[test]
    fn add_edge_with_multiple_properties() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        let edge_weights = g
            .vertex_edges(11, Direction::OUT)
            .flat_map(|e| {
                let weight = g
                    .edge_prop(e.edge_id, "weight")
                    .map(|x| x.collect::<Vec<_>>());
                let amount = g
                    .edge_prop(e.edge_id, "amount")
                    .map(|x| x.collect::<Vec<_>>());
                let label = g
                    .edge_prop(e.edge_id, "label")
                    .map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![
                (&4, Prop::U32(12)),
                (&4, Prop::F64(12.34)),
                (&4, Prop::Str("blerg".into())),
            ]
        )
    }

    #[test]
    fn add_edge_with_1_property_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(4, 11, 22, &vec![("amount".into(), Prop::U32(12))]);
        g.add_edge_with_props(7, 11, 22, &vec![("amount".into(), Prop::U32(24))]);
        g.add_edge_with_props(19, 11, 22, &vec![("amount".into(), Prop::U32(48))]);

        let edge_weights = g
            .vertex_edges_window(11, &(4..8), Direction::OUT)
            .flat_map(|e| {
                g.edge_prop_window(e.edge_id, "amount", 4..8).map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)]);

        let edge_weights = g
            .vertex_edges_window(22, &(4..8), Direction::IN)
            .flat_map(|e| {
                g.edge_prop_window(e.edge_id, "amount", 4..8).map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            3,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![("label".into(), Prop::Str("blerg_again".into()))],
        );

        g.add_edge_with_props(
            5,
            22,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        let edge_weights = g
            .vertex_edges_window(11, &(3..5), Direction::OUT)
            .flat_map(|e| {
                let weight = g
                    .edge_prop_window(e.edge_id, "weight", 3..5)
                    .map(|x| x.collect::<Vec<_>>());
                let amount = g
                    .edge_prop_window(e.edge_id, "amount", 3..5)
                    .map(|x| x.collect::<Vec<_>>());
                let label = g
                    .edge_prop_window(e.edge_id, "label", 3..5)
                    .map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![
                (&3, Prop::U32(12)),
                (&3, Prop::Str("blerg".into())),
                (&4, Prop::Str("blerg_again".into()))
            ]
        )
    }

    #[test]
    fn edge_metadata_id_bug() {
        let mut g = TemporalGraph::default();

        let edges: Vec<(i64, u64, u64)> = vec![(1, 1, 2), (2, 3, 4), (3, 5, 4), (4, 1, 4)];

        for (t, src, dst) in edges {
            g.add_vertex(t, src);
            g.add_vertex(t, dst);
            g.add_edge_with_props(t, src, dst, &vec![("amount".into(), Prop::U64(12))]);
        }
    }

    #[test]
    fn add_multiple_edges_with_1_property_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::F32(1122.0))]);
        g.add_edge_with_props(4, 11, 33, &vec![("weight".into(), Prop::F32(1133.0))]);
        g.add_edge_with_props(4, 44, 11, &vec![("weight".into(), Prop::F32(4411.0))]);

        let edge_weights_out_11 = g
            .vertex_edges(11, Direction::OUT)
            .flat_map(|e| {
                g.edge_prop(e.edge_id, "weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights_out_11, vec![(&4, 1122.0), (&4, 1133.0)]);

        let edge_weights_into_11 = g
            .vertex_edges(11, Direction::IN)
            .flat_map(|e| {
                g.edge_prop(e.edge_id, "weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights_into_11, vec![(&4, 4411.0)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            3,
            22,
            33,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            4,
            33,
            44,
            &vec![("label".into(), Prop::Str("blerg".into()))],
        );

        g.add_edge_with_props(
            5,
            44,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        // betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
        let vs = g.vertices_window(2..4).map(|v| v.g_id).collect::<Vec<_>>();

        assert_eq!(vs, vec![11, 22, 33]);

        // between t: 3 and t:6 (excluded) show the visible outbound edges
        let vs = g
            .vertices_window(3..6)
            .flat_map(|v| {
                g.vertex_edges_window(v.g_id, &(3..6), Direction::OUT)
                    .map(|e| e.dst_g_id)
                    .collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
            })
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![33, 44, 11]);

        let edge_weights = g
            .vertex_edges(11, Direction::OUT)
            .flat_map(|e| {
                let weight = g
                    .edge_prop(e.edge_id, "weight")
                    .map(|x| x.collect::<Vec<_>>());
                let amount = g
                    .edge_prop(e.edge_id, "amount")
                    .map(|x| x.collect::<Vec<_>>());
                let label = g
                    .edge_prop(e.edge_id, "label")
                    .map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![(&2, Prop::F64(12.34)), (&2, Prop::Str("blerg".into()))]
        )
    }

    #[test]
    fn get_edges() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge(4, 11, 22);
        g.add_edge(5, 22, 33);
        g.add_edge(6, 11, 44);

        assert_eq!(
            g.edge(11, 22),
            Some(EdgeView {
                edge_id: 1,
                src_g_id: 11,
                dst_g_id: 22,
                src_id: 0,
                dst_id: 1,
                time: None,
                is_remote: false
            })
        );
        assert_eq!(g.edge(11, 33), None);

        assert_eq!(
            g.edge_window(11, 22, &(1..5)),
            Some(EdgeView {
                edge_id: 1,
                src_g_id: 11,
                dst_g_id: 22,
                src_id: 0,
                dst_id: 1,
                time: None,
                is_remote: false
            })
        );
        assert_eq!(g.edge_window(11, 22, &(1..4)), None);
        assert_eq!(g.edge_window(11, 22, &(5..6)), None);
        assert_eq!(
            g.edge_window(11, 22, &(4..5)),
            Some(EdgeView {
                edge_id: 1,
                src_g_id: 11,
                dst_g_id: 22,
                src_id: 0,
                dst_id: 1,
                time: None,
                is_remote: false
            })
        );

        let mut g = TemporalGraph::default();
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            g.add_edge(t, src, dst)
        }
        assert_eq!(
            g.edge_window(1, 3, &(i64::MIN..i64::MAX)).unwrap().src_g_id,
            1u64
        );
        assert_eq!(
            g.edge_window(1, 3, &(i64::MIN..i64::MAX)).unwrap().dst_g_id,
            3u64
        );
    }

    #[test]
    fn correctness_degree_test() {
        let mut g = TemporalGraph::default();

        let triplets = vec![
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        for i in 1..4 {
            let out1 = g
                .vertex_edges(i, Direction::OUT)
                .map(|e| e.dst_g_id)
                .collect_vec();
            let out2 = g
                .vertex_edges_window(i, &(1..7), Direction::OUT)
                .map(|e| e.dst_g_id)
                .collect_vec();

            assert_eq!(out1, out2);
            assert_eq!(
                g.degree(i, Direction::OUT),
                g.degree_window(i, &(1..7), Direction::OUT)
            );
            assert_eq!(
                g.degree(i, Direction::IN),
                g.degree_window(i, &(1..7), Direction::IN)
            );
        }

        let degrees = g
            .vertices()
            .map(|v| {
                (
                    v.g_id,
                    g.degree(v.g_id, Direction::IN),
                    g.degree(v.g_id, Direction::OUT),
                    g.degree(v.g_id, Direction::BOTH),
                )
            })
            .collect_vec();

        let degrees_window = g
            .vertices_window(1..7)
            .map(|v| {
                (
                    v.g_id,
                    g.degree(v.g_id, Direction::IN),
                    g.degree(v.g_id, Direction::OUT),
                    g.degree(v.g_id, Direction::BOTH),
                )
            })
            .collect_vec();

        let expected = vec![(1, 1, 2, 2), (2, 1, 0, 1), (3, 1, 1, 1)];

        assert_eq!(degrees, expected);
        assert_eq!(degrees_window, expected);
    }

    #[test]
    fn lotr_degree() {
        let mut g = TemporalGraph::default();

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        let data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv"]
            .iter()
            .collect();

        if !data_dir.exists() {
            panic!("Missing data dir = {}", data_dir.to_str().unwrap())
        }

        if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = utils::calculate_hash(&src);

                        let dst_id = utils::calculate_hash(&dst);

                        g.add_vertex(t, src_id);
                        g.add_vertex(t, dst_id);
                        g.add_edge_with_props(t, src_id, dst_id, &vec![]);
                    }
                }
            }
        }

        // query the various graph windows
        // 9501 .. 10001

        let w = 9501..10001;
        let mut degrees_w1 = g
            .vertices_window(w.clone())
            .map(|v| {
                (
                    v.g_id,
                    g.degree_window(v.g_id, &w, Direction::IN),
                    g.degree_window(v.g_id, &w, Direction::OUT),
                    g.degree_window(v.g_id, &w, Direction::BOTH),
                )
            })
            .collect_vec();

        let mut expected_degrees_w1 = vec![
            ("Balin", 0, 5, 5),
            ("Frodo", 4, 4, 8),
            ("Thorin", 0, 1, 1),
            ("Fundin", 1, 0, 1),
            ("Ori", 0, 1, 1),
            ("Pippin", 0, 3, 3),
            ("Merry", 2, 1, 3),
            ("Bilbo", 4, 0, 4),
            ("Gimli", 2, 2, 4),
            ("Legolas", 2, 0, 2),
            ("Sam", 0, 1, 1),
            ("Gandalf", 1, 2, 3),
            ("Boromir", 1, 0, 1),
            ("Aragorn", 3, 1, 4),
            ("Daeron", 1, 0, 1),
        ]
        .into_iter()
        .map(|(name, indeg, outdeg, deg)| (utils::calculate_hash(&name), indeg, outdeg, deg))
        .collect_vec();

        expected_degrees_w1.sort();
        degrees_w1.sort();

        assert_eq!(degrees_w1, expected_degrees_w1);

        // 19001..20001
        let mut expected_degrees_w2 = vec![
            ("Elrond", 1, 0, 1),
            ("Peregrin", 0, 1, 1),
            ("Pippin", 0, 4, 4),
            ("Merry", 2, 1, 3),
            ("Gimli", 0, 2, 2),
            ("Wormtongue", 0, 1, 1),
            ("Legolas", 1, 1, 2),
            ("Sam", 1, 0, 1),
            ("Saruman", 1, 1, 2),
            ("Treebeard", 0, 1, 1),
            ("Gandalf", 3, 3, 6),
            ("Aragorn", 7, 0, 7),
            ("Shadowfax", 1, 1, 2),
            ("Elendil", 0, 1, 1),
        ]
        .into_iter()
        .map(|(name, indeg, outdeg, deg)| (utils::calculate_hash(&name), indeg, outdeg, deg))
        .collect_vec();

        let w = 19001..20001;
        let mut degrees_w2 = g
            .vertices_window(w.clone())
            .map(|v| {
                (
                    v.g_id,
                    g.degree_window(v.g_id, &w, Direction::IN),
                    g.degree_window(v.g_id, &w, Direction::OUT),
                    g.degree_window(v.g_id, &w, Direction::BOTH),
                )
            })
            .collect_vec();

        expected_degrees_w2.sort();
        degrees_w2.sort();

        assert_eq!(degrees_w2, expected_degrees_w2);
    }

    #[test]
    fn vertex_neighbours() {
        let mut g = TemporalGraph::default();

        let triplets = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        for (t, src, dst) in triplets {
            g.add_edge(t, src, dst);
        }

        let neighbours = g
            .vertices()
            .map(|v| {
                (
                    v.g_id,
                    g.neighbours(v.g_id, Direction::IN)
                        .map(|v| v.g_id)
                        .collect_vec(),
                    g.neighbours(v.g_id, Direction::OUT)
                        .map(|v| v.g_id)
                        .collect_vec(),
                    g.neighbours(v.g_id, Direction::BOTH)
                        .map(|v| v.g_id)
                        .collect_vec(),
                )
            })
            .collect_vec();

        let w = i64::MIN..i64::MAX;
        let neighbours_window = g
            .vertices_window(w.clone())
            .map(|v| {
                (
                    v.g_id,
                    g.neighbours_window(v.g_id, &w, Direction::IN)
                        .map(|v| v.g_id)
                        .collect_vec(),
                    g.neighbours_window(v.g_id, &w, Direction::OUT)
                        .map(|v| v.g_id)
                        .collect_vec(),
                    g.neighbours_window(v.g_id, &w, Direction::BOTH)
                        .map(|v| v.g_id)
                        .collect_vec(),
                )
            })
            .collect_vec();

        let expected = vec![
            (1, vec![1, 2], vec![1, 2, 3], vec![1, 2, 1, 2, 3]),
            (2, vec![1, 3], vec![1], vec![1, 3, 1]),
            (3, vec![1], vec![2], vec![1, 2]),
        ];

        assert_eq!(neighbours, expected);
        assert_eq!(neighbours_window, expected);
    }

    #[test]
    fn find_vertex() {
        let mut g = TemporalGraph::default();

        let triplets = vec![
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        let pid = *(g.logical_to_physical.get(&1).unwrap());

        let actual = g.vertex(1);
        let expected = Some(VertexView {
            g_id: 1,
            pid: Some(pid),
        });

        assert_eq!(actual, expected);

        let actual = g.vertex(10);
        let expected = None;

        assert_eq!(actual, expected);

        let actual = g.vertex_window(1, &(0..3));
        let expected = Some(VertexView {
            g_id: 1,
            pid: Some(pid),
        });

        assert_eq!(actual, expected);

        let actual = g.vertex_window(10, &(0..3));
        let expected = None;

        assert_eq!(actual, expected);

        let actual = g.vertex_window(1, &(0..1));
        let expected = None;

        assert_eq!(actual, expected);
    }

    #[quickcheck]
    fn add_vertices_into_two_graph_partitions(vs: Vec<(u64, u64)>) {
        let mut g1 = TemporalGraph::default();

        let mut g2 = TemporalGraph::default();

        let mut shards = vec![&mut g1, &mut g2];
        let some_props: Vec<(String, Prop)> = vec![("bla".to_string(), Prop::U32(1))];

        let n_shards = shards.len();
        for (t, (src, dst)) in vs.into_iter().enumerate() {
            let src_shard = utils::get_shard_id_from_global_vid(src, n_shards);
            let dst_shard = utils::get_shard_id_from_global_vid(dst, n_shards);

            shards[src_shard].add_vertex(t.try_into().unwrap(), src.into());
            shards[dst_shard].add_vertex(t.try_into().unwrap(), dst.into());

            if src_shard == dst_shard {
                shards[src_shard].add_edge_with_props(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
                    &some_props,
                );
            } else {
                shards[src_shard].add_edge_remote_out(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
                    &some_props,
                );
                shards[dst_shard].add_edge_remote_into(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
                    &some_props,
                );
            }
        }
    }

    #[test]
    fn adding_remote_edge_does_not_break_local_indices() {
        let mut g1 = TemporalGraph::default();
        g1.add_edge_remote_out(11, 1, 1, &vec![("bla".to_string(), Prop::U32(1))]);
        g1.add_edge_with_props(11, 0, 2, &vec![("bla".to_string(), Prop::U32(1))]);
    }

    #[test]
    fn check_edges_after_adding_remote() {
        let mut g1 = TemporalGraph::default();
        g1.add_vertex(1, 11);

        g1.add_edge_remote_out(2, 11, 22, &vec![("bla".to_string(), Prop::U32(1))]);

        let actual = g1
            .vertex_edges_window(11, &(1..3), Direction::OUT)
            .map(|e| e.dst_g_id)
            .collect_vec();
        assert_eq!(actual, vec![22]);

        let actual = g1
            .edges_iter_window(0, &(1..3), Direction::OUT)
            .map(|(id, edge)| (id, edge.is_local()))
            .collect_vec();
        assert_eq!(actual, vec![(22, false)])
    }

    // this test checks TemporalGraph can be serialized and deserialized
    #[test]
    fn serialize_and_deserialize_with_bincode() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 1);
        g.add_vertex(2, 2);

        g.add_vertex(3, 3);
        g.add_vertex(4, 1);

        g.add_edge_with_props(1, 2, 3, &vec![("bla".to_string(), Prop::U32(1))]);
        g.add_edge_with_props(3, 4, 4, &vec![("bla1".to_string(), Prop::U64(1))]);
        g.add_edge_with_props(
            4,
            1,
            5,
            &vec![("bla2".to_string(), Prop::Str("blergo blargo".to_string()))],
        );

        let mut buffer: Vec<u8> = Vec::new();

        bincode::serialize_into(&mut buffer, &g).unwrap();

        let g2: TemporalGraph = bincode::deserialize_from(&mut buffer.as_slice()).unwrap();
        assert_eq!(g, g2);
    }
}
