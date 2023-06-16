//! A data structure for representing temporal graphs.

use self::errors::MutateGraphError;
use crate::core::edge_layer::{EdgeLayer, VID};
use crate::core::edge_ref::EdgeRef;
use crate::core::props::Props;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex::InputVertex;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::Direction;
use crate::core::{Prop, Time};
use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Range};

pub(crate) mod errors {
    use crate::core::props::IllegalMutate;

    #[derive(thiserror::Error, Debug, PartialEq)]
    pub enum MutateGraphError {
        #[error("Create vertex '{vertex_id}' first before adding static properties to it")]
        VertexNotFoundError { vertex_id: u64 },
        #[error("cannot change property for vertex '{vertex_id}'")]
        IllegalVertexPropertyChange {
            vertex_id: u64,
            source: IllegalMutate,
        },
        #[error("Failed to update graph property")]
        IllegalGraphPropertyChange { source: IllegalMutate },
        #[error("Create edge '{0}' -> '{1}' first before adding static properties to it")]
        MissingEdge(u64, u64), // src, dst
        #[error("cannot change property for edge '{src_id}' -> '{dst_id}'")]
        IllegalEdgePropertyChange {
            src_id: u64,
            dst_id: u64,
            source: IllegalMutate,
        },
        #[error("cannot update property as is '{first_type}' and '{second_type}' given'")]
        PropertyChangedType {
            first_type: &'static str,
            second_type: &'static str,
        },
    }
}

pub type MutateGraphResult = Result<(), MutateGraphError>;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TemporalGraph {
    id: usize,
    // Maps global (logical) id to the local (physical) id which is an index to the adjacency list vector
    pub(crate) logical_to_physical: FxHashMap<u64, usize>,

    // global ids in insertion order for fast iterations, maps physical ids to logical ids
    pub(crate) logical_ids: Vec<u64>,

    // Set of timestamps per vertex for fast window filtering
    timestamps: Vec<TimeIndex>,

    // Properties abstraction for both vertices and edges
    pub(crate) vertex_props: Props,

    pub(crate) graph_props: Props,

    // Edge layers
    pub(crate) layers: Vec<EdgeLayer>,

    //earliest time seen in this graph
    pub(crate) earliest_time: i64,

    //latest time seen in this graph
    pub(crate) latest_time: i64,
}

impl TemporalGraph {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            logical_to_physical: Default::default(),
            logical_ids: Default::default(),
            timestamps: Default::default(),
            vertex_props: Default::default(),
            graph_props: Default::default(),
            layers: vec![EdgeLayer::new(0, id)],
            earliest_time: i64::MAX,
            latest_time: i64::MIN,
        }
    }
}

// Internal helpers
impl TemporalGraph {
    /// Checks if vertex ref is actually local and returns appropriate ID (either local pid or global id)
    #[inline(always)]
    fn local_id(&self, v: VertexRef) -> VID {
        match v {
            VertexRef::Local(LocalVertexRef { pid, .. }) => VID::Local(pid),
            VertexRef::Remote(gid) => match self.logical_to_physical.get(&gid) {
                Some(v_pid) => VID::Local(*v_pid),
                None => VID::Remote(gid),
            },
        }
    }

    fn new_local_vertex(&self, pid: usize) -> LocalVertexRef {
        LocalVertexRef {
            shard_id: self.id,
            pid,
        }
    }

    pub fn local_vertex(&self, v: VertexRef) -> Option<LocalVertexRef> {
        match v {
            VertexRef::Local(v) => {
                (v.shard_id == self.id && v.pid < self.logical_ids.len()).then_some(v)
            }
            VertexRef::Remote(gid) => self.vertex(gid),
        }
    }
}

// Layer management:
impl TemporalGraph {
    // TODO: we can completely replace this function with `layer_iter` if we are sure that doesn't
    // affect performance
    fn layer_iter(&self, id: Option<usize>) -> LayerIterator {
        if self.layers.len() == 1 {
            LayerIterator::Single(&self.layers[0])
        } else {
            match id {
                Some(id) => LayerIterator::Single(&self.layers[id]),
                None => LayerIterator::Vector(&self.layers),
            }
        }
    }
}

enum LayerIterator<'a> {
    Single(&'a EdgeLayer),
    Vector(&'a Vec<EdgeLayer>),
}

impl TemporalGraph {
    /// Global id of vertex
    pub fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        self.logical_ids[v.pid]
    }

    pub(crate) fn allocate_layer(&mut self, id: usize) {
        self.layers.push(EdgeLayer::new(id, self.id));
        assert_eq!(self.layers.len(), id + 1)
    }

    pub(crate) fn len(&self) -> usize {
        self.logical_ids.len()
    }

    pub(crate) fn out_edges_len(&self, layer: Option<usize>) -> usize {
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => layer.out_edges_len(),
            LayerIterator::Vector(_) => self
                .vertices()
                .map(|v| self.degree(v, Direction::OUT, None))
                .sum(),
        }
    }

    pub(crate) fn has_edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.layers[layer].has_edge(self.local_id(src), self.local_id(dst), None)
    }

    pub(crate) fn has_edge_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        w: &Range<i64>,
        layer: usize,
    ) -> bool {
        self.layers[layer].has_edge(self.local_id(src), self.local_id(dst), Some(w.clone()))
    }

    pub(crate) fn has_vertex(&self, v: VertexRef) -> bool {
        self.local_vertex(v).is_some()
    }

    pub(crate) fn add_vertex<T: InputVertex>(&mut self, t: i64, v: T) -> MutateGraphResult {
        self.add_vertex_with_props(t, v, &vec![])
    }

    pub(crate) fn add_vertex_with_props<T: InputVertex>(
        &mut self,
        t: i64,
        v: T,
        props: &Vec<(String, Prop)>,
    ) -> MutateGraphResult {
        //Updating time - only needs to be here as every other adding function calls this one
        if self.earliest_time > t {
            self.earliest_time = t
        }
        if self.latest_time < t {
            self.latest_time = t
        }

        let index = match self.logical_to_physical.get(&v.id()) {
            None => {
                let physical_id: usize = self.logical_ids.len();
                self.logical_to_physical.insert(v.id(), physical_id);
                self.logical_ids.push(v.id());
                let timestamps = TimeIndex::one(t);
                self.timestamps.push(timestamps);
                physical_id
            }
            Some(pid) => {
                self.timestamps[*pid].insert(t);
                *pid
            }
        };
        if let Some(n) = v.name_prop() {
            let result = self
                .vertex_props
                .set_static_props(index, &vec![("_id".to_string(), n)]);
            result.map_err(|e| MutateGraphError::IllegalVertexPropertyChange {
                vertex_id: v.id(),
                source: e,
            })?
        }
        Ok(self.vertex_props.upsert_temporal_props(t, index, props))
    }

    pub(crate) fn add_property(&mut self, t: i64, props: &Vec<(String, Prop)>) {
        self.graph_props.upsert_temporal_props(t, 0, props)
    }

    pub(crate) fn add_static_property(&mut self, props: &Vec<(String, Prop)>) -> MutateGraphResult {
        self.graph_props
            .set_static_props(0, props)
            .map_err(|e| MutateGraphError::IllegalGraphPropertyChange { source: e })
    }

    pub(crate) fn add_vertex_properties(
        &mut self,
        v: u64,
        data: &Vec<(String, Prop)>,
    ) -> MutateGraphResult {
        let index = *(self
            .logical_to_physical
            .get(&v)
            .ok_or(MutateGraphError::VertexNotFoundError { vertex_id: v })?);
        let result = self.vertex_props.set_static_props(index, data);
        result.map_err(|e| MutateGraphError::IllegalVertexPropertyChange {
            vertex_id: v,
            source: e,
        }) // TODO: use the name here if exists
    }

    // TODO: remove this??? it's only used for tests, we can use the other one instead
    pub fn add_edge<T: InputVertex>(&mut self, t: i64, src: T, dst: T, layer: usize) {
        self.add_edge_with_props(t, src, dst, &vec![], layer)
    }

    pub(crate) fn add_edge_with_props<T: InputVertex>(
        &mut self,
        t: i64,
        src: T,
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) {
        let src_id = src.id();
        let dst_id = dst.id();
        // mark the times of the vertices at t
        self.add_vertex(t, src)
            .map_err(|err| println!("{:?}", err))
            .ok();
        self.add_vertex(t, dst)
            .map_err(|err| println!("{:?}", err))
            .ok();

        let src_pid = self.logical_to_physical[&src_id];
        let dst_pid = self.logical_to_physical[&dst_id];

        self.layers[layer].add_edge_with_props(t, src_pid, dst_pid, props)
    }

    pub(crate) fn delete_edge<T: InputVertex>(&mut self, t: i64, src: T, dst: T, layer: usize) {
        let src_id = src.id();
        let dst_id = dst.id();
        // mark the times of the vertices at t
        self.add_vertex(t, src)
            .map_err(|err| println!("{:?}", err))
            .ok();
        self.add_vertex(t, dst)
            .map_err(|err| println!("{:?}", err))
            .ok();

        let src_pid = self.logical_to_physical[&src_id];
        let dst_pid = self.logical_to_physical[&dst_id];

        self.layers[layer].delete_edge(t, src_pid, dst_pid)
    }

    pub(crate) fn add_edge_remote_out<T: InputVertex>(
        &mut self,
        t: i64,
        src: T, // we are on the source shard
        dst: T,
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) {
        let src_id = src.id();
        let dst_id = dst.id();

        self.add_vertex(t, src)
            .map_err(|err| println!("{:?}", err))
            .ok();
        let src_pid = self.logical_to_physical[&src_id];
        self.layers[layer].add_edge_remote_out(t, src_pid, dst_id, props)
    }

    pub(crate) fn delete_edge_remote_out<T: InputVertex>(
        &mut self,
        t: i64,
        src: T, // we are on the source shard
        dst: T,
        layer: usize,
    ) {
        let src_id = src.id();
        let dst_id = dst.id();

        self.add_vertex(t, src)
            .map_err(|err| println!("{:?}", err))
            .ok();
        let src_pid = self.logical_to_physical[&src_id];
        self.layers[layer].delete_edge_remote_out(t, src_pid, dst_id)
    }

    pub(crate) fn add_edge_remote_into<T: InputVertex>(
        &mut self,
        t: i64,
        src: T,
        dst: T, // we are on the destination shard
        props: &Vec<(String, Prop)>,
        layer: usize,
    ) {
        let src_id = src.id();
        let dst_id = dst.id();
        self.add_vertex(t, dst)
            .map_err(|err| println!("{:?}", err))
            .ok();
        let dst_pid = self.logical_to_physical[&dst_id];
        self.layers[layer].add_edge_remote_into(t, src_id, dst_pid, props)
    }

    pub(crate) fn delete_edge_remote_into<T: InputVertex>(
        &mut self,
        t: i64,
        src: T,
        dst: T, // we are on the destination shard
        layer: usize,
    ) {
        let src_id = src.id();
        let dst_id = dst.id();
        self.add_vertex(t, dst)
            .map_err(|err| println!("{:?}", err))
            .ok();
        let dst_pid = self.logical_to_physical[&dst_id];
        self.layers[layer].delete_edge_remote_into(t, src_id, dst_pid)
    }

    pub(crate) fn add_edge_properties(
        &mut self,
        src: u64,
        dst: u64,
        data: &Vec<(String, Prop)>,
        layer: usize,
    ) -> MutateGraphResult {
        let edge = self
            .edge(src.into(), dst.into(), layer)
            .ok_or(MutateGraphError::MissingEdge(src, dst))?;
        let result = self.layers[edge.layer()]
            .edge_props_mut(edge)
            .set_static_props(edge.pid(), data);
        result.map_err(|e| MutateGraphError::IllegalEdgePropertyChange {
            src_id: src,
            dst_id: src,
            source: e,
        })
    }

    pub(crate) fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        let v_pid = v.pid;
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => layer.degree(v_pid, d),
            LayerIterator::Vector(layers) => layers
                .iter()
                .map(|layer| layer.vertex_neighbours(v_pid, d))
                .kmerge()
                .dedup()
                .count(),
        }
    }

    pub fn vertex(&self, v: u64) -> Option<LocalVertexRef> {
        let pid = self.logical_to_physical.get(&v)?;
        Some(self.new_local_vertex(*pid))
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send + '_> {
        Box::new((0..self.logical_ids.len()).map(|pid| self.new_local_vertex(pid)))
    }

    pub(crate) fn edge(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        let src = self.local_id(src);
        let dst = self.local_id(dst);
        self.layers[layer].edge(src, dst, None)
    }

    pub(crate) fn edge_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        w: &Range<i64>,
        layer: usize,
    ) -> Option<EdgeRef> {
        let src = self.local_id(src);
        let dst = self.local_id(dst);
        self.layers[layer].edge(src, dst, Some(w.clone()))
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    // This returns edges sorted by neighbour so they are easy to dedup inside neighbours and degree
    pub(crate) fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = v.pid;
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => layer.vertex_edges_iter(v_pid, d),
            LayerIterator::Vector(layers) => {
                let iter = layers
                    .iter()
                    .map(|layer| layer.vertex_edges_iter(v_pid, d))
                    .kmerge_by(|left, right| left.merge_cmp(right));
                Box::new(iter)
            }
        }
    }

    // This returns edges sorted by neighbour so they are easy to dedup inside neighbours_window()
    pub(crate) fn vertex_edges_window(
        &self,
        v: LocalVertexRef,
        w: &Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = v.pid;
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => layer.vertex_edges_iter_window(v_pid, w, d),
            LayerIterator::Vector(layers) => {
                let iter = layers
                    .iter()
                    .map(|layer| layer.vertex_edges_iter_window(v_pid, w, d))
                    .kmerge_by(|left, right| left.merge_cmp(right));
                Box::new(iter)
            }
        }
    }

    #[inline(always)]
    fn vertex_ref_from_vid(&self, vid: VID) -> VertexRef {
        match vid {
            VID::Local(pid) => VertexRef::Local(self.new_local_vertex(pid)),
            VID::Remote(gid) => VertexRef::Remote(gid),
        }
    }

    pub(crate) fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = v.pid;
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => Box::new(
                layer
                    .vertex_neighbours(v_pid, d)
                    .map(|v| self.vertex_ref_from_vid(v)),
            ),

            LayerIterator::Vector(layers) => Box::new(
                layers
                    .iter()
                    .map(|layer| layer.vertex_neighbours(v_pid, d))
                    .kmerge()
                    .dedup()
                    .map(|v| self.vertex_ref_from_vid(v)),
            ),
        }
    }

    pub fn neighbours_window(
        &self,
        v: LocalVertexRef,
        w: &Range<i64>,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = v.pid;
        match self.layer_iter(layer) {
            LayerIterator::Single(layer) => Box::new(
                layer
                    .vertex_neighbours_window(v_pid, d, w)
                    .map(|v| self.vertex_ref_from_vid(v)),
            ),

            LayerIterator::Vector(layers) => Box::new(
                layers
                    .iter()
                    .map(|layer| layer.vertex_neighbours_window(v_pid, d, w))
                    .kmerge()
                    .dedup()
                    .map(|v| self.vertex_ref_from_vid(v)),
            ),
        }
    }

    pub fn static_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<Prop> {
        self.vertex_props.static_prop(v.pid, name)
    }

    pub fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.vertex_props.static_names(v.pid)
    }

    pub fn static_prop_names(&self) -> Vec<String> {
        self.graph_props.static_names(0)
    }

    pub fn static_prop(&self, name: &str) -> Option<Prop> {
        self.graph_props.static_prop(0, name)
    }

    pub(crate) fn temporal_vertex_prop(&self, v: LocalVertexRef, name: &str) -> Option<&TProp> {
        self.vertex_props.temporal_prop(v.pid, name)
    }

    pub fn temporal_prop(&self, name: &str) -> Option<&TProp> {
        self.graph_props.temporal_prop(0, name)
    }

    pub(crate) fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        self.vertex_props.temporal_names(v.pid)
    }

    pub fn temporal_prop_names(&self) -> Vec<String> {
        self.graph_props.temporal_names(0)
    }

    pub fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        self.layers[e.layer()]
            .edge_props(e)
            .static_prop(e.pid(), name)
    }

    pub fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.layers[e.layer()].edge_props(e).static_names(e.pid())
    }

    pub fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<&TProp> {
        self.layers[e.layer()]
            .edge_props(e)
            .temporal_prop(e.pid(), name)
    }

    pub fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.layers[e.layer()].edge_props(e).temporal_names(e.pid())
    }

    pub(crate) fn vertex_additions(&self, src: LocalVertexRef) -> &TimeIndex {
        &self.timestamps[src.pid]
    }

    pub(crate) fn edge_additions(&self, edge: EdgeRef) -> &TimeIndex {
        let layer = &self.layers[edge.layer()];
        &layer.edge_additions(edge)
    }

    pub(crate) fn edge_deletions(&self, edge: EdgeRef) -> &TimeIndex {
        let layer = &self.layers[edge.layer()];
        &layer.edge_deletions(edge)
    }
}

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
mod graph_test {
    use std::{path::PathBuf, vec};

    use csv::StringRecord;
    use itertools::chain;

    use crate::core::utils;

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
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 9).unwrap();

        assert!(g.has_vertex(9.into()));
        // assert!(g.has_vertex_window(9.into(), &(1..15)));
        assert_eq!(
            g.vertices().map(|v| g.vertex_id(v)).collect::<Vec<u64>>(),
            vec![9]
        );
    }

    #[test]
    fn add_vertices_with_1_property() {
        let mut g = TemporalGraph::new(0);

        let v_id = 1;
        let ts = 1;
        g.add_vertex_with_props(ts, v_id, &vec![("type".into(), Prop::Str("wallet".into()))])
            .unwrap();

        assert!(g.has_vertex(v_id.into()));
        // assert!(g.has_vertex_window(v_id.into(), &(1..15)));
        assert_eq!(
            g.vertices().map(|v| g.vertex_id(v)).collect::<Vec<u64>>(),
            vec![v_id]
        );

        let res: Vec<(i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                g.temporal_vertex_prop(v, "type")
                    .into_iter()
                    .flat_map(|p| p.iter())
            })
            .collect();

        assert_eq!(res, vec![(1i64, Prop::Str("wallet".into()))]);
    }

    #[test]
    fn add_vertices_with_multiple_properties() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        )
        .unwrap();

        let res: Vec<(i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                let type_ = g
                    .temporal_vertex_prop(v, "type")
                    .into_iter()
                    .flat_map(|p| p.iter());
                let active = g
                    .temporal_vertex_prop(v, "active")
                    .into_iter()
                    .flat_map(|p| p.iter());
                chain!(type_, active)
            })
            .collect();

        assert_eq!(
            res,
            vec![(1i64, Prop::Str("wallet".into())), (1i64, Prop::U32(0)),]
        );
    }

    #[test]
    fn add_vertices_with_1_property_different_times() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        )
        .unwrap();

        g.add_vertex_with_props(
            2,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(1)),
            ],
        )
        .unwrap();

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(2)),
            ],
        )
        .unwrap();

        let res: Vec<(i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                let type_ = g
                    .temporal_vertex_prop(v, "type")
                    .into_iter()
                    .flat_map(|p| p.iter_window(2..3));
                let active = g
                    .temporal_vertex_prop(v, "active")
                    .into_iter()
                    .flat_map(|p| p.iter_window(2..3));
                chain!(type_, active)
            })
            .collect();

        assert_eq!(
            res,
            vec![(2i64, Prop::Str("wallet".into())), (2i64, Prop::U32(1)),]
        );
    }

    #[test]
    fn add_vertices_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        )
        .unwrap();

        g.add_vertex_with_props(2, 1, &vec![("label".into(), Prop::I32(12345))])
            .unwrap();

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("origin".into(), Prop::F32(0.1)),
                ("active".into(), Prop::U32(2)),
            ],
        )
        .unwrap();

        let res: Vec<(i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                let type_ = g
                    .temporal_vertex_prop(v, "type")
                    .into_iter()
                    .flat_map(|p| p.iter_window(1..2));
                let active = g
                    .temporal_vertex_prop(v, "active")
                    .into_iter()
                    .flat_map(|p| p.iter_window(2..5));
                let label = g
                    .temporal_vertex_prop(v, "label")
                    .into_iter()
                    .flat_map(|p| p.iter_window(2..5));
                let origin = g
                    .temporal_vertex_prop(v, "origin")
                    .into_iter()
                    .flat_map(|p| p.iter_window(2..5));
                chain!(type_, active, label, origin)
            })
            .collect();

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
    #[ignore = "Undecided on the semantics of the time window over vertices shoule be supported in raphtory"]
    fn add_vertex_at_time_t1_window() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(9, 1).unwrap();

        assert!(g.has_vertex(9.into()));
        // assert!(g.has_vertex_window(9.into(), &(1..15)));
        // assert!(g.has_vertex_window(9.into(), &(5..15))); // FIXME: this is wrong and we might need a different kind of window here
    }

    // #[test]
    // fn add_vertex_at_time_t1_t2() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     g.add_vertex(1, 9).unwrap();
    //     g.add_vertex(2, 1).unwrap();
    //
    //     let actual: Vec<u64> = g.vertices_window(0..2).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![9]);
    //     let actual: Vec<u64> = g.vertices_window(2..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![1]);
    //     let actual: Vec<u64> = g.vertices_window(0..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![9, 1]);
    // }

    // #[test]
    // fn add_edge_at_time_t1() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     g.add_vertex(1, 9).unwrap();
    //     g.add_vertex(2, 1).unwrap();
    //
    //     let v9 = g.vertex(9).unwrap();
    //     let v1 = g.vertex(1).unwrap();
    //
    //     // 9 and 1 are not visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     let expected: Vec<u64> = vec![];
    //     assert_eq!(actual, expected);
    //
    //     g.add_edge(3, 9, 1, 0);
    //
    //     // 9 and 1 are now visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![9, 1]);
    //
    //     // the outbound neighbours of 9 at time 0..2 is the empty set
    //     let actual: Vec<EdgeRef> = g
    //         .vertex_edges_window(v9, &(0..2), Direction::OUT, None)
    //         .collect();
    //     assert!(actual.is_empty());
    //
    //     // the outbound neighbours of 9 at time 0..4 are 1
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..4), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![1]);
    //
    //     // the inbound neighbours of 1 at time 0..4 are 9
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v1, &(0..4), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.src().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![9]);
    // }

    #[test]
    fn has_edge() {
        let mut g = TemporalGraph::new(0);
        g.add_vertex(1, 8).unwrap();
        g.add_vertex(1, 9).unwrap();
        g.add_vertex(2, 10).unwrap();
        g.add_vertex(2, 11).unwrap();
        g.add_edge(3, 9, 8, 0);
        g.add_edge(3, 8, 9, 0);
        g.add_edge(3, 9, 11, 0);

        assert_eq!(g.has_edge(8.into(), 9.into(), 0), true);
        assert_eq!(g.has_edge(9.into(), 8.into(), 0), true);
        assert_eq!(g.has_edge(9.into(), 11.into(), 0), true);
        assert_eq!(g.has_edge(11.into(), 9.into(), 0), false);
        assert_eq!(g.has_edge(10.into(), 11.into(), 0), false);
        assert_eq!(g.has_edge(10.into(), 9.into(), 0), false);
        assert_eq!(g.has_edge(100.into(), 101.into(), 0), false);
    }

    #[test]
    fn edge_exists_inside_window() {
        let mut g = TemporalGraph::new(0);
        g.add_vertex(1, 5).unwrap();
        g.add_vertex(2, 7).unwrap();
        g.add_edge(3, 5, 7, 0);

        let v5 = g.vertex(5).unwrap();

        let actual: Vec<bool> = g
            .vertex_edges_window(v5, &(0..4), Direction::OUT, None)
            .map(|e| g.has_edge(e.src(), e.dst(), 0))
            .collect();

        assert_eq!(actual, vec![true]);
    }

    #[test]
    fn edge_does_not_exists_outside_window() {
        let mut g = TemporalGraph::new(0);
        g.add_vertex(5, 9).unwrap();
        g.add_vertex(7, 10).unwrap();
        g.add_edge(8, 9, 10, 0);
        let v9 = g.vertex(9).unwrap();

        let actual: Vec<bool> = g
            .vertex_edges_window(v9, &(0..4), Direction::OUT, None)
            .map(|e| g.has_edge(e.src(), e.dst(), 0))
            .collect();

        //return empty as no edges in this window
        assert_eq!(actual, Vec::<bool>::new());
    }

    // #[test]
    // fn add_edge_at_time_t1_t2_t3() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     g.add_vertex(1, 9).unwrap();
    //     g.add_vertex(2, 1).unwrap();
    //
    //     let v9 = g.vertex(9).unwrap();
    //     let v1 = g.vertex(1).unwrap();
    //
    //     // 9 and 1 are not visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, Vec::<u64>::new());
    //
    //     g.add_edge(3, 9, 1, 0);
    //
    //     // 9 and 1 are now visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![9, 1]);
    //
    //     // the outbound neighbours of 9 at time 0..2 is the empty set
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..2), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     let expected: Vec<u64> = vec![];
    //     assert_eq!(actual, expected);
    //
    //     // the outbound neighbours of 9 at time 0..4 are 1
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..4), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![1]);
    //
    //     // the outbound neighbours of 9 at time 0..4 are 1
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v1, &(0..4), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.src().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![9]);
    // }

    // #[test]
    // fn add_edge_at_time_t1_t2_t3_overwrite() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     g.add_vertex(1, 9).unwrap();
    //     g.add_vertex(2, 1).unwrap();
    //     let v1 = g.vertex(1).unwrap();
    //     let v9 = g.vertex(9).unwrap();
    //
    //     // 9 and 1 are not visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, Vec::<u64>::new());
    //
    //     g.add_edge(3, 9, 1, 0);
    //     g.add_edge(12, 9, 1, 0); // add the same edge again at different time
    //
    //     // 9 and 1 are now visible at time 3
    //     let actual: Vec<u64> = g.vertices_window(3..10).map(|v| g.vertex_id(v)).collect();
    //     assert_eq!(actual, vec![9, 1]);
    //
    //     // the outbound neighbours of 9 at time 0..2 is the empty set
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..2), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     let expected: Vec<u64> = vec![];
    //     assert_eq!(actual, expected);
    //
    //     // the outbound_t neighbours of 9 at time 0..4 are 1
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..4), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![1]);
    //
    //     // the outbound_t neighbours of 9 at time 0..4 are 1
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v1, &(0..4), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.src().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![9]);
    //
    //     let actual: Vec<u64> = g
    //         .vertex_edges_window(v9, &(0..13), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect();
    //     assert_eq!(actual, vec![1]);
    //
    //     // when we look for time we see both variants
    //     let actual: Vec<(i64, u64)> = g
    //         .vertex_edges_window_t(v9, &(0..13), Direction::OUT, None)
    //         .map(|e| (e.time().unwrap(), g.vertex_id(e.dst().local().unwrap())))
    //         .collect();
    //     assert_eq!(actual, vec![(3, 1), (12, 1)]);
    //
    //     let actual: Vec<(i64, u64)> = g
    //         .vertex_edges_window_t(v1, &(0..13), Direction::IN, None)
    //         .map(|e| (e.time().unwrap(), g.vertex_id(e.src().local().unwrap())))
    //         .collect();
    //     assert_eq!(actual, vec![(3, 9), (12, 9)]);
    // }

    // #[test]
    // fn add_edges_at_t1t2t3_check_times() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     g.add_vertex(1, 11).unwrap();
    //     g.add_vertex(2, 22).unwrap();
    //     g.add_vertex(3, 33).unwrap();
    //     g.add_vertex(4, 44).unwrap();
    //     let v11 = g.vertex(11).unwrap();
    //     let v44 = g.vertex(44).unwrap();
    //
    //     g.add_edge(4, 11, 22, 0);
    //     g.add_edge(5, 22, 33, 0);
    //     g.add_edge(6, 11, 44, 0);
    //
    //     let actual = g
    //         .vertices_window(1..4)
    //         .map(|v| g.vertex_id(v))
    //         .collect::<Vec<_>>();
    //
    //     assert_eq!(actual, vec![11, 22, 33]);
    //
    //     let actual = g
    //         .vertices_window(1..6)
    //         .map(|v| g.vertex_id(v))
    //         .collect::<Vec<_>>();
    //
    //     assert_eq!(actual, vec![11, 22, 33, 44]);
    //
    //     let actual = g
    //         .vertex_edges_window(v11, &(1..5), Direction::OUT, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect::<Vec<_>>();
    //     assert_eq!(actual, vec![22]);
    //
    //     let actual = g
    //         .vertex_edges_window_t(v11, &(1..5), Direction::OUT, None)
    //         .map(|e| (e.time().unwrap(), g.vertex_id(e.dst().local().unwrap())))
    //         .collect::<Vec<_>>();
    //     assert_eq!(actual, vec![(4, 22)]);
    //
    //     let actual = g
    //         .vertex_edges_window_t(v44, &(1..17), Direction::IN, None)
    //         .map(|e| (e.time().unwrap(), g.vertex_id(e.src().local().unwrap())))
    //         .collect::<Vec<_>>();
    //     assert_eq!(actual, vec![(6, 11)]);
    //
    //     let actual = g
    //         .vertex_edges_window(v44, &(1..6), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect::<Vec<_>>();
    //     let expected: Vec<u64> = vec![];
    //     assert_eq!(actual, expected);
    //
    //     let actual = g
    //         .vertex_edges_window(v44, &(1..7), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.src().local().unwrap()))
    //         .collect::<Vec<_>>();
    //     let expected: Vec<u64> = vec![11];
    //     assert_eq!(actual, expected);
    //
    //     let actual = g
    //         .vertex_edges_window(v44, &(9..100), Direction::IN, None)
    //         .map(|e| g.vertex_id(e.dst().local().unwrap()))
    //         .collect::<Vec<_>>();
    //     let expected: Vec<u64> = vec![];
    //     assert_eq!(actual, expected)
    // }

    #[test]
    fn add_the_same_edge_multiple_times() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();

        g.add_edge(4, 11, 22, 0);
        g.add_edge(4, 11, 22, 0);

        let v11 = g.vertex(11).unwrap();

        let actual = g
            .vertex_edges_window(v11, &(1..5), Direction::OUT, None)
            .map(|e| g.vertex_id(e.dst().local().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);
    }

    #[test]
    fn add_edge_with_1_property() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        let v11 = g.vertex(11).unwrap();

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::U32(12))], 0);

        let edge_weights = g
            .vertex_edges(v11, Direction::OUT, None)
            .flat_map(|e| {
                g.temporal_edge_prop(e, "weight")
                    .unwrap()
                    .iter()
                    .flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect_vec();

        assert_eq!(edge_weights, vec![(4, 12)])
    }

    #[test]
    fn add_edge_with_multiple_properties() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();

        let v11 = g.vertex(11).unwrap();

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
            0,
        );

        let edge_weights = g
            .vertex_edges(v11, Direction::OUT, None)
            .flat_map(|e| {
                let weight = g.temporal_edge_prop(e, "weight").unwrap().iter();
                let amount = g.temporal_edge_prop(e, "amount").unwrap().iter();
                let label = g.temporal_edge_prop(e, "label").unwrap().iter();
                weight.chain(amount).chain(label)
            })
            .collect_vec();

        assert_eq!(
            edge_weights,
            vec![
                (4, Prop::U32(12)),
                (4, Prop::F64(12.34)),
                (4, Prop::Str("blerg".into())),
            ]
        )
    }

    #[test]
    fn add_edge_with_1_property_different_times() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        let v11 = g.vertex(11).unwrap();
        let v22 = g.vertex(22).unwrap();

        g.add_edge_with_props(4, 11, 22, &vec![("amount".into(), Prop::U32(12))], 0);
        g.add_edge_with_props(7, 11, 22, &vec![("amount".into(), Prop::U32(24))], 0);
        g.add_edge_with_props(19, 11, 22, &vec![("amount".into(), Prop::U32(48))], 0);

        let edge_weights = g
            .vertex_edges_window(v11, &(4..8), Direction::OUT, None)
            .flat_map(|e| {
                g.temporal_edge_prop(e, "amount")
                    .unwrap()
                    .iter_window(4..8)
                    .flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect_vec();

        assert_eq!(edge_weights, vec![(4, 12), (7, 24)]);

        let edge_weights = g
            .vertex_edges_window(v22, &(4..8), Direction::IN, None)
            .flat_map(|e| {
                g.temporal_edge_prop(e, "amount")
                    .unwrap()
                    .iter_window(4..8)
                    .flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect_vec();

        assert_eq!(edge_weights, vec![(4, 12), (7, 24)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        let v11 = g.vertex(11).unwrap();

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
            0,
        );

        g.add_edge_with_props(
            3,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
            0,
        );

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![("label".into(), Prop::Str("blerg_again".into()))],
            0,
        );

        g.add_edge_with_props(
            5,
            22,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
            0,
        );

        let edge_weights = g
            .vertex_edges_window(v11, &(3..5), Direction::OUT, None)
            .flat_map(|e| {
                let weight = g.temporal_edge_prop(e, "weight").unwrap().iter_window(3..5);
                let amount = g.temporal_edge_prop(e, "amount").unwrap().iter_window(3..5);
                let label = g.temporal_edge_prop(e, "label").unwrap().iter_window(3..5);
                weight.chain(amount).chain(label)
            })
            .collect_vec();

        assert_eq!(
            edge_weights,
            vec![
                (3, Prop::U32(12)),
                (3, Prop::Str("blerg".into())),
                (4, Prop::Str("blerg_again".into())),
            ]
        )
    }

    #[test]
    fn edge_metadata_id_bug() {
        let mut g = TemporalGraph::new(0);

        let edges: Vec<(i64, u64, u64)> = vec![(1, 1, 2), (2, 3, 4), (3, 5, 4), (4, 1, 4)];

        for (t, src, dst) in edges {
            g.add_vertex(t, src).unwrap();
            g.add_vertex(t, dst).unwrap();
            g.add_edge_with_props(t, src, dst, &vec![("amount".into(), Prop::U64(12))], 0);
        }
    }

    #[test]
    fn add_multiple_edges_with_1_property_same_time() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        g.add_vertex(3, 33).unwrap();
        g.add_vertex(4, 44).unwrap();
        let v11 = g.vertex(11).unwrap();

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::F32(1122.0))], 0);
        g.add_edge_with_props(4, 11, 33, &vec![("weight".into(), Prop::F32(1133.0))], 0);
        g.add_edge_with_props(4, 44, 11, &vec![("weight".into(), Prop::F32(4411.0))], 0);

        let edge_weights_out_11 = g
            .vertex_edges(v11, Direction::OUT, None)
            .flat_map(|e| {
                g.temporal_edge_prop(e, "weight")
                    .unwrap()
                    .iter()
                    .flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect_vec();

        assert_eq!(edge_weights_out_11, vec![(4, 1122.0), (4, 1133.0)]);

        let edge_weights_into_11 = g
            .vertex_edges(v11, Direction::IN, None)
            .flat_map(|e| {
                g.temporal_edge_prop(e, "weight")
                    .unwrap()
                    .iter()
                    .flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect_vec();

        assert_eq!(edge_weights_into_11, vec![(4, 4411.0)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        g.add_vertex(3, 33).unwrap();
        g.add_vertex(4, 44).unwrap();
        let v11 = g.vertex(11).unwrap();

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
            0,
        );

        g.add_edge_with_props(
            3,
            22,
            33,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
            0,
        );

        g.add_edge_with_props(
            4,
            33,
            44,
            &vec![("label".into(), Prop::Str("blerg".into()))],
            0,
        );

        g.add_edge_with_props(
            5,
            44,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
            0,
        );

        // // betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
        // let vs = g
        //     .vertices_window(2..4)
        //     .map(|v| g.vertex_id(v))
        //     .collect::<Vec<_>>();
        //
        // assert_eq!(vs, vec![11, 22, 33]);
        //
        // // between t: 3 and t:6 (excluded) show the visible outbound edges
        // let vs = g
        //     .vertices_window(3..6)
        //     .flat_map(|v| {
        //         g.vertex_edges_window(v, &(3..6), Direction::OUT, None)
        //             .map(|e| g.vertex_id(e.dst().local().unwrap()))
        //             .collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
        //     })
        //     .collect::<Vec<_>>();
        //
        // assert_eq!(vs, vec![33, 44, 11]);

        let edge_weights = g
            .vertex_edges(v11, Direction::OUT, None)
            .flat_map(|e| {
                let weight = g
                    .temporal_edge_prop(e, "weight")
                    .into_iter()
                    .flat_map(|p| p.iter());
                let amount = g
                    .temporal_edge_prop(e, "amount")
                    .into_iter()
                    .flat_map(|p| p.iter());
                let label = g
                    .temporal_edge_prop(e, "label")
                    .into_iter()
                    .flat_map(|p| p.iter());
                weight.chain(amount).chain(label)
            })
            .collect_vec();

        assert_eq!(
            edge_weights,
            vec![(2, Prop::F64(12.34)), (2, Prop::Str("blerg".into()))]
        )
    }

    #[test]
    fn get_edges() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 11).unwrap();
        g.add_vertex(2, 22).unwrap();
        g.add_vertex(3, 33).unwrap();
        g.add_vertex(4, 44).unwrap();

        g.add_edge(4, 11, 22, 0);
        g.add_edge(5, 22, 33, 0);
        g.add_edge(6, 11, 44, 0);

        let edge = g.edge(11.into(), 22.into(), 0).expect("exists");
        assert_eq!(g.vertex_id(edge.src().local().unwrap()), 11);
        assert_eq!(g.vertex_id(edge.dst().local().unwrap()), 22);
        assert_eq!(edge.layer(), 0);

        assert_eq!(g.edge(11.into(), 33.into(), 0), None);

        let edge = g
            .edge_window(11.into(), 22.into(), &(1..5), 0)
            .expect("exists");
        assert_eq!(g.vertex_id(edge.src().local().unwrap()), 11);
        assert_eq!(g.vertex_id(edge.dst().local().unwrap()), 22);
        assert_eq!(edge.layer(), 0);
        assert_eq!(g.edge_window(11.into(), 22.into(), &(1..4), 0), None);
        assert_eq!(g.edge_window(11.into(), 22.into(), &(5..6), 0), None);

        let edge = g
            .edge_window(11.into(), 22.into(), &(4..5), 0)
            .expect("exists");
        assert_eq!(g.vertex_id(edge.src().local().unwrap()), 11);
        assert_eq!(g.vertex_id(edge.dst().local().unwrap()), 22);
        assert_eq!(edge.layer(), 0);

        let mut g = TemporalGraph::new(0);
        let es = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];
        for (t, src, dst) in es {
            g.add_edge(t, src, dst, 0)
        }
        assert_eq!(
            g.vertex_id(
                g.edge_window(1.into(), 3.into(), &(i64::MIN..i64::MAX), 0)
                    .unwrap()
                    .src()
                    .local()
                    .unwrap()
            ),
            1u64
        );
        assert_eq!(
            g.vertex_id(
                g.edge_window(1.into(), 3.into(), &(i64::MIN..i64::MAX), 0)
                    .unwrap()
                    .dst()
                    .local()
                    .unwrap()
            ),
            3u64
        );
    }

    #[test]
    fn correctness_degree_test() {
        let mut g = TemporalGraph::new(0);

        let triplets = vec![
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        let out_degrees = HashMap::from([(1, 2), (2, 0), (3, 1)]);
        let in_degrees = HashMap::from([(1, 1), (2, 1), (3, 1)]);
        let degrees = HashMap::from([(1, 2), (2, 1), (3, 1)]);

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))], 0);
        }

        for i in 1..4 {
            let vi = g.vertex(i).unwrap();
            assert_eq!(g.degree(vi, Direction::OUT, None), out_degrees[&i]);
            assert_eq!(g.degree(vi, Direction::IN, None), in_degrees[&i]);
            assert_eq!(g.degree(vi, Direction::BOTH, None), degrees[&i])
        }

        let degrees = g
            .vertices()
            .map(|v| {
                (
                    g.vertex_id(v),
                    g.degree(v, Direction::IN, None),
                    g.degree(v, Direction::OUT, None),
                    g.degree(v, Direction::BOTH, None),
                )
            })
            .collect_vec();

        let expected = vec![(1, 1, 2, 2), (2, 1, 0, 1), (3, 1, 1, 1)];

        assert_eq!(degrees, expected);
    }

    #[test]
    fn vertex_neighbours() {
        let mut g = TemporalGraph::new(0);

        let triplets = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        for (t, src, dst) in triplets {
            g.add_edge(t, src, dst, 0);
        }

        let neighbours = g
            .vertices()
            .map(|v| {
                (
                    g.vertex_id(v),
                    g.neighbours(v, Direction::IN, None)
                        .map(|v| g.vertex_id(v.local().unwrap()))
                        .collect_vec(),
                    g.neighbours(v, Direction::OUT, None)
                        .map(|v| g.vertex_id(v.local().unwrap()))
                        .collect_vec(),
                    g.neighbours(v, Direction::BOTH, None)
                        .map(|v| g.vertex_id(v.local().unwrap()))
                        .collect_vec(),
                )
            })
            .collect_vec();

        // let w = i64::MIN..i64::MAX;
        // let neighbours_window = g
        //     .vertices_window(w.clone())
        //     .map(|v| {
        //         (
        //             g.vertex_id(v),
        //             g.neighbours_window(v, &w, Direction::IN, None)
        //                 .map(|v| g.vertex_id(v.local().unwrap()))
        //                 .collect_vec(),
        //             g.neighbours_window(v, &w, Direction::OUT, None)
        //                 .map(|v| g.vertex_id(v.local().unwrap()))
        //                 .collect_vec(),
        //             g.neighbours_window(v, &w, Direction::BOTH, None)
        //                 .map(|v| g.vertex_id(v.local().unwrap()))
        //                 .collect_vec(),
        //         )
        //     })
        //     .collect_vec();

        let expected = vec![
            (1, vec![1, 2], vec![1, 2, 3], vec![1, 2, 3]),
            (2, vec![1, 3], vec![1], vec![1, 3]),
            (3, vec![1], vec![2], vec![1, 2]),
        ];

        assert_eq!(neighbours, expected);
        // assert_eq!(neighbours_window, expected);
    }

    // #[test]
    // fn len_window() {
    //     let mut g = TemporalGraph::new(0);
    //
    //     let triplets = vec![
    //         (1, 1, 2),
    //         (2, 1, 3),
    //         (-2, 2, 5),
    //         (-1, 2, 1),
    //         (0, 1, 1),
    //         (7, 3, 2),
    //         (1, 1, 1),
    //     ];
    //
    //     for (t, src, dst) in triplets {
    //         g.add_edge(t, src, dst, 0);
    //     }
    //
    //     let w = 0..5;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 3);
    //
    //     let w = 0..1;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 1);
    //
    //     let w = 0..0;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 0);
    //
    //     let w = -2..0;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 3);
    //
    //     let w = 0..i64::MAX;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 3);
    //
    //     let w = i64::MIN..i64::MAX;
    //     let len = g.len_window(&w);
    //     assert_eq!(len, 4);
    // }

    #[test]
    fn find_vertex() {
        let mut g = TemporalGraph::new(0);

        let triplets = vec![
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))], 0);
        }

        let actual = g.vertex_id(g.vertex(1).unwrap());
        assert_eq!(actual, 1);

        let actual = g.vertex(10);
        let expected = None;

        assert_eq!(actual, expected);

        // let actual = g.vertex_id(g.vertex_window(1, &(0..3)).unwrap());
        // assert_eq!(actual, 1);
        //
        // let actual = g.vertex_window(10, &(0..3));
        // let expected = None;
        //
        // assert_eq!(actual, expected);
        //
        // let actual = g.vertex_window(1, &(0..1));
        // let expected = None;
        //
        // assert_eq!(actual, expected);
    }

    #[quickcheck]
    fn add_vertices_into_two_graph_partitions(vs: Vec<(u64, u64)>) {
        let mut g1 = TemporalGraph::new(0);
        let mut g2 = TemporalGraph::new(1);

        let mut shards = vec![&mut g1, &mut g2];
        let some_props: Vec<(String, Prop)> = vec![("bla".to_string(), Prop::U32(1))];

        let n_shards = shards.len();
        for (t, (src, dst)) in vs.into_iter().enumerate() {
            let src_shard = utils::get_shard_id_from_global_vid(src, n_shards);
            let dst_shard = utils::get_shard_id_from_global_vid(dst, n_shards);

            shards[src_shard]
                .add_vertex(t.try_into().unwrap(), src as u64)
                .unwrap();
            shards[dst_shard]
                .add_vertex(t.try_into().unwrap(), dst as u64)
                .unwrap();

            if src_shard == dst_shard {
                shards[src_shard].add_edge_with_props(
                    t.try_into().unwrap(),
                    src,
                    dst,
                    &some_props,
                    0,
                );
            } else {
                shards[src_shard].add_edge_remote_out(
                    t.try_into().unwrap(),
                    src,
                    dst,
                    &some_props,
                    0,
                );
                shards[dst_shard].add_edge_remote_into(
                    t.try_into().unwrap(),
                    src,
                    dst,
                    &some_props,
                    0,
                );
            }
        }
    }

    #[test]
    fn adding_remote_edge_does_not_break_local_indices() {
        let mut g1 = TemporalGraph::new(0);
        g1.add_edge_remote_out(11, 1, 1, &vec![("bla".to_string(), Prop::U32(1))], 0);
        g1.add_edge_with_props(11, 0, 2, &vec![("bla".to_string(), Prop::U32(1))], 0);
    }

    #[test]
    fn check_edges_after_adding_remote() {
        let mut g1 = TemporalGraph::new(0);
        g1.add_vertex(1, 11).unwrap();
        let v11 = g1.vertex(11).unwrap();

        g1.add_edge_remote_out(2, 11, 22, &vec![("bla".to_string(), Prop::U32(1))], 0);

        let actual = g1
            .vertex_edges_window(v11, &(1..3), Direction::OUT, None)
            .map(|e| (e.dst(), e.is_remote()))
            .collect_vec();
        assert_eq!(actual, vec![(VertexRef::Remote(22), true)]);
    }

    // this test checks TemporalGraph can be serialized and deserialized
    #[test]
    fn serialize_and_deserialize_with_bincode() {
        let mut g = TemporalGraph::new(0);

        g.add_vertex(1, 1).unwrap();
        g.add_vertex(2, 2).unwrap();

        g.add_vertex(3, 3).unwrap();
        g.add_vertex(4, 1).unwrap();

        g.add_edge_with_props(1, 2, 3, &vec![("bla".to_string(), Prop::U32(1))], 0);
        g.add_edge_with_props(3, 4, 4, &vec![("bla1".to_string(), Prop::U64(1))], 0);
        g.add_edge_with_props(
            4,
            1,
            5,
            &vec![("bla2".to_string(), Prop::Str("blergo blargo".to_string()))],
            0,
        );

        let mut buffer: Vec<u8> = Vec::new();

        bincode::serialize_into(&mut buffer, &g).unwrap();

        let g2: TemporalGraph = bincode::deserialize_from(&mut buffer.as_slice()).unwrap();
        assert_eq!(g, g2);
    }
}
