use crate::{
    entities::{
        edges::edge_ref::EdgeRef,
        nodes::structure::adj::Adj,
        properties::{
            props::{MetadataError, Props},
            tcell::TCell,
        },
        LayerIds, EID, GID, VID,
    },
    storage::{
        timeindex::{TimeIndexEntry, TimeIndexWindow},
        NodeEntry,
    },
    utils::iter::GenLockedIter,
};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GidRef, LayerVariants, ELID},
        storage::timeindex::{TimeIndexLike, TimeIndexOps},
        Direction,
    },
    iter::BoxedLIter,
};
use serde::{Deserialize, Serialize};
use std::{iter, ops::Range};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct NodeStore {
    pub global_id: GID,
    pub vid: VID,
    // each layer represents a separate view of the graph
    pub(crate) layers: Vec<Adj>,
    // props for node
    pub(crate) props: Option<Props>,
    pub node_type: usize,

    /// For every property id keep a hash map of timestamps to values pointing to the property entries in the props vector
    timestamps: NodeTimestamps,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct NodeTimestamps {
    // all the timestamps that have been seen by this node
    pub edge_ts: TCell<ELID>,
    pub props_ts: TCell<Option<usize>>,
}

impl NodeTimestamps {
    pub fn edge_ts(&self) -> &TCell<ELID> {
        &self.edge_ts
    }

    pub fn props_ts(&self) -> &TCell<Option<usize>> {
        &self.props_ts
    }
}

impl<'a> TimeIndexOps<'a> for &'a NodeTimestamps {
    type IndexType = TimeIndexEntry;
    type RangeType = TimeIndexWindow<'a, TimeIndexEntry, NodeTimestamps>;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        self.edge_ts().active(w.clone()) || self.props_ts().active(w)
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        TimeIndexWindow::Range {
            timeindex: *self,
            range: w,
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        let first = self.edge_ts().first();
        let other = self.props_ts().first();

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last(&self) -> Option<Self::IndexType> {
        let last = self.edge_ts().last();
        let other = self.props_ts().last();

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts
            .iter()
            .map(|(t, _)| *t)
            .merge(self.props_ts.iter().map(|(t, _)| *t))
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts
            .iter()
            .rev()
            .map(|(t, _)| *t)
            .merge_by(self.props_ts.iter().rev().map(|(t, _)| *t), |lt, rt| {
                lt >= rt
            })
    }

    fn len(&self) -> usize {
        self.edge_ts.len() + self.props_ts.len()
    }
}

impl<'a> TimeIndexLike<'a> for &'a NodeTimestamps {
    fn range_iter(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts()
            .range_iter(w.clone())
            .merge(self.props_ts().range_iter(w))
    }

    fn range_iter_rev(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.edge_ts()
            .range_iter_rev(w.clone())
            .merge_by(self.props_ts().range_iter_rev(w), |lt, rt| lt >= rt)
    }

    fn range_count(&self, w: Range<Self::IndexType>) -> usize {
        self.edge_ts().range_count(w.clone()) + self.props_ts().range_count(w)
    }

    fn first_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let first = self
            .edge_ts()
            .iter_window(w.clone())
            .next()
            .map(|(t, _)| *t);
        let other = self.props_ts().iter_window(w).next().map(|(t, _)| *t);

        first
            .zip(other)
            .map(|(a, b)| a.min(b))
            .or_else(|| first.or(other))
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        let last = self
            .edge_ts
            .iter_window(w.clone())
            .next_back()
            .map(|(t, _)| *t);
        let other = self.props_ts.iter_window(w).next_back().map(|(t, _)| *t);

        last.zip(other)
            .map(|(a, b)| a.max(b))
            .or_else(|| last.or(other))
    }
}

impl NodeStore {
    #[inline]
    pub fn is_initialised(&self) -> bool {
        self.vid != VID::default()
    }

    #[inline]
    pub fn init(&mut self, vid: VID, gid: GidRef) {
        if !self.is_initialised() {
            self.vid = vid;
            self.global_id = gid.to_owned();
        }
    }

    pub fn empty(global_id: GID) -> Self {
        let layers = vec![Adj::Solo];
        Self {
            global_id,
            vid: VID(0),
            timestamps: Default::default(),
            layers,
            props: None,
            node_type: 0,
        }
    }

    pub fn resolved(global_id: GID, vid: VID) -> Self {
        Self {
            global_id,
            vid,
            timestamps: Default::default(),
            layers: vec![],
            props: None,
            node_type: 0,
        }
    }

    pub fn global_id(&self) -> &GID {
        &self.global_id
    }

    pub fn timestamps(&self) -> &NodeTimestamps {
        &self.timestamps
    }

    #[inline]
    pub fn update_time(&mut self, t: TimeIndexEntry, eid: ELID) {
        self.timestamps.edge_ts.set(t, eid);
    }

    pub fn update_node_type(&mut self, node_type: usize) -> usize {
        self.node_type = node_type;
        node_type
    }

    pub fn add_metadata(&mut self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.add_metadata(prop_id, prop)
    }

    pub fn update_metadata(&mut self, prop_id: usize, prop: Prop) -> Result<(), MetadataError> {
        let props = self.props.get_or_insert_with(Props::new);
        props.update_metadata(prop_id, prop)
    }

    pub fn update_t_prop_time(&mut self, t: TimeIndexEntry, prop_i: Option<usize>) {
        self.timestamps.props_ts.set(t, prop_i);
    }

    #[inline(always)]
    pub fn find_edge_eid(&self, dst: VID, layer_id: &LayerIds) -> Option<EID> {
        match layer_id {
            LayerIds::All => match self.layers.len() {
                0 => None,
                1 => self.layers[0].get_edge(dst, Direction::OUT),
                _ => self
                    .layers
                    .iter()
                    .find_map(|layer| layer.get_edge(dst, Direction::OUT)),
            },
            LayerIds::One(layer_id) => self
                .layers
                .get(*layer_id)
                .and_then(|layer| layer.get_edge(dst, Direction::OUT)),
            LayerIds::Multiple(layers) => layers.iter().find_map(|layer_id| {
                self.layers
                    .get(layer_id)
                    .and_then(|layer| layer.get_edge(dst, Direction::OUT))
            }),
            LayerIds::None => None,
        }
    }

    pub fn add_edge(&mut self, v_id: VID, dir: Direction, layer: usize, edge_id: EID) {
        if layer >= self.layers.len() {
            self.layers.resize_with(layer + 1, || Adj::Solo);
        }

        match dir {
            Direction::IN => self.layers[layer].add_edge_into(v_id, edge_id),
            Direction::OUT => self.layers[layer].add_edge_out(v_id, edge_id),
            _ => {}
        }
    }

    #[inline]
    pub fn edge_tuples<'a>(&'a self, layers: &LayerIds, d: Direction) -> BoxedLIter<'a, EdgeRef> {
        let self_id = self.vid;
        let iter: BoxedLIter<'a, EdgeRef> = match d {
            Direction::OUT => self.merge_layers(layers, Direction::OUT, self_id),
            Direction::IN => self.merge_layers(layers, Direction::IN, self_id),
            Direction::BOTH => Box::new(
                self.edge_tuples(layers, Direction::OUT)
                    .filter(|e| e.src() != e.dst())
                    .merge_by(self.edge_tuples(layers, Direction::IN), |e1, e2| {
                        e1.remote() < e2.remote()
                    }),
            ),
        };
        iter
    }

    fn merge_layers(&self, layers: &LayerIds, d: Direction, self_id: VID) -> BoxedLIter<EdgeRef> {
        match layers {
            LayerIds::All => Box::new(
                self.layers
                    .iter()
                    .map(|adj| self.iter_adj(adj, d, self_id))
                    .kmerge_by(|e1, e2| e1.remote() < e2.remote())
                    .dedup(),
            ),
            LayerIds::One(id) => {
                if let Some(layer) = self.layers.get(*id) {
                    Box::new(self.iter_adj(layer, d, self_id))
                } else {
                    Box::new(iter::empty())
                }
            }
            LayerIds::Multiple(ids) => Box::new(
                ids.into_iter()
                    .filter_map(|id| self.layers.get(id))
                    .map(|layer| self.iter_adj(layer, d, self_id))
                    .kmerge_by(|e1, e2| e1.remote() < e2.remote())
                    .dedup(),
            ),
            LayerIds::None => Box::new(iter::empty()),
        }
    }

    fn iter_adj<'a>(
        &'a self,
        layer: &'a Adj,
        d: Direction,
        self_id: VID,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a {
        let iter: BoxedLIter<'a, EdgeRef> = match d {
            Direction::IN => Box::new(
                layer
                    .iter(d)
                    .map(move |(src_pid, e_id)| EdgeRef::new_incoming(e_id, src_pid, self_id)),
            ),
            Direction::OUT => Box::new(
                layer
                    .iter(d)
                    .map(move |(dst_pid, e_id)| EdgeRef::new_outgoing(e_id, self_id, dst_pid)),
            ),
            _ => Box::new(iter::empty()),
        };
        iter
    }

    pub fn degree(&self, layers: &LayerIds, d: Direction) -> usize {
        match layers {
            LayerIds::All => match self.layers.len() {
                0 => 0,
                1 => self.layers[0].degree(d),
                _ => self
                    .layers
                    .iter()
                    .map(|l| l.node_iter(d))
                    .kmerge()
                    .dedup()
                    .count(),
            },
            LayerIds::One(l) => self
                .layers
                .get(*l)
                .map(|layer| layer.degree(d))
                .unwrap_or(0),
            LayerIds::None => 0,
            LayerIds::Multiple(ids) => ids
                .iter()
                .flat_map(|l_id| self.layers.get(l_id).map(|layer| layer.node_iter(d)))
                .kmerge()
                .dedup()
                .count(),
        }
    }

    // every neighbour apears once in the iterator
    // this is important because it calculates degree
    pub fn neighbours<'a>(
        &'a self,
        layers: &LayerIds,
        d: Direction,
    ) -> impl Iterator<Item = VID> + use<'a> {
        match layers {
            LayerIds::All => {
                let iter = self
                    .layers
                    .iter()
                    .map(move |layer| layer.node_iter(d))
                    .kmerge()
                    .dedup();
                LayerVariants::All(iter)
            }
            LayerIds::One(one) => {
                let iter = self
                    .layers
                    .get(*one)
                    .into_iter()
                    .flat_map(move |layer| layer.node_iter(d));
                LayerVariants::One(iter)
            }
            LayerIds::Multiple(layers) => {
                let iter = layers
                    .into_iter()
                    .filter_map(|l| self.layers.get(l))
                    .map(move |layer| self.neighbours_from_adj(layer, d))
                    .kmerge()
                    .dedup();
                LayerVariants::Multiple(iter)
            }
            LayerIds::None => LayerVariants::None(iter::empty()),
        }
    }

    fn neighbours_from_adj<'a>(&'a self, layer: &'a Adj, d: Direction) -> BoxedLIter<'a, VID> {
        let iter: BoxedLIter<'a, VID> = match d {
            Direction::IN => Box::new(layer.iter(d).map(|(from_v, _)| from_v)),
            Direction::OUT => Box::new(layer.iter(d).map(|(to_v, _)| to_v)),
            Direction::BOTH => Box::new(
                self.neighbours_from_adj(layer, Direction::OUT)
                    .merge(self.neighbours_from_adj(layer, Direction::IN))
                    .dedup(),
            ),
        };
        iter
    }

    pub fn metadata_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.props
            .as_ref()
            .into_iter()
            .flat_map(|ps| ps.metadata_ids())
    }

    pub fn metadata(&self, prop_id: usize) -> Option<&Prop> {
        self.props.as_ref().and_then(|ps| ps.metadata(prop_id))
    }
}

impl<'a> NodeEntry<'a> {
    pub fn into_edges(
        self,
        layers: &LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + 'a {
        GenLockedIter::from(self, |node| node.as_ref().node().edge_tuples(layers, dir))
    }
}
