use crate::core::{
    entities::{
        edges::edge_store::EdgeStore,
        graph::{
            tgraph::TGraph,
            tgraph_storage::{GraphEntry, LockedGraphStorage},
        },
        nodes::node::Node,
        properties::tprop::{LockedLayeredTProp, TProp},
        GraphItem, LayerIds, VRef, EID, VID,
    },
    storage::{
        locked_view::LockedView,
        timeindex::{LayeredIndex, TimeIndex, TimeIndexOps},
        Entry,
    },
    Direction, Prop,
};
// use crate::prelude::Layer::Default;
use crate::core::storage::timeindex::{LockedLayeredIndex, TimeIndexEntry};
use std::{
    default::Default,
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Debug)]
pub(crate) enum ERef<'a, const N: usize> {
    ERef(Entry<'a, EdgeStore, N>),
    ELock {
        lock: Arc<LockedGraphStorage<N>>,
        eid: EID,
    },
}

// impl fn edge_id for ERef
impl<'a, const N: usize> ERef<'a, N> {
    pub(crate) fn edge_id(&self) -> EID {
        match self {
            ERef::ELock { lock: _, eid } => *eid,
            ERef::ERef(es) => es.index().into(),
        }
    }

    fn node_ref(&self, src: VID) -> Option<VRef<'a, N>> {
        match self {
            ERef::ELock { lock, .. } => {
                Some(VRef::LockedEntry(GraphEntry::new(lock.clone(), src.into())))
            }
            _ => None,
        }
    }
}

impl<'a, const N: usize> Deref for ERef<'a, N> {
    type Target = EdgeStore;

    fn deref(&self) -> &Self::Target {
        match self {
            ERef::ERef(e) => e,
            ERef::ELock { lock, eid } => lock.get_edge((*eid).into()),
        }
    }
}

impl<'a, const N: usize> GraphItem<'a, N> for EdgeView<'a, N> {
    fn from_edge_ids(
        src: VID,
        dst: VID,
        e_id: ERef<'a, N>,
        dir: Direction,
        graph: &'a TGraph<N>,
    ) -> Self {
        EdgeView::from_edge_ids(src, dst, e_id, dir, graph)
    }
}
#[derive(Debug)]
pub struct EdgeView<'a, const N: usize> {
    src: VID,
    dst: VID,
    edge_id: ERef<'a, N>,
    dir: Direction,
    graph: &'a TGraph<N>,
}

impl<'a, const N: usize> PartialEq for EdgeView<'a, N> {
    fn eq(&self, other: &Self) -> bool {
        self.edge_id.edge_id() == other.edge_id.edge_id()
            && self.src == other.src
            && self.dst == other.dst
    }
}

impl<'a, const N: usize> PartialOrd for EdgeView<'a, N> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.origin()
            .eq(&other.origin())
            .then(|| self.neighbour().cmp(&other.neighbour()))
    }
}

impl<'a, const N: usize> EdgeView<'a, N> {
    pub(crate) fn additions(
        self,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredIndex<'a, TimeIndexEntry>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                let t_index = entry.map(|entry| entry.additions());
                Some(LayeredIndex::new(layer_ids, t_index))
            }
            _ => None,
        }
    }

    pub(crate) fn deletions(
        self,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredIndex<'a, TimeIndexEntry>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                let t_index = entry.map(|entry| entry.deletions());
                Some(LayeredIndex::new(layer_ids, t_index))
            }
            _ => None,
        }
    }

    pub(crate) fn temporal_property(
        self,
        layer_ids: LayerIds,
        prop_id: usize,
    ) -> Option<LockedLayeredTProp<'a>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                if entry.has_temporal_prop(&layer_ids, prop_id) {
                    match layer_ids {
                        LayerIds::None => None,
                        LayerIds::All => {
                            let props: Vec<_> = entry
                                .layer_ids_iter()
                                .flat_map(|id| {
                                    entry.temporal_prop_layer(id, prop_id).is_some().then(|| {
                                        entry
                                            .clone()
                                            .map(|e| e.temporal_prop_layer(id, prop_id).unwrap())
                                    })
                                })
                                .collect();
                            Some(LockedLayeredTProp::new(props))
                        }
                        LayerIds::One(id) => Some(LockedLayeredTProp::new(vec![entry.map(|e| {
                            e.temporal_prop_layer(id, prop_id)
                                .expect("already checked in the beginning")
                        })])),
                        LayerIds::Multiple(ids) => {
                            let props: Vec<_> = ids
                                .iter()
                                .flat_map(|&id| {
                                    entry.temporal_prop_layer(id, prop_id).is_some().then(|| {
                                        entry
                                            .clone()
                                            .map(|e| e.temporal_prop_layer(id, prop_id).unwrap())
                                    })
                                })
                                .collect();
                            Some(LockedLayeredTProp::new(props))
                        }
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn neighbour(&self) -> VID {
        match self.dir {
            Direction::OUT => self.dst,
            Direction::IN => self.src,
            _ => panic!("Invalid direction"), // FIXME: perhaps we should have 2 enums for direction one strict and one not
        }
    }

    fn origin(&self) -> VID {
        match self.dir {
            Direction::OUT => self.src,
            Direction::IN => self.dst,
            _ => panic!("Invalid direction"), // FIXME: perhaps we should have 2 enums for direction one strict and one not
        }
    }

    pub fn src_id(&self) -> VID {
        self.src
    }

    pub fn dst_id(&self) -> VID {
        self.dst
    }

    pub fn edge_id(&self) -> EID {
        self.edge_id.edge_id()
    }

    pub fn src(&self) -> Node<'a, N> {
        if let Some(v_ref) = self.edge_id.node_ref(self.src) {
            Node::new(v_ref, self.graph)
        } else {
            self.graph.node(self.src)
        }
    }

    pub fn dst(&self) -> Node<'a, N> {
        if let Some(v_ref) = self.edge_id.node_ref(self.dst) {
            Node::new(v_ref, self.graph)
        } else {
            self.graph.node(self.dst)
        }
    }

    pub(crate) fn from_edge_ids(
        v1: VID, // the initiator of the edges call
        v2: VID, // the edge on the other side
        edge_id: ERef<'a, N>,
        dir: Direction,
        graph: &'a TGraph<N>,
    ) -> Self {
        let (src, dst) = match dir {
            Direction::OUT => (v1, v2),
            Direction::IN => (v2, v1),
            _ => panic!("Invalid direction"),
        };
        EdgeView {
            src,
            dst,
            edge_id,
            graph,
            dir,
        }
    }

    pub(crate) fn from_entry(entry: Entry<'a, EdgeStore, N>, graph: &'a TGraph<N>) -> Self {
        Self {
            src: entry.src().into(),
            dst: entry.dst().into(),
            edge_id: ERef::ERef(entry),
            dir: Direction::OUT,
            graph,
        }
    }

    pub(crate) fn active(&'a self, layer_ids: LayerIds, w: Range<i64>) -> bool {
        match &self.edge_id {
            ERef::ELock { lock, .. } => {
                let e = lock.get_edge(self.edge_id().into());
                self.check_layers(layer_ids, e, |t| t.active(w.clone()))
            }
            ERef::ERef(entry) => {
                let e = entry.deref();
                self.check_layers(layer_ids, e, |t| t.active(w.clone()))
            }
        }
    }

    fn check_layers<E: Deref<Target = EdgeStore>, F: Fn(&TimeIndex<TimeIndexEntry>) -> bool>(
        &self,
        layer_ids: LayerIds,
        e: E,
        f: F,
    ) -> bool {
        match layer_ids {
            LayerIds::All => e.additions().iter().any(f),
            LayerIds::One(id) => f(&e.additions()[id]),
            LayerIds::Multiple(ids) => ids.iter().any(|id| f(&e.additions()[*id])),
            LayerIds::None => false,
        }
    }

    pub(crate) fn layer_ids(&self) -> LayerIds {
        match &self.edge_id {
            ERef::ELock { lock, .. } => {
                let e = lock.get_edge(self.edge_id().into());
                e.layer_ids()
            }
            ERef::ERef(entry) => (*entry).layer_ids(),
        }
    }
}
