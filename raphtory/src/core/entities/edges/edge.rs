use crate::core::{
    entities::{
        edges::edge_store::EdgeStore,
        graph::{
            tgraph::TGraph,
            tgraph_storage::{GraphEntry, LockedGraphStorage},
        },
        properties::tprop::TProp,
        vertices::vertex::Vertex,
        GraphItem, VRef, EID, VID, LayerIds,
    },
    storage::{
        locked_view::LockedView,
        timeindex::{TimeIndex, TimeIndexOps, LockedLayeredIndex},
        Entry,
    },
    Direction, Prop,
};
use std::{
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Debug)]
pub(crate) enum ERef<'a, const N: usize> {
    ERef(Entry<'a, EdgeStore<N>, N>),
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

    fn vertex_ref(&self, src: VID) -> Option<VRef<'a, N>> {
        match self {
            ERef::ELock { lock, .. } => {
                Some(VRef::LockedEntry(GraphEntry::new(lock.clone(), src.into())))
            }
            _ => None,
        }
    }
}

impl<'a, const N: usize> Deref for ERef<'a, N> {
    type Target = EdgeStore<N>;

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
    pub fn temporal_properties(
        &'a self,
        name: &str,
        layer: usize,
        window: Option<Range<i64>>,
    ) -> Vec<(i64, Prop)> {
        let prop_id = self.graph.edge_meta.resolve_prop_id(name, false);
        let store = &self.edge_id;
        let out = store
            .layer(layer)
            .unwrap()
            .temporal_properties(prop_id, window)
            .collect();
        out
    }

    pub(crate) fn additions(self, layer_ids: LayerIds) -> Option<LockedLayeredIndex<'a>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                let t_index = entry.map(|entry| entry.additions());
                Some(LockedLayeredIndex::new(layer_ids, t_index))
            }
            _ => None,
        }
    }

    pub(crate) fn deletions(self, layer_ids: LayerIds) -> Option<LockedLayeredIndex<'a>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                let t_index = entry.map(|entry| entry.deletions());
                Some(LockedLayeredIndex::new(layer_ids, t_index))
            }
            _ => None,
        }
    }

    pub(crate) fn temporal_property(
        self,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<LockedView<'a, TProp>> {
        match self.edge_id {
            ERef::ERef(entry) => {
                let prop_exists = entry
                    .layer(layer_id)
                    .map(|layer| layer.temporal_property(prop_id).is_some())
                    .unwrap_or(false);
                if !prop_exists {
                    return None;
                }

                let t_index = entry.map(|entry| entry.temporal_prop(layer_id, prop_id).unwrap());
                Some(t_index)
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

    pub fn src(&self) -> Vertex<'a, N> {
        if let Some(v_ref) = self.edge_id.vertex_ref(self.src) {
            Vertex::new(v_ref, self.graph)
        } else {
            self.graph.vertex(self.src)
        }
    }

    pub fn dst(&self) -> Vertex<'a, N> {
        if let Some(v_ref) = self.edge_id.vertex_ref(self.dst) {
            Vertex::new(v_ref, self.graph)
        } else {
            self.graph.vertex(self.dst)
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

    pub(crate) fn from_entry(entry: Entry<'a, EdgeStore<N>, N>, graph: &'a TGraph<N>) -> Self {
        Self {
            src: entry.src().into(),
            dst: entry.dst().into(),
            edge_id: ERef::ERef(entry),
            dir: Direction::OUT,
            graph,
        }
    }

    pub(crate) fn active(&'a self, layer_id: usize, w: Range<i64>) -> bool {
        match &self.edge_id {
            ERef::ELock { lock, .. } => {
                let e = lock.get_edge(self.edge_id().into());
                e.additions()[layer_id].active(w)
            }
            ERef::ERef(entry) => (*entry).additions()[layer_id].active(w),
        }
    }
}
