use std::rc::Rc;

use crate::core::Direction;

use super::{
    tgraph::TGraph,
    tgraph_storage::{GraphEntry, LockedGraphStorage},
    vertex::Vertex,
    GraphItem, VRef, EID, VID,
};

#[derive(Debug)]
pub(crate) enum ERef<'a, const N: usize, L: lock_api::RawRwLock> {
    EId(EID),
    ELock {
        lock: Rc<LockedGraphStorage<'a, N, L>>,
        eid: EID,
    },
}

// impl fn edge_id for ERef
impl<'a, const N: usize, L: lock_api::RawRwLock> ERef<'a, N, L> {
    pub(crate) fn edge_id(&self) -> EID {
        match self {
            ERef::EId(eid) => *eid,
            ERef::ELock { lock: _, eid } => *eid,
        }
    }

    fn vertex_ref(&self, src: VID) -> Option<VRef<'a, N, L>> {
        match self {
            ERef::EId(_) => None,
            ERef::ELock { lock, eid } => {
                Some(VRef::LockedEntry(GraphEntry::new(lock.clone(), src.into())))
            }
        }
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> GraphItem<'a, N, L> for Edge<'a, N, L> {
    fn from_edge_ids(
        src: VID,
        dst: VID,
        e_id: ERef<'a, N, L>,
        dir: Direction,
        graph: &'a TGraph<N, L>,
    ) -> Self {
        Edge::from_edge_ids(src, dst, e_id, dir, graph)
    }
}
#[derive(Debug)]
pub struct Edge<'a, const N: usize, L: lock_api::RawRwLock> {
    src: VID,
    dst: VID,
    edge_id: ERef<'a, N, L>,
    dir: Direction,
    graph: &'a TGraph<N, L>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> PartialEq for Edge<'a, N, L> {
    fn eq(&self, other: &Self) -> bool {
        self.edge_id.edge_id() == other.edge_id.edge_id()
            && self.src == other.src
            && self.dst == other.dst
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> PartialOrd for Edge<'a, N, L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.origin()
            .eq(&other.origin())
            .then(|| self.neighbour().cmp(&other.neighbour()))
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> Edge<'a, N, L> {
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

    pub(crate) fn new(
        src: VID,
        dst: VID,
        edge_id: ERef<'a, N, L>,
        dir: Direction,
        graph: &'a TGraph<N, L>,
    ) -> Self {
        Self {
            src,
            dst,
            edge_id,
            graph,
            dir,
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

    pub fn src(&self) -> Vertex<'a, N, L> {
        if let Some(v_ref) = self.edge_id.vertex_ref(self.src) {
            Vertex::new(v_ref, self.graph)
        } else {
            self.graph.vertex(self.src)
        }
    }

    pub fn dst(&self) -> Vertex<'a, N, L> {
        if let Some(v_ref) = self.edge_id.vertex_ref(self.dst) {
            Vertex::new(v_ref, self.graph)
        } else {
            self.graph.vertex(self.dst)
        }
    }

    pub(crate) fn from_edge_ids(
        v1: VID, // the initiator of the edges call
        v2: VID, // the edge on the other side
        edge_id: ERef<'a, N, L>,
        dir: Direction,
        graph: &'a TGraph<N, L>,
    ) -> Self {
        let (src, dst) = match dir {
            Direction::OUT => (v1, v2),
            Direction::IN => (v2, v1),
            _ => panic!("Invalid direction"),
        };
        Edge {
            src,
            dst,
            edge_id,
            graph,
            dir,
        }
    }
}
