use crate::core::Direction;

use super::{tgraph::TGraph, vertex::Vertex, GraphItem, EID, VID};

impl<'a, const N: usize, L: lock_api::RawRwLock> GraphItem<'a, N, L> for Edge<'a, N, L> {
    fn from_edge_ids(
        src: VID,
        dst: VID,
        e_id: EID,
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
    edge_id: EID,
    dir: Direction,
    graph: &'a TGraph<N, L>,
}

impl<'a, const N: usize, L: lock_api::RawRwLock> PartialEq for Edge<'a, N, L> {
    fn eq(&self, other: &Self) -> bool {
        self.edge_id == other.edge_id && self.src == other.src && self.dst == other.dst
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock> PartialOrd for Edge<'a, N, L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.origin()
            .eq(&other.origin())
            .then(|| self.neighbour().cmp(&other.neighbour()))
    }
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Edge<'a, N, L> {
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
        edge_id: EID,
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
        self.edge_id
    }

    pub fn src(&self) -> Vertex<'a, N, L> {
        self.graph.vertex(self.src)
    }

    pub fn dst(&self) -> Vertex<'a, N, L> {
        self.graph.vertex(self.dst)
    }

    pub fn from_edge_ids(
        v1: VID, // the initiator of the edges call
        v2: VID, // the edge on the other side
        edge_id: EID,
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
