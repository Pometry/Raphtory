use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::storage::{self, iter::{RefT, RefX}, Entry, EntryMut, PairEntryMut};

use super::{edge_store::EdgeStore, node_store::NodeStore, VID};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct GraphStorage<const N: usize, L: lock_api::RawRwLock> {
    // node storage with having (id, time_index, properties, adj list for each layer)
    nodes: storage::RawStorage<NodeStore<N>, L, N>,

    // edge storage with having (src, dst, time_index, properties) for each layer
    edges: storage::RawStorage<EdgeStore<N>, L, N>,
}

impl<const N: usize, L: lock_api::RawRwLock> GraphStorage<N, L> {
    fn iter_nodes<'a>(&'a self) -> impl Iterator<Item = RefT<'a, NodeStore<N>, L, N>> {
        self.nodes.iter2()
    }

    pub(crate) fn new() -> Self {
        Self {
            nodes: storage::RawStorage::new(),
            edges: storage::RawStorage::new(),
        }
    }

    pub(crate) fn push_node(&self, node: NodeStore<N>) -> usize {
        self.nodes.push(node)
    }

    pub(crate) fn push_edge(&self, edge: EdgeStore<N>) -> usize {
        self.edges.push(edge)
    }

    pub(crate) fn get_node_mut(&self, id: usize) -> EntryMut<'_, NodeStore<N>, L> {
        self.nodes.entry_mut(id)
    }

    pub(crate) fn get_edge_mut(&self, id: usize) -> EntryMut<'_, EdgeStore<N>, L> {
        self.edges.entry_mut(id)
    }

    pub(crate) fn get_node(&self, id: usize) -> Entry<'_, NodeStore<N>, L, N> {
        self.nodes.entry(id)
    }

    pub(crate) fn get_edge(&self, id: usize) -> Entry<'_, EdgeStore<N>, L, N> {
        self.edges.entry(id)
    }

    pub(crate) fn pair_node_mut(&self, i:usize, j:usize) -> PairEntryMut<'_, NodeStore<N>, L>{
        self.nodes.pair_entry_mut(i, j)
    }

    pub(crate) fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn edges_len(&self) -> usize {
        self.edges.len()
    }

    fn lock(&self) -> LockedGraphStorage<'_, N, L> {
        LockedGraphStorage {
            nodes: self.nodes.read_lock(),
            edges: self.edges.read_lock(),
        }
    }

    pub(crate) fn locked_nodes(&self) -> impl Iterator<Item = RefY<'_, NodeStore<N>, L, N>> {
        LockedVIter{
            gs: self,
            from: 0,
            to: self.nodes.len(),
            locked_gs: self.lock()
        }        
        
    }
}


struct LockedVIter<'a, const N: usize, L: lock_api::RawRwLock> {
    gs: &'a GraphStorage<N, L>,
    from: usize,
    to: usize,
    locked_gs: LockedGraphStorage<'a, N, L>
}

impl<'a, const N: usize, L: lock_api::RawRwLock> Iterator for LockedVIter<'a, N, L> {
    type Item = RefY<'a, NodeStore<N>, L, N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.from < self.to {
            let node = Some(RefY{
                locked_gs: &self.locked_gs,
                i: self.from,
                _marker: std::marker::PhantomData
            });
            self.from += 1;
            node
        } else {
            None
        }
    }
}

// FIXME: this will be the next Vertex
pub(crate) struct RefY<'a, T, L: lock_api::RawRwLock, const N: usize>{
    locked_gs: &'a LockedGraphStorage<'a, N, L>,
    i: usize,
    _marker: std::marker::PhantomData<T>
}


// impl Deref for RefY of NodeStore<N>
impl<'a, const N: usize, L: lock_api::RawRwLock> Deref for RefY<'a, NodeStore<N>, L, N> {
    type Target = NodeStore<N>;

    fn deref(&self) -> &Self::Target {
        self.locked_gs.get_node(self.i)
    }
}

pub(crate) struct LockedGraphStorage<'a, const N: usize, L: lock_api::RawRwLock> {
    nodes: storage::ReadLockedStorage<'a, NodeStore<N>, L, N>,
    edges: storage::ReadLockedStorage<'a, EdgeStore<N>, L, N>,
}


impl<'a, const N: usize, L: lock_api::RawRwLock> LockedGraphStorage<'a, N, L> {
    pub(crate) fn new(storage: &'a GraphStorage<N, L>) -> Self {
        Self {
            nodes: storage.nodes.read_lock(),
            edges: storage.edges.read_lock(),
        }
    }

    pub(crate) fn get_node(&self, id: usize) -> &'a NodeStore<N> {
        self.nodes.get(id)
    }
    // pub(crate) fn iter_nodes(self) -> impl Iterator<Item = &'a NodeStore<N>> {
    //     self.nodes.into_iter()
    // }

    // pub(crate) fn iter_edges(&'a self) -> impl Iterator<Item = &'a EdgeStore<N>> {
    //     self.edges.iter2()
    // }
}

// pub(crate) trait GStorage<'a, const N: usize> {
//     type Ptr<A: 'static>: Deref<Target = A>
//     where
//         Self: 'a;
//     // type PtrMut<A>: DerefMut<Target = A>;

//     fn get_node(&'a self, id: VID) -> Self::Ptr<NodeStore<N>>;
//     // fn get_edge(&'a self, id: EID) -> Self::Ptr<'a, EdgeStore<N>>;

//     // fn get_node_mut(&mut self, id: VID) -> &mut NodeStore<N>;
//     // fn get_edge_mut(&mut self, id: EID) -> &mut EdgeStore<N>;
// }

// impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> GStorage<'a, N> for GraphStorage<N, L> {
//     type Ptr<A: 'static> = Entry<'a, A, L, N> where Self: 'a;
//     fn get_node(&'a self, id: VID) -> Self::Ptr<NodeStore<N>> {
//         self.nodes.entry(id.into())
//     }

//     // fn get_edge(&self, id: EID) -> &EdgeStore<N> {
//     //     self.edges.entry(id.into())
//     // }

//     // fn get_node_mut(&mut self, id: VID) -> &mut NodeStore<N> {
//     //     self.nodes.entry_mut(id)
//     // }

//     // fn get_edge_mut(&mut self, id: EID) -> &mut EdgeStore<N> {
//     //     self.edges.entry_mut(id)
//     // }
// }

// impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> GStorage<'a, N>
//     for LockedGraphStorage<'a, N, L>
// {
//     type Ptr<A: 'static> = &'a A where Self: 'a;

//     fn get_node(&'a self, id: VID) -> Self::Ptr<NodeStore<N>> {
//         self.nodes.get(id.into())
//     }
// }

// pub(crate) trait GIter<'a, const N: usize> {
//     type Ptr<A: 'static>: Deref<Target = A>
//     where
//         Self: 'a;
//     // type PtrMut<A>: DerefMut<Target = A>;
//     type Iter<A: 'static>: Iterator<Item = Self::Ptr<A>>
//     where
//         Self: 'a;

//     fn iter_nodes(&'a self) -> Self::Iter<NodeStore<N>>;
//     fn iter_edges(&'a self) -> Self::Iter<EdgeStore<N>>;
//     // fn get_edge(&'a self, id: EID) -> Self::Ptr<'a, EdgeStore<N>>;

//     // fn get_node_mut(&mut self, id: VID) -> &mut NodeStore<N>;
//     // fn get_edge_mut(&mut self, id: EID) -> &mut EdgeStore<N>;
// }

// // impl for LockedGraphStorage
// impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> GIter<'a, N>
//     for LockedGraphStorage<'a, N, L>
// {
//     type Ptr<A: 'static> = &'a A where Self: 'a;
//     type Iter<A: 'static> = Box<dyn Iterator<Item = Self::Ptr<A>> + 'a>;

//     fn iter_nodes(&'a self) -> Self::Iter<NodeStore<N>> {
//         Box::new(self.nodes.iter2())
//     }

//     fn iter_edges(&'a self) -> Self::Iter<EdgeStore<N>> {
//         Box::new(self.edges.iter2())
//     }
// }

// // impl for GraphStorage uses RefT
// impl<'a, const N: usize, L: lock_api::RawRwLock + 'static> GIter<'a, N> for GraphStorage<N, L> {
//     type Ptr<A: 'static> = RefT<'a, A, L, N> where Self: 'a;
//     type Iter<A: 'static> = Box<dyn Iterator<Item = Self::Ptr<A>> + 'a>;

//     fn iter_nodes(&'a self) -> Self::Iter<NodeStore<N>> {
//         Box::new(self.nodes.iter2())
//     }

//     fn iter_edges(&'a self) -> Self::Iter<EdgeStore<N>> {
//         Box::new(self.edges.iter2())
//     }
// }
