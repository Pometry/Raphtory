use crate::{
    core::entities::{EID, VID},
    db::api::{state::Index, view::Base},
};
use enum_dispatch::enum_dispatch;
use rayon::{iter::Either, prelude::*};
use std::sync::Arc;

#[enum_dispatch]
pub trait ListOps {
    fn node_list(&self) -> NodeList;

    fn edge_list(&self) -> EdgeList;
}

pub trait InheritListOps: Base {}

impl<G: InheritListOps> ListOps for G
where
    <G as Base>::Base: ListOps,
{
    fn node_list(&self) -> NodeList {
        self.base().node_list()
    }

    fn edge_list(&self) -> EdgeList {
        self.base().edge_list()
    }
}

#[derive(Debug, Clone)]
pub enum NodeList {
    All { num_nodes: usize },
    List { nodes: Index<VID> },
}

impl NodeList {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = VID> + '_ {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..*num_nodes).into_par_iter().map(VID)),
            NodeList::List { nodes } => Either::Right(nodes.par_iter()),
        }
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = VID> {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..num_nodes).into_par_iter().map(VID)),
            NodeList::List { nodes } => Either::Right(nodes.into_par_iter()),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = VID> + '_ {
        match self {
            NodeList::All { num_nodes } => Either::Left((0..*num_nodes).map(VID)),
            NodeList::List { nodes } => Either::Right(nodes.iter()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NodeList::All { num_nodes } => *num_nodes,
            NodeList::List { nodes } => nodes.len(),
        }
    }
}

impl IntoIterator for NodeList {
    type Item = VID;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + Sync>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            NodeList::All { num_nodes } => Box::new((0..num_nodes).map(VID)),
            NodeList::List { nodes } => Box::new(nodes.into_iter()),
        }
    }
}

#[derive(Clone)]
pub enum EdgeList {
    All { num_edges: usize },
    List { edges: Arc<[EID]> },
}

impl EdgeList {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = EID> + '_ {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..*num_edges).into_par_iter().map(EID)),
            EdgeList::List { edges } => Either::Right(edges.par_iter().copied()),
        }
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = EID> {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..num_edges).into_par_iter().map(EID)),
            EdgeList::List { edges } => {
                Either::Right((0..edges.len()).into_par_iter().map(move |i| edges[i]))
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = EID> + '_ {
        match self {
            EdgeList::All { num_edges } => Either::Left((0..*num_edges).map(EID)),
            EdgeList::List { edges } => Either::Right(edges.iter().copied()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EdgeList::All { num_edges } => *num_edges,
            EdgeList::List { edges } => edges.len(),
        }
    }
}

impl IntoIterator for EdgeList {
    type Item = EID;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + Sync>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            EdgeList::All { num_edges } => Box::new((0..num_edges).map(EID)),
            EdgeList::List { edges } => Box::new((0..edges.len()).map(move |i| edges[i])),
        }
    }
}
