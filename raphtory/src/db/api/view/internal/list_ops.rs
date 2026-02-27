use crate::{
    core::entities::{EID, VID},
    db::api::{state::Index, view::Base},
};
use raphtory_core::entities::LayerIds;
use raphtory_storage::graph::graph::GraphStorage;
use rayon::{iter::Either, prelude::*};
use std::{hash::Hash, sync::Arc};
use storage::utils::Iter3;

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

#[derive(Debug)]
pub enum List<I> {
    All { layers: LayerIds },
    List { elems: Index<I> },
}

pub type NodeList = List<VID>;
pub type EdgeList = List<EID>;

impl<I> Clone for List<I> {
    fn clone(&self) -> Self {
        match self {
            List::All { layers } => List::All {
                layers: layers.clone(),
            },
            List::List { elems } => List::List {
                elems: elems.clone(),
            },
        }
    }
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> List<I> {
    pub fn intersection(&self, other: &List<I>) -> List<I> {
        match (self, other) {
            (List::All { layers: a }, List::All { layers: b }) => {
                let layers = a.intersect(b);
                List::All { layers }
            }
            (List::List { .. }, List::All { .. }) => self.clone(),
            (List::All { .. }, List::List { .. }) => other.clone(),
            (List::List { elems: a }, List::List { elems: b }) => {
                let elems = a.intersection(b);
                List::List { elems }
            }
        }
    }

    pub fn unfiltered(&self) -> bool {
        matches!(self, List::All{layers: LayerIds::All})
    }
}

impl List<VID> {
    pub fn nodes_iter(self, g: &GraphStorage) -> impl Iterator<Item = VID> {
        match self {
            List::All {
                layers: LayerIds::All,
            } => {
                let sc = g.node_segment_counts();
                Iter3::I(sc.into_iter())
            }
            List::All { layers } => Iter3::J(g.nodes().layer_iter(layers)),
            List::List { elems } => Iter3::K(elems.into_iter()),
        }
    }

    pub fn into_index(self, g: &GraphStorage) -> Index<VID> {
        match self {
            List::All { .. } => Index::Full(Arc::new(g.node_state_index())),
            List::List { elems } => elems,
        }
    }

    pub fn nodes_par_iter(self, g: &GraphStorage) -> impl ParallelIterator<Item = VID> {
        match self {
            List::All { .. } => {
                let sc = g.node_segment_counts();
                Either::Left(sc.into_par_iter())
            }
            List::List { elems } => Either::Right(elems.into_par_iter()),
        }
    }
}
