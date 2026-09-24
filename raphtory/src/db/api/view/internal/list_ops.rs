use crate::{
    core::entities::{EID, VID},
    db::api::{state::Index, view::Base},
};
use itertools::Itertools;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_entry::NodeStorageEntry},
};
use rayon::prelude::*;
use std::{hash::Hash, sync::Arc};
use storage::{api::node_type_index::NodeTypeIndexOps, utils::Iter3};

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
    All,
    NodeTypeIdx { types: Arc<[usize]> },
    List { elems: Index<I> },
}

pub type NodeList = List<VID>;
pub type EdgeList = List<EID>;

impl<I> Clone for List<I> {
    fn clone(&self) -> Self {
        match self {
            List::All => List::All,
            List::NodeTypeIdx { types } => List::NodeTypeIdx {
                types: types.clone(),
            },
            List::List { elems } => List::List {
                elems: elems.clone(),
            },
        }
    }
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> List<I> {
    /// Drops any exactness claim; see [`Index::into_inexact`].
    pub fn into_inexact(self) -> List<I> {
        match self {
            List::All => List::All,
            List::List { elems } => List::List {
                elems: elems.into_inexact(),
            },
        }
    }

    pub fn intersection(&self, other: &List<I>, g: &GraphStorage) -> List<I> {
        match (self, other) {
            (List::All, other) => other.clone(),
            (other, List::All) => other.clone(),

            (List::List { elems: a }, List::List { elems: b }) => {
                let elems = a.intersection(b);
                List::List { elems }
            }
            (List::List { elems }, List::NodeTypeIdx { types })
            | (List::NodeTypeIdx { types }, List::List { elems }) => Self::List {
                elems: elems
                    .iter()
                    .filter(|&vid| {
                        let vid = VID(vid.into());
                        let node_type = g.node_type_id(vid);
                        types.contains(&node_type)
                    })
                    .collect(),
            },
            (List::NodeTypeIdx { types: left }, List::NodeTypeIdx { types: right }) => {
                List::NodeTypeIdx {
                    types: left.iter().copied().filter(|i| right.contains(i)).collect(),
                }
            }
        }
    }

    pub fn union(&self, other: &List<I>, g: &GraphStorage) -> List<I> {
        match (self, other) {
            (List::All, _) | (_, List::All) => List::All,
            (List::List { elems: left }, List::List { elems: right }) => List::List {
                elems: left.union(right),
            },
            (List::NodeTypeIdx { types }, List::List { elems })
            | (List::List { elems }, List::NodeTypeIdx { types }) => {
                let entry = g.node_type_index().node_type_entry(&types);
                List::List {
                    elems: entry
                        .iter()
                        .map(|id| id.0)
                        .merge(elems.iter().map(Into::into))
                        .map(From::from)
                        .collect(),
                }
            }
            (List::NodeTypeIdx { types: left }, List::NodeTypeIdx { types: right }) => {
                List::NodeTypeIdx {
                    types: left
                        .iter()
                        .copied()
                        .chain(right.iter().copied())
                        .sorted()
                        .dedup()
                        .collect(),
                }
            }
        }
    }

    pub fn unfiltered(&self) -> bool {
        matches!(self, List::All)
    }

    /// True when the list is a pushdown candidate list whose producer proved
    /// every key matches its filter (see [`Index::dynamically_exact`]).
    pub fn dynamically_trusted(&self) -> bool {
        match self {
            List::All => false,
            List::List { elems } => elems.dynamically_exact(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            List::All => false,
            List::NodeTypeIdx { types } => todo!(),
            List::List { elems } => elems.is_empty(),
        }
    }

    pub fn empty() -> Self {
        List::List {
            elems: Index::default(),
        }
    }

    pub fn is_subset(&self, other: &List<I>) -> bool {
        match (self, other) {
            (_, List::All) => true,
            (List::All, List::List { .. }) => false,
            (List::List { elems: a }, List::List { elems: b }) => a.is_subset(b),
        }
    }
}

impl List<VID> {
    pub fn nodes_iter(self, g: &GraphStorage) -> impl Iterator<Item = VID> {
        match self {
            List::All => {
                let sc = g.node_segment_counts();
                Iter3::I(sc.into_iter())
            }
            List::NodeTypeIdx { types } => {
                Iter3::J(g.node_type_index().arc_node_type_entry(&types).into_iter())
            }
            List::List { elems } => Iter3::K(elems.into_iter()),
        }
    }

    pub fn node_entries(self, g: &GraphStorage) -> impl Iterator<Item = NodeStorageEntry<'_>> {
        match self {
            List::All => Iter3::I(g.node_entries()),
            List::NodeTypeIdx { types } => Iter3::J(
                g.node_type_index()
                    .arc_node_type_entry(&types)
                    .into_iter()
                    .map(|vid| g.core_node(vid)),
            ),
            List::List { elems } => Iter3::K(elems.into_iter().map(|vid| g.core_node(vid))),
        }
    }

    pub fn into_index(self, g: &GraphStorage) -> Index<VID> {
        match self {
            List::All => Index::Full(Arc::new(g.node_state_index())),
            List::NodeTypeIdx { types } => g
                .node_type_index()
                .arc_node_type_entry(&types)
                .into_iter()
                .collect(),
            List::List { elems } => elems,
        }
    }

    pub fn nodes_par_iter(self, g: &GraphStorage) -> impl ParallelIterator<Item = VID> {
        match self {
            List::All => {
                let sc = g.node_segment_counts();
                Iter3::I(sc.into_par_iter())
            }
            List::NodeTypeIdx { types } => {
                Iter3::J(
                    g.node_type_index()
                        .arc_node_type_entry(&types)
                        .into_iter()
                        .par_bridge(),
                ) // TODO: can node type entry give us a better par_iter?
            }
            List::List { elems } => Iter3::K(elems.into_par_iter()),
        }
    }
}
