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

/// The nodes of the given types in ascending order.
fn node_type_vids(g: &GraphStorage, types: &[usize]) -> Vec<VID> {
    g.node_type_index().node_type_entry(types).iter().collect()
}

fn has_node_type<I: Into<usize>>(g: &GraphStorage, types: &[usize], key: I) -> bool {
    types.contains(&g.node_type_id(VID(key.into())))
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> List<I> {
    /// Drops any exactness claim; see [`Index::into_inexact`].
    pub fn into_inexact(self) -> List<I> {
        match self {
            List::List { elems } => List::List {
                elems: elems.into_inexact(),
            },
            other => other,
        }
    }

    pub fn intersection(&self, other: &List<I>, g: &GraphStorage) -> List<I> {
        match (self, other) {
            (List::All, other) | (other, List::All) => other.clone(),
            (List::List { elems: a }, List::List { elems: b }) => List::List {
                elems: a.intersection(b),
            },
            // `Full` holds every node
            (
                List::List {
                    elems: Index::Full(_),
                },
                list @ List::NodeTypeIdx { .. },
            )
            | (
                list @ List::NodeTypeIdx { .. },
                List::List {
                    elems: Index::Full(_),
                },
            ) => list.clone(),
            // exactness is not carried through a node type list
            (List::List { elems }, List::NodeTypeIdx { types })
            | (List::NodeTypeIdx { types }, List::List { elems }) => List::List {
                elems: match elems {
                    Index::Sorted { keys, .. } => Index::from_sorted(
                        keys.iter()
                            .copied()
                            .filter(|k| has_node_type(g, types, *k))
                            .collect(),
                        false,
                    ),
                    _ => elems
                        .iter()
                        .filter(|k| has_node_type(g, types, *k))
                        .collect(),
                },
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
            (
                list @ List::List {
                    elems: Index::Full(_),
                },
                List::NodeTypeIdx { .. },
            )
            | (
                List::NodeTypeIdx { .. },
                list @ List::List {
                    elems: Index::Full(_),
                },
            ) => list.clone(),
            // TODO: this collects every node of `types`
            (List::NodeTypeIdx { types }, List::List { elems })
            | (List::List { elems }, List::NodeTypeIdx { types }) => {
                let typed = Index::from_sorted(
                    node_type_vids(g, types)
                        .into_iter()
                        .map(|vid| I::from(vid.0))
                        .collect(),
                    false,
                );
                List::List {
                    elems: typed.union(elems),
                }
            }
            (List::NodeTypeIdx { types: left }, List::NodeTypeIdx { types: right }) => {
                List::NodeTypeIdx {
                    types: left
                        .iter()
                        .copied()
                        .merge(right.iter().copied())
                        .dedup()
                        .collect(),
                }
            }
        }
    }

    pub fn contains(&self, key: &I, g: &GraphStorage) -> bool {
        match self {
            List::All => true,
            List::NodeTypeIdx { types } => has_node_type(g, types, *key),
            List::List { elems } => elems.contains(key),
        }
    }

    pub fn unfiltered(&self) -> bool {
        matches!(self, List::All)
    }

    pub fn dynamically_trusted(&self) -> bool {
        match self {
            List::All => false,
            List::NodeTypeIdx { .. } => false,
            List::List { elems } => elems.dynamically_exact(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            List::All => false,
            List::NodeTypeIdx { types } => types.is_empty(),
            List::List { elems } => elems.is_empty(),
        }
    }

    pub fn empty() -> Self {
        List::List {
            elems: Index::default(),
        }
    }

    /// Whether every key of `self` is also in `other`; see [`Index::is_subset`]
    /// for why a false positive is unsound.
    pub fn is_subset(&self, other: &List<I>, g: &GraphStorage) -> bool {
        match (self, other) {
            (_, List::All) => true,
            (List::All, _) => false,
            (List::List { elems: a }, List::List { elems: b }) => a.is_subset(b),
            // every node has exactly one type
            (List::NodeTypeIdx { types: a }, List::NodeTypeIdx { types: b }) => {
                a.iter().all(|t| b.contains(t))
            }
            (List::List { elems }, List::NodeTypeIdx { types }) => {
                elems.iter().all(|k| has_node_type(g, types, k))
            }
            (List::NodeTypeIdx { types }, List::List { elems }) => g
                .node_type_index()
                .node_type_entry(types)
                .iter()
                .all(|vid| elems.contains(&I::from(vid.0))),
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
            List::NodeTypeIdx { .. } => Iter3::J(self.nodes_iter(g).map(|vid| g.core_node(vid))),
            List::List { elems } => Iter3::K(elems.into_iter().map(|vid| g.core_node(vid))),
        }
    }

    pub fn into_index(self, g: &GraphStorage) -> Index<VID> {
        match self {
            List::All => Index::Full(Arc::new(g.node_state_index())),
            List::NodeTypeIdx { types } => Index::from_sorted(node_type_vids(g, &types), false),
            List::List { elems } => elems,
        }
    }

    pub fn nodes_par_iter(self, g: &GraphStorage) -> impl ParallelIterator<Item = VID> {
        match self {
            List::All => {
                let sc = g.node_segment_counts();
                Iter3::I(sc.into_par_iter())
            }
            // TODO: split the node type index by segment instead of materialising
            list @ List::NodeTypeIdx { .. } => Iter3::J(list.into_index(g).into_par_iter()),
            List::List { elems } => Iter3::K(elems.into_par_iter()),
        }
    }
}
