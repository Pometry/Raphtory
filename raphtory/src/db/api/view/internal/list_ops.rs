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
    /// Every node whose type id is in `types` (ascending, deduplicated), served
    /// lazily from the node type index. `exact` has the same meaning as for
    /// [`Index::Sorted`]: every node here satisfies the filters that produced
    /// the list. Only meaningful for [`NodeList`].
    NodeTypeIdx {
        types: Arc<[usize]>,
        exact: bool,
    },
    List {
        elems: Index<I>,
    },
}

pub type NodeList = List<VID>;
pub type EdgeList = List<EID>;

impl<I> Clone for List<I> {
    fn clone(&self) -> Self {
        match self {
            List::All => List::All,
            List::NodeTypeIdx { types, exact } => List::NodeTypeIdx {
                types: types.clone(),
                exact: *exact,
            },
            List::List { elems } => List::List {
                elems: elems.clone(),
            },
        }
    }
}

/// The nodes of the given types, ascending and deduplicated.
fn node_type_vids(g: &GraphStorage, types: &[usize]) -> Vec<VID> {
    g.node_type_index()
        .node_type_entry(types)
        .iter()
        .dedup()
        .collect()
}

/// `elems` restricted to the nodes whose type is in `types`, keeping the shape
/// of `elems` where possible (a `Full` index holds every node, so the result is
/// read straight from the node type index).
pub(crate) fn index_with_node_types<I>(
    elems: &Index<I>,
    types: &[usize],
    exact: bool,
    g: &GraphStorage,
) -> Index<I>
where
    I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync,
{
    let has_type = |k: &I| types.contains(&g.node_type_id(VID((*k).into())));
    match elems {
        Index::Full(_) => Index::from_sorted(
            node_type_vids(g, types)
                .into_iter()
                .map(|vid| I::from(vid.0))
                .collect(),
            exact,
        ),
        Index::Partial(index) => index.iter().copied().filter(has_type).collect(),
        Index::Sorted { keys, exact: e } => {
            Index::from_sorted(keys.iter().copied().filter(has_type).collect(), *e && exact)
        }
    }
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> List<I> {
    /// Drops any exactness claim; see [`Index::into_inexact`].
    pub fn into_inexact(self) -> List<I> {
        match self {
            List::All => List::All,
            List::NodeTypeIdx { types, .. } => List::NodeTypeIdx {
                types,
                exact: false,
            },
            List::List { elems } => List::List {
                elems: elems.into_inexact(),
            },
        }
    }

    pub fn intersection(&self, other: &List<I>, g: &GraphStorage) -> List<I> {
        match (self, other) {
            (List::All, other) | (other, List::All) => other.clone(),
            (List::List { elems: a }, List::List { elems: b }) => List::List {
                elems: a.intersection(b),
            },
            (List::List { elems }, List::NodeTypeIdx { types, exact })
            | (List::NodeTypeIdx { types, exact }, List::List { elems }) => List::List {
                elems: index_with_node_types(elems, types, *exact, g),
            },
            (
                List::NodeTypeIdx {
                    types: left,
                    exact: el,
                },
                List::NodeTypeIdx {
                    types: right,
                    exact: er,
                },
            ) => List::NodeTypeIdx {
                types: left.iter().copied().filter(|i| right.contains(i)).collect(),
                exact: *el && *er,
            },
        }
    }

    pub fn union(&self, other: &List<I>, g: &GraphStorage) -> List<I> {
        match (self, other) {
            (List::All, _) | (_, List::All) => List::All,
            (List::List { elems: left }, List::List { elems: right }) => List::List {
                elems: left.union(right),
            },
            (List::NodeTypeIdx { types, exact }, List::List { elems })
            | (List::List { elems }, List::NodeTypeIdx { types, exact }) => {
                let typed = Index::from_sorted(
                    node_type_vids(g, types)
                        .into_iter()
                        .map(|vid| I::from(vid.0))
                        .collect(),
                    *exact,
                );
                List::List {
                    elems: typed.union(elems),
                }
            }
            (
                List::NodeTypeIdx {
                    types: left,
                    exact: el,
                },
                List::NodeTypeIdx {
                    types: right,
                    exact: er,
                },
            ) => List::NodeTypeIdx {
                types: left
                    .iter()
                    .copied()
                    .merge(right.iter().copied())
                    .dedup()
                    .collect(),
                exact: *el && *er,
            },
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
            List::NodeTypeIdx { exact, .. } => *exact,
            List::List { elems } => elems.dynamically_exact(),
        }
    }

    /// Whether the list is known to be empty. A `NodeTypeIdx` list is only
    /// reported empty when it selects no types, as answering precisely needs
    /// the storage; a false negative only costs callers an optimisation.
    pub fn is_empty(&self) -> bool {
        match self {
            List::All => false,
            List::NodeTypeIdx { types, .. } => types.is_empty(),
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
            (List::NodeTypeIdx { types: a, .. }, List::NodeTypeIdx { types: b, .. }) => {
                a.iter().all(|t| b.contains(t))
            }
            (List::List { elems }, List::NodeTypeIdx { types, .. }) => elems
                .iter()
                .all(|k| types.contains(&g.node_type_id(VID(k.into())))),
            (List::NodeTypeIdx { types, .. }, List::List { elems }) => g
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
            List::NodeTypeIdx { types, .. } => Iter3::J(
                g.node_type_index()
                    .arc_node_type_entry(&types)
                    .into_iter()
                    .dedup(),
            ),
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
            List::NodeTypeIdx { types, exact } => {
                Index::from_sorted(node_type_vids(g, &types), exact)
            }
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
