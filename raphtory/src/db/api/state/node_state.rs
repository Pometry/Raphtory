use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{
                node_state_ops::{NodeStateOps, ToOwnedValue},
                ops::Const,
            },
            view::{
                history::{
                    compose_history_from_items, CompositeHistory, History, HistoryDateTime,
                    HistoryEventId, HistoryTimestamp,
                },
                internal::{FilterOps, NodeList},
                DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use ahash::RandomState;
use indexmap::IndexSet;
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use itertools::Itertools;
use raphtory_api::core::storage::timeindex::EventTime;
use rayon::{iter::Either, prelude::*};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    ops::Range,
    sync::Arc,
};
use storage::state::{StateIndex, StateIndexIter};

#[derive(Debug)]
pub enum Index<K> {
    Full(Arc<StateIndex<K>>),
    Partial(Arc<IndexSet<K, RandomState>>),
    /// Keys in ascending `usize`-key order, deduplicated; positions are ranks
    /// in that order (membership by binary search, no hashing). `exact` means
    /// every key is known to satisfy the filter that produced this index, so
    /// consumers may skip per-key verification.
    Sorted {
        keys: Arc<[K]>,
        exact: bool,
    },
}

/// Two-pointer intersection of ascending, deduplicated key slices.
fn sorted_intersect<K: Copy + Into<usize>>(a: &[K], b: &[K]) -> Vec<K> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);

    while i < a.len() && j < b.len() {
        let (ka, kb): (usize, usize) = (a[i].into(), b[j].into());

        match ka.cmp(&kb) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }

    out
}

fn sorted_rank<K: Copy + Into<usize>>(keys: &[K], key: &K) -> Option<usize> {
    let needle: usize = (*key).into();
    keys.binary_search_by_key(&needle, |k| (*k).into()).ok()
}

impl<K> From<StateIndex<K>> for Index<K> {
    fn from(index: StateIndex<K>) -> Self {
        Self::Full(Arc::new(index))
    }
}

impl<K> From<IndexSet<K, RandomState>> for Index<K> {
    fn from(index: IndexSet<K, RandomState>) -> Self {
        Self::Partial(Arc::new(index))
    }
}

impl<K> Default for Index<K> {
    fn default() -> Self {
        Self::Partial(Arc::new(IndexSet::default()))
    }
}

impl<K> Clone for Index<K> {
    fn clone(&self) -> Self {
        match self {
            Index::Full(index) => Index::Full(index.clone()),
            Index::Partial(index) => Index::Partial(index.clone()),
            Index::Sorted { keys, exact } => Index::Sorted {
                keys: keys.clone(),
                exact: *exact,
            },
        }
    }
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> FromIterator<K> for Index<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        Self::Partial(Arc::new(IndexSet::from_iter(iter)))
    }
}

impl Index<VID> {
    pub fn for_graph<'graph>(graph: impl GraphViewOps<'graph>) -> Self {
        let (node_list, trusted) = graph.trusted_node_list();
        if trusted {
            match node_list {
                NodeList::All { .. } => Self::Full(graph.core_graph().node_state_index().into()),
                NodeList::List { elems } => elems,
            }
        } else {
            Self::from_iter(graph.nodes().iter().map(|node| node.node))
        }
    }
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> Index<K> {
    pub fn new(keys: impl Into<Arc<IndexSet<K, RandomState>>>) -> Self {
        Self::Partial(keys.into())
    }

    /// Keys already in ascending `usize`-key order and deduplicated (e.g.
    /// index-pushdown candidates). Unlike `FromIterator` (which preserves
    /// insertion order in a hash set), positions are ranks in key order.
    pub fn from_sorted(keys: Vec<K>, exact: bool) -> Self {
        debug_assert!(
            keys.windows(2)
                .all(|w| Into::<usize>::into(w[0]) < Into::<usize>::into(w[1])),
            "from_sorted requires ascending deduplicated keys"
        );
        Self::Sorted {
            keys: keys.into(),
            exact,
        }
    }

    /// True when this is a pushdown candidate list whose producer proved
    /// every key matches its filter.
    pub fn dynamically_exact(&self) -> bool {
        matches!(self, Index::Sorted { exact: true, .. })
    }

    /// Drops any exactness claim, for when a filter that is *not* reflected in
    /// the keys is applied on top of this index. `exact` means every key
    /// satisfies the filter that produced the index, so it stops being true
    /// the moment the caller's predicate grows past that filter.
    pub fn into_inexact(self) -> Self {
        match self {
            Index::Sorted { keys, exact: true } => Index::Sorted { keys, exact: false },
            other => other,
        }
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = K> + '_ {
        match self {
            Index::Full(index) => Either::Left(index.iter()),
            Index::Partial(index) => Either::Right(Either::Left(index.iter().copied())),
            Index::Sorted { keys, .. } => Either::Right(Either::Right(keys.iter().copied())),
        }
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = K> {
        match self {
            Index::Full(index) => Either::Left(index.into_par_iter().map(|(_, k)| k)),
            Index::Partial(index) => Either::Right(Either::Left(
                (0..index.len())
                    .into_par_iter()
                    .map(move |i| *index.get_index(i).unwrap()),
            )),
            Index::Sorted { keys, .. } => Either::Right(Either::Right(
                (0..keys.len()).into_par_iter().map(move |i| keys[i]),
            )),
        }
    }

    #[inline]
    pub fn index(&self, key: &K) -> Option<usize> {
        match self {
            Index::Full(index) => index.resolve(*key),
            Index::Partial(index) => index.get_index_of(key),
            Index::Sorted { keys, .. } => sorted_rank(keys, key),
        }
    }

    #[inline]
    pub fn value(&self, i: usize) -> Option<K> {
        match self {
            Index::Full(index) => index.global_index(i),
            Index::Partial(index) => index.get_index(i).copied(),
            Index::Sorted { keys, .. } => keys.get(i).copied(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Index::Full(index) => index.len(),
            Index::Partial(index) => index.len(),
            Index::Sorted { keys, .. } => keys.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, key: &K) -> bool {
        match self {
            Index::Full(index) => index.resolve(*key).is_some(),
            Index::Partial(index) => index.contains(key),
            Index::Sorted { keys, .. } => sorted_rank(keys, key).is_some(),
        }
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = (usize, K)> + '_ {
        match self {
            Index::Full(index) => Either::Left(index.par_iter()),
            Index::Partial(index) => Either::Right(Either::Left(
                index.par_iter().enumerate().map(|(i, v)| (i, *v)),
            )),
            Index::Sorted { keys, .. } => Either::Right(Either::Right(
                keys.par_iter().enumerate().map(|(i, v)| (i, *v)),
            )),
        }
    }

    pub fn intersection(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full(_), Self::Partial(a)) => Self::Partial(a.clone()),
            (Self::Partial(a), Self::Full(_)) => Self::Partial(a.clone()),
            (Self::Partial(a), Self::Partial(b)) => a.intersection(b).copied().collect(),
            (Self::Sorted { keys, exact }, Self::Full(_)) => Self::Sorted {
                keys: keys.clone(),
                exact: *exact,
            },
            (Self::Full(_), Self::Sorted { keys, exact }) => Self::Sorted {
                keys: keys.clone(),
                exact: *exact,
            },
            (Self::Sorted { keys: a, exact: ea }, Self::Sorted { keys: b, exact: eb }) => {
                Self::Sorted {
                    keys: sorted_intersect(a, b).into(),
                    exact: *ea && *eb,
                }
            }
            // a hash-set side carries no exactness claim
            (Self::Sorted { keys: a, .. }, Self::Partial(b)) => Self::Sorted {
                keys: a
                    .iter()
                    .copied()
                    .filter(|k| b.contains(k))
                    .collect::<Vec<_>>()
                    .into(),
                exact: false,
            },
            // keeps the left side's insertion order, so stays Partial
            (Self::Partial(a), Self::Sorted { keys: b, .. }) => a
                .iter()
                .copied()
                .filter(|k| sorted_rank(b, k).is_some())
                .collect(),
            _ => self.clone(),
        }
    }

    pub fn union(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full(index), Self::Partial(_) | Self::Sorted { .. })
            | (Self::Partial(_) | Self::Sorted { .. }, Self::Full(index)) => {
                Self::Full(index.clone())
            }
            (Self::Full(left), Self::Full(right)) => Self::Full(Arc::new(left.union(right))),
            (Self::Partial(left), Self::Partial(right)) => left.union(right).copied().collect(),
            (Self::Sorted { keys: a, exact: ea }, Self::Sorted { keys: b, exact: eb }) => {
                Self::Sorted {
                    keys: a
                        .iter()
                        .copied()
                        .merge_by(b.iter().copied(), |x, y| {
                            Into::<usize>::into(*x) <= Into::<usize>::into(*y)
                        })
                        .dedup_by(|x, y| Into::<usize>::into(*x) == Into::<usize>::into(*y))
                        .collect::<Vec<_>>()
                        .into(),
                    exact: *ea && *eb,
                }
            }
            // mixed orders have no common canonical form: collect to Partial
            (Self::Sorted { keys: a, .. }, Self::Partial(b)) => {
                a.iter().copied().chain(b.iter().copied()).collect()
            }
            (Self::Partial(a), Self::Sorted { keys: b, .. }) => {
                a.iter().copied().chain(b.iter().copied()).collect()
            }
        }
    }

    /// Whether every key of `self` is also in `other`.
    ///
    /// A false negative only costs a caller an optimisation, but a false
    /// positive is unsound — `OrOp::const_value_in_domain` answers
    /// `Some(true)` on the strength of this — so the wildcard arms are only
    /// the two directions that are safe for *any* variant, and every other
    /// pairing is spelled out. `exact` plays no part: this is a question about
    /// key sets, not about what the keys satisfy.
    pub fn is_subset(&self, other: &Self) -> bool {
        match (self, other) {
            // `Full` holds every key, so it contains anything
            (_, Index::Full(_)) => true,
            // and nothing else is known to hold every key
            (Index::Full(_), _) => false,
            (Index::Partial(a), Index::Partial(b)) => a.is_subset(b.as_ref()),
            (Index::Sorted { keys: a, .. }, Index::Partial(b)) => {
                a.iter().all(|key| b.contains(key))
            }
            (Index::Partial(a), Index::Sorted { keys: b, .. }) => {
                a.iter().all(|key| sorted_contains(b, *key))
            }
            (Index::Sorted { keys: a, .. }, Index::Sorted { keys: b, .. }) => {
                sorted_is_subset(a, b)
            }
        }
    }
}

/// Membership in a [`Index::Sorted`] key slice, which ascends by `usize` key.
fn sorted_contains<K: Copy + Into<usize>>(keys: &[K], key: K) -> bool {
    let key: usize = key.into();
    keys.binary_search_by(|probe| Into::<usize>::into(*probe).cmp(&key))
        .is_ok()
}

/// Whether every key of `a` is in `b`, both ascending by `usize` key: one pass
/// over each rather than `a.len()` binary searches. Tolerates duplicates on
/// either side, though [`Index::Sorted`] does not produce them.
fn sorted_is_subset<K: Copy + Into<usize>>(a: &[K], b: &[K]) -> bool {
    let mut j = 0usize;
    for key in a.iter().map(|key| Into::<usize>::into(*key)) {
        while j < b.len() && Into::<usize>::into(b[j]) < key {
            j += 1;
        }
        if j == b.len() || Into::<usize>::into(b[j]) != key {
            return false;
        }
    }
    true
}

#[derive(Clone)]
pub struct PartialIndexIntoIter<K> {
    range: Range<usize>,
    index: Arc<IndexSet<K, RandomState>>,
}

impl<K: Eq + Hash + Copy> Iterator for PartialIndexIntoIter<K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        self.index.get_index(i).copied()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.range.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.range.nth(n)?;
        self.index.get_index(i).copied()
    }
}

impl<K: Eq + Hash + Copy> DoubleEndedIterator for PartialIndexIntoIter<K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let i = self.range.next_back()?;
        self.index.get_index(i).copied()
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.range.nth_back(n)?;
        self.index.get_index(i).copied()
    }
}

impl<K: Eq + Hash + Copy> ExactSizeIterator for PartialIndexIntoIter<K> {}

#[derive(Clone)]
pub struct SortedIndexIntoIter<K> {
    range: Range<usize>,
    index: Arc<[K]>,
}

impl<K: Copy> Iterator for SortedIndexIntoIter<K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        self.index.get(i).copied()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.range.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.range.nth(n)?;
        self.index.get(i).copied()
    }
}

impl<K: Copy> DoubleEndedIterator for SortedIndexIntoIter<K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let i = self.range.next_back()?;
        self.index.get(i).copied()
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.range.nth_back(n)?;
        self.index.get(i).copied()
    }
}

impl<K: Copy> ExactSizeIterator for SortedIndexIntoIter<K> {}

#[derive(Clone, Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator)]
pub enum IndexIntoIter<K> {
    Full(StateIndexIter<Arc<StateIndex<K>>, K>),
    Partial(PartialIndexIntoIter<K>),
    Sorted(SortedIndexIntoIter<K>),
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> IntoIterator for Index<K> {
    type Item = K;
    type IntoIter = IndexIntoIter<K>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Index::Full(index) => IndexIntoIter::Full(index.arc_into_iter()),
            Index::Partial(index) => IndexIntoIter::Partial(PartialIndexIntoIter {
                range: 0..index.len(),
                index,
            }),
            Index::Sorted { keys, .. } => IndexIntoIter::Sorted(SortedIndexIntoIter {
                range: 0..keys.len(),
                index: keys,
            }),
        }
    }
}

#[derive(Clone)]
pub struct NodeState<'graph, V, G> {
    base_graph: G,
    values: Arc<[V]>,
    keys: Index<VID>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, V: Debug + Clone + Send + Sync + 'graph, G: GraphViewOps<'graph>> Debug
    for NodeState<'graph, V, G>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(node, value)| (node.id(), value)))
            .finish()
    }
}

impl<'graph, RHS: Send + Sync, V: PartialEq<RHS> + Send + Sync + Clone + 'graph, G>
    PartialEq<Vec<RHS>> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values.par_iter().eq(other)
    }
}

impl<'graph, RHS: Send + Sync, V: PartialEq<RHS> + Send + Sync + Clone + 'graph, G>
    PartialEq<&[RHS]> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &&[RHS]) -> bool {
        self.values.par_iter().eq(*other)
    }
}

impl<'a, 'graph, V: Clone + Send + Sync + PartialEq + 'graph, G: GraphViewOps<'graph>>
    PartialEq<NodeState<'graph, V, G>> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &NodeState<'graph, V, G>) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| <&V as ToOwnedValue<V>>::to_owned_value(v) == value.clone())
                    .unwrap_or(false)
            })
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: Send + Sync,
        V: PartialEq<RHS> + Send + Sync + Clone + 'graph,
        G: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|&lhs| lhs == rhs).is_some())
    }
}

impl<'graph, V, G: IntoDynamic> NodeState<'graph, V, G> {
    pub fn into_dyn(self) -> NodeState<'graph, V, DynamicGraph> {
        NodeState::new(self.base_graph.into_dynamic(), self.values, self.keys)
    }
}

impl<'graph, V, G: GraphViewOps<'graph>> NodeState<'graph, V, G> {
    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    pub fn new_from_eval(graph: G, values: Vec<V>) -> Self {
        let index = Index::for_graph(graph.clone());
        // Values are already in flat index order from TaskRunner
        Self::new(graph, values.into(), index)
    }

    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `index`: the index mapping VID to flat position in values
    pub fn new_from_eval_with_index(graph: G, values: Vec<V>, index: Index<VID>) -> Self {
        // Values are already in flat index order from TaskRunner
        Self::new(graph, values.into(), index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped_with_index<R: Clone>(
        graph: G,
        values: Vec<R>,
        index: Index<VID>,
        map: impl Fn(R) -> V,
    ) -> Self
    where
        V: std::fmt::Debug,
    {
        // Values are already in flat index order from TaskRunner, just map them
        let values = values.into_iter().map(map).collect();
        Self::new(graph, values, index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: Clone>(graph: G, values: Vec<R>, map: impl Fn(R) -> V) -> Self
    where
        V: std::fmt::Debug,
    {
        let index = Index::for_graph(graph.clone());
        // Values are already in flat index order from TaskRunner, just map them
        let values = values.into_iter().map(map).collect();
        Self::new(graph, values, index)
    }

    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(graph, [].into(), index)
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(graph: G, values: impl Into<Arc<[V]>>) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(graph, values.into(), index)
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R, S: BuildHasher>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
    ) -> Self {
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            Self::new_from_values(graph, values)
        } else {
            let (index, values): (IndexSet<VID, RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            Self::new(graph, values.into(), Index::Partial(index.into()))
        }
    }

    pub fn keys(&self) -> &Index<VID> {
        &self.keys
    }
}

impl<'graph, V, G: GraphViewOps<'graph>> NodeState<'graph, V, G> {
    pub fn new(base_graph: G, values: Arc<[V]>, keys: Index<VID>) -> Self {
        Self {
            base_graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }

    pub fn values(&self) -> &Arc<[V]> {
        &self.values
    }

    pub fn ids(&self) -> &Index<VID> {
        &self.keys
    }
}

impl<'graph, V: Send + Sync + Clone + 'graph, G: GraphViewOps<'graph>> IntoIterator
    for NodeState<'graph, V, G>
{
    type Item = (NodeView<'graph, G>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes()
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<'a, 'graph: 'a, V: Clone + Send + Sync + 'graph, G: GraphViewOps<'graph>>
    NodeStateOps<'a, 'graph> for NodeState<'graph, V, G>
{
    type Graph = G;
    type BaseGraph = G;
    type Select = Const<bool>;
    type Value = &'a V;
    type OwnedValue = V;
    type OutputType = Self;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn iter_values(&'a self) -> impl Iterator<Item = Self::Value> + 'a
    where
        'graph: 'a,
    {
        self.values.iter()
    }

    fn par_iter_values(&'a self) -> impl ParallelIterator<Item = Self::Value> + 'a
    where
        'graph: 'a,
    {
        self.values.par_iter()
    }

    #[allow(refining_impl_trait)]
    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len()).map(move |i| self.values[i].clone())
    }

    #[allow(refining_impl_trait)]
    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len())
            .into_par_iter()
            .map(move |i| self.values[i].clone())
    }

    fn iter(&'a self) -> impl Iterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value)> + 'a
    where
        'graph: 'a,
    {
        self.keys
            .iter()
            .zip(self.values.iter())
            .map(move |(n, v)| (NodeView::new_internal(&self.base_graph, n), v))
    }

    fn nodes<'g>(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph, Self::Select> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.base_graph.clone(),
            Const(true),
            self.keys.clone(),
        )
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a <Self as NodeStateOps<'a, 'graph>>::Graph>,
            <Self as NodeStateOps<'a, 'graph>>::Value,
        ),
    >
    where
        'graph: 'a,
    {
        self.keys.par_iter().map(move |(val_id, n)| {
            (
                NodeView::new_internal(&self.base_graph, n),
                &self.values[val_id],
            )
        })
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(NodeView<'a, &'a Self::Graph>, Self::Value)> {
        let vid = self.keys.value(index)?;
        Some((
            NodeView::new_internal(&self.base_graph, vid),
            &self.values[index],
        ))
    }

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let id = self.base_graph.internalise_node(node.as_node_ref())?;
        self.keys.index(&id).map(|i| &self.values[i])
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn construct(
        &self,
        base_graph: Self::BaseGraph,
        _graph: Self::Graph,
        keys: IndexSet<VID, RandomState>,
        values: Vec<Self::OwnedValue>,
    ) -> Self
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        NodeState::new(base_graph, values.into(), Index::new(keys))
    }
}

impl<'graph, G: GraphViewOps<'graph>>
    NodeState<'graph, History<'graph, NodeView<'graph, DynamicGraph>>, G>
{
    pub fn t(&self) -> NodeState<'graph, HistoryTimestamp<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().t())
            .collect::<Vec<HistoryTimestamp<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn dt(&self) -> NodeState<'graph, HistoryDateTime<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().dt())
            .collect::<Vec<HistoryDateTime<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn event_id(&self) -> NodeState<'graph, HistoryEventId<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().event_id())
            .collect::<Vec<HistoryEventId<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn earliest_time(&self) -> Option<EventTime> {
        self.values.iter().filter_map(|h| h.earliest_time()).min()
    }

    pub fn latest_time(&self) -> Option<EventTime> {
        self.values.iter().filter_map(|h| h.latest_time()).max()
    }

    /// Collect and return all the contained time entries as a sorted list
    pub fn collect_time_entries(&self) -> Vec<EventTime> {
        let mut entries: Vec<EventTime> = self
            .par_iter_values()
            .flat_map_iter(|hist| hist.iter())
            .collect();
        entries.par_sort_unstable();
        entries
    }

    /// Flattens all history objects into a single history object with all time entries ordered.
    pub fn flatten(
        &self,
    ) -> History<'graph, CompositeHistory<'graph, NodeView<'graph, DynamicGraph>>> {
        let histories: Vec<_> = self.par_iter_values().map(|hist| hist.0.clone()).collect();
        compose_history_from_items(histories)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::api::state::{node_state::NodeState, AsOrderedNodeStateOps, OrderedNodeStateOps},
        prelude::*,
    };

    #[test]
    fn float_state() {
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None, None).unwrap();
        let float_state = NodeState::new_from_values(g.clone(), [0.0f64]);
        let int_state = NodeState::new_from_values(g.clone(), [1i64]);
        let min_float = float_state.min_item().unwrap().1;
        let min_int = int_state.min_item().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}

#[cfg(test)]
mod index_subset_test {
    use super::*;
    use crate::core::entities::VID;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    fn sorted(keys: &[usize]) -> Index<VID> {
        let mut keys: Vec<VID> = keys.iter().map(|k| VID(*k)).collect();
        keys.sort_by_key(|k| k.0);
        keys.dedup();
        Index::Sorted {
            keys: keys.into(),
            exact: true,
        }
    }

    fn partial(keys: &[usize]) -> Index<VID> {
        keys.iter().map(|k| VID(*k)).collect()
    }

    proptest! {
        /// Every representation pairing must agree with `BTreeSet::is_subset`.
        #[test]
        fn agrees_with_a_set_reference(
            a in proptest::collection::vec(0usize..12, 0..8),
            b in proptest::collection::vec(0usize..12, 0..8),
        ) {
            let want = BTreeSet::from_iter(a.iter().copied())
                .is_subset(&BTreeSet::from_iter(b.iter().copied()));

            prop_assert_eq!(sorted(&a).is_subset(&sorted(&b)), want, "sorted/sorted");
            prop_assert_eq!(sorted(&a).is_subset(&partial(&b)), want, "sorted/partial");
            prop_assert_eq!(partial(&a).is_subset(&sorted(&b)), want, "partial/sorted");
            prop_assert_eq!(partial(&a).is_subset(&partial(&b)), want, "partial/partial");
        }
    }

    fn full(len: usize) -> Index<VID> {
        Index::Full(Arc::new(StateIndex::new([len], len as u32)))
    }

    #[test]
    fn full_is_only_contained_by_full() {
        assert!(full(4).is_subset(&full(4)));
        // conservatively false even though these do hold every key of a
        // 3-node graph: `Full` makes no claim about the other side's contents
        assert!(!full(3).is_subset(&sorted(&[0, 1, 2])));
        assert!(!full(3).is_subset(&partial(&[0, 1, 2])));
        // and `Full` contains everything
        assert!(sorted(&[0, 1]).is_subset(&full(4)));
        assert!(partial(&[0, 1]).is_subset(&full(4)));
    }
}
