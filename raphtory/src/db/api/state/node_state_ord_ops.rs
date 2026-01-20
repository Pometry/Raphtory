use crate::db::{api::state::node_state_ops::NodeStateOps, graph::node::NodeView};
use num_traits::float::FloatCore;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fmt::{Debug, Formatter},
    ops::Deref,
};

trait AsOrd {
    type Ordered: ?Sized + Ord;
    /// Converts reference of this type into reference of an ordered Type.
    ///
    /// This is the same as AsRef (with the additional constraint that the target type needs to be ordered).
    ///
    /// Importantly, unlike AsRef, this blanket-implements the trivial conversion from a type to itself!
    fn as_ord(&self) -> &Self::Ordered;
}

trait IsFloat: FloatCore {}

impl IsFloat for f32 {}

impl IsFloat for f64 {}

// implement on references to avoid conflicts with Ord
impl<T: IsFloat> AsOrd for &T {
    type Ordered = OrderedFloat<T>;
    fn as_ord(&self) -> &OrderedFloat<T> {
        (*self).into()
    }
}

impl<T: IsFloat> AsOrd for &(T, T) {
    type Ordered = (OrderedFloat<T>, OrderedFloat<T>);
    fn as_ord(&self) -> &(OrderedFloat<T>, OrderedFloat<T>) {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values, i.e. there is no physical difference between OrderedFloat and Float.
        unsafe { &*(*self as *const (T, T) as *const (OrderedFloat<T>, OrderedFloat<T>)) }
    }
}

pub trait OrderedNodeStateOps<'a, 'graph: 'a>: NodeStateOps<'a, 'graph>
where
    Self::Value: Ord,
{
    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    fn sort_by_values(&'a self, reverse: bool) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// Arguments:
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// Returns:
    ///
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    fn top_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    fn bottom_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    /// Returns a tuple of the min result with its key
    fn min_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    /// Min result.
    ///
    /// Returns:
    ///     PropValue:
    fn min(&'a self) -> Option<Self::Value> {
        self.min_item().map(|(_, v)| v)
    }

    /// Returns a tuple of the max result with its key
    fn max_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    fn max(&'a self) -> Option<Self::Value> {
        self.max_item().map(|(_, v)| v)
    }

    /// Returns a tuple of the median result with its key
    fn median_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    /// Returns:
    ///     PropValue:
    fn median(&'a self) -> Option<Self::Value> {
        self.median_item().map(|(_, v)| v)
    }
}

pub trait AsOrderedNodeStateOps<'a, 'graph: 'a>: NodeStateOps<'a, 'graph> {
    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    fn sort_by_values(&'a self, reverse: bool) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// Arguments:
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// Returns:
    ///
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    fn top_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    fn bottom_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    /// Returns a tuple of the min result with its key
    fn min_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    /// Min result.
    ///
    /// Returns:
    ///     PropValue:
    fn min(&'a self) -> Option<Self::Value> {
        self.min_item().map(|(_, v)| v)
    }

    /// Returns a tuple of the max result with its key
    fn max_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    fn max(&'a self) -> Option<Self::Value> {
        self.max_item().map(|(_, v)| v)
    }

    /// Returns a tuple of the median result with its key
    fn median_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )>;

    /// Returns:
    ///     PropValue:
    fn median(&'a self) -> Option<Self::Value> {
        self.median_item().map(|(_, v)| v)
    }
}

impl<'a, 'graph: 'a, V: NodeStateOps<'a, 'graph>> OrderedNodeStateOps<'a, 'graph> for V
where
    V::Value: Ord,
{
    fn sort_by_values(&'a self, reverse: bool) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        if reverse {
            self.sort_by_values_by(|a, b| a.cmp(b).reverse())
        } else {
            self.sort_by_values_by(Ord::cmp)
        }
    }

    fn top_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.top_k_by(Ord::cmp, k)
    }

    fn bottom_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.bottom_k_by(Ord::cmp, k)
    }

    fn min_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.min_item_by(Ord::cmp)
    }

    fn max_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.max_item_by(Ord::cmp)
    }

    fn median_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.median_item_by(Ord::cmp)
    }
}

impl<'a, 'graph: 'a, V: NodeStateOps<'a, 'graph>> AsOrderedNodeStateOps<'a, 'graph> for V
where
    V::Value: AsOrd,
{
    fn sort_by_values(&'a self, reverse: bool) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        if reverse {
            self.sort_by_values_by(|a, b| a.as_ord().cmp(b.as_ord()).reverse())
        } else {
            self.sort_by_values_by(|a, b| a.as_ord().cmp(b.as_ord()))
        }
    }

    fn top_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.top_k_by(|a, b| a.as_ord().cmp(b.as_ord()), k)
    }

    fn bottom_k(&'a self, k: usize) -> Self::OutputType
    //NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.bottom_k_by(|a, b| a.as_ord().cmp(b.as_ord()), k)
    }

    fn min_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.min_item_by(|a, b| a.as_ord().cmp(b.as_ord()))
    }

    fn max_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.max_item_by(|a, b| a.as_ord().cmp(b.as_ord()))
    }

    fn median_item(
        &'a self,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        self.median_item_by(|a, b| a.as_ord().cmp(b.as_ord()))
    }
}

struct Ordered<V, F> {
    value: V,
    cmp_fn: F,
}

impl<V: Debug, F> Debug for Ordered<V, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<V, F> PartialEq for Ordered<V, F>
where
    F: Fn(&V, &V) -> Ordering,
{
    fn eq(&self, other: &Self) -> bool {
        matches!(self.cmp(other), Ordering::Equal)
    }
}

impl<V, F> Eq for Ordered<V, F> where F: Fn(&V, &V) -> Ordering {}

impl<V, F> PartialOrd for Ordered<V, F>
where
    F: Fn(&V, &V) -> Ordering,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<V, F> Ord for Ordered<V, F>
where
    F: Fn(&V, &V) -> Ordering,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = &self.cmp_fn;
        cmp(&self.value, &other.value)
    }
}

pub fn top_k<V: Send + Sync, F>(iter: impl IntoIterator<Item = V>, cmp: F, k: usize) -> Vec<V>
where
    F: Fn(&V, &V) -> Ordering + Send + Sync,
{
    let mut heap: BinaryHeap<Reverse<Ordered<V, &F>>> = BinaryHeap::with_capacity(k);

    for v in iter {
        let elem = Reverse(Ordered {
            value: v,
            cmp_fn: &cmp,
        });
        if heap.len() < k {
            // heap is still not full, push the element and return
            heap.push(elem);
        } else if heap.peek() > Some(&elem) {
            // May need to push this element, drop the read guard and wait for write access
            if let Some(mut first_mut) = heap.peek_mut() {
                *first_mut = elem;
            };
        }
    }

    let values: Vec<V> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|Reverse(Ordered { value, .. })| value)
        .collect();

    values
}

#[allow(dead_code)]
pub fn par_top_k<V: Send + Sync, F>(
    iter: impl IntoParallelIterator<Item = V>,
    cmp: F,
    k: usize,
) -> Vec<V>
where
    F: Fn(&V, &V) -> Ordering + Send + Sync,
{
    let heap: RwLock<BinaryHeap<Reverse<Ordered<V, &F>>>> =
        RwLock::new(BinaryHeap::with_capacity(k));

    iter.into_par_iter().for_each(|v| {
        let elem = Reverse(Ordered {
            value: v,
            cmp_fn: &cmp,
        });
        if heap.read().len() < k {
            let mut write_guard = heap.write();
            if write_guard.len() < k {
                // heap is still not full, push the element and return
                return write_guard.push(elem);
            }
        }
        if heap.read().peek() >= Some(&elem) {
            // May need to push this element, drop the read guard and wait for write access
            let mut write_guard = heap.write();
            if let Some(mut first_mut) = write_guard.peek_mut() {
                if first_mut.deref() > &elem {
                    *first_mut = elem;
                }
            };
        }
    });

    let values: Vec<V> = heap
        .into_inner()
        .into_sorted_vec()
        .into_iter()
        .map(|Reverse(Ordered { value, .. })| value)
        .collect();

    values
}

#[cfg(test)]
mod test {
    use crate::db::api::state::node_state_ord_ops::{par_top_k, top_k};

    use rand; // 0.8.5

    use rand::distributions::{Distribution, Uniform};
    use tokio::time::Instant;

    fn gen_x_ints(
        count: u32,
        distribution: impl Distribution<u32>,
        rng: &mut (impl rand::Rng + ?Sized),
    ) -> Vec<u32> {
        let mut results = Vec::with_capacity(count as usize);
        let iter = distribution.sample_iter(rng);
        for (_, sample) in (0..count).zip(iter) {
            results.push(sample);
        }
        results
    }

    #[test]
    fn test_top_k() {
        let values = gen_x_ints(
            100_000_000,
            Uniform::new(0, 10000000),
            &mut rand::thread_rng(),
        ); // [4i32, 2, 3, 100, 4, 2];
        let timer = Instant::now();
        let res1 = top_k(values.clone(), |a, b| a.cmp(b), 100);
        println!("Top K in: {:?}", timer.elapsed());
        let timer = Instant::now();
        let res2 = par_top_k(values, |a, b| a.cmp(b), 100);
        println!("Par Top K in: {:?}", timer.elapsed());
        assert_eq!(res1, res2);
        //assert_eq!(res, par_top_k(values, |a, b| a.cmp(b), 100))  //[100, 4, 4])
    }
}
