//!  Defines the `Program` trait, which represents code that is used to evaluate
//!  algorithms and custom code that can be run on the graph.

use std::{
    cell::{Ref, RefCell},
    fmt::Debug,
    rc::Rc,
    sync::Arc,
};

use crate::core::{
    agg::Accumulator,
    state::{AccId, ShuffleComputeState},
    state::{ComputeStateMap, StateType},
};
use crate::db::edge::EdgeView;
use crate::db::graph_window::WindowedGraph;
use crate::db::vertex::VertexView;
use crate::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use itertools::Itertools;
use rand_distr::weighted_alias::AliasableWeight;
use rayon::prelude::*;
use rustc_hash::FxHashSet;

type CS = ComputeStateMap;

/// A reference to an accumulator for aggregation operations.
/// `A` is the type of the state being accumulated.
/// `IN` is the type of the input messages.
/// `OUT` is the type of the output messages.
/// `ACC` is the type of the accumulator.
#[derive(Debug, Clone)]
pub struct AggRef<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(AccId<A, IN, OUT, ACC>)
where
    A: StateType;

// impl AggRef new
impl<A, IN, OUT, ACC: Accumulator<A, IN, OUT>> AggRef<A, IN, OUT, ACC>
where
    A: StateType,
{
    /// Creates a new `AggRef` object.
    ///
    /// # Arguments
    ///
    /// * `agg_ref` - The ID of the accumulator to reference.
    ///
    /// # Returns
    ///
    /// An `AggRef` object.
    pub(crate) fn new(agg_ref: AccId<A, IN, OUT, ACC>) -> Self {
        Self(agg_ref)
    }
}

/// A struct representing the local state of a shard.
pub struct LocalState<G: GraphViewOps> {
    ss: usize,
    shard: usize,
    graph: G,
    shard_local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    next_vertex_set: Option<Arc<FxHashSet<u64>>>,
}

impl<G: GraphViewOps> LocalState<G> {
    /// Creates a new `LocalState` object.
    ///
    /// # Arguments
    ///
    /// * `ss` - `The evaluation super step.
    /// * `shard` - The shard index.
    /// * `graph` - The graph to be processed.
    /// * `window` - The range of the window.
    /// * `shard_local_state` - The local state of the shard.
    /// * `next_vertex_set` - An optional set of vertices to process in the next iteration.
    ///
    /// # Returns
    ///
    /// A new `LocalState` object.
    pub fn new(
        ss: usize,
        shard: usize,
        graph: G,
        shard_local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
        next_vertex_set: Option<Arc<FxHashSet<u64>>>,
    ) -> Self {
        Self {
            ss,
            shard,
            graph,
            shard_local_state,
            next_vertex_set,
        }
    }

    /// Creates an `AggRef` object for the specified accumulator.
    ///
    /// # Arguments
    ///
    /// * `agg_ref` - The ID of the accumulator to reference.
    ///
    /// # Returns
    ///
    /// An `AggRef` object.
    pub(crate) fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_ref: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        AggRef(agg_ref)
    }

    /// Creates an `AggRef` object for the specified global accumulator.
    ///
    /// # Arguments
    ///
    /// * `agg_ref` - The ID of the global accumulator to reference.
    ///
    /// # Returns
    ///
    /// An `AggRef` object.
    pub(crate) fn global_agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_ref: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        AggRef(agg_ref)
    }

    /// Performs a computation step for the vertices assigned to the worker.
    /// The given function is applied to each vertex, represented as an EvalVertexView instance.
    ///
    ///
    /// # Arguments
    ///
    /// * `f` - The function to execute on each vertex.
    pub(crate) fn step<F>(&self, f: F)
    where
        F: Fn(EvalVertexView<G>),
    {
        let graph = self.graph.clone();

        let iter: Box<dyn Iterator<Item = VertexView<G>>> = match self.next_vertex_set {
            None => Box::new(
                graph
                    .vertices_shard(self.shard)
                    .map(|vref| VertexView::new(graph.clone(), vref)),
            ),
            Some(ref next_vertex_set) => Box::new(
                next_vertex_set
                    .iter()
                    .flat_map(|&v| graph.vertex_ref(v))
                    .map(|vref| VertexView::new(graph.clone(), vref)),
            ),
        };

        let mut c = 0;
        iter.for_each(|v| {
            f(EvalVertexView::new(
                self.ss,
                v,
                self.shard_local_state.clone(),
            ));
            c += 1;
        });
    }

    /// Returns the local state of the worker as a ShuffleComputeState instance.
    fn consume(self) -> ShuffleComputeState<CS> {
        Rc::try_unwrap(self.shard_local_state).unwrap().into_inner()
    }
}

/// GlobalEvalState represents the state of the computation across all shards.
///
/// # Arguments
///
/// * `ss`: represents the number of steps the evaluation loop ran for.
///  * `g`: an instance of the Graph struct.
///  * `window`: a range of signed integers that represents the window of the computation.
///  * `keep_past_state`: a boolean that indicates whether past state should be retained.
///  * `next_vertex_set`: an optional vector of arc pointers to hash sets of unsigned 64-bit integers. The vector represents the next set of vertices that will be processed. If the option is None, then all vertices have been processed.
///  * `states`: a vector of arc pointers to read-write locks that contain the state of the computation for each shard.
///  * `post_agg_state`: an arc pointer to a read-write lock that contains the state of the computation after aggregation.
///
#[derive(Debug)]
pub struct GlobalEvalState<G: GraphViewOps> {
    pub ss: usize,
    g: G,
    pub keep_past_state: bool,
    // running state
    pub next_vertex_set: Option<Vec<Arc<FxHashSet<u64>>>>,
    states: Vec<Arc<parking_lot::RwLock<Option<ShuffleComputeState<CS>>>>>,
    resetable_states: Vec<u32>,
}

/// Implementation of the GlobalEvalState struct.
/// The GlobalEvalState struct is used to represent the state of the computation across all shards.
///
/// # Arguments
///
///
///
impl<G: GraphViewOps> GlobalEvalState<G> {
    /// Reads the vector partitions for the given accumulator.
    ///
    /// # Arguments
    ///
    /// * `agg` - An accumulator ID.
    ///
    /// # Returns
    ///
    /// A vector of vector partitions.
    ///
    /// # Type Parameters
    ///
    /// * `A` - A state type.
    /// * `IN` - A state type.
    /// * `OUT` - A state type.
    /// * `ACC` - An accumulator.
    ///
    /// # Constraints
    ///
    /// * `OUT: StateType`
    /// * `A: 'static`
    pub fn read_vec_partitions<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
    ) -> Vec<Vec<Vec<(u64, OUT)>>>
    // ) -> Vec<Vec<Vec<(u64, OUT)>>>
    where
        OUT: StateType,
        A: 'static,
    {
        self.states
            .iter()
            .map(|state| {
                let state = state.read();
                let state = state.as_ref().unwrap();
                state.read_vec_partition::<A, IN, OUT, ACC>(self.ss, agg)
            })
            .collect()
    }

    /// Folds the state for the given accumulator, partition ID, initial value, and folding function.
    /// It returns the result of folding the accumulated values for the specified shard
    /// using the provided closure.
    ///
    /// # Arguments
    ///
    /// * `agg` - An accumulator ID.
    /// * `part_id` - A partition ID.
    /// * `b` - An initial value.
    /// * `f` - A folding function.
    ///
    /// # Returns
    ///
    /// The folded state.
    ///
    /// # Type Parameters
    ///
    /// * `A` - A state type.
    /// * `IN` - A state type.
    /// * `OUT` - A state type.
    /// * `ACC` - An accumulator.
    /// * `B` - The output type of the folding function.
    /// * `F` - The type of the folding function.
    ///
    /// # Constraints
    ///
    /// * `OUT: StateType`
    /// * `A: StateType`
    /// * `F: Fn(B, &u64, OUT) -> B + std::marker::Copy`
    pub fn fold_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, B, F>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
        part_id: usize,
        b: B,
        f: F,
    ) -> B
    where
        OUT: StateType,
        A: StateType,
        B: Debug,
        F: Fn(B, &u64, OUT) -> B + Copy,
    {
        let part_state = self.states[part_id].read();
        let part_state = part_state.as_ref().unwrap();

        part_state.fold_state::<A, IN, OUT, ACC, B, F>(self.ss, b, agg, f)
    }

    /// Reads the global state for a given accumulator, returned value is the global
    /// accumulated value for all shards. If the state does not exist, returns None.
    ///
    /// # Arguments
    ///
    /// * `agg` - A reference to the `AccId` struct representing the accumulator.
    ///
    /// # Type Parameters
    ///
    /// * `A` - The type of the state that the accumulator uses.
    /// * `IN` - The input type of the accumulator.
    /// * `OUT` - The output type of the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Return Value
    ///
    /// An optional `OUT` value representing the global state for the accumulator.
    pub fn read_global_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        OUT: StateType,
        A: StateType,
    {
        let state = self.states[0].read();
        let state = state.as_ref().unwrap();
        state.read_global(self.ss, agg)
    }

    /// Determines whether the `next_vertex_set` is empty or not.
    ///
    /// # Return Value
    ///
    /// A boolean value indicating whether the `next_vertex_set` is empty or not.
    fn do_loop(&self) -> bool {
        if self.next_vertex_set.is_none() {
            return true;
        }
        self.next_vertex_set.as_ref().map(|next_vertex_set_shard| {
            next_vertex_set_shard
                .iter()
                .any(|next_vertex_set| !next_vertex_set.is_empty())
        }) == Some(true)
    }

    /// Creates a new `Context` object with the specified parameters with n_parts as input.
    ///
    /// # Arguments
    ///
    /// * `g` - The `Graph` object to use.
    /// * `window` - The range of timestamps to consider.
    /// * `keep_past_state` - Whether to keep the past state or not.
    ///
    /// # Return Value
    ///
    /// A new `Context` object.
    pub fn new(g: G, keep_past_state: bool) -> Self {
        let n_parts = g.num_shards();
        let mut states = Vec::with_capacity(n_parts);
        for _ in 0..n_parts {
            states.push(Arc::new(parking_lot::RwLock::new(Some(
                ShuffleComputeState::new(n_parts),
            ))));
        }
        Self {
            ss: 0,
            g,
            keep_past_state,
            next_vertex_set: None,
            states,
            resetable_states: Vec::new(),
        }
    }

    /// Runs the global aggregation function for the given accumulator.
    ///
    /// # Arguments
    ///
    /// * `agg` - The `AccId` object representing the accumulator.
    ///
    /// # Type Parameters
    ///
    /// * `A` - The type of the state that the accumulator uses.
    /// * `IN` - The input type of the accumulator.
    /// * `OUT` - The output type of the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Return Value
    ///
    /// An `AggRef` object representing the new state for the accumulator.
    pub(crate) fn global_agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        self.agg_internal(agg, false)
    }

    /// Runs the global aggregation function for the given accumulator.
    ///
    /// # Arguments
    ///
    /// * `agg` - The `AccId` object representing the accumulator.
    ///
    /// # Type Parameters
    ///
    /// * `A` - The type of the state that the accumulator uses.
    /// * `IN` - The input type of the accumulator.
    /// * `OUT` - The output type of the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Return Value
    ///
    /// An `AggRef` object representing the new state for the accumulator.
    pub(crate) fn global_agg_reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        self.agg_internal(agg, true)
    }

    /// Applies an accumulator to the states of all shards in parallel.
    ///
    /// This method removes the accumulated state represented by `agg_ref` from the states,
    /// then merges it across all states (in parallel), and updates the post_agg_state.
    /// If the new state is not the same as the old one, then it merges them too.
    /// Finally, it resets the accumulator while keeping the old value available
    ///
    /// # Arguments
    ///
    /// * `self` - A mutable reference to a `Shuffle` instance.
    /// * `agg` - The accumulator function to apply to the states.
    ///
    /// # Type parameters
    ///
    /// * `A` - The type of the state.
    /// * `IN` - The type of the input to the accumulator.
    /// * `OUT` - The type of the output from the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Constraints
    ///
    /// * `A` must implement `StateType`.
    ///
    /// # Returns
    ///
    /// An `AggRef` representing the result of the accumulator operation.
    ///
    pub(crate) fn agg_reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        self.agg_internal(agg, true)
    }

    /// Applies an accumulator to the states of all shards in parallel.
    ///
    /// This method removes the accumulated state represented by `agg_ref` from the states,
    /// then merges it across all states (in parallel), and updates the post_agg_state.
    /// If the new state is not the same as the old one, then it merges them too.
    ///
    /// # Arguments
    ///
    /// * `self` - A mutable reference to a `Shuffle` instance.
    /// * `agg` - The accumulator function to apply to the states.
    ///
    /// # Type parameters
    ///
    /// * `A` - The type of the state.
    /// * `IN` - The type of the input to the accumulator.
    /// * `OUT` - The type of the output from the accumulator.
    /// * `ACC` - The type of the accumulator.
    ///
    /// # Constraints
    ///
    /// * `A` must implement `StateType`.
    ///
    /// # Returns
    ///
    /// An `AggRef` representing the result of the accumulator operation.
    ///
    pub(crate) fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        self.agg_internal(agg, false)
    }

    fn agg_internal<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: AccId<A, IN, OUT, ACC>,
        reset: bool,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        if reset {
            self.resetable_states.push(agg.id());
        }

        let states = self.states.clone();

        // remove the accumulated state represendet by agg_ref from the states
        // then merge it accross all states (in parallel)
        // update the post_agg_state
        let new_global_state = states
            .into_par_iter()
            .reduce_with(|left, right| {
                // peel left
                let left_placeholder = &mut left.write();
                let mut state1 = left_placeholder.take().unwrap();
                // peel right
                let right_placeholder = &mut right.write();
                let state2 = right_placeholder.take().unwrap();

                state1.merge_mut(&state2, &agg, self.ss);
                state1.merge_mut_global(&state2, &agg, self.ss);

                **left_placeholder = Some(state1);
                **right_placeholder = Some(state2);

                left.clone()
            })
            .unwrap();

        // selective broadcast
        // we set the state with id agg in shard_state to the value in global_state
        self.states.par_iter().for_each(|shard_state| {
            if !Arc::ptr_eq(&new_global_state, &shard_state) {
                let shard_state_pl = &mut shard_state.write();
                let mut shard_state = shard_state_pl.take().unwrap();

                let global_state_pl = new_global_state.read();

                if let Some(global_state) = &global_state_pl.as_ref() {
                    shard_state.set_from_other(&global_state, &agg, self.ss);
                }

                **shard_state_pl = Some(shard_state);
            }
        });

        // if the new state is not the same as the old one then we merge them too
        AggRef(agg)
    }

    /// Executes a single step computation.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure taking an `EvalVertexView` and returning a boolean value.
    /// The closure is used to determine which vertices to include in the next step.
    ///
    pub(crate) fn step<F>(&mut self, f: F)
    where
        F: Fn(EvalVertexView<G>) -> bool + Sync,
    {
        let ss = self.ss;
        let graph = Arc::new(self.g.clone());
        let next_vertex_set = (0..self.g.num_shards())
            .collect_vec()
            .par_iter()
            .map(|shard| {
                let i = *shard;
                let local_state = self.states[i].clone();
                // take control of the actual state
                let local_state = &mut local_state.write();
                let own_state = (local_state).take().unwrap();

                let mut next_vertex_set = own_state.changed_keys(i, ss).collect::<FxHashSet<_>>(); // FxHashSet::default();
                let prev_vertex_set = self
                    .next_vertex_set
                    .as_ref()
                    .map(|vs| vs[i].clone())
                    .unwrap_or_else(|| Arc::new(own_state.keys(i).collect()));

                let rc_state = Rc::new(RefCell::new(own_state));

                for vv in prev_vertex_set.iter().flat_map(|v_id| graph.vertex(*v_id)) {
                    let evv = EvalVertexView::new(self.ss, vv, rc_state.clone());
                    let g_id = evv.global_id();
                    // we need to account for the vertices that will be included in the next step
                    if f(evv) {
                        next_vertex_set.insert(g_id);
                    }
                }

                // put back the modified keys
                let mut own_state: ShuffleComputeState<CS> =
                    Rc::try_unwrap(rc_state).unwrap().into_inner();
                if self.keep_past_state {
                    own_state.copy_over_next_ss(self.ss);
                }

                own_state.reset_states(self.ss, &self.resetable_states);

                // put back the local state
                **local_state = Some(own_state);
                Arc::new(next_vertex_set)
            })
            .collect::<Vec<_>>();

        self.resetable_states.clear();
        self.next_vertex_set = Some(next_vertex_set);
    }
}

/// Represents an entry in the shuffle table.
///
/// The entry contains a reference to a `ShuffleComputeState` and an `AccId` representing the accumulator
/// for which the entry is being accessed. It also contains the index of the entry in the shuffle table
/// and the super-step counter.
pub struct Entry<'a, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>> {
    state: Ref<'a, ShuffleComputeState<CS>>,
    acc_id: AccId<A, IN, OUT, ACC>,
    i: usize,
    ss: usize,
}

// Entry implementation has read_ref function to access Option<&A>
impl<'a, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>> Entry<'a, A, IN, OUT, ACC> {
    /// Creates a new `Entry` instance.
    ///
    /// # Arguments
    ///
    /// * `state` - A reference to a `ShuffleComputeState` instance.
    /// * `acc_id` - An `AccId` representing the accumulator for which the entry is being accessed.
    /// * `i` - The index of the entry in the shuffle table.
    /// * `ss` - The super-step counter.
    pub fn new(
        state: Ref<'a, ShuffleComputeState<CS>>,
        acc_id: AccId<A, IN, OUT, ACC>,
        i: usize,
        ss: usize,
    ) -> Entry<'a, A, IN, OUT, ACC> {
        Entry {
            state,
            acc_id,
            i,
            ss,
        }
    }

    /// Returns a reference to the value stored in the `Entry` if it exists.
    pub fn read_ref(&self) -> Option<&A> {
        self.state.read_ref(self.ss, self.i, &self.acc_id)
    }
}

/// `EvalVertexView` represents a view of a vertex in a computation graph.
///
/// The view contains the evaluation step, the `WindowedVertex` representing the vertex, and a shared
/// reference to the `ShuffleComputeState`.
pub struct EvalVertexView<G: GraphViewOps> {
    ss: usize,
    vv: VertexView<G>,
    state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

/// `EvalVertexView` represents a view of a vertex in a computation graph.
impl<G: GraphViewOps> EvalVertexView<G> {
    pub fn update<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
        a: IN,
    ) where
        A: StateType,
    {
        let AggRef(agg) = agg_r;
        self.state
            .borrow_mut()
            .accumulate_into(self.ss, self.vv.id() as usize, a, agg)
    }

    /// Update the global state with the provided input value using the given accumulator.
    pub fn global_update<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
        a: IN,
    ) where
        A: StateType,
    {
        let AggRef(agg) = agg_r;
        self.state.borrow_mut().accumulate_global(self.ss, a, agg)
    }

    /// Try to read the current value of the vertex state using the given accumulator.
    /// Returns an error if the value is not present.
    pub fn try_read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Result<OUT, OUT>
    where
        A: StateType,
        OUT: Debug,
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, &agg_r.0)
            .ok_or(ACC::finish(&ACC::zero()))
    }

    /// Read the current value of the vertex state using the given accumulator.
    /// Returns a default value if the value is not present.
    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: Debug,
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, &agg_r.0)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    pub fn read_global<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: Debug,
    {
        self.state
            .borrow()
            .read_global(self.ss, &agg_r.0)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    /// Returns an entry object representing the current state of the vertex with the given accumulator.
    pub fn entry<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Entry<'_, A, IN, OUT, ACC>
    where
        A: StateType,
    {
        let ref_state = self.state.borrow();
        Entry::new(ref_state, agg_r.0.clone(), self.vv.id() as usize, self.ss)
    }

    /// Try to read the previous value of the vertex state using the given accumulator.
    pub fn try_read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Result<OUT, OUT>
    where
        A: StateType,
        OUT: Debug,
    {
        self.state
            .borrow()
            .read(self.ss + 1, self.vv.id() as usize, &agg_r.0)
            .ok_or(ACC::finish(&ACC::zero()))
    }

    /// Read the previous value of the vertex state using the given accumulator.
    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: Debug,
    {
        self.try_read_prev::<A, IN, OUT, ACC>(agg_r)
            .or_else(|v| Ok::<OUT, OUT>(v))
            .unwrap()
    }

    /// Create a new `EvalVertexView` from the given super-step counter, `WindowedVertex` and
    /// `ShuffleComputeState`.
    ///
    /// # Arguments
    ///
    /// * `ss` - super-step counter
    /// * `vv` - The `WindowedVertex` representing the vertex.
    /// * `state` - The `ShuffleComputeState` shared between all `EvalVertexView`s.
    ///
    /// # Returns
    ///
    /// A new `EvalVertexView`.
    pub fn new(ss: usize, vv: VertexView<G>, state: Rc<RefCell<ShuffleComputeState<CS>>>) -> Self {
        Self { ss, vv, state }
    }

    pub fn name(&self) -> String {
        self.vv.name()
    }

    /// Obtain the global id of the vertex.
    pub fn global_id(&self) -> u64 {
        self.vv.id()
    }

    pub fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    /// Return an iterator over the out-neighbors of this vertex.
    ///
    /// Each neighbor is returned as an `EvalVertexView`, which can be used to read and update the
    /// neighbor's state.
    pub fn neighbours_out(&self) -> impl Iterator<Item = Self> + '_ {
        self.vv
            .out_neighbours()
            .into_iter()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    /// Return an iterator over the in-neighbors of this vertex.
    ///
    /// Each neighbor is returned as an `EvalVertexView`, which can be used to read and update the
    /// neighbor's state.
    pub fn neighbours_in(&self) -> impl Iterator<Item = Self> + '_ {
        self.vv
            .in_neighbours()
            .iter()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    /// Return an iterator over the neighbors of this vertex (inbound and outbound).
    ///
    /// Each neighbor is returned as an `EvalVertexView`, which can be used to read and update the
    /// neighbor's state.
    pub fn neighbours(&self) -> impl Iterator<Item = Self> + '_ {
        self.vv
            .neighbours()
            .iter()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    pub fn out_edges(
        &self,
        after: i64,
    ) -> impl Iterator<Item = EvalEdgeView<WindowedGraph<G>>> + '_ {
        self.vv.window(after, i64::MAX).out_edges().map(move |ev| {
            // let et = ev.history().into_iter().filter(|t| t >= &after).min().unwrap();
            // ev.dst();
            EvalEdgeView::new(self.ss, ev, self.state.clone())
        })
    }
}

/// `EvalEdgeView` represents a view of a edge in a computation graph.
///
/// The view contains the evaluation step, the `WindowedEdge` representing the edge.
pub struct EvalEdgeView<G: GraphViewOps> {
    ss: usize,
    ev: EdgeView<G>,
    state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

/// `EvalEdgeView` represents a view of a edge in a computation graph.
impl<G: GraphViewOps> EvalEdgeView<G> {
    /// Create a new `EvalEdgeView` from the given super-step counter, `WindowedEdge`
    ///
    /// # Arguments
    ///
    /// * `ss` - super-step counter
    /// * `ev` - The `WindowedEdge` representing the edge.
    ///
    /// # Returns
    ///
    /// A new `EvalVertexView`.
    pub fn new(ss: usize, ev: EdgeView<G>, state: Rc<RefCell<ShuffleComputeState<CS>>>) -> Self {
        Self { ss, ev, state }
    }

    pub fn dst(&self) -> EvalVertexView<G> {
        self.ev.dst();
        EvalVertexView::new(self.ss, self.ev.dst(), self.state.clone())
    }

    pub fn history(&self) -> Vec<i64> {
        self.ev.history()
    }
}

/// Represents a program that can be executed on a graph. We use this to run algorithms on graphs.
pub trait Program {
    /// The output type of the program.
    type Out;

    /// Performs local evaluation of the program on a local state.
    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>);

    /// Performs post-evaluation of the program on a global evaluation state.
    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>);

    /// Runs a single step of the program on a graph and a global evaluation state.
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph on which the program should be run.
    /// * `c` - A mutable reference to the global evaluation state.
    ///
    /// # Panics
    ///
    /// Panics if the state lock is contended.
    fn run_step<G: GraphViewOps>(&self, g: &G, c: &mut GlobalEvalState<G>)
    where
        Self: Sync,
    {
        let next_vertex_set = c.next_vertex_set.clone();
        let graph = g.clone();

        (0..g.num_shards())
            .collect_vec()
            .par_iter()
            .for_each(|shard| {
                let i = *shard;
                let local_state = c.states[i].clone();
                // take control of the actual state
                let local_state = &mut local_state
                    .try_write()
                    .expect("STATE LOCK SHOULD NOT BE CONTENDED");
                let own_state = (local_state).take().unwrap();

                let rc_state = LocalState::new(
                    c.ss,
                    i,
                    graph.clone(),
                    Rc::new(RefCell::new(own_state)),
                    next_vertex_set.as_ref().map(|v| v[i].clone()),
                );

                self.local_eval(&rc_state);

                // put back the state
                **local_state = Some(rc_state.consume());
            });

        // here we merge all the accumulators
        self.post_eval(c);
    }

    /// Runs the program on a graph, with a given window and iteration count.
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph on which the program should be run.
    /// * `window` - A range specifying the window for the evaluation.
    /// * `keep_past_state` - A boolean value indicating whether past states should be kept.
    /// * `iter_count` - The maximum number of iterations to run.
    ///
    /// # Returns
    ///
    /// A global evaluation state representing the result of running the program.
    ///
    /// # Panics
    ///
    /// Panics if the state lock is contended.
    fn run<G: GraphViewOps>(
        &self,
        g: &G,
        keep_past_state: bool,
        iter_count: usize,
    ) -> GlobalEvalState<G>
    where
        Self: Sync,
    {
        let mut c = GlobalEvalState::new(g.clone(), keep_past_state);

        let mut i = 0;
        while c.do_loop() && i < iter_count {
            self.run_step(g, &mut c);
            if c.keep_past_state {
                c.ss += 1;
            }
            i += 1;
        }
        c
    }

    /// Produces the output of the program for a given graph and global evaluation state.
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph on which the program was run.
    /// * `window` - A range specifying the window for the evaluation.
    /// * `gs` - A reference to the global evaluation state.
    ///
    /// # Returns
    ///
    /// The output of the program.
    ///
    /// # Panics
    ///
    /// Panics if the state lock is contended.
    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync;
}
