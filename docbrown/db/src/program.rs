//!  Defines the `Program` trait, which represents code that is used to evaluate
//!  algorithms and custom code that can be run on the graph.

use std::collections::HashSet;
use std::ops::Add;
use std::{
    cell::{Ref, RefCell},
    fmt::Debug,
    rc::Rc,
    sync::Arc,
};

use crate::vertex::VertexView;
use crate::view_api::{GraphViewOps, VertexViewOps};
use docbrown_core::{
    agg::Accumulator,
    state::{self, AccId, ShuffleComputeState},
    state::{ComputeStateMap, StateType},
};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

/// Module containing graph algorithms that can be run on docbrown graphs
pub mod algo {

    use crate::algorithms::triplet_count::TripletCount;
    use rustc_hash::FxHashMap;

    use crate::view_api::GraphViewOps;

    use super::{
        GlobalEvalState, Program, SimpleConnectedComponents, TriangleCountS1, TriangleCountS2,
    };

    /// Computes the connected components of a graph using the Simple Connected Components algorithm
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph
    /// * `window` - A range indicating the temporal window to consider
    /// * `iter_count` - The number of iterations to run
    ///
    /// # Returns
    ///
    /// A hash map containing the mapping from component ID to the number of vertices in the component
    pub fn connected_components<G: GraphViewOps>(g: &G, iter_count: usize) -> FxHashMap<u64, u64> {
        let cc = SimpleConnectedComponents {};

        let gs = cc.run(g, true, iter_count);

        cc.produce_output(g, &gs)
    }

    /// Computes the number of triangles in a graph using a fast algorithm
    ///
    /// # Arguments
    ///
    /// * `g` - A reference to the graph
    /// * `window` - A range indicating the temporal window to consider
    ///
    /// # Returns
    ///
    /// An optional integer containing the number of triangles in the graph. If the computation failed,
    /// the function returns `None`.
    ///
    /// # Example
    /// ```rust
    /// use std::{cmp::Reverse, iter::once};
    /// use docbrown_db::graph::Graph;
    /// use docbrown_db::program::algo::triangle_counting_fast;
    /// let graph = Graph::new(2);
    ///
    /// let edges = vec![
    ///     // triangle 1
    ///     (1, 2, 1),
    ///     (2, 3, 1),
    ///     (3, 1, 1),
    ///     //triangle 2
    ///     (4, 5, 1),
    ///     (5, 6, 1),
    ///     (6, 4, 1),
    ///     // triangle 4 and 5
    ///     (7, 8, 2),
    ///     (8, 9, 3),
    ///     (9, 7, 4),
    ///     (8, 10, 5),
    ///     (10, 9, 6),
    /// ];
    ///
    /// for (src, dst, ts) in edges {
    ///     graph.add_edge(ts, src, dst, &vec![]);
    /// }
    ///
    /// let actual_tri_count = triangle_counting_fast(&graph);
    /// ```
    ///
    pub fn triangle_counting_fast<G: GraphViewOps>(g: &G) -> Option<usize> {
        let mut gs = GlobalEvalState::new(g.clone(), false);
        let tc = TriangleCountS1 {};

        tc.run_step(g, &mut gs);

        let tc = TriangleCountS2 {};

        tc.run_step(g, &mut gs);

        tc.produce_output(g, &gs)
    }
}

/// Alias for ComputeStateMap
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
        println!("LOCAL STEP KICK-OFF");
        iter.for_each(|v| {
            f(EvalVertexView::new(
                self.ss,
                v,
                self.shard_local_state.clone(),
            ));
            c += 1;
            if c % 100000 == 0 {
                let t_id = std::thread::current().id();
                println!("LOCAL STEP {} vertices on {t_id:?}", c);
            }
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
    ss: usize,
    g: G,
    keep_past_state: bool,
    // running state
    next_vertex_set: Option<Vec<Arc<FxHashSet<u64>>>>,
    states: Vec<Arc<parking_lot::RwLock<Option<ShuffleComputeState<CS>>>>>,
    post_agg_state: Arc<parking_lot::RwLock<Option<ShuffleComputeState<CS>>>>, // FIXME this is a pointer to one of the states in states, beware of deadlocks
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
    ) -> Vec<Vec<Vec<OUT>>>
    where
        OUT: StateType,
        A: 'static,
    {
        // println!("read_vec_partitions: {:#?}", self.states);
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
        let state = self.post_agg_state.read();
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
            post_agg_state: Arc::new(parking_lot::RwLock::new(None)),
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
        self.agg(agg)
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
        let states = self.states.clone();

        // remove the accumulated state represendet by agg_ref from the states
        // then merge it accross all states (in parallel)
        // update the post_agg_state
        let new_global_state = states
            .into_par_iter()
            .reduce_with(|left, right| {
                let t_id = std::thread::current().id();
                println!("MERGING aggregator states! {t_id:?}");
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

                println!("DONE MERGING aggregator states! {t_id:?}");
                left.clone()
            })
            .unwrap();

        if !Arc::ptr_eq(&self.post_agg_state, &new_global_state)
            && self.post_agg_state.read().is_some()
        {
            let left_placeholder = &mut self.post_agg_state.write();
            let mut state1 = left_placeholder.take().unwrap();

            let right_placeholder = &mut new_global_state.write();
            let state2 = right_placeholder.take().unwrap();
            state1.merge_mut(&state2, &agg, self.ss);
        } else {
            self.post_agg_state = new_global_state;
        }

        // if the new state is not the same as the old one then we merge them too
        println!("DONE FULL MERGE!");
        AggRef(agg)
    }

    /// Broadcasts the post_agg_state to all shards.
    ///
    /// This method sets the state of each shard to the current value of `post_agg_state`.
    ///
    fn broadcast_state(&mut self) {
        let broadcast_state = self.post_agg_state.read();

        for state in self.states.iter() {
            // this avoids a deadlock since we may already hold the read lock
            if Arc::ptr_eq(state, &self.post_agg_state) {
                continue;
            }

            let mut state = state.write();

            let prev = state.take();
            drop(prev); // not sure if this is needed but I really want the old state to be dropped
            let new_shard_state = broadcast_state.clone();
            *state = new_shard_state;
        }
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
        println!("START BROADCAST STATE");
        self.broadcast_state();
        println!("DONE BROADCAST STATE");

        let ss = self.ss;
        let graph = Arc::new(self.g.clone());
        let next_vertex_set = (0..self.g.num_shards())
            .collect_vec()
            .par_iter()
            .map(|shard| {
                println!("STARTED POST_EVAL SHARD {:#?}", shard);
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
                // put back the local state
                **local_state = Some(own_state);
                println!("DONE POST_EVAL SHARD {:#?}", shard);
                Arc::new(next_vertex_set)
            })
            .collect::<Vec<_>>();

        println!("DONE POST_EVAL SHARD ALL");
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
    /// Update the vertex state with the provided input value using the given accumulator.
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
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, &agg_r.0)
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

    /// Obtain the global id of the vertex.
    pub fn global_id(&self) -> u64 {
        self.vv.id()
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
        println!("RUN STEP {:#?}", c.ss);

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

                let t_id = std::thread::current().id();
                println!(
                    "DONE LOCAL STEP ss: {}, shard: {}, thread: {t_id:?}",
                    c.ss, i
                );
                // put back the state
                **local_state = Some(rc_state.consume());
            });

        // here we merge all the accumulators
        self.post_eval(c);
        println!("DONE POST STEP ss: {}", c.ss)
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
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync;
}

/// A simple connected components algorithm
#[derive(Default)]
struct SimpleConnectedComponents {}

/// A simple connected components algorithm
impl Program for SimpleConnectedComponents {
    type Out = FxHashMap<u64, u64>;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let min = c.agg(state::def::min(0));

        c.step(|vv| {
            let g_id = vv.global_id();
            vv.update(&min, g_id);

            for n in vv.neighbours() {
                let my_min = vv.read(&min);
                n.update(&min, my_min);
            }
        })
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        // this will make the global state merge all the values for all partitions
        let min = c.agg(state::def::min::<u64>(0));

        c.step(|vv| {
            let current = vv.read(&min);
            let prev = vv.read_prev(&min);
            current != prev
        })
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        let agg = state::def::min::<u64>(0);

        let mut results: FxHashMap<u64, u64> = FxHashMap::default();

        (0..g.num_shards())
            .into_iter()
            .fold(&mut results, |res, part_id| {
                gs.fold_state(&agg, part_id, res, |res, v_id, cc| {
                    res.insert(*v_id, cc);
                    res
                })
            });

        results
    }
}

/// Step 1 for the triangle counting algorithm
pub struct TriangleCountS1 {}

/// Step 1 for the triangle counting algorithm
impl Program for TriangleCountS1 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let neighbors_set = c.agg(state::def::hash_set(0));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    t.update(&neighbors_set, s.global_id());
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(state::def::hash_set::<u64>(0));
        c.step(|_| false)
    }

    fn produce_output<G: GraphViewOps>(&self, _g: &G, _gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

/// Step 2 for the triangle counting algorithm
pub struct TriangleCountS2 {}

/// Step 2 for the triangle counting algorithm
impl Program for TriangleCountS2 {
    type Out = Option<usize>;
    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let neighbors_set = c.agg(state::def::hash_set::<u64>(0));
        let count = c.global_agg(state::def::sum::<usize>(1));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    let intersection_count = {
                        // when using entry() we need to make sure the reference is released before we can update the state, otherwise we break the Rc<RefCell<_>> invariant
                        // where there can either be one mutable or many immutable references

                        match (
                            s.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                            t.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                        ) {
                            (s_set, t_set) => {
                                let intersection = s_set.intersection(t_set);
                                intersection.count()
                            }
                        }
                    };

                    s.global_update(&count, intersection_count);
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(state::def::sum::<usize>(1));
        c.step(|_| false)
    }

    fn produce_output<G: GraphViewOps>(&self, _g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        gs.read_global_state(&state::def::sum::<usize>(1))
    }
}

/// A slower Step 2 for the triangle counting algorithm
pub struct TriangleCountSlowS2 {}

/// A slower Step 2 for the triangle counting algorithm
impl Program for TriangleCountSlowS2 {
    type Out = usize;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let count = c.global_agg(state::def::sum::<usize>(0));

        c.step(|v| {
            let my_neighbours_less_myself = v
                .neighbours()
                .map(|n| n.global_id())
                .filter(|n| *n != v.global_id())
                .collect::<FxHashSet<_>>();

            let c1 = my_neighbours_less_myself.len();

            for n in v.neighbours() {
                if v.global_id() > n.global_id() {
                    let nn_less_itself = n
                        .neighbours()
                        .map(|n| n.global_id())
                        .filter(|v| *v != n.global_id())
                        .collect::<FxHashSet<_>>();

                    let c2 = my_neighbours_less_myself
                        .difference(&nn_less_itself)
                        .count();
                    v.global_update(&count, c1 - c2);
                }
            }
        })
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let a = c.global_agg(state::def::sum::<usize>(0));
        c.step(|_| false)
    }

    fn produce_output<G: GraphViewOps>(&self, _g: &G, _gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        todo!()
    }
}

#[cfg(test)]
mod program_test {
    use std::{cmp::Reverse, iter::once};

    use crate::program::algo::{connected_components, triangle_counting_fast};

    use super::*;
    use crate::graph::Graph;
    use crate::view_api::*;
    use docbrown_core::state;
    use itertools::chain;
    use pretty_assertions::assert_eq;
    use rustc_hash::FxHashMap;

    #[test]
    fn triangle_count_1() {
        let graph = Graph::new(2);

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let actual_tri_count = triangle_counting_fast(&graph);

        assert_eq!(actual_tri_count, Some(4))
    }

    #[test]
    fn triangle_count_1_slow() {
        let graph = Graph::new(2);

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let program_s1 = TriangleCountSlowS2 {};
        let agg = state::def::sum::<usize>(0);

        let windowed_graph = graph.window(0, 95);
        let mut gs = GlobalEvalState::new(windowed_graph.clone(), false);

        program_s1.run_step(&windowed_graph, &mut gs);

        let actual_tri_count = gs.read_global_state(&agg).map(|v| v / 3);

        assert_eq!(actual_tri_count, Some(4))
    }

    #[test]
    fn triangle_count_2() {
        let graph = Graph::new(4);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let program_s1 = TriangleCountSlowS2 {};
        let agg = state::def::sum::<usize>(0);
        let graph_window = graph.window(0, 64);

        let mut gs = GlobalEvalState::new(graph_window.clone(), false);

        program_s1.run_step(&graph_window, &mut gs);

        let actual_tri_count = gs.read_global_state(&agg).map(|v| v / 3);

        assert_eq!(actual_tri_count, Some(8));
    }

    #[test]
    fn triangle_count_3() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let graph_window = graph.window(0, 27);

        let actual_tri_count = triangle_counting_fast(&graph_window);

        assert_eq!(actual_tri_count, Some(8))
    }

    #[test]
    fn simple_connected_components() {
        let program = SimpleConnectedComponents::default();

        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let graph_window = graph.window(0, 10);
        let mut gs = GlobalEvalState::new(graph_window.clone(), true);
        program.run_step(&graph_window, &mut gs);

        let agg = state::def::min::<u64>(0);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1, 3, 3], // shard 0 (2, 4, 6, 8)
                vec![3, 7, 1, 2], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph_window, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1, 2, 3], // shard 0 (2, 4, 6, 8)
                vec![2, 7, 1, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph_window, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1, 1, 2], // shard 0 (2, 4, 6, 8)
                vec![1, 7, 1, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);

        program.run_step(&graph_window, &mut gs);

        let expected =             // output from the eval running on the first shard
            vec![
                vec![7, 1, 1, 1], // shard 0 (2, 4, 6, 8)
                vec![1, 7, 1, 1], // shard 1 (1, 3, 5, 7)
            ];

        let actual_part1 = &gs.read_vec_partitions(&agg)[0];
        let actual_part2 = &gs.read_vec_partitions(&agg)[1];

        // after one step all partitions have the same data since it's been merged and broadcasted
        assert_eq!(actual_part1, actual_part2);
        println!("ACTUAL: {:?}", actual_part1);
        assert_eq!(actual_part1, &expected);
    }

    #[test]
    fn run_loop_simple_connected_components() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let graph_window = graph.window(0, 10);

        let results: FxHashMap<u64, u64> = connected_components(&graph_window, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect();

        assert_eq!(
            results,
            vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 7),
                (8, 7),
            ]
            .into_iter()
            .collect::<FxHashMap<u64, u64>>()
        );
    }

    #[test]
    fn simple_connected_components_2() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let graph_window = graph.window(0, 25);

        let results: FxHashMap<u64, u64> = connected_components(&graph_window, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect();

        assert_eq!(
            results,
            vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 1),
                (8, 1),
                (9, 1),
                (10, 1),
                (11, 1),
            ]
            .into_iter()
            .collect::<FxHashMap<u64, u64>>()
        );
    }

    // connected components on a graph with 1 node and a self loop
    #[test]
    fn simple_connected_components_3() {
        let graph = Graph::new(2);

        let edges = vec![(1, 1, 1)];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let graph_window = graph.window(0, 25);

        let results: FxHashMap<u64, u64> = connected_components(&graph_window, usize::MAX);

        assert_eq!(
            results,
            vec![(1, 1),].into_iter().collect::<FxHashMap<u64, u64>>()
        );
    }

    #[quickcheck]
    fn circle_graph_the_smallest_value_is_the_cc(vs: Vec<u64>) {
        if !vs.is_empty() {
            let vs = vs.into_iter().unique().collect::<Vec<u64>>();

            let smallest = vs.iter().min().unwrap();

            let first = vs[0];
            // pairs of vertices from vs one after the next
            let edges = vs
                .iter()
                .zip(chain!(vs.iter().skip(1), once(&first)))
                .map(|(a, b)| (*a, *b))
                .collect::<Vec<(u64, u64)>>();

            assert_eq!(edges[0].0, first);
            assert_eq!(edges.last().unwrap().1, first);

            let graph = Graph::new(2);

            for (src, dst) in edges.iter() {
                graph.add_edge(0, *src, *dst, &vec![]);
            }

            // now we do connected components over window 0..1

            let graph_window = graph.window(0, 1);

            let components: FxHashMap<u64, u64> = connected_components(&graph_window, usize::MAX);

            let actual = components
                .iter()
                .group_by(|(_, cc)| *cc)
                .into_iter()
                .map(|(cc, group)| (cc, Reverse(group.count())))
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .map(|(cc, count)| (*cc, count.0))
                .take(1)
                .next();

            assert_eq!(
                actual,
                Some((*smallest, edges.len())),
                "actual: {:?}",
                actual
            );
        }
    }
}
