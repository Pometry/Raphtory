use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{GenericNodeState, Index, TypedNodeState},
            view::{internal::filtered_node::FilteredNodeStorageOps, StaticGraphViewOps},
        },
        task::{custom_pool, POOL},
    },
    errors::GraphError,
    prelude::*,
};
use rand::Rng;
use raphtory_api::core::{entities::VID, utils::hashing::calculate_hash, Direction};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::debug;

/// Label carried by a node that has not (yet) been assigned a community.
///
/// Only used when `init_state` is supplied for unseeded nodes.
/// Safe as a sentinel because `usize::MAX` is already the codebase's "not a node"
/// marker (see `VID::is_initialised`), so it can never collide with a node index.
const NO_LABEL: usize = usize::MAX;

/// Default relative-improvement threshold for the plateau stopping criterion.
const DEFAULT_REL_TOL: f64 = 3e-4;

/// Default number of consecutive iterations without progress before stopping.
const DEFAULT_PATIENCE: usize = 10;

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LabelPropState {
    pub community_id: usize,
    pub alternate_id: Option<usize>, // set to previous value when community_id has changed; None once settled
    /// Votes for `community_id` as a share of the votes cast, from the last super-step in which this
    /// node re-evaluated -- so it always describes the vote that produced the standing label. A
    /// winner always holds a vote, which is what leaves the `Default` 0.0 free to mean "never voted".
    pub confidence: f64,
    #[serde(skip)]
    is_changed: bool, // derive(Default) initializes to false
}

/// Deterministic pseudorandom rank for `label` as seen by a node whose `node_key` is
/// `calculate_hash(&(seed, node))`, used only to break vote-count ties.
/// Splitting the mix in two lets the sweep hoist the per-node half out of its per-label loop.
#[inline]
fn tie_rank(node_key: u64, label: usize) -> u64 {
    let mut x = node_key ^ (label as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// An initial community assignment, as accepted by [`label_propagation`].
///
/// Implemented for `()`, meaning unseeded, and for a `HashMap` keyed by anything that names a node
/// -- a name, a global id, or a `Node` -- so that callers never have to reach for the internal
/// `VID`s the algorithms work in. The output of a previous run can be fed back in as a seed.
pub trait IntoInitState {
    /// Resolves the caller's node references into the `VID`-keyed map the algorithms use, or
    /// `None` when there are no seeds at all.
    fn into_init_state<G: StaticGraphViewOps>(
        self,
        graph: &G,
    ) -> Result<Option<HashMap<usize, usize>>, GraphError>;
}

impl IntoInitState for () {
    fn into_init_state<G: StaticGraphViewOps>(
        self,
        _graph: &G,
    ) -> Result<Option<HashMap<usize, usize>>, GraphError> {
        Ok(None)
    }
}

impl<V: AsNodeRef> IntoInitState for HashMap<V, usize> {
    fn into_init_state<G: StaticGraphViewOps>(
        self,
        graph: &G,
    ) -> Result<Option<HashMap<usize, usize>>, GraphError> {
        let mut resolved = HashMap::with_capacity(self.len());
        for (node, label) in self {
            let node_ref = node.as_node_ref();
            match graph.node(node_ref) {
                Some(n) => {
                    resolved.insert(n.node.index(), label);
                }
                None => {
                    let gid = match node_ref {
                        NodeRef::Internal(vid) => graph.node_id(vid),
                        NodeRef::External(gid) => gid.to_owned(),
                    };
                    return Err(GraphError::NodeMissingError(gid));
                }
            }
        }
        Ok(Some(resolved))
    }
}

impl<T: IntoInitState> IntoInitState for Option<T> {
    fn into_init_state<G: StaticGraphViewOps>(
        self,
        graph: &G,
    ) -> Result<Option<HashMap<usize, usize>>, GraphError> {
        match self {
            Some(seeds) => seeds.into_init_state(graph),
            None => Ok(None),
        }
    }
}

/// Scratch vote counter for the node currently being evaluated, reused across the nodes of a
/// rayon job. Both variants answer the same question -- how many votes has this slot collected --
/// and differ only in how a slot reaches its counter.
///
/// `Dense` indexes an array directly, which is the whole reason [`label_propagation`]
/// compacts labels into slots: with a seeded run the label space is the handful of labels named in
/// `init_state`, so the array is small enough to stay in cache. `Sparse` hashes instead, and is
/// there for the unseeded case, where every node starts in its own community and the label space
/// is the graph -- a dense array would then need `n` cells *per rayon job* to count the `deg + 1`
/// votes a node actually casts. Its footprint tracks the largest neighbourhood a job happens to
/// see, never `n`.
///
/// Neither variant is ever walked to find the winner or to sum the votes: the caller tracks both
/// incrementally and resets only the slots it recorded in `touched`, so a reset costs the node's
/// degree and not the counter's capacity.
enum Counts {
    Dense(Vec<u32>),
    Sparse(FxHashMap<usize, u32>),
}

impl Counts {
    /// Records the self-vote. Always a node's first vote, so the slot is known to be clear.
    #[inline]
    fn set_one(&mut self, slot: usize) {
        match self {
            Counts::Dense(counts) => counts[slot] = 1,
            Counts::Sparse(counts) => {
                counts.insert(slot, 1);
            }
        }
    }

    /// Records a vote for `slot` and returns its new total.
    #[inline]
    fn incr(&mut self, slot: usize) -> u32 {
        match self {
            Counts::Dense(counts) => {
                counts[slot] += 1;
                counts[slot]
            }
            Counts::Sparse(counts) => {
                let count = counts.entry(slot).or_insert(0);
                *count += 1;
                *count
            }
        }
    }

    /// Returns `slot` to zero, ready for the next node.
    #[inline]
    fn clear_slot(&mut self, slot: usize) {
        match self {
            Counts::Dense(counts) => counts[slot] = 0,
            Counts::Sparse(counts) => {
                counts.remove(&slot);
            }
        }
    }
}

/// Computes communities using a label propagation algorithm.
///
/// Labels live in flat arrays held alongside the graph rather than in per-node task state, which is
/// where the speed comes from:
///
/// - votes are counted in a buffer indexed by a compacted label, so resetting it between nodes is
///   proportional to the node's degree rather than to the high-water capacity of a hash map;
/// - the adjacency is read through a [`GraphStorage::lock`]ed view of the storage, taken once,
///   so traversing a node's neighbours does not re-acquire a segment lock per node;
/// - a node that changes its label activates its neighbours directly, so no separate full pass over
///   the graph is needed to build the next frontier.
///
/// The label space decides how the votes are counted; see [`Counts`].
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - Number of iterations
/// - `seed` - (Optional) Seeds the tie-break draw. The value used is printed in the stopping summary
///   and can be passed back here to reproduce a run.
/// - `threads` - (Optional) Number of threads to use
/// - `init_state` - `()` for unseeded, or a HashMap of node to community ID. When unseeded, every node starts
///   in its own community. When present, only the nodes it names are labelled and active; the rest
///   start unlabelled and may acquire a label from their neighbours. A map covering every node
///   reproduces a previous warm-start behaviour exactly. Seed label values are unconstrained -- they
///   are compacted internally, so the vote counter is sized by the number of *distinct* labels, not
///   by the largest one.
/// - `rel_tol` - (Optional) Relative-improvement threshold to track convergence. An iteration counts
///   as progress only if its changed-node count drops below `best * (1 - rel_tol)`. Defaults to 3e-4.
/// - `patience` - (Optional) Stop after this many consecutive iterations without progress. Defaults to 10.
///
/// # Returns
///
/// A `TypedNodeState` mapping each node to its `LabelPropState`: its `community_id`, plus
/// `alternate_id` (the previous label it swaps with while oscillating; `None` once converged, and
/// also `None` until the node has been labeled a whole iteration, and `confidence`, the share of
/// its votes that went to `community_id`.
pub fn label_propagation<G>(
    g: &G,
    iter_count: usize,
    seed: Option<u64>,
    threads: Option<usize>,
    init_state: impl IntoInitState,
    rel_tol: Option<f64>,
    patience: Option<usize>,
) -> Result<TypedNodeState<'static, LabelPropState, G>, GraphError>
where
    G: StaticGraphViewOps,
{
    let init_state = init_state.into_init_state(g)?;
    let index = Index::for_graph(g.clone());
    let n = index.len();

    // Unseeded runs draw random number
    let tie_seed: u64 = seed.unwrap_or_else(|| rand::rng().random());
    let rel_tol = rel_tol.unwrap_or(DEFAULT_REL_TOL);
    let patience = patience.unwrap_or(DEFAULT_PATIENCE);

    let seeded = init_state.is_some();

    let (labels, slot_of): (Vec<usize>, FxHashMap<usize, usize>) = match &init_state {
        // Seeded
        Some(map) => {
            let mut labels: Vec<usize> = map.values().copied().collect();
            labels.sort_unstable();
            labels.dedup(); // only removes consecutive
            let slot_of = labels
                .iter()
                .enumerate()
                .map(|(slot, &label)| (label, slot))
                .collect();
            (labels, slot_of)
        }
        // Unseeded
        // The position -> VID table, which is what makes `tie_rank` hash the VID rather than the
        // position. `slot_of` goes unused: a node's slot is its position.
        None => {
            let table: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
            index
                .par_iter()
                .for_each(|(pos, vid)| table[pos].store(vid.index(), Ordering::Relaxed));
            (
                table.into_iter().map(AtomicUsize::into_inner).collect(),
                FxHashMap::default(),
            )
        }
    };

    // One lock for the whole run: `core_node` on a locked storage is an indexed read
    let locked = g.core_graph().lock();
    let layer_ids = g.layer_ids();
    // Neighbours as flat index positions, matching `NodeViewOps::neighbours` (both directions,
    // deduplicated, view filters applied).
    let collect_nbors = |vid: VID, out: &mut Vec<usize>| {
        out.clear();
        let node = locked.core_node(vid);
        out.extend(
            node.as_ref()
                .filtered_neighbours_iter(g, layer_ids, Direction::BOTH)
                .map(|nbor| index.index(&nbor).expect("neighbour VID not in index")),
        );
    };

    // Labels, held as slots and indexed by flat position. `cur` starts as a copy of `prev` so that
    // an `iter_count` of 0 still reports the seeding.
    let mut prev: Vec<AtomicUsize> = match seeded {
        // Unlabelled until the front reaches them; the seeds themselves get their slot below.
        true => (0..n).map(|_| AtomicUsize::new(NO_LABEL)).collect(),
        // Every node starts in its own community, whose slot is the node's own position.
        false => (0..n).map(AtomicUsize::new).collect(),
    };
    // The share of votes that each node's standing label won, held as `f64::to_bits` so that
    // an atomic can carry it. Zero bits are `0.0`, which is the "never voted" sentinel, so the
    // initial value needs no special case.
    //
    // NOT DOUBLE-BUFFERED, unlike `prev`/`cur`: confidence is an output and no neighbour reads it,
    // so there is nothing for a sweep to race against and nothing to swap. A node that does not
    // re-evaluate leaves its entry alone, so no votes at all leaves `community_id` standing --
    // and its confidence with it.
    let votes: Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(0)).collect();
    // Unseeded leaves every entry at 0, deliberately: no confidence is set when that case is
    // seeded, so an `iter_count` of 0 reports 0.0; and from the first sweep on, every entry is
    // overwritten, because a labelled node always casts a self-vote and so never takes the
    // `total == 0` branch.
    if let Some(map) = &init_state {
        index.par_iter().for_each(|(pos, vid)| {
            if let Some(slot) = map.get(&vid.index()).and_then(|l| slot_of.get(l)) {
                prev[pos].store(*slot, Ordering::Relaxed);
                // A seed's label is GIVEN, not inferred, so it starts fully confident.
                votes[pos].store(1.0f64.to_bits(), Ordering::Relaxed);
            }
        });
    }
    let mut cur: Vec<AtomicUsize> = prev
        .iter()
        .map(|slot| AtomicUsize::new(slot.load(Ordering::Relaxed)))
        .collect();

    // The frontier for the first sweep. Seeded: the neighbours of the seeded nodes, which costs a
    // pass to collect. Unseeded: every node starts active, so there is nothing to collect and the
    // pass is skipped.
    let mut active_cur: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(!seeded)).collect();
    let mut active_next: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(false)).collect();
    if seeded {
        index
            .par_iter()
            .for_each_init(Vec::new, |nbors, (pos, vid)| {
                if prev[pos].load(Ordering::Relaxed) != NO_LABEL {
                    collect_nbors(vid, nbors); // collects into nbors
                    for &nbor in nbors.iter() {
                        active_cur[nbor].store(true, Ordering::Relaxed);
                    }
                }
            });
    }

    // Synchronous LPA never reaches global_diff == 0 on graphs with locally-bipartite pockets
    // (results in ~period-2 oscillations), so the stopping criterion we use is to wait for
    // `patience` iterations since the improvement was no better than `rel_tol`.
    let pool: Arc<rayon::ThreadPool> = threads.map(custom_pool).unwrap_or_else(|| POOL.clone());
    pool.install(|| {
        let mut best = usize::MAX;
        let mut stale = 0usize;
        for n_iter in 1..=iter_count {
            let changed = AtomicUsize::new(0); // a counter
            index.par_iter().for_each_init(
                || {
                    let counts = if seeded {
                        Counts::Dense(vec![0u32; labels.len()])
                    } else {
                        Counts::Sparse(FxHashMap::default())
                    };
                    (counts, Vec::new(), Vec::new())
                },
                |(counts, touched, nbors), (pos, vid)| {
                    let prev_slot = prev[pos].load(Ordering::Relaxed);
                    // Gate: consume this node's activation flag atomically. A node that does not run
                    // still has to carry its label over, which is why the inactive branch stores
                    // `prev_slot` into `cur` rather than leaving the entry stale.
                    if !active_cur[pos].swap(false, Ordering::AcqRel) {
                        cur[pos].store(prev_slot, Ordering::Relaxed);
                        return;
                    }

                    collect_nbors(vid, nbors);
                    let node_key = calculate_hash(&(tie_seed, vid.index())); // tie_rank's 1st arg
                    let mut best_slot = prev_slot;
                    let mut best_count = 0u32;
                    let mut best_rank = 0u64;
                    // The denominator of `confidence`: the votes CAST -- labelled neighbours plus the
                    // self-vote -- and not the degree, since an unlabelled neighbour is skipped below
                    // and has no opinion to divide by. `counts` is never walked a second time, in
                    // either variant, so the total is accumulated as the votes land.
                    let mut total = 0u32;
                    if prev_slot != NO_LABEL {
                        // initialised nodes vote for their own label
                        counts.set_one(prev_slot);
                        touched.push(prev_slot);
                        best_count = 1;
                        total = 1;
                        best_rank = tie_rank(node_key, labels[prev_slot]);
                    }
                    for &nbor in nbors.iter() {
                        let nbor_slot = prev[nbor].load(Ordering::Relaxed);
                        if nbor_slot == NO_LABEL {
                            continue; // unlabelled neighbours don't cast a vote
                        }
                        total += 1;
                        let count = counts.incr(nbor_slot);
                        // if count increased from 0 -> 1 we keep track of activated nbors
                        if count == 1 {
                            touched.push(nbor_slot);
                        }

                        if nbor_slot == best_slot {
                            best_count = count;
                        } else if count >= best_count {
                            let rank = tie_rank(node_key, labels[nbor_slot]);
                            if (count, rank) > (best_count, best_rank) {
                                best_slot = nbor_slot;
                                best_count = count;
                                best_rank = rank;
                            }
                        }
                    }
                    // Reset
                    for &slot in touched.iter() {
                        counts.clear_slot(slot);
                    }
                    touched.clear();

                    // `best_slot` is still `prev_slot` when no voting happened, so an unlabelled node
                    // with no labelled neighbours keeps its label.
                    cur[pos].store(best_slot, Ordering::Relaxed);
                    // `best_count` is the winner's FINAL tally: a vote landing on the reigning
                    // `best_slot` refreshes it through the `nbor_slot == best_slot` arm above, and a
                    // challenger that takes over brings its own count with it.
                    //
                    // `total == 0` only when this node is unlabelled AND no neighbour is labelled, in
                    // which case it keeps NO_LABEL and its 0.0 -- the sentinel's own meaning -- so the
                    // guard is also what keeps the division off a zero denominator.
                    if total > 0 {
                        let share = best_count as f64 / total as f64;
                        votes[pos].store(share.to_bits(), Ordering::Relaxed);
                    }
                    if best_slot != prev_slot {
                        changed.fetch_add(1, Ordering::Relaxed);
                        // The frontier
                        for &nbor in nbors.iter() {
                            active_next[nbor].store(true, Ordering::Relaxed);
                        }
                    }
                },
            );
            let diff = changed.into_inner();
            // `prev` now holds the labels this sweep produced, `cur` the ones it started from.
            std::mem::swap(&mut prev, &mut cur);
            std::mem::swap(&mut active_cur, &mut active_next);

            // check for improvement
            let improved = (diff as f64) < (best as f64) * (1.0 - rel_tol);
            best = best.min(diff);
            stale = if improved { 0 } else { stale + 1 };
            // Stop once fully converged (diff == 0) or the changed-node count has plateaued.
            if diff == 0 || stale >= patience {
                let pct = 100.0 * diff as f64 / n as f64;
                debug!(
                    "label_propagation: stopped after {n_iter} iters; \
                     diff={diff} ({pct:.2}%); seed={tie_seed}"
                );
                break;
            }
        }
    });

    // `cur` still holds the labels from before the last sweep, which is what `alternate_id` and
    // `is_changed` report on.
    let values: Vec<LabelPropState> = (0..n)
        .into_par_iter()
        .map(|pos| {
            let slot = prev[pos].load(Ordering::Relaxed);
            let was = cur[pos].load(Ordering::Relaxed);
            // `prev` holds the labels the last sweep produced and `votes` the share that produced
            // them, written in the same iteration -- so the two always describe the same vote.
            let confidence = f64::from_bits(votes[pos].load(Ordering::Relaxed));
            LabelPropState {
                community_id: if slot == NO_LABEL {
                    NO_LABEL
                } else {
                    labels[slot]
                },
                alternate_id: (slot != was && was != NO_LABEL).then(|| labels[was]),
                confidence,
                is_changed: slot != was,
            }
        })
        .collect();

    Ok(TypedNodeState::new(
        GenericNodeState::new_from_eval_with_index(g.clone(), values, index, None),
    ))
}
