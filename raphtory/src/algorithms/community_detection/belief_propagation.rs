use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{GenericNodeState, Index, TypedNodeState},
            view::{
                internal::{filtered_node::FilteredNodeStorageOps, InternalLayerOps},
                Filter, StaticGraphViewOps,
            },
        },
        graph::views::filter::model::{
            edge_filter::EdgeFilter, ComposableFilter, EdgeViewFilterOps,
        },
        task::{custom_pool, POOL},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{entities::VID, Direction};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Default cap on sweeps per label
const DEFAULT_MAX_ITER: usize = 100;

/// Default number of strongest labels kept per node.
const DEFAULT_TOP_R: usize = 5;

/// Label slots are `u16`, with `u16::MAX` reserved as the empty-slot sentinel of the top-r arrays.
const EMPTY_SLOT: u16 = u16::MAX;

/// Most distinct seed labels a run accepts: every `u16` except the sentinel.
const MAX_LABELS: usize = EMPTY_SLOT as usize;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct BeliefPropState {
    /// The strongest labels, at most `top_r` sorted by descending D-belief.
    pub top_labels: Vec<usize>,
    /// Dirichlet beliefs `b_u(i)`: concentration parameters (minus 1), not probabilities.
    pub top_values: Vec<f64>,
    /// Certainty: `Σ_i b_u(i)` over every label above the floor, not only the top-r.
    pub mass: f64,
    /// Entropy 1: `Σ_i b_u(i) ln b_u(i)`.
    pub sum_b_ln_b: f64,
    /// Entropy 2: `Σ_i (b_u(i) + 1) ln(b_u(i) + 1)`.
    pub sum_b1_ln_b1: f64,
}

/// Seed evidence, as accepted by [`belief_propagation`].
///
/// Implemented for a `HashMap` keyed by anything that names a node -- a name, a global id, or a
/// `Node` -- so that callers never have to reach for the internal `VID`s the algorithm works in.
/// Each node maps to its `(label, mass)` pairs.
pub trait IntoSeedBeliefs {
    /// Resolves the caller's node references into the `VID`-keyed map the algorithm uses.
    fn into_seed_beliefs<G: StaticGraphViewOps>(
        self,
        graph: &G,
    ) -> Result<HashMap<VID, Vec<(usize, f64)>>, GraphError>;
}

impl<V: AsNodeRef> IntoSeedBeliefs for HashMap<V, Vec<(usize, f64)>> {
    fn into_seed_beliefs<G: StaticGraphViewOps>(
        self,
        graph: &G,
    ) -> Result<HashMap<VID, Vec<(usize, f64)>>, GraphError> {
        let mut resolved = HashMap::with_capacity(self.len());
        for (node, beliefs) in self {
            let node_ref = node.as_node_ref();
            match graph.node(node_ref) {
                Some(n) => {
                    resolved.insert(n.node, beliefs);
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
        Ok(resolved)
    }
}

fn invalid(reason: String) -> GraphError {
    GraphError::InvalidValue { reason }
}

/// Checks the scalar parameters.
fn validate_params(
    c: f64,
    epsilon: f64,
    activation_tol: f64,
    top_r: usize,
) -> Result<(), GraphError> {
    if !(c > 0.0 && c < 1.0) {
        return Err(invalid(format!("c must be in (0, 1), got {c}")));
    }
    if !(epsilon.is_finite() && epsilon > 0.0) {
        return Err(invalid(format!(
            "epsilon must be finite and > 0, got {epsilon}"
        )));
    }
    if !(activation_tol > 0.0) {
        return Err(invalid(format!(
            "activation_tol must be > 0, got {activation_tol}"
        )));
    }
    if top_r == 0 {
        return Err(invalid("top_r must be at least 1".to_string()));
    }
    Ok(())
}

/// Validates the seed masses and compacts the seed labels into slots.
///
/// Returns the slot -> user label table (labels ascending, so slot order is label order) and, per
/// slot, that label's seeds as `(index position, mass)` sorted by position. Entries with mass below
/// `epsilon` are dropped after validation, and a label left with no seeds gets no slot.
fn compact_seeds(
    seeds: HashMap<VID, Vec<(usize, f64)>>,
    index: &Index<VID>,
    epsilon: f64,
) -> Result<(Vec<usize>, Vec<Vec<(usize, f64)>>), GraphError> {
    // convert VID -> [(label, mass), ...] to label -> [(pos, mass), ...]
    let mut seeds_by_label: FxHashMap<usize, Vec<(usize, f64)>> = FxHashMap::default();
    for (vid, beliefs) in seeds {
        let pos = index.index(&vid).expect("seed VID not in index");
        let mut seen: FxHashSet<usize> = FxHashSet::default();
        for (label, mass) in beliefs {
            if !(mass.is_finite() && mass >= 0.0) {
                return Err(invalid(format!(
                    "seed mass must be finite and >= 0, got {mass} for label {label}"
                )));
            }
            if !seen.insert(label) {
                return Err(invalid(format!(
                    "label {label} appears more than once in one node's seeds"
                )));
            }
            if mass >= epsilon {
                seeds_by_label.entry(label).or_default().push((pos, mass));
            }
        }
    }
    if seeds_by_label.len() > MAX_LABELS {
        return Err(invalid(format!(
            "at most {MAX_LABELS} distinct seed labels are supported, got {}",
            seeds_by_label.len()
        )));
    }

    // for each label (slot) sort the [(pos, mass), ...] tuples by pos
    let mut labels: Vec<usize> = seeds_by_label.keys().copied().collect();
    labels.sort_unstable();
    let seeds_by_slot = labels
        .iter()
        .map(|label| {
            // move the Vec in seeds_by_label instead of copying it
            let mut slot_seeds = seeds_by_label
                .remove(label)
                .expect("label taken from seeds_by_label");
            slot_seeds.sort_unstable_by_key(|&(pos, _)| pos);
            slot_seeds
        })
        .collect();
    // labels and seeds_by_slot are both indexed by slot
    Ok((labels, seeds_by_slot))
}

/// Inserts `(slot, value)` into one node's top-r row, which is kept by value descending.
///
/// Labels are folded in ascending slot order, so inserting only ahead of a strictly smaller value
/// breaks ties by slot ascending without comparing slots.
#[inline]
fn insert_top(slots: &mut [u16], values: &mut [f64], slot: u16, value: f64) {
    // strict inequality breaks ties
    let Some(k) = (0..slots.len()).find(|&k| slots[k] == EMPTY_SLOT || value > values[k]) else {
        return;
    };
    // shift smaller elements, overwrite with new value at position k
    slots[k..].rotate_right(1);
    values[k..].rotate_right(1);
    slots[k] = slot;
    values[k] = value;
}

/// Belief propagation with certainty: the NetConf model (Dirichlet-multinomial belief propagation)
/// with modulation matrix `M = cI` (homophily).
///
/// Eswaran, Günnemann & Faloutsos, The Power of Certainty: A Dirichlet-Multinomial Model for
/// Belief Propagation, SIAM SDM 2017, pp. 144–152, doi:10.1137/1.9781611974973.17.
/// <https://dhivyaeswaran.github.io/papers/sdm17-netconf.pdf>
///
/// Each node carries a Dirichlet belief per label: its D-belief `b_u(i)` is the Dirichlet parameter
/// **minus 1** -- unnormalised, `>= 0`, and not a probability. `b_u = 0` means no evidence, and the
/// node's certainty is `mass = Σ_i b_u(i)`. Under `M = cI` (diagonal modulation matrix) the labels
/// decouple exactly, so they are propagated one at a time.
///
/// The graph is read as unweighted and undirected: both directions and all layers of the view are
/// merged, and duplicate edges count once. Self-loops are removed.
///
/// # Update rule
///
/// The paper iterates `b_u = e_u + [c·Σ_{v∈N(u)} b_v − c²·d_u·b_u] / (1 − c²)`. We solve that
/// fixed-point equation for `b_u` instead:
///
/// `b_u ← [(1 − c²)·e_u + c·Σ_{v∈N(u)} b_v] / (1 − c² + c²·d_u)`
///
/// It has the same unique fixed point, but every term is non-negative and there is no `c ≲ 1/√d_max`
/// constraint from hubs: it converges whenever `c < 1/ρ(B_nb)`, the spectral radius of the
/// non-backtracking matrix. Starting from zero the values increase monotonically, so every value,
/// converged or not, is a lower bound on the evidence reaching that node. Not converging within
/// `max_iter` sweeps means `c` is too large (`c >= 1/ρ(B_nb)`); it is reported, not an error.
///
/// # Seeds
///
/// Seeds are soft and are not clamped: a seed's final belief is its own prior plus the evidence
/// flowing back to it. There is no base rate -- unseeded nodes start with no evidence. The
/// computation is linear in the seed masses, so scaling them all by α scales every belief by α:
/// only mass ratios, and masses relative to `epsilon`, carry meaning.
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `seeds` - A HashMap of node to its `(label, mass)` pairs. Masses must be finite and `>= 0`;
///   a label repeated within one node's pairs is an error, not a sum. Label values are
///   unconstrained but they are held as `u16` so at most 65,535 distinct seed labels are supported.
/// - `c` - Decay per hop, in `(0, 1)`.
/// - `epsilon` - Evidence floor: a belief below it counts as no evidence, is set to 0 and does not
///   propagate. Seed entries with a mass below it are dropped. It bounds how far each label spreads.
/// - `activation_tol` - (Optional) A node re-activates its neighbours only if its belief rose by
///   more than this in a sweep. Defaults to `epsilon`.
/// - `max_iter` - (Optional) Maximum sweeps per label. Defaults to 100.
/// - `top_r` - (Optional) Number of strongest labels kept per node. Defaults to 5.
/// - `threads` - (Optional) Number of threads to use
///
/// # Returns
///
/// A `TypedNodeState` mapping each node to its `BeliefPropState`: its `top_r` strongest labels and
/// their D-beliefs, its `mass`, and the two entropy sums `Σ b ln b` and `Σ (b+1) ln(b+1)` over
/// every label above the floor. Nodes with no evidence get empty vectors and zeros. The output is
/// bit-identical across thread counts.
///
/// One line of statistics per label (iterations, convergence, nodes reached, largest belief) is
/// printed; it is not part of the return value.
#[allow(clippy::too_many_arguments)]
pub fn belief_propagation<G: StaticGraphViewOps>(
    g: &G,
    seeds: impl IntoSeedBeliefs,
    c: f64,
    epsilon: f64,
    activation_tol: Option<f64>,
    max_iter: Option<usize>,
    top_r: Option<usize>,
    threads: Option<usize>,
) -> Result<TypedNodeState<'static, BeliefPropState, G>, GraphError> {
    let activation_tol = activation_tol.unwrap_or(epsilon);
    let max_iter = max_iter.unwrap_or(DEFAULT_MAX_ITER);
    let top_r = top_r.unwrap_or(DEFAULT_TOP_R);
    validate_params(c, epsilon, activation_tol, top_r)?;

    // Seeds, the index and the output all live on `g` itself; the self-loop filter only shapes
    // the adjacency. A node whose only edges are self-loops drops out of the filtered view, but
    // it stays here, with no neighbours.
    let seeds = seeds.into_seed_beliefs(g)?;
    let index = Index::for_graph(g.clone());
    let n = index.len();
    let (labels, seeds_by_slot) = compact_seeds(seeds, &index, epsilon)?;

    let filtered = g.filter(EdgeFilter.is_self_loop().not())?;
    // One lock for the whole run: `core_node` on a locked storage is an indexed read
    let locked = g.core_graph().lock();
    let layer_ids = filtered.layer_ids();
    // Neighbours as index positions: both directions and all layers merged, sorted by VID and
    // deduplicated -- the undirected 0/1 adjacency, so `d_u` is the length of the list.
    let collect_nbors = |pos: usize, out: &mut Vec<usize>| {
        out.clear();
        let vid = index.value(pos).expect("position not in index");
        out.extend(
            locked
                .core_node(vid)
                .as_ref()
                .filtered_neighbours_iter(&filtered, layer_ids, Direction::BOTH)
                .map(|nbor| index.index(&nbor).expect("neighbour VID not in index")),
        );
    };

    let c2 = c * c;
    let one_minus_c2 = 1.0 - c2;

    // Per label. `b` is all zeros between labels: only `touched` is ever reset, never the whole array.
    let mut b = vec![0.0f64; n];
    let active: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(false)).collect();
    let mut touched: Vec<usize> = Vec::new();
    let mut new_vals: Vec<f64> = Vec::new();
    // Whole run. The top-r is structure-of-arrays, `top_r` entries per node.
    let mut top_slots = vec![EMPTY_SLOT; n * top_r];
    let mut top_values = vec![0.0f64; n * top_r];
    let mut mass = vec![0.0f64; n];
    let mut sum_b_ln_b = vec![0.0f64; n];
    let mut sum_b1_ln_b1 = vec![0.0f64; n];

    let pool: Arc<rayon::ThreadPool> = threads.map(custom_pool).unwrap_or_else(|| POOL.clone());
    pool.install(|| {
        for (slot, slot_seeds) in seeds_by_slot.iter().enumerate() {
            // `slot_seeds` is sorted by position, so the prior is a binary search, never a dense array.
            let prior = |u: usize| {
                slot_seeds
                    .binary_search_by_key(&u, |&(pos, _)| pos)
                    .map_or(0.0, |i| slot_seeds[i].1)
            };
            let mut frontier: Vec<usize> = slot_seeds.iter().map(|&(pos, _)| pos).collect();
            let mut iterations = 0;
            while !frontier.is_empty() && iterations < max_iter {
                iterations += 1;

                // Sweep: reads only `b` at t; `new_vals` holds t+1 until the write-back.
                let b_t = &b;
                new_vals.clear();
                new_vals.resize(frontier.len(), 0.0);
                let next_frontier: Vec<usize> = new_vals
                    .par_iter_mut()
                    .zip(frontier.par_iter())
                    .fold(
                        || (Vec::new(), Vec::new()),
                        |(mut nbors, mut next), (new_val, &u)| {
                            collect_nbors(u, &mut nbors);
                            // Sequential over the sorted list, so the summation order is fixed.
                            let sum: f64 = nbors.iter().map(|&v| b_t[v]).sum();
                            let d_u = nbors.len() as f64;
                            let mut new =
                                (one_minus_c2 * prior(u) + c * sum) / (one_minus_c2 + c2 * d_u);
                            if new < epsilon {
                                new = 0.0;
                            }
                            // Activate: whoever sets a neighbour's flag first queues it.
                            if new - b_t[u] > activation_tol {
                                for &v in nbors.iter() {
                                    if !active[v].swap(true, Ordering::Relaxed) {
                                        next.push(v);
                                    }
                                }
                            }
                            *new_val = new;
                            (nbors, next)
                        },
                    )
                    .flat_map_iter(|(_, next)| next)
                    .collect();

                // Write back. Values only grow, so a node joins `touched` exactly once.
                for (&u, &new) in frontier.iter().zip(new_vals.iter()) {
                    if b[u] == 0.0 && new > 0.0 {
                        touched.push(u);
                    }
                    b[u] = new;
                }
                next_frontier
                    .par_iter()
                    .for_each(|&v| active[v].store(false, Ordering::Relaxed));
                frontier = next_frontier;
            }
            let converged = frontier.is_empty();

            // Fold this label into the whole-run accumulators, then reset `b` where it was touched.
            let reached = touched.len();
            let mut max_belief = 0.0f64;
            for &u in touched.iter() {
                let value = b[u];
                if value > 0.0 {
                    max_belief = max_belief.max(value);
                    let row = u * top_r..(u + 1) * top_r;
                    insert_top(
                        &mut top_slots[row.clone()],
                        &mut top_values[row],
                        slot as u16,
                        value,
                    );
                    mass[u] += value;
                    sum_b_ln_b[u] += value * value.ln();
                    sum_b1_ln_b1[u] += (value + 1.0) * value.ln_1p();  // ln(1+x)
                }
                b[u] = 0.0;
            }
            touched.clear();

            println!(
                "belief_propagation: label={} iterations={iterations} converged={} reached={reached} \
                 max_belief={max_belief}",
                labels[slot],
                if converged { "yes" } else { "no" },
            );
        }
    });

    let values: Vec<BeliefPropState> = (0..n)
        .into_par_iter()
        .map(|pos| {
            let row = pos * top_r..(pos + 1) * top_r;
            let (row_labels, row_values) = top_slots[row.clone()]
                .iter()
                .zip(&top_values[row])
                .take_while(|(&s, _)| s != EMPTY_SLOT)
                .map(|(&s, &v)| (labels[s as usize], v))
                .unzip();
            BeliefPropState {
                top_labels: row_labels,
                top_values: row_values,
                mass: mass[pos],
                sum_b_ln_b: sum_b_ln_b[pos],
                sum_b1_ln_b1: sum_b1_ln_b1[pos],
            }
        })
        .collect();

    Ok(TypedNodeState::new(
        GenericNodeState::new_from_eval_with_index(g.clone(), values, index, None),
    ))
}
