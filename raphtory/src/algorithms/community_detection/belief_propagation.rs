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
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

const DEFAULT_MAX_ITER: usize = 100;
const DEFAULT_TOP_R: usize = 5;

/// Label slots are `u16`, with `u16::MAX` reserved as the empty-slot sentinel of the top-r arrays.
const EMPTY_SLOT: u16 = u16::MAX;
const MAX_LABELS: usize = EMPTY_SLOT as usize;

/// Defaults used to determine divergence of the algorithm.
const RHO_WARMUP_SWEEPS: usize = 7;
const RHO_DIVERGENCE_THRESHOLD: f64 = 1.05;
const RHO_DIVERGENCE_SWEEPS: usize = 3;

/// Defaults for [`belief_propagation_probe_convergence`].
const DEFAULT_PROBE_MAX_ITER: usize = 30;
const DEFAULT_PROBE_EPSILON: f64 = 1e-8;

/// Temporary diagnostic: appends a summary of each label's last 10 sweeps to its stats line.
const BP_TRACE: bool = true;

/// One sweep as `BP_TRACE` records it: `(ρ̂, frontier, dS, touched, max)`.
type Sweep = (Option<f64>, usize, f64, usize, f64);

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

/// Mean and population standard deviation.
fn mean_std(xs: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    (mean, var.sqrt())
}

/// `↑` or `↓` when `last` differs from `first` by more than 5% of `|first|`, `~` otherwise.
fn trend(first: f64, last: f64) -> char {
    if first == 0.0 {
        return if last > 0.0 { '↑' } else { '~' };
    }
    let change = (last - first) / first.abs();
    if change > 0.05 {
        '↑'
    } else if change < -0.05 {
        '↓'
    } else {
        '~'
    }
}

/// The `BP_TRACE` suffix of a label's stats line: `ρ̂` and the frontier size as mean ± std, the last
/// `dS`, and trend arrows comparing the first and last sweeps of the window.
fn trace_summary(window: &VecDeque<Sweep>) -> String {
    let (Some(first), Some(last)) = (window.front(), window.back()) else {
        return String::new();
    };
    let rs: Vec<f64> = window.iter().filter_map(|sweep| sweep.0).collect();
    let r = match (rs.first(), rs.last()) {
        (Some(&r_first), Some(&r_last)) => {
            let (mean, std) = mean_std(&rs);
            format!("{mean:.3}±{std:.3}{}", trend(r_first, r_last))
        }
        _ => "-".to_string(),
    };
    let frontiers: Vec<f64> = window.iter().map(|sweep| sweep.1 as f64).collect();
    let (f_mean, f_std) = mean_std(&frontiers);
    format!(
        " | last {}: rho={r} f={f_mean:.3e}±{f_std:.2e}{} dS={:.3e}{} reached{} max{}",
        window.len(),
        trend(first.1 as f64, last.1 as f64),
        last.2,
        trend(first.2, last.2),
        trend(first.3 as f64, last.3 as f64),
        trend(first.4, last.4),
    )
}

/// Builds the adjacency both [`belief_propagation`] and [`belief_propagation_probe_convergence`]
/// sweep over, and runs `f` with it.
///
/// `f` gets `collect_nbors(pos, out)`, which fills `out` with the neighbours of the node at index
/// position `pos`. A node whose only edges are self-loops gets no neighbours.
fn with_neighbours<G: StaticGraphViewOps, R>(
    g: &G,
    index: &Index<VID>,
    f: impl FnOnce(&(dyn Fn(usize, &mut Vec<usize>) + Sync)) -> R,
) -> Result<R, GraphError> {
    let filtered = g.filter(EdgeFilter.is_self_loop().not())?;
    // One lock for the whole run: `core_node` on a locked storage is an indexed read
    let locked = g.core_graph().lock();
    let layer_ids = filtered.layer_ids();
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
    Ok(f(&collect_nbors))
}

/// Propagates one label from its seeds until its frontier empties, `max_iter` sweeps pass, or `ρ̂`
/// shows divergence.
///
/// `b`, `active`, `touched`, `new_vals` and `window` are per-run buffers: `b` and `active` must be
/// all zeros / `false` on entry, and `b` is left holding the label's beliefs on the `touched` nodes
/// for the caller to fold and reset. `window` receives the last 10 sweeps when `BP_TRACE` is on.
///
/// Returns `(iterations, converged, diverged, ρ̂)`, where `ρ̂` is the last valid two-step estimate.
#[allow(clippy::too_many_arguments)]
fn propagate_label(
    slot_seeds: &[(usize, f64)],
    collect_nbors: &(dyn Fn(usize, &mut Vec<usize>) + Sync),
    b: &mut [f64],
    active: &[AtomicBool],
    touched: &mut Vec<usize>,
    new_vals: &mut Vec<f64>,
    window: &mut VecDeque<Sweep>,
    c: f64,
    epsilon: f64,
    activation_tol: f64,
    max_iter: usize,
) -> (usize, bool, bool, Option<f64>) {
    let c2 = c * c;
    let one_minus_c2 = 1.0 - c2;
    // `slot_seeds` is sorted by position, so the prior is a binary search, never a dense array.
    let prior = |u: usize| {
        slot_seeds
            .binary_search_by_key(&u, |&(pos, _)| pos)
            .map_or(0.0, |i| slot_seeds[i].1)
    };
    let mut frontier: Vec<usize> = slot_seeds.iter().map(|&(pos, _)| pos).collect();
    let mut iterations = 0;
    // `dS` one and two sweeps back, and the last valid `ρ̂`.
    let mut ds_1 = 0.0f64;
    let mut ds_2 = 0.0f64;
    let mut rho: Option<f64> = None;
    // Consecutive valid sweeps with `ρ̂` above `RHO_DIVERGENCE_THRESHOLD`.
    let mut diverging = 0;
    // `BP_TRACE` only: the last 10 sweeps and the running max.
    window.clear();
    let mut max_b = 0.0f64;
    while !frontier.is_empty() && iterations < max_iter {
        iterations += 1;
        let sweep_start = Instant::now();

        // Sweep: reads only `b` at t; `new_vals` holds t+1 until the write-back.
        let b_t = &*b;
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
                    let mut new = (one_minus_c2 * prior(u) + c * sum) / (one_minus_c2 + c2 * d_u);
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
        let mut ds = 0.0f64;
        for (&u, &new) in frontier.iter().zip(new_vals.iter()) {
            if b[u] == 0.0 && new > 0.0 {
                touched.push(u);
            }
            ds += new - b[u];
            if BP_TRACE {
                max_b = max_b.max(new);
            }
            b[u] = new;
        }
        // Two-step ratio: cancels the swing of near-bipartite graphs. Valid only after the
        // warm-up, and before the mean increase drops below `activation_tol`, where the
        // activation rule distorts it.
        let rho_t = (iterations > RHO_WARMUP_SWEEPS
            && ds_2 > 0.0
            && ds / frontier.len() as f64 >= activation_tol)
            .then(|| (ds / ds_2).sqrt());
        if rho_t.is_some() {
            rho = rho_t;
        }
        // Any sweep that isn't valid and above the threshold breaks the streak.
        diverging = match rho_t {
            Some(r) if r > RHO_DIVERGENCE_THRESHOLD => diverging + 1,
            _ => 0,
        };
        // `BP_TRACE` only: the raw one-step and two-step ratios, valid or not.
        let r_raw = (ds_1 > 0.0).then(|| ds / ds_1);
        let rho_raw = (ds_2 > 0.0).then(|| (ds / ds_2).sqrt());
        ds_2 = ds_1;
        ds_1 = ds;
        if BP_TRACE {
            if window.len() == 10 {
                window.pop_front();
            }
            window.push_back((rho_t, frontier.len(), ds, touched.len(), max_b));
        }
        next_frontier
            .par_iter()
            .for_each(|&v| active[v].store(false, Ordering::Relaxed));
        if BP_TRACE {
            println!(
                "  sweep={iterations} frontier={} reached={} max={max_b:.3e} dS={ds:.3e} \
                 r={} rho={}{} time={:.3}s",
                frontier.len(),
                touched.len(),
                r_raw.map_or("-".to_string(), |r| format!("{r:.4}")),
                rho_raw.map_or("-".to_string(), |r| format!("{r:.4}")),
                if rho_t.is_some() { "" } else { "*" },
                sweep_start.elapsed().as_secs_f64(),
            );
        }
        if diverging == RHO_DIVERGENCE_SWEEPS {
            return (iterations, false, true, rho);
        }
        frontier = next_frontier;
    }
    (iterations, frontier.is_empty(), false, rho)
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
/// Divergence detected earlier is an error: each sweep's increase in belief `S_t` gives the
/// two-step estimate `ρ̂ = √(S_t / S_{t−2})` of the update's spectral radius, and once it exceeds
/// 1.05 for 3 consecutive sweeps after a 7-sweep warm-up, the whole run stops.
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

    // Per label. `b` is all zeros between labels: only `touched` is ever reset, never the whole array.
    let mut b = vec![0.0f64; n];
    let active: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(false)).collect();
    let mut touched: Vec<usize> = Vec::new();
    let mut new_vals: Vec<f64> = Vec::new();
    // `BP_TRACE` only: the last 10 sweeps of the current label.
    let mut window: VecDeque<Sweep> = VecDeque::with_capacity(10);
    // Whole run. The top-r is structure-of-arrays, `top_r` entries per node.
    let mut top_slots = vec![EMPTY_SLOT; n * top_r];
    let mut top_values = vec![0.0f64; n * top_r];
    let mut mass = vec![0.0f64; n];
    let mut sum_b_ln_b = vec![0.0f64; n];
    let mut sum_b1_ln_b1 = vec![0.0f64; n];

    let pool: Arc<rayon::ThreadPool> = threads.map(custom_pool).unwrap_or_else(|| POOL.clone());
    with_neighbours(g, &index, |collect_nbors| {
        pool.install(|| {
            if BP_TRACE {
                println!(
                    "bp start: nodes={n} labels={} c={c} epsilon={epsilon} \
                 activation_tol={activation_tol} max_iter={max_iter} threads={}",
                    labels.len(),
                    rayon::current_num_threads(),
                );
            }
            for (slot, slot_seeds) in seeds_by_slot.iter().enumerate() {
                if BP_TRACE {
                    println!("label={} seeds={}", labels[slot], slot_seeds.len());
                }
                let (iterations, converged, diverged, rho) = propagate_label(
                    slot_seeds,
                    collect_nbors,
                    &mut b,
                    &active,
                    &mut touched,
                    &mut new_vals,
                    &mut window,
                    c,
                    epsilon,
                    activation_tol,
                    max_iter,
                );
                if diverged {
                    let r = rho.expect("a diverging label has a valid rho");
                    return Err(invalid(format!(
                        "c={c} diverges: rho={r:.4} > {RHO_DIVERGENCE_THRESHOLD} for \
                     {RHO_DIVERGENCE_SWEEPS} sweeps on label {} at sweep {iterations}; \
                     1/rho(B_nb) <= c/rho = {:.3e}",
                        labels[slot],
                        c / r,
                    )));
                }

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
                        sum_b1_ln_b1[u] += (value + 1.0) * value.ln_1p(); // ln(1+x)
                    }
                    b[u] = 0.0;
                }
                touched.clear();

                let trace = if BP_TRACE {
                    trace_summary(&window)
                } else {
                    String::new()
                };
                println!(
                    "label={} iterations={iterations} converged={} reached={reached} \
                 max_belief={max_belief} rho={}{trace}",
                    labels[slot],
                    if converged { "yes" } else { "no" },
                    rho.map_or("-".to_string(), |r| format!("{r:.4}")),
                );
            }
            Ok(())
        })
    })??;

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

/// Estimates the spectral radius `ρ(W)` of the [`belief_propagation`] update at decay `c`, to
/// use to find the correct decay before a full run.
///
/// Sums every node's seed masses across labels into a single label, propagates it once
/// and returns the last valid two-step estimate `ρ̂ = √(S_t / S_{t−2})`. The run
/// converges at decay `c` if `ρ < 1`, and diverges if `ρ > 1`.
///
/// `ρ` depends only on the graph and `c`, not on the seeds: they only change how quickly `ρ̂`
/// settles. Merging the labels makes one run reach every component that any seed touches, and
/// the largest `ρ` among them is the one that limits `c`.
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `seeds` - The same seeds as [`belief_propagation`]; they are validated the same way.
/// - `c` - Decay per hop, in `(0, 1)`.
/// - `epsilon` - (Optional) Evidence floor. Defaults to `1e-8`: it should be small so the floor doesn't
///   stop the probe short of the dense core of the graph.
/// - `max_iter` - (Optional) Maximum sweeps. Defaults to 30.
/// - `threads` - (Optional) Number of threads to use
///
/// # Returns
///
/// `ρ̂`, or `None` if no sweep was valid: the probe died out before the warm-up ended. Divergence
/// is not an error here: it stops the probe early and returns `ρ̂ > 1`.
pub fn belief_propagation_probe_convergence<G: StaticGraphViewOps>(
    g: &G,
    seeds: impl IntoSeedBeliefs,
    c: f64,
    epsilon: Option<f64>,
    max_iter: Option<usize>,
    threads: Option<usize>,
) -> Result<Option<f64>, GraphError> {
    let epsilon = epsilon.unwrap_or(DEFAULT_PROBE_EPSILON);
    let max_iter = max_iter.unwrap_or(DEFAULT_PROBE_MAX_ITER);
    validate_params(c, epsilon, epsilon, 1)?;

    let seeds = seeds.into_seed_beliefs(g)?;
    let index = Index::for_graph(g.clone());
    let n = index.len();

    let (_, seeds_by_slot) = compact_seeds(seeds, &index, epsilon)?;
    // Merged labels: every slot's seeds, sorted by position and summed where a node seeds several labels.
    let mut merged: Vec<(usize, f64)> = seeds_by_slot.into_iter().flatten().collect();
    merged.sort_by_key(|&(pos, _)| pos);
    merged.dedup_by(|next, kept| {
        let same = next.0 == kept.0;
        if same {
            kept.1 += next.1;
        }
        same
    });

    let mut b = vec![0.0f64; n];
    let active: Vec<AtomicBool> = (0..n).map(|_| AtomicBool::new(false)).collect();
    let mut touched: Vec<usize> = Vec::new();
    let mut new_vals: Vec<f64> = Vec::new();
    let mut window: VecDeque<Sweep> = VecDeque::with_capacity(10);

    let pool: Arc<rayon::ThreadPool> = threads.map(custom_pool).unwrap_or_else(|| POOL.clone());
    let (iterations, converged, diverged, rho) = with_neighbours(g, &index, |collect_nbors| {
        pool.install(|| {
            propagate_label(
                &merged,
                collect_nbors,
                &mut b,
                &active,
                &mut touched,
                &mut new_vals,
                &mut window,
                c,
                epsilon,
                epsilon,
                max_iter,
            )
        })
    })?;
    println!(
        "probe c={c} seeds={} iterations={iterations} converged={} diverged={} reached={} rho={}",
        merged.len(),
        if converged { "yes" } else { "no" },
        if diverged { "yes" } else { "no" },
        touched.len(),
        rho.map_or("-".to_string(), |r| format!("{r:.4}")),
    );
    Ok(rho)
}
