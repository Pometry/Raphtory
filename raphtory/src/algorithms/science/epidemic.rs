use crate::{core::entities::nodes::node_ref::NodeRef, db::graph::node::NodeView};
/// Epidemic modeling library
///
/// This library provides various utilities and models for simulating and analyzing epidemic
/// spread on graphs.
use crate::{
    core::utils::time::IntoTime,
    db::{api::view::StaticGraphViewOps, graph::views::window_graph::WindowedGraph},
    prelude::*,
};
use rand::{
    prelude::{SliceRandom, StdRng},
    Rng, SeedableRng,
};
use std::collections::HashMap;
use kdam::format::time;

// Constants representing different states in epidemic models.
const SUSCEPTIBLE: u8 = 0u8;
const INFECTIOUS: u8 = 1u8;
const RECOVERED: u8 = 2u8;
const EXPOSED: u8 = 3u8;

/// An enum representing the initial seeding strategy for an epidemic model.
pub enum SeedSet<V>
where
    V: Into<NodeRef>,
{
    /// Represents a set of nodes to be initially infected.
    NodeSet(Vec<V>),
    /// Represents an initial infection rate.
    InitialInfect(f64),
}

/// Converts a `Vec<V>` into a `SeedSet`.
impl<V> From<Vec<V>> for SeedSet<V>
where
    V: Into<NodeRef> + Copy,
{
    fn from(node_set: Vec<V>) -> Self {
        SeedSet::NodeSet(node_set)
    }
}

/// Converts a `f64` seed value into a `SeedSet`.
impl From<f64> for SeedSet<NodeRef> {
    fn from(seed: f64) -> Self {
        SeedSet::InitialInfect(seed)
    }
}

/// Sets up an SI (Susceptible-Infected) model on a graph.
///
/// # Arguments
/// * `graph` - A reference to the graph.
/// * `initial_seed_set` - The initial seeding strategy. Either be an f64 or a hashset of nodes
/// * `seed` - An optional seed for random number generation.
/// * `infected_state` - The state representing infection.
///
/// # Returns
/// A tuple containing the random number generator and a map of node views to their states.
fn setup_si<G, V, W>(
    graph: &G,
    initial_seed_set: W,
    seed: Option<[u8; 32]>,
    infected_state: u8,
) -> (StdRng, HashMap<NodeView<G>, (u8, i64)>)
where
    G: StaticGraphViewOps,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    let mut rng: StdRng = SeedableRng::from_seed(seed.unwrap_or_default());
    let seed_set = initial_seed_set.into();
    match seed_set {
        SeedSet::InitialInfect(initial_infected_ratio) => {
            let infected_count = graph.count_nodes() as f64 * initial_infected_ratio;
            let mut population: Vec<NodeView<G>> = graph.nodes().iter().collect();
            // Infect initial number of infected nodes
            population.shuffle(&mut rng);
            let prev_nodes_status: HashMap<NodeView<G>, (u8, i64)> = population
                .clone()
                .into_iter()
                .take(infected_count as usize)
                .map(|v| {
                    (
                        v.clone(),
                        (infected_state, graph.start().unwrap_or(-1i64) + 1i64),
                    )
                })
                .collect();
            (rng, prev_nodes_status)
        }
        SeedSet::NodeSet(initial_infected_nodes) => {
            let prev_nodes_status: HashMap<NodeView<G>, (u8, i64)> = initial_infected_nodes
                .iter()
                .map(|v| {
                    (
                        graph.node(*v).unwrap(),
                        (infected_state, graph.start().unwrap_or(-1i64) + 1i64),
                    )
                })
                .collect();
            (rng, prev_nodes_status)
        }
    }
}

/// Changes the state of a node based on a probability.
///
/// # Arguments
/// * `recovery_rate` - Probability of recovery.
/// * `rng` - Random number generator.
/// * `new_nodes_status` - Map of new node statuses.
/// * `v` - Vertex view.
/// * `new_state` - The new state to be assigned.
/// * `infection_time` - The time of infection.
fn change_state_by_prob<G, T>(
    recovery_rate: f64,
    rng: &mut StdRng,
    prev_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    new_nodes_status: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    v: &NodeView<WindowedGraph<G>>,
    new_state: u8,
    infection_time: T,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    let prev_infected_time = prev_nodes_state.get(v).unwrap().1;
    if (rng.gen::<f64>() < recovery_rate) & (prev_infected_time < infection_time.into_time()) {
        let _ = new_nodes_status.insert(v.clone(), (new_state, infection_time.into_time()));
        println!(
            "Changed state by p ({:?}) {:?}->{:?}",
            infection_time.into_time(),
            v.name(),
            new_state
        );
    }
}

/// Changes the state of a node based on its neighbors' states.
///
/// # Arguments
/// * `transition_probability` - Probability of state transition.
/// * `rng` - Random number generator.
/// * `prev_nodes_status` - Map of previous node statuses.
/// * `new_nodes_state` - Map of new node states.
/// * `v` - Vertex view.
/// * `new_state` - The new state to be assigned.
fn change_state_by_neighbors<G>(
    transition_probability: f64,
    rng: &mut StdRng,
    prev_nodes_status: &HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    new_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    v: &NodeView<WindowedGraph<G>>,
    new_state: u8,
) where
    G: StaticGraphViewOps,
{
    for edgeview in v.edges().map(|e| e.explode()).flatten() {
        let neighbour = if edgeview.src() != *v {
            edgeview.src()
        } else {
            edgeview.dst()
        };
        let current_time = edgeview.latest_time().unwrap();
        let not_infected_state = (SUSCEPTIBLE, current_time);
        let neighbour_status = prev_nodes_status
            .get(&neighbour)
            .unwrap_or(&not_infected_state);
        if (neighbour_status.0 == new_state) // if the neighbour is the new (infected) state
            & (neighbour_status.1 <= current_time) // if the neighbour was infected before the interaction, it means they can infect again
            & (rng.gen::<f64>() < transition_probability)
        // then roll the dice and infect them
        {
            let _ = new_nodes_state.insert(v.clone(), (new_state, current_time));
            println!(
                "Changed state by n ({:?}) {:?}->{:?}",
                current_time,
                v.name(),
                new_state
            );
            break;
        }
    }
}

/// Gets the state of a node.
///
/// # Arguments
/// * `prev_nodes_status` - Map of previous node statuses.
/// * `v` - Vertex view.
/// * `initial_infection_time` - The initial time of infection.
///
/// # Returns
/// The state of the node.
fn get_node_state<G, T>(
    prev_nodes_status: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    v: &NodeView<WindowedGraph<G>, WindowedGraph<G>>,
    initial_infection_time: T,
) -> u8
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match prev_nodes_status.get(&v) {
        None => {
            prev_nodes_status.insert(v.clone(), (SUSCEPTIBLE, initial_infection_time.into_time()));
            SUSCEPTIBLE
        }
        Some((v_status, _)) => *v_status,
    }
}

/// Implements the SIR/SIRS infection strategy.
///
/// # Arguments
/// * `prev_nodes_state` - Map of previous node statuses.
/// * `new_nodes_state` - Map of new node states.
/// * `rng` - Random number generator.
/// * `v` - Vertex view.
/// * `rates` - Tuple of rates (infection, recovery, recovery_to_susceptible, exposure).
/// * `initial_infection_time` - The initial time of infection.
/// * `is_sirs` - Flag to indicate if SIRS model is used.
fn sir_sirs_strategy<G, T>(
    prev_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    new_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &NodeView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, recovery_to_sus_rate, _): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_sirs: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_nodes_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            prev_nodes_state,
            new_nodes_state,
            v,
            INFECTIOUS,
        ),
        INFECTIOUS => change_state_by_prob(
            recovery_rate,
            rng,
            prev_nodes_state,
            new_nodes_state,
            v,
            RECOVERED,
            initial_infection_time,
        ),
        RECOVERED => {
            if is_sirs {
                change_state_by_prob(
                    recovery_to_sus_rate,
                    rng,
                    prev_nodes_state,
                    new_nodes_state,
                    v,
                    SUSCEPTIBLE,
                    initial_infection_time,
                )
            }
        }
        _ => {}
    }
}

/// Simulates the SIR (Susceptible-Infected-Recovery) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of an infected node recovering.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 1 - Infected, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sir_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model::<G, T, V, W, _>(
        graph,
        initial_seed_set,
        initial_infection_time,
        INFECTIOUS,
        (infection_rate, recovery_rate, 0.0f64, 0.0f64),
        seed,
        time_hops,
        sir_sirs_strategy,
        false,
    )
}

/// Simulates the SIRS (Susceptible-Infected-Recovery-Susceptible) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of an infected node recovering, going from infection to recovered
/// * `rec_to_sus_rate` - The probability of an recovered node going back to susceptible.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 1 - Infected, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sirs_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64,
    recovery_rate: f64,
    recovery_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model::<G, T, V, W, _>(
        graph,
        initial_seed_set,
        initial_infection_time,
        INFECTIOUS,
        (infection_rate, recovery_rate, recovery_to_sus_rate, 0.0f64),
        seed,
        time_hops,
        sir_sirs_strategy,
        true,
    )
}

fn seir_seirs_strategy<G, T>(
    prev_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    new_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &NodeView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_seirs: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_nodes_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            prev_nodes_state,
            new_nodes_state,
            v,
            EXPOSED,
        ),
        EXPOSED => change_state_by_prob(
            exposure_rate,
            rng,
            prev_nodes_state,
            new_nodes_state,
            v,
            INFECTIOUS,
            initial_infection_time,
        ),
        INFECTIOUS => change_state_by_prob(
            recovery_rate,
            rng,
            prev_nodes_state,
            new_nodes_state,
            v,
            RECOVERED,
            initial_infection_time,
        ),
        RECOVERED => {
            if is_seirs {
                change_state_by_prob(
                    recovery_to_sus_rate,
                    rng,
                    prev_nodes_state,
                    new_nodes_state,
                    v,
                    SUSCEPTIBLE,
                    initial_infection_time,
                )
            }
        }
        _ => {}
    }
}

/// Simulates the SEIR (Susceptible-Exposed-Infectious-Recovered ) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from Susceptible to Exposed.
/// * `exposure_rate` - The probability of a node moving from Exposed to Infectious.
/// * `recovery_rate` - The probability of an infected node recovering, going from Infectious to recovered
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
/// * `initial_state` - A state, u8, the infected nodes should start at e.g. 3 for Exposed
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 3 - Exposed, 2 - Infectious, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn seir_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
    initial_state: u8,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model::<G, T, V, W, _>(
        graph,
        initial_seed_set,
        initial_infection_time,
        initial_state,
        (infection_rate, recovery_rate, 0.0f64, exposure_rate),
        seed,
        time_hops,
        seir_seirs_strategy,
        false,
    )
}

/// Simulates the SEIRS (Susceptible-Exposed-Infectious-Recovered-Susceptible) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from Susceptible to Exposed.
/// * `exposure_rate` - The probability of a node moving from Exposed to Infectious.
/// * `recovery_rate` - The probability of an infected node recovering, going from Infectious to recovered
/// * `rec_to_sus_rate` - The probability of an recovered node going back to susceptible.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
/// * `initial_state` - The state of which infected nodes should start at.
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 3 - Exposed, 2 - Infectious, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn seirs_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
    initial_state: u8,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model::<G, T, V, W, _>(
        graph,
        initial_seed_set,
        initial_infection_time,
        initial_state,
        (
            infection_rate,
            recovery_rate,
            rec_to_sus_rate,
            exposure_rate,
        ),
        seed,
        time_hops,
        seir_seirs_strategy,
        true,
    )
}

fn unified_model<G, T, V, W, F>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    initial_infection_state: u8,
    rates: (f64, f64, f64, f64), // infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
    state_transition_strategy: F,
    is_alt: bool,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
    F: Fn(
        &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
        &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
        &mut StdRng,
        &NodeView<WindowedGraph<G>>,
        (f64, f64, f64, f64),
        T,
        bool,
    ) -> (),
{
    let sat_initial_infection_time = initial_infection_time.into_time().saturating_sub(1);
    let mut g_after = graph.after(sat_initial_infection_time);
    let (mut rng, mut prev_nodes_state) =
        setup_si(&g_after, initial_seed_set, seed, initial_infection_state);
    let max_time = match time_hops {
        None => {
            graph.end().unwrap()+1i64
        }
        Some(val) => {
            initial_infection_time.into_time() + val
        }
    };
    for cur_time in initial_infection_time.into_time()..max_time {
        let mut new_nodes_state: HashMap<NodeView<WindowedGraph<G>>, (u8, i64)> = HashMap::new();
        for v in graph.before(cur_time+1).nodes().iter() {
            println!("Checking {:?} at t){:?}", v.name(), cur_time);
            state_transition_strategy(
                &mut prev_nodes_state,
                &mut new_nodes_state,
                &mut rng,
                &v,
                rates,
                initial_infection_time,
                is_alt,
            );
        }
        prev_nodes_state.extend(new_nodes_state);
    }
    Ok(prev_nodes_state
        .iter()
        .map(|(k, v)| (k.clone(), v.0))
        .collect())
}

/// Simulates the SI (Susceptible-Infected) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 1 - Infected).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn si_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64, // beta
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model(
        graph,
        initial_seed_set,
        initial_infection_time,
        INFECTIOUS,
        (infection_rate, 0.0f64, 0.0f64, 0.0f64),
        seed,
        time_hops,
        si_sis_strategy,
        false,
    )
}

/// Simulates the SIS (Susceptible-Infected-Susceptible) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_seed_set` - The initial seed status for initial infection, can be either an f64 (to allow random nodes to begin infected) or a Vec of nodes that should start as infected.
/// * `initial_infection_time` - The initial time for infection to start at
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of recovery from an infected state to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `time_hops` - An optional int for the amount of times to advance through the graph for infection, default 1. The graph advance forward from infection time by this amount
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<NodeView<G>, u8>)` - A hashmap of nodes with their state (0 - Susceptible, 1 - Infected).
/// * `Err(&'static str)` - An error message in case of failure.
pub fn sis_model<G, T, V, W>(
    graph: &G,
    initial_seed_set: W,
    initial_infection_time: T,
    infection_rate: f64, // beta
    recovery_rate: f64,  // Y
    seed: Option<[u8; 32]>,
    time_hops: Option<i64>,
) -> Result<HashMap<NodeView<WindowedGraph<G>>, u8>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: Into<NodeRef> + Copy,
    W: Into<SeedSet<V>>,
{
    unified_model::<G, T, V, W, _>(
        graph,
        initial_seed_set,
        initial_infection_time,
        INFECTIOUS,
        (infection_rate, recovery_rate, 0.0f64, 0.0f64),
        seed,
        time_hops,
        si_sis_strategy,
        true,
    )
}

fn si_sis_strategy<G, T>(
    prev_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    new_nodes_state: &mut HashMap<NodeView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &NodeView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, _, _): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_sis: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_nodes_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            &prev_nodes_state,
            new_nodes_state,
            v,
            INFECTIOUS,
        ),
        INFECTIOUS => {
            if is_sis {
                change_state_by_prob(
                    recovery_rate,
                    rng,
                    prev_nodes_state,
                    new_nodes_state,
                    v,
                    SUSCEPTIBLE,
                    initial_infection_time,
                )
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod si_tests {
    use super::*;
    use crate::prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS};

    fn gen_graph() -> Graph {
        let graph: Graph = Graph::new();
        let edges = vec![
            (3, "A", "B"),
            (2, "A", "C"),
            (4, "B", "E"),
            (2, "B", "D"),
            (1, "E", "D"),
            (5, "E", "G"),
            (5, "D", "G"),
            (3, "C", "F"),
            (4, "F", "G"),
        ];
        for (ts, src, dst) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn si_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = si_model(&graph, vec!["A"], 2, 1.00f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>>, u8> = HashMap::from([
            (g_after.node("A").unwrap(), INFECTIOUS),
            (g_after.node("B").unwrap(), INFECTIOUS),
            (g_after.node("C").unwrap(), SUSCEPTIBLE),
            (g_after.node("D").unwrap(), SUSCEPTIBLE),
            (g_after.node("E").unwrap(), INFECTIOUS),
            (g_after.node("F").unwrap(), SUSCEPTIBLE),
            (g_after.node("G").unwrap(), INFECTIOUS),
        ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn sis_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sis_model(&graph, vec!["A"], 2, 1.00f64, 0.5f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>>, u8> = HashMap::from([
            (g_after.node("A").unwrap(), INFECTIOUS),
            (g_after.node("B").unwrap(), INFECTIOUS),
            (g_after.node("C").unwrap(), SUSCEPTIBLE),
            (g_after.node("D").unwrap(), SUSCEPTIBLE),
            (g_after.node("E").unwrap(), INFECTIOUS),
            (g_after.node("F").unwrap(), SUSCEPTIBLE),
            (g_after.node("G").unwrap(), INFECTIOUS),
        ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn sir_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sir_model(&graph, vec!["A"], 0, 1.0f64, 0.5f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>, WindowedGraph<Graph>>, u8> =
            HashMap::from([
                (g_after.node("A").unwrap(), RECOVERED),
                (g_after.node("B").unwrap(), INFECTIOUS),
                (g_after.node("C").unwrap(), RECOVERED),
                (g_after.node("D").unwrap(), SUSCEPTIBLE),
                (g_after.node("E").unwrap(), RECOVERED),
                (g_after.node("F").unwrap(), INFECTIOUS),
                (g_after.node("G").unwrap(), INFECTIOUS),
            ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn sirs_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result =
            sirs_model(&graph, vec!["A"], 0, 1.0f64, 0.5f64, 0.3f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>, WindowedGraph<Graph>>, u8> =
            HashMap::from([
                (g_after.node("A").unwrap(), RECOVERED),
                (g_after.node("B").unwrap(), INFECTIOUS),
                (g_after.node("C").unwrap(), SUSCEPTIBLE),
                (g_after.node("D").unwrap(), SUSCEPTIBLE),
                (g_after.node("E").unwrap(), INFECTIOUS),
                (g_after.node("F").unwrap(), INFECTIOUS),
                (g_after.node("G").unwrap(), INFECTIOUS),
            ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn seir_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = seir_model(
            &graph,
            vec!["A"],
            2,
            1.0f64,
            1.0f64,
            0.5f64,
            seed,
            None,
            EXPOSED,
        )
        .unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>, WindowedGraph<Graph>>, u8> =
            HashMap::from([
                (g_after.node("A").unwrap(), RECOVERED),
                (g_after.node("B").unwrap(), INFECTIOUS),
                (g_after.node("C").unwrap(), SUSCEPTIBLE),
                (g_after.node("D").unwrap(), SUSCEPTIBLE),
                (g_after.node("E").unwrap(), INFECTIOUS),
                (g_after.node("F").unwrap(), SUSCEPTIBLE),
                (g_after.node("G").unwrap(), EXPOSED),
            ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn seirs_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = seirs_model(
            &graph,
            vec!["A"],
            0,
            1.0f64,
            1.0f64,
            0.5f64,
            0.4f64,
            seed,
            None,
            EXPOSED,
        )
        .unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<NodeView<WindowedGraph<Graph>, WindowedGraph<Graph>>, u8> =
            HashMap::from([
                (g_after.node("A").unwrap(), RECOVERED),
                (g_after.node("B").unwrap(), INFECTIOUS),
                (g_after.node("C").unwrap(), RECOVERED),
                (g_after.node("D").unwrap(), SUSCEPTIBLE),
                (g_after.node("E").unwrap(), INFECTIOUS),
                (g_after.node("F").unwrap(), INFECTIOUS),
                (g_after.node("G").unwrap(), EXPOSED),
            ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }
}
