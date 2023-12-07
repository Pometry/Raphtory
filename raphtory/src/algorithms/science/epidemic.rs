use crate::{
    core::utils::time::IntoTime,
    db::{
        api::view::StaticGraphViewOps,
        graph::{vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::{GraphViewOps, TimeOps, VertexViewOps},
};
use rand::{
    prelude::{SliceRandom, StdRng},
    Rng, SeedableRng,
};
use std::collections::HashMap;
use crate::prelude::EdgeViewOps;

const SUSCEPTIBLE: u8 = 0u8;
const INFECTIOUS: u8 = 1u8;
const RECOVERED: u8 = 2u8;
const EXPOSED: u8 = 3u8;
const BETA: &str = "b";
const GAMMA: &str = "g";
const SIGMA: &str = "0";
const XI: &str = "X";

fn setup_si<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    seed: Option<[u8; 32]>,
) -> (StdRng, HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>)
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    let mut rng: StdRng = SeedableRng::from_seed(seed.unwrap_or_default());
    let infected_count =
        graph.after(initial_infection_time).count_vertices() as f64 * initial_infected_ratio;
    let mut population: Vec<VertexView<WindowedGraph<G>>> = graph
        .after(initial_infection_time.into_time().clone())
        .vertices()
        .iter()
        .collect();
    // Infect initial number of infected nodes
    population.shuffle(&mut rng);
    let mut all_vertices_status: HashMap<VertexView<WindowedGraph<G>>, (u8, i64)> = population
        .clone()
        .into_iter()
        .take(infected_count as usize)
        .map(|v| (v.clone(), (SUSCEPTIBLE, initial_infection_time.into_time())))
        .collect();
    (rng, all_vertices_status)
}

fn change_state_by_prob<G, T>(
    recovery_rate: f64,
    rng: &mut StdRng,
    all_vertices_status: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: VertexView<WindowedGraph<G>>,
    new_state: u8,
    infection_time: T
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    if rng.gen::<f64>() < recovery_rate {
        let _ = all_vertices_status.insert(v.clone(), (new_state, infection_time.into_time()));
    }
}

fn change_state_by_neighbors<G, T>(
    transition_probability: f64,
    rng: &mut StdRng,
    all_vertices_status: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: VertexView<WindowedGraph<G>>,
    new_state: u8,
    infection_time: T,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    for edgeview in v.edges() {
        let neighbour = if edgeview.src() != v {
            edgeview.src()
        }  else {
            edgeview.dst()
        };
        let neighbour_status = all_vertices_status.get(&neighbour).unwrap_or(&(SUSCEPTIBLE, infection_time.into_time()));
        if (neighbour_status.0 == new_state) // if the neighbour has a different state
            & (neighbour_status.1 <= edgeview.latest_time().unwrap()) // if the neighbour was infected before the interaction, it means they can infect again
            & (rng.gen::<f64>() < transition_probability) // then roll the dice and infect them
        {
            let _ = all_vertices_status.insert(neighbour, (new_state, infection_time.into_time()));
            break;
        }
    }
}

fn get_node_state<G, T>(
    all_vertices_status: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: &VertexView<WindowedGraph<G>, WindowedGraph<G>>,
    initial_infection_time: T,
) -> u8
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match all_vertices_status.get(&v) {
        // None => {
        // }
        // Some(&v_status) => v_status,
        None => {
                all_vertices_status.insert(v.clone(), (SUSCEPTIBLE, initial_infection_time.into_time()));
                SUSCEPTIBLE
        }
        Some((v_status, infection_time)) => {
            *v_status
        }
    }
}

fn si_sis_model<G: StaticGraphViewOps, T: IntoTime + Copy>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64, // beta
    recovery_rate: f64,  // Y
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
    is_sis: bool,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str> {
    let (mut rng, mut all_vertices_status) =
        setup_si(graph, initial_infection_time, initial_infected_ratio, seed);
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph
            .after(initial_infection_time.into_time().clone())
            .vertices()
            .iter()
        {
            match get_node_state(&mut all_vertices_status, &v, initial_infection_time) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                    initial_infection_time,
                ),
                // infected
                INFECTIOUS => {
                    if is_sis {
                        change_state_by_prob(
                            recovery_rate,
                            &mut rng,
                            &mut all_vertices_status,
                            v,
                            SUSCEPTIBLE,
                            initial_infection_time,
                        )
                    }
                }
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
}

/// Simulates the SI (Susceptible-Infected) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SI state (0 - Susceptible, 1 - Infected).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn si_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    si_sis_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        0.0f64,
        seed,
        steps,
        false,
    )
}

/// Simulates the SIR (Susceptible-Infected-Recovery) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of recovery from an infected state to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIS state (0 - Susceptible, 1 - Infected).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sis_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64, // beta
    recovery_rate: f64,  // Y
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    si_sis_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        recovery_rate,
        seed,
        steps,
        true,
    )
}

fn sir_sirs_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
    is_sirs: bool,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    let (mut rng, mut all_vertices_status) =
        setup_si(graph, initial_infection_time, initial_infected_ratio, seed);
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph
            .after(initial_infection_time.into_time().clone())
            .vertices()
            .iter()
        {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v, initial_infection_time) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                    initial_infection_time,
                ),
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                    initial_infection_time,
                ),
                RECOVERED => {
                    if is_sirs {
                        change_state_by_prob(
                            rec_to_sus_rate,
                            &mut rng,
                            &mut all_vertices_status,
                            v,
                            SUSCEPTIBLE,
                            initial_infection_time,
                        )
                    }
                }
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
}

/// Simulates the SIR (Susceptible-Infected-Recovery) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of an infected node recovering.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIR state (0 - Susceptible, 1 - Infected, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sir_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    sir_sirs_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        recovery_rate,
        0.0f64,
        seed,
        steps,
        false,
    )
}

/// Simulates the SIRS (Susceptible-Infected-Recovery-Susceptible ) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of an infected node recovering, going from infection to recovered
/// * `rec_to_sus_rate` - The probability of an recovered node going back to susceptible.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIRS state (0 - Susceptible, 1 - Infected, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sirs_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    sir_sirs_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        recovery_rate,
        rec_to_sus_rate,
        seed,
        steps,
        true,
    )
}

fn seir_seirs_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
    is_seirs: bool,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    let (mut rng, mut all_vertices_status) =
        setup_si(graph, initial_infection_time, initial_infected_ratio, seed);
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph
            .after(initial_infection_time.into_time().clone())
            .vertices()
            .iter()
        {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v, initial_infection_time) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    EXPOSED,
                    initial_infection_time,
                ),
                EXPOSED => change_state_by_prob(
                    exposure_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                    initial_infection_time,
                ),
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                    initial_infection_time,
                ),
                RECOVERED => {
                    if is_seirs {
                        change_state_by_prob(
                            rec_to_sus_rate,
                            &mut rng,
                            &mut all_vertices_status,
                            v,
                            SUSCEPTIBLE,
                            initial_infection_time,
                        )
                    }
                }
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
}

/// Simulates the SEIR (Susceptible-Exposed-Infectious-Recovered ) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from Susceptible to Exposed.
/// * `exposure_rate` - The probability of a node moving from Exposed to Infectious.
/// * `recovery_rate` - The probability of an infected node recovering, going from Infectious to recovered
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIR state (0 - Susceptible, 3 - Exposed, 2 - Infectious, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn seir_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    seir_seirs_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        exposure_rate,
        recovery_rate,
        0.0f64,
        seed,
        steps,
        false,
    )
}

/// Simulates the SEIRS (Susceptible-Exposed-Infectious-Recovered-Susceptible) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_rate` - The probability of infection spreading from Susceptible to Exposed.
/// * `exposure_rate` - The probability of a node moving from Exposed to Infectious.
/// * `recovery_rate` - The probability of an infected node recovering, going from Infectious to recovered
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIR state (0 - Susceptible, 3 - Exposed, 2 - Infectious, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn seirs_model<G, T>(
    graph: &G,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    seir_seirs_model(
        graph,
        initial_infection_time,
        initial_infected_ratio,
        infection_rate,
        exposure_rate,
        recovery_rate,
        rec_to_sus_rate,
        seed,
        steps,
        true,
    )
}

#[cfg(test)]
mod si_tests {
    use super::*;
    use crate::prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS};

    fn gen_graph() -> Graph {
        let graph: Graph = Graph::new();
        let edges = vec![
            (1, "R1", "R2"),
            (1, "R2", "R3"),
            (1, "R3", "G"),
            (1, "G", "B1"),
            (1, "G", "B3"),
            (1, "B1", "B2"),
            (1, "B2", "B3"),
            (1, "B2", "B4"),
            (1, "B3", "B4"),
            (1, "B3", "B5"),
            (1, "B4", "B5"),
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
        let result = si_model(&graph, 0, 0.2f64, 0.2f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected = HashMap::from([
            (g_after.vertex("B1").unwrap(), INFECTIOUS),
            (g_after.vertex("B3").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R1").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("B5").unwrap(), INFECTIOUS),
            (g_after.vertex("B4").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R2").unwrap(), INFECTIOUS),
            (g_after.vertex("B2").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R3").unwrap(), INFECTIOUS),
            (g_after.vertex("G").unwrap(), SUSCEPTIBLE),
        ]);
        assert_eq!(expected, result);
    }

    #[test]
    fn sis_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sis_model(&graph, 0, 0.2f64, 0.2f64, 0.2f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected = HashMap::from([
            (g_after.vertex("B1").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("B3").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R1").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("B5").unwrap(), INFECTIOUS),
            (g_after.vertex("B4").unwrap(), INFECTIOUS),
            (g_after.vertex("R2").unwrap(), INFECTIOUS),
            (g_after.vertex("B2").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R3").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("G").unwrap(), INFECTIOUS),
        ]);
        assert_eq!(expected, result);
    }

    #[test]
    fn sir_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sir_model(&graph, 0, 0.3f64, 0.2f64, 0.5f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<VertexView<WindowedGraph<Graph>>, u8> = HashMap::from([
            (g_after.vertex("B1").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("B3").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R1").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("B5").unwrap(), INFECTIOUS),
            (g_after.vertex("B4").unwrap(), RECOVERED),
            (g_after.vertex("R2").unwrap(), INFECTIOUS),
            (g_after.vertex("B2").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("R3").unwrap(), SUSCEPTIBLE),
            (g_after.vertex("G").unwrap(), RECOVERED),
        ]);
        assert_eq!(expected, result);
    }
}

// for (k, v) in result.iter() {
//     println!("{:?} {:?}", k.name(), v)
// }
