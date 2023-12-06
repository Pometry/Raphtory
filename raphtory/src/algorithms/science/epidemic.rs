use crate::{
    db::{api::view::StaticGraphViewOps, graph::vertex::VertexView},
    prelude::VertexViewOps,
};
use rand::{
    prelude::{SliceRandom, StdRng},
    Rng, SeedableRng,
};
use std::collections::{HashMap, HashSet};

const SUSCEPTIBLE: u8 = 0u8;
const INFECTIOUS: u8 = 1u8;
const RECOVERED: u8 = 2u8;
const EXPOSED: u8 = 3u8;
const BETA: &str = "b";
const GAMMA: &str = "g";
const SIGMA: &str = "0";
const XI: &str = "X";

fn setup_si<G>(
    graph: &G,
    initial_infected_ratio: f64,
    seed: Option<[u8; 32]>,
) -> (StdRng, HashMap<VertexView<G, G>, u8>)
where
    G: StaticGraphViewOps,
{
    let mut rng: StdRng = SeedableRng::from_seed(seed.unwrap_or_default());
    let infected_count = graph.count_vertices() as f64 * initial_infected_ratio;
    let mut population: Vec<VertexView<G>> = graph.vertices().iter().collect();
    // Infect initial number of infected nodes
    population.shuffle(&mut rng);
    let mut all_vertices_status: HashMap<VertexView<G>, u8> = population
        .clone()
        .into_iter()
        .take(infected_count as usize)
        .map(|v| (v.clone(), SUSCEPTIBLE))
        .collect();
    (rng, all_vertices_status)
}

fn change_state_by_prob<G>(
    recovery_rate: f64,
    rng: &mut StdRng,
    all_vertices_status: &mut HashMap<VertexView<G, G>, u8>,
    v: VertexView<G, G>,
    new_state: u8,
) where
    G: StaticGraphViewOps,
{
    if rng.gen::<f64>() < recovery_rate {
        let _ = all_vertices_status.insert(v.clone(), new_state);
    }
}

fn change_state_by_neighbors<G>(
    transition_probability: f64,
    rng: &mut StdRng,
    all_vertices_status: &mut HashMap<VertexView<G, G>, u8>,
    v: VertexView<G, G>,
    new_state: u8,
) where
    G: StaticGraphViewOps,
{
    for neighbour in v.neighbours() {
        if (all_vertices_status.get(&neighbour).unwrap_or(&SUSCEPTIBLE) == &SUSCEPTIBLE)
            & (rng.gen::<f64>() < transition_probability)
        {
            let _ = all_vertices_status.insert(neighbour, new_state);
            break;
        }
    }
}

fn get_node_state<G>(
    all_vertices_status: &mut HashMap<VertexView<G, G>, u8>,
    v: &VertexView<G, G>,
) -> u8
where
    G: StaticGraphViewOps,
{
    let v_status = match all_vertices_status.get(&v) {
        None => {
            all_vertices_status.insert(v.clone(), SUSCEPTIBLE);
            SUSCEPTIBLE
        }
        Some(&v_status) => v_status,
    };
    v_status
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
pub fn si_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G, G>, u8>, &'static str>
where
    G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            match get_node_state(&mut all_vertices_status, &v) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
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
/// * `recovery_rate` - The probability of recovery from an infected state to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashMap<VertexView<G>, u8>)` - A hashmap of vertices with their SIS state (0 - Susceptible, 1 - Infected).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sis_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64, // beta
    recovery_rate: f64,  // Y
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
where
    G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            match get_node_state(&mut all_vertices_status, &v) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
                // infected
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    SUSCEPTIBLE,
                ),
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
pub fn sir_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
where
    G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);

    // per step
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v) {
                // susceptible
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
                // infected
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                ),
                // recovered
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
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
pub fn sirs_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
    where
        G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);
    // per step
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                ),
                RECOVERED => change_state_by_prob(
                    rec_to_sus_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    SUSCEPTIBLE,
                ),
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
pub fn seir_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
    where
        G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);
    // per step
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    EXPOSED,
                ),
                EXPOSED => change_state_by_prob(
                    exposure_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                ),
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
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
pub fn seirs_model<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_rate: f64,
    exposure_rate: f64,
    recovery_rate: f64,
    rec_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
    where
        G: StaticGraphViewOps,
{
    let (mut rng, mut all_vertices_status) = setup_si(graph, initial_infected_ratio, seed);
    // per step
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            // if they are susceptible, then check their neighbours
            match get_node_state(&mut all_vertices_status, &v) {
                SUSCEPTIBLE => change_state_by_neighbors(
                    infection_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    EXPOSED,
                ),
                EXPOSED => change_state_by_prob(
                    exposure_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    INFECTIOUS,
                ),
                INFECTIOUS => change_state_by_prob(
                    recovery_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    RECOVERED,
                ),
                RECOVERED => change_state_by_prob(
                    rec_to_sus_rate,
                    &mut rng,
                    &mut all_vertices_status,
                    v,
                    SUSCEPTIBLE,
                ),
                _ => {}
            }
        }
    }
    Ok(all_vertices_status)
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
        let result = si_model(&graph, 0.2f64, 0.2f64, seed, None).unwrap();
        let expected = HashMap::from([
            (graph.vertex("B1").unwrap(), INFECTIOUS),
            (graph.vertex("B3").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R1").unwrap(), SUSCEPTIBLE),
            (graph.vertex("B5").unwrap(), INFECTIOUS),
            (graph.vertex("B4").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R2").unwrap(), INFECTIOUS),
            (graph.vertex("B2").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R3").unwrap(), INFECTIOUS),
            (graph.vertex("G").unwrap(), SUSCEPTIBLE),
        ]);
        assert_eq!(expected, result);
    }

    #[test]
    fn sis_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sis_model(&graph, 0.2f64, 0.2f64, 0.2f64, seed, None).unwrap();
        let expected = HashMap::from([
            (graph.vertex("B1").unwrap(), SUSCEPTIBLE),
            (graph.vertex("B3").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R1").unwrap(), SUSCEPTIBLE),
            (graph.vertex("B5").unwrap(), INFECTIOUS),
            (graph.vertex("B4").unwrap(), INFECTIOUS),
            (graph.vertex("R2").unwrap(), INFECTIOUS),
            (graph.vertex("B2").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R3").unwrap(), SUSCEPTIBLE),
            (graph.vertex("G").unwrap(), INFECTIOUS),
        ]);
        assert_eq!(expected, result);
    }

    #[test]
    fn sir_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sir_model(&graph, 0.3f64, 0.2f64, 0.5f64, seed, None).unwrap();
        let expected: HashMap<VertexView<Graph>, u8> = HashMap::from([
            (graph.vertex("B1").unwrap(), SUSCEPTIBLE),
            (graph.vertex("B3").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R1").unwrap(), SUSCEPTIBLE),
            (graph.vertex("B5").unwrap(), INFECTIOUS),
            (graph.vertex("B4").unwrap(), RECOVERED),
            (graph.vertex("R2").unwrap(), INFECTIOUS),
            (graph.vertex("B2").unwrap(), SUSCEPTIBLE),
            (graph.vertex("R3").unwrap(), SUSCEPTIBLE),
            (graph.vertex("G").unwrap(), RECOVERED),
        ]);
        assert_eq!(expected, result);
    }
}

// for (k, v) in result.iter() {
//     println!("{:?} {:?}", k.name(), v)
// }
