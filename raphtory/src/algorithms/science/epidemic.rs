use crate::{
    core::{
        entities::vertices::{input_vertex::InputVertex, vertex_ref::VertexRef},
        utils::time::IntoTime,
    },
    db::{
        api::view::StaticGraphViewOps,
        graph::{vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::{EdgeViewOps, GraphViewOps, TimeOps, VertexViewOps},
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

fn setup_si<G, V>(
    graph: &G,
    initial_infected_ratio: f64,
    initial_infected_nodes: Option<HashSet<V>>,
    seed: Option<[u8; 32]>,
    infected_state: u8,
) -> (StdRng, HashMap<VertexView<G>, (u8, i64)>)
where
    G: StaticGraphViewOps,
    V: InputVertex,
{
    let mut rng: StdRng = SeedableRng::from_seed(seed.unwrap_or_default());
    match initial_infected_nodes {
        None => {
            let infected_count = graph.count_vertices() as f64 * initial_infected_ratio;
            let mut population: Vec<VertexView<G>> = graph.vertices().iter().collect();
            // Infect initial number of infected nodes
            population.shuffle(&mut rng);
            let mut prev_vertices_status: HashMap<VertexView<G>, (u8, i64)> = population
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
            (rng, prev_vertices_status)
        }
        Some(initial_infected) => {
            let prev_vertices_status: HashMap<VertexView<G>, (u8, i64)> = initial_infected
                .iter()
                .map(|v| {
                    (
                        graph.vertex(v.clone()).unwrap(),
                        (infected_state, graph.start().unwrap_or(-1i64) + 1i64),
                    )
                })
                .collect();
            (rng, prev_vertices_status)
        }
    }
}

fn change_state_by_prob<G, T>(
    recovery_rate: f64,
    rng: &mut StdRng,
    new_vertices_status: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: &VertexView<WindowedGraph<G>>,
    new_state: u8,
    infection_time: T,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    if rng.gen::<f64>() < recovery_rate {
        let _ = new_vertices_status.insert(v.clone(), (new_state, infection_time.into_time()));
    }
}

fn change_state_by_neighbors<G>(
    transition_probability: f64,
    rng: &mut StdRng,
    prev_vertices_status: &HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    new_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: &VertexView<WindowedGraph<G>>,
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
        let neighbour_status = prev_vertices_status
            .get(&neighbour)
            .unwrap_or(&not_infected_state);
        if (neighbour_status.0 == new_state) // if the neighbour is the new (infected) state
            & (neighbour_status.1 <= current_time) // if the neighbour was infected before the interaction, it means they can infect again
            & (rng.gen::<f64>() < transition_probability)
        // then roll the dice and infect them
        {
            let _ = new_vertices_state.insert(v.clone(), (new_state, current_time));
            break;
        }
    }
}

fn get_node_state<G, T>(
    prev_vertices_status: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    v: &VertexView<WindowedGraph<G>, WindowedGraph<G>>,
    initial_infection_time: T,
) -> u8
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match prev_vertices_status.get(&v) {
        None => {
            prev_vertices_status
                .insert(v.clone(), (SUSCEPTIBLE, initial_infection_time.into_time()));
            SUSCEPTIBLE
        }
        Some((v_status, infection_time)) => *v_status,
    }
}

fn sir_sirs_strategy<G, T>(
    prev_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    new_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &VertexView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_sirs: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_vertices_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            prev_vertices_state,
            new_vertices_state,
            v,
            INFECTIOUS,
        ),
        INFECTIOUS => change_state_by_prob(
            recovery_rate,
            rng,
            new_vertices_state,
            v,
            RECOVERED,
            initial_infection_time,
        ),
        RECOVERED => {
            if is_sirs {
                change_state_by_prob(
                    recovery_to_sus_rate,
                    rng,
                    new_vertices_state,
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
pub fn sir_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
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
    V: InputVertex,
{
    unified_model::<G, T, V, _>(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (infection_rate, recovery_rate, 0.0f64, 0.0f64),
        seed,
        steps,
        sir_sirs_strategy,
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
pub fn sirs_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64,
    recovery_rate: f64,
    recovery_to_sus_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: InputVertex,
{
    unified_model::<G, T, V, _>(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (infection_rate, recovery_rate, recovery_to_sus_rate, 0.0f64),
        seed,
        steps,
        sir_sirs_strategy,
        true,
    )
}

fn seir_seirs_strategy<G, T>(
    prev_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    new_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &VertexView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_seirs: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_vertices_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            prev_vertices_state,
            new_vertices_state,
            v,
            EXPOSED,
        ),
        EXPOSED => change_state_by_prob(
            exposure_rate,
            rng,
            new_vertices_state,
            v,
            INFECTIOUS,
            initial_infection_time,
        ),
        INFECTIOUS => change_state_by_prob(
            recovery_rate,
            rng,
            new_vertices_state,
            v,
            RECOVERED,
            initial_infection_time,
        ),
        RECOVERED => {
            if is_seirs {
                change_state_by_prob(
                    recovery_to_sus_rate,
                    rng,
                    new_vertices_state,
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
pub fn seir_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
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
    V: InputVertex,
{
    unified_model(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (infection_rate, recovery_rate, 0.0f64, exposure_rate),
        seed,
        steps,
        seir_seirs_strategy,
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
pub fn seirs_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
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
    V: InputVertex,
{
    unified_model(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (
            infection_rate,
            recovery_rate,
            rec_to_sus_rate,
            exposure_rate,
        ),
        seed,
        steps,
        seir_seirs_strategy,
        true,
    )
}

fn unified_model<G, T, V, F>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    initial_infection_state: u8,
    rates: (f64, f64, f64, f64), // infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
    state_transition_strategy: F,
    is_alt: bool,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: InputVertex,
    F: Fn(
        &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
        &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
        &mut StdRng,
        &VertexView<WindowedGraph<G>>,
        (f64, f64, f64, f64),
        T,
        bool,
    ) -> (),
{
    let sat_initial_infection_time = initial_infection_time.into_time().saturating_sub(1);
    let g_after = graph.after(sat_initial_infection_time);
    let (mut rng, mut prev_vertices_state) = setup_si(
        &g_after,
        initial_infected_ratio,
        initial_input_vertices,
        seed,
        initial_infection_state,
    );
    for _ in 0..steps.unwrap_or(1i32) {
        let mut new_vertices_state: HashMap<VertexView<WindowedGraph<G>>, (u8, i64)> =
            HashMap::new();
        for v in g_after.vertices().iter() {
            state_transition_strategy(
                &mut prev_vertices_state,
                &mut new_vertices_state,
                &mut rng,
                &v,
                rates,
                initial_infection_time,
                is_alt,
            );
        }
        prev_vertices_state.extend(new_vertices_state);
    }
    Ok(prev_vertices_state)
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
pub fn si_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
    initial_infection_time: T,
    initial_infected_ratio: f64,
    infection_rate: f64, // beta
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>, &'static str>
where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
    V: InputVertex,
{
    unified_model(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (infection_rate, 0.0f64, 0.0f64, 0.0f64),
        seed,
        steps,
        si_sis_strategy,
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
pub fn sis_model<G, T, V>(
    graph: &G,
    initial_input_vertices: Option<HashSet<V>>,
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
    V: InputVertex,
{
    unified_model::<G, T, V, _>(
        graph,
        initial_input_vertices,
        initial_infection_time,
        initial_infected_ratio,
        INFECTIOUS,
        (infection_rate, recovery_rate, 0.0f64, 0.0f64),
        seed,
        steps,
        si_sis_strategy,
        true,
    )
}

fn si_sis_strategy<G, T>(
    prev_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    new_vertices_state: &mut HashMap<VertexView<WindowedGraph<G>>, (u8, i64)>,
    rng: &mut StdRng,
    v: &VertexView<WindowedGraph<G>>,
    (infection_rate, recovery_rate, recovery_to_sus_rate, exposure_rate): (f64, f64, f64, f64),
    initial_infection_time: T,
    is_sis: bool,
) where
    G: StaticGraphViewOps,
    T: IntoTime + Copy,
{
    match get_node_state(prev_vertices_state, &v, initial_infection_time) {
        SUSCEPTIBLE => change_state_by_neighbors(
            infection_rate,
            rng,
            &prev_vertices_state,
            new_vertices_state,
            v,
            INFECTIOUS,
        ),
        INFECTIOUS => {
            if is_sis {
                change_state_by_prob(
                    recovery_rate,
                    rng,
                    new_vertices_state,
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
    use crate::{
        db,
        prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS},
    };

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
        let result = si_model(&graph, None, 0, 0.3f64, 0.75f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<VertexView<WindowedGraph<Graph>, WindowedGraph<Graph>>, (u8, i64)> =
            HashMap::from([
                (g_after.vertex("B1").unwrap(), (INFECTIOUS, 0)),
                (g_after.vertex("B3").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("R1").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("B5").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("B4").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("R2").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("B2").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("R3").unwrap(), (INFECTIOUS, 0)),
                (g_after.vertex("G").unwrap(), (SUSCEPTIBLE, 0)),
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
        let result = sis_model(&graph, None, 0, 0.2f64, 0.2f64, 0.2f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<VertexView<WindowedGraph<Graph>, WindowedGraph<Graph>>, (u8, i64)> =
            HashMap::from([
                (g_after.vertex("B1").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("B3").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("R1").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("B5").unwrap(), (INFECTIOUS, 0)),
                (g_after.vertex("B4").unwrap(), (INFECTIOUS, 0)),
                (g_after.vertex("R2").unwrap(), (INFECTIOUS, 0)),
                (g_after.vertex("B2").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("R3").unwrap(), (SUSCEPTIBLE, 0)),
                (g_after.vertex("G").unwrap(), (INFECTIOUS, 0)),
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
        let result = sir_model(&graph, None, 0, 0.2f64, 0.2f64, 0.2f64, seed, None).unwrap();
        let g_after = graph.after(0);
        let expected: HashMap<VertexView<WindowedGraph<Graph>, WindowedGraph<Graph>>, (u8, i64)> =
            HashMap::from([
                (g_after.vertex("B1").unwrap(), (SUSCEPTIBLE, 1)),
                (g_after.vertex("B3").unwrap(), (SUSCEPTIBLE, 1)),
                (g_after.vertex("R1").unwrap(), (SUSCEPTIBLE, 1)),
                (g_after.vertex("B5").unwrap(), (INFECTIOUS, 1)),
                (g_after.vertex("B4").unwrap(), (RECOVERED, 1)),
                (g_after.vertex("R2").unwrap(), (INFECTIOUS, 1)),
                (g_after.vertex("B2").unwrap(), (SUSCEPTIBLE, 1)),
                (g_after.vertex("R3").unwrap(), (SUSCEPTIBLE, 1)),
                (g_after.vertex("G").unwrap(), (RECOVERED, 1)),
            ]);
        for (k, v) in result.iter() {
            println!("{:?} {:?}", k.name(), v)
        }
        assert_eq!(expected, result);
    }
}
