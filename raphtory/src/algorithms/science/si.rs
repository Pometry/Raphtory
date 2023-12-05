use crate::{
    db::{api::view::StaticGraphViewOps, graph::vertex::VertexView},
    prelude::*,
};
use rand::{prelude::SliceRandom, rngs::StdRng, Rng, SeedableRng};
use std::collections::{HashMap, HashSet};

/// Simulates the SI (Susceptible-Infected) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_probability` - The probability of infection spreading from an infected node to a susceptible one.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashSet<VertexView<G>>)` - A set of vertices that are infected at the end of the simulation.
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn si<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_probability: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashSet<VertexView<G>>, &'static str>
where
    G: StaticGraphViewOps,
{
    let mut rng: StdRng = SeedableRng::from_seed(seed.unwrap_or_default());
    let infected_count = graph.count_vertices() as f64 * initial_infected_ratio;
    let mut population: Vec<VertexView<G>> = graph.vertices().iter().collect();
    // Infect initial number of infected nodes
    population.shuffle(&mut rng);
    let mut infected: HashSet<VertexView<G>> = population
        .clone()
        .into_iter()
        .take(infected_count as usize)
        .collect();

    // simulate future infections
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            if infected.contains(&v) {
                let neighbours = v.neighbours();
                for neighbour in neighbours.iter() {
                    if !infected.contains(&neighbour) & (rng.gen::<f64>() < infection_probability) {
                        let _ = infected.insert(neighbour);
                    }
                }
            }
        }
    }
    Ok(infected)
}


/// Simulates the SIR (Susceptible-Infected-Recovery) infection model on a graph.
///
/// # Arguments
/// * `graph` - The graph on which to run the simulation.
/// * `initial_infected_ratio` - The initial ratio of infected nodes.
/// * `infection_probability` - The probability of infection spreading from an infected node to a susceptible one.
/// * `recovery_rate` - The probability of an infected node recovering.
/// * `seed` - An optional seed for the random number generator for reproducibility.
/// * `steps` - An optional int for the number of times to iterate through the graph for infection, default 1
///
/// # Returns
/// A `Result` which is either:
/// * `Ok(HashSet<VertexView<G>, u8>)` - A hashmap of vertices with their SIR state (0 - Susceptible, 1 - Infected, 2 - Recovered).
/// * `Err(&'static str)` - An error message in case of failure.
///
pub fn sir<G>(
    graph: &G,
    initial_infected_ratio: f64,
    infection_probability: f64,
    recovery_rate: f64,
    seed: Option<[u8; 32]>,
    steps: Option<i32>,
) -> Result<HashMap<VertexView<G>, u8>, &'static str>
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
        .map(|v| (v.clone(), 1))
        .collect();

    // per step
    for i in 0..steps.unwrap_or(1i32) {
        for v in graph.vertices().iter() {
            // get val of current node
            let v_status = match all_vertices_status.get(&v) {
                None => {
                    all_vertices_status.insert(v.clone(), 0);
                    0
                }
                Some(&v_status) => v_status,
            };
            let prob_event = rng.gen::<f64>();
            // if they are susceptible, then check their neighbours
            match v_status {
                // susceptible
                0 => {
                    // get num of infected neighbours
                    let infected_nbrs = v
                        .neighbours()
                        .iter()
                        .filter_map(|nbr| {
                            if all_vertices_status
                                .get(&nbr)
                                .unwrap_or(&0)
                                == &1
                            {
                                Some(nbr)
                            } else {
                                None
                            }
                        })
                        .count();
                    // check probability, if so then infect
                    if prob_event < (infection_probability * infected_nbrs as f64) {
                        let _ = all_vertices_status.insert(v.clone(), 1);
                    }
                }
                // infected
                1 => {
                    // if infected then they can heal
                    if prob_event < recovery_rate {
                        let _ = all_vertices_status.insert(v.clone(), 2);
                    }
                }
                // recovered
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
    use std::collections::HashSet;

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
        let result = si(&graph, 0.2f64, 0.2f64, seed, None).unwrap();
        let expected = HashSet::from([graph.vertex("G").unwrap(), graph.vertex("B1").unwrap()]);
        assert_eq!(expected, result);
    }

    #[test]
    fn sir_test() {
        let graph = gen_graph();
        let seed = Some([5; 32]);
        let result = sir(&graph, 0.2f64, 0.2f64, 0.2f64, seed, None).unwrap();
        let expected: HashMap<VertexView<Graph>, u8> = HashMap::from([
            (graph.vertex("B1").unwrap(), 1),
            (graph.vertex("B3").unwrap(), 0),
            (graph.vertex("R1").unwrap(), 0),
            (graph.vertex("B5").unwrap(), 0),
            (graph.vertex("B4").unwrap(), 0),
            (graph.vertex("R2").unwrap(), 0),
            (graph.vertex("B2").unwrap(), 0),
            (graph.vertex("R3").unwrap(), 0),
            (graph.vertex("G").unwrap(), 1),
        ]);
        assert_eq!(expected, result);
    }
}
