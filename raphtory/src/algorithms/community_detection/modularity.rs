use crate::{db::graph::vertex::VertexView, prelude::*};
use std::collections::HashSet;

pub fn modularity<G>(
    graph: &G,
    communities: &Vec<HashSet<u64>>,
    weight: Option<&str>,
    resolution: f64,
    is_directed: bool,
) -> f64
where
    G: GraphViewOps,
{
    if !is_partition(graph, &communities) {
        println!("Not a partition");
        return 0.0f64;
    }

    let norm: f64;
    let m: f64;

    if is_directed {
        let out_degree_weight_sum: f64 = graph
            .vertices()
            .iter()
            .map(|v| degree_sum(&v, weight, Direction::Out))
            .sum::<f64>();
        // let in_degree_weight_sum: f64 = graph
        //     .vertices()
        //     .iter()
        //     .map(|v| degree_sum(&v, weight, Direction::In))
        //     .sum::<f64>();
        m = out_degree_weight_sum;
        norm = 1.0 / out_degree_weight_sum.powf(2.0);
    } else {
        let degree_weight_sum: f64 = graph
            .vertices()
            .iter()
            .map(|v| degree_sum(&v, weight, Direction::Both))
            .sum::<f64>();
        m = degree_weight_sum / 2.0;
        norm = 1.0 / degree_weight_sum.powf(2.0);
    }
    communities
        .iter()
        .map(|comm| community_contribution(comm, graph, is_directed, weight, m, norm, resolution))
        .sum::<f64>()
}

fn is_partition<G>(graph: &G, communities: &Vec<HashSet<u64>>) -> bool
where
    G: GraphViewOps,
{
    let nodes: HashSet<u64> = communities
        .iter()
        .flat_map(|community| community.iter().filter(|&node| graph.has_vertex(*node)))
        .cloned()
        .collect();

    let total_nodes: usize = graph.count_vertices();
    let sum_communities: usize = communities.iter().map(|c| c.len()).sum();

    total_nodes == nodes.len() && nodes.len() == sum_communities
}

enum Direction {
    In,
    Out,
    Both,
}

fn degree_sum<G>(v: &VertexView<G>, weight: Option<&str>, direction: Direction) -> f64
where
    G: GraphViewOps,
{
    let weight_key = weight.unwrap_or("");
    match direction {
        Direction::In => v
            .in_edges()
            .map(|e| {
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(1.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
        Direction::Out => v
            .out_edges()
            .map(|e| {
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(1.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
        Direction::Both => v
            .edges()
            .map(|e| {
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(1.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
    }
}

pub fn community_contribution<G>(
    community: &HashSet<u64>,
    graph: &G,
    directed: bool,
    weight: Option<&str>,
    m: f64,
    norm: f64,
    resolution: f64,
) -> f64
where
    G: GraphViewOps,
{
    let weight_key = weight.unwrap_or("");
    let comm: HashSet<VertexView<G>> = community
        .iter()
        .map(|vid| graph.vertex(*vid).unwrap())
        .collect();
    let l_c: f64 = comm
        .iter()
        .flat_map(|v| v.out_edges())
        .filter_map(|e| match comm.contains(&e.dst()) {
            true => Some(
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(1.0f64))
                    .unwrap_f64(),
            ),
            false => Some(Prop::F64(0.0f64).unwrap_f64()),
        })
        .sum::<f64>();
    let out_degree_sum: f64;
    let in_degree_sum: f64;
    if directed {
        out_degree_sum = comm
            .iter()
            .map(|v| degree_sum(v, weight, Direction::Out))
            .sum();
        in_degree_sum = comm
            .iter()
            .map(|v| degree_sum(v, weight, Direction::In))
            .sum();
    } else {
        in_degree_sum = comm
            .iter()
            .map(|v| degree_sum(v, weight, Direction::Both))
            .sum();
        out_degree_sum = in_degree_sum.clone();
    }
    l_c / m - resolution * out_degree_sum * in_degree_sum * norm
}

#[cfg(test)]
mod modularity_test {
    use super::*;

    #[test]
    fn test_modularity() {
        let graph = Graph::new();
        let edges = vec![
            (1, "1", "2", 2.0f64),
            (1, "1", "3", 3.0f64),
            (1, "2", "3", 8.5f64),
            (1, "3", "4", 1.0f64),
            (1, "4", "5", 1.5f64),
        ];
        for (ts, src, dst, wt) in edges {
            graph
                .add_edge(ts, src, dst, [("weight", wt)], None)
                .unwrap();
        }
        let communities = vec![
            HashSet::from([
                graph.vertex("1").unwrap().id(),
                graph.vertex("2").unwrap().id(),
                graph.vertex("3").unwrap().id(),
            ]),
            HashSet::from([
                graph.vertex("4").unwrap().id(),
                graph.vertex("5").unwrap().id(),
            ]),
        ];
        let results = modularity(
            &graph,
            &communities,
            None,
            1.0f64,
            false,
        );
        assert_eq!(results, 0.22f64);

        let results = modularity(
            &graph,
            &communities,
            None,
            1.0f64,
            true,
        );
        assert_eq!(results, 0.24f64);

        let results = modularity(
            &graph,
            &communities,
            Some("weight"),
            1.0f64,
            false,
        );
        assert_eq!(results, 0.15625f64);

        let results = modularity(
            &graph,
            &communities,
            Some("weight"),
            1.0f64,
            true,
        );
        assert_eq!(results, 0.158203125f64)
    }
}
