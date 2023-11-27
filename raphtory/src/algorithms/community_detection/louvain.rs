use std::collections::HashSet;
use crate::prelude::*;
use crate::db::graph::vertex::VertexView;


pub fn modularity<G>(
    graph: &G,
    communities: Vec<Vec<VertexView<G>>>,
    weight: &str,
    resolution: u64,
    is_directed: bool
) -> f64
where
    G: GraphViewOps
{
    if is_partition(graph, &communities) {
        println!("Not a partition");
        return 0.0f64;
    }

    let mut norm = 0.0;
    let mut m = 0.0;

    if is_directed {
        let out_degree_weight_sum: f64 = graph.vertices().iter().map(|v|
            degree_sum(&v, weight, Direction::Out)
        ).sum::<f64>();
        let in_degree_weight_sum: f64 = graph.vertices().iter().map(|v|
            degree_sum(&v, weight, Direction::In)
        ).sum::<f64>();
        m = out_degree_weight_sum;
        norm = 1.0 / out_degree_weight_sum.powf(2.0);
    } else {
        let degree_weight_sum: f64 = graph.vertices().iter().map(|v|
            degree_sum(&v, weight, Direction::Both)
        ).sum::<f64>();
        m = degree_weight_sum / 2.0;
        norm = 1.0 / degree_weight_sum.powf(2.0);
    }
    communities.iter().map(|comm|
        community_contribution(comm,
                               graph,
                               is_directed,
                               weight,
                               m,
                               norm,
                               resolution
        )
    ).sum::<f64>()
}

fn is_partition<G>(graph: &G, communities: &Vec<Vec<VertexView<G>>>) -> bool
where
    G: GraphViewOps
{
    let nodes: HashSet<_> = communities
        .iter()
        .flat_map(|community| community.iter().filter(|&node| graph.has_vertex(node)))
        .cloned()
        .collect();

    let total_nodes = graph.count_vertices();
    let sum_communities: usize = communities.iter().map(|c| c.len()).sum();

    total_nodes == nodes.len() && nodes.len() == sum_communities
}

enum Direction {
    In,
    Out,
    Both,
}

fn degree_sum<G>(v: &VertexView<G>, weight_key: &str, direction: Direction) -> f64
where
    G: GraphViewOps
{
    match direction {
        Direction::In => {
            v.in_edges()
                .map(|e|
                    e.properties().get(weight_key).unwrap_or(Prop::F64(0.0f64)).unwrap_f64()
                )
                .sum::<f64>()
        }
        Direction::Out => {
            v.out_edges()
                .map(|e|
                    e.properties().get(weight_key).unwrap_or(Prop::F64(0.0f64)).unwrap_f64()
                )
                .sum::<f64>()
        }
        Direction::Both => {
            v.edges()
                .map(|e|
                    e.properties().get(weight_key).unwrap_or(Prop::F64(0.0f64)).unwrap_f64()
                )
                .sum::<f64>()
        }
    }
}

pub fn community_contribution<G>(
    community: &Vec<VertexView<G>>,
    graph: &G,
    directed: bool,
    weight: &str,
    m: f64,
    norm: f64,
    resolution: u64
) -> f64
where
    G: GraphViewOps,
{
    let comm: HashSet<VertexView<G>> = community.clone().into_iter().collect();
    let l_c: f64 = graph.edges().filter_map(|e|
        match comm.contains(&e.dst()) {
            true =>  Some(e.properties().get(weight).unwrap_or(Prop::F64(1.0f64)).unwrap_f64()),
            false => Some(Prop::F64(1.0f64).unwrap_f64())
        }
    ).sum();
    let out_degree_sum: f64 = comm.iter().map(|v|
        degree_sum(v, weight, Direction::Out)
    ).sum();
    let in_degree_sum: f64 = if directed {
        comm.iter().map(|v|
                degree_sum(v, weight, Direction::In)
        ).sum()
    }
    else {
        out_degree_sum
    };
     l_c / m - (resolution as f64) * out_degree_sum * in_degree_sum * norm
}

#[cfg(test)]
mod louvain_test {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn test_community_contribution() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1, 2.0),
            (1, 3, 2, 3.0),
            (1, 4, 3, 1.0),
            (1, 5, 4, 2.0),
            (1, 5, 5, 1.0),
            (1, 8, 6, 2.0),
            (1, 7, 7, 3.0),
        ];
        for (src, dst, ts, wt) in edges {
            graph.add_edge(ts, src, dst, [("weight", wt)], None).unwrap();
        }

        let results = community_contribution(
            &vec![graph.vertex(2).unwrap(), graph.vertex(4).unwrap()],
            &graph,
            true,
            "weight",
            1.0,
            1.0,
            1
        );

        print!("{:?}", results)

    }

}