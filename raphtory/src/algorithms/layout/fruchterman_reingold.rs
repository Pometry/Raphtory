use std::collections::HashMap;
use num_traits::Pow;
use crate::db::api::view::*;
use rand::distributions::{Distribution, Uniform};

fn repulsive_force(
    repulsion: f64,
    k: i64,
    d: f64,
) -> f64 {
  repulsion * k as f64 * k as f64 / d
}

fn attractive_force(
    attraction: f64,
    k: i64,
    d: f64,
) -> f64 {
    d * d / (attraction * k as f64)
}

fn calculate_distance(
    delta: &[f64; 2],
) -> f64 {
    let new_sum: f64 = delta.get(0).unwrap().clone().pow(2) + delta.get(1).unwrap().clone().pow(2);
    new_sum.sqrt()
}

fn limit_position(
    value: f64,
    min_value: f64,
    max_value: f64,
) -> f64 {
    min_value.max(max_value.min(value))
}

pub fn fruchterman_reingold<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    width: f64,
    height: f64,
    repulsion: f64,
    attraction: f64,
) -> HashMap<u64, [f64; 2]> {
    let mut rng = rand::thread_rng();
    let uni_sample = Uniform::from(0f64..1f64);
    let area = width * height;
    let k = (area as f64 / graph.count_nodes() as f64).sqrt() as i64 + 1;
    let mut temperature: f64 = width as f64 / 10.0f64;
    let mut node_pos: HashMap<u64, [f64; 2]> = HashMap::new();
    let mut node_disp: HashMap<u64, [f64; 2]> = HashMap::new();
    for _ in 0..iterations {
        //  Calculate repulsive forces
        for node_v in graph.nodes() {
            let node_v_id = node_v.id();
            if !node_pos.contains_key(&node_v_id) {
                node_disp.insert(node_v_id, [0.0f64, 0.0f64]);
                node_pos.insert(node_v_id, [uni_sample.sample(&mut rng), uni_sample.sample(&mut rng)]);
            }
            for node_u in graph.nodes() {
                if node_v != node_u {
                    let node_u_id = node_u.id();
                    if !node_pos.contains_key(&node_u_id) {
                        node_disp.insert(node_u_id, [0.0f64, 0.0f64]);
                        node_pos.insert(node_u_id, [uni_sample.sample(&mut rng), uni_sample.sample(&mut rng)]);
                    }
                    let delta = [
                        node_pos.get(&node_v_id).unwrap().get(0).unwrap() - node_pos.get(&node_u_id).unwrap().get(0).unwrap(),
                        node_pos.get(&node_v_id).unwrap().get(1).unwrap() - node_pos.get(&node_u_id).unwrap().get(1).unwrap(),
                    ];
                    let distance = calculate_distance(&delta);
                    if distance > 0.0f64 {
                        let repulsive_f = repulsive_force(repulsion, k, distance);
                        // Modify the first value of the array for key 1
                        if let Some(arr) = node_disp.get_mut(&node_v_id) {
                            arr[0] += (delta.get(0).unwrap() / distance * repulsive_f);
                            arr[1] += (delta.get(1).unwrap() / distance * repulsive_f);
                        }
                    }
                }
            }
        }
        // Calculate attractive forces
        for edge in graph.edges() {
            let u = edge.src();
            let v = edge.dst();
            let node_u_id = u.id();
            let node_v_id = v.id();
            let delta = [
                node_pos.get(&node_u_id).unwrap().get(0).unwrap() - node_pos.get(&node_v_id).unwrap().get(0).unwrap(),
                node_pos.get(&node_u_id).unwrap().get(1).unwrap() - node_pos.get(&node_v_id).unwrap().get(1).unwrap(),
            ];
            let distance = calculate_distance(&delta);
            let attractive_f = attractive_force(attraction, k, distance);
            if let Some(arr) = node_disp.get_mut(&node_v_id) {
                arr[0] -= (delta.get(0).unwrap() / distance * attractive_f);
                arr[1] -= (delta.get(1).unwrap() / distance * attractive_f);
            }
            if let Some(arr) = node_disp.get_mut(&node_u_id) {
                arr[0] += (delta.get(0).unwrap() / distance * attractive_f);
                arr[1] += (delta.get(1).unwrap() / distance * attractive_f);
            }
        }
        // Limit maximum displacement and prevent being displaced outside frame
        for v in graph.nodes() {
            let v_disp = node_disp.get(&v.id()).unwrap();
            let disp_length = calculate_distance(v_disp);
            if disp_length > 0.0f64 {
                let v_disp = node_disp.get(&v.id()).unwrap();
                if let Some(arr) = node_pos.get_mut(&v.id()) {
                    arr[0] += (v_disp.get(0).unwrap() / disp_length) * disp_length.min(temperature);
                    arr[1] += (v_disp.get(1).unwrap() / disp_length) * disp_length.min(temperature);
                    arr[0] = limit_position(arr[0], 0.0f64, width);
                    arr[1] = limit_position(arr[1], 0.0f64, height);
                }
            }
        }
        // Reduce the temperature as the layout stabilizes
        temperature *= 0.95
    }
    node_pos
}

#[cfg(test)]
mod cc_test {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    /// Test the global clustering coefficient
    #[test]
    fn test_fr() {
        let graph = Graph::new();

        // Graph has 2 triangles and 20 triplets
        let edges = vec![
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 4),
            (3, 7),
            (4, 1),
            (4, 3),
            (4, 5),
            (4, 6),
            (5, 4),
            (5, 6),
            (6, 5),
            (6, 2),
            (7, 2),
            (7, 3),
        ];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }

        let results = fruchterman_reingold(
            &graph,
            100,
            100f64,
            100f64,
            1f64,
            1f64,
        );
        println!("{:?}", results);
    }
}
