use num::range;
use num_traits::Pow;
use crate::db::api::view::*;


fn repulsive_force(
    repulsion: u64,
    k: i64,
    d: u64,
) -> u64 {
  repulsion * k as u64 * k as u64 / d
}

fn attractive_force(
    attraction: u64,
    k: i64,
    d: u64,
) -> u64 {
    d * d / (attraction * k as u64)
}

fn calculate_distance(
    delta: [f64; 2],
) -> f64 {
    let new_sum: f64 = delta.get(0).unwrap().pow(2) + delta.get(1).unwrap().pow(2);
    new_sum.sqrt()
}

fn limit_position(
    value: u64,
    min_value: u64,
    max_value: u64,
) -> u64 {
    min_value.max(max_value.min(value))
}

pub fn fruchterman_reingold<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    width: u64,
    height: u64,
    repulsion: u64,
    attraction: u64,
) -> f64 {
    let area = width * height;
    let k = (area as f64 / graph.count_nodes() as f64).sqrt() as i64 + 1;
    let temperature = width / 10;
    for _ in range(0, iterations) {

    }
    0.0f64
}