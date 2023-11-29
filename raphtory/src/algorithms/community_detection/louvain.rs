use crate::{
    algorithms::community_detection::modularity::modularity,
    db::{api::view::internal::CoreGraphOps, graph::vertex::VertexView},
    prelude::*,
    vectors::graph_entity::GraphEntity,
};
use itertools::{partition, Itertools};
use num_traits::Pow;
use std::{
    collections::{HashMap, HashSet},
    ops::{Add, Sub},
};

fn weight_sum<G>(graph: &G, weight: Option<&str>) -> f64
where
    G: GraphViewOps,
{
    match weight {
        None => graph.count_edges() as f64,
        Some(weight_attr) => graph
            .edges()
            .map(|e| {
                e.properties()
                    .get(weight_attr)
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
    }
}

pub fn louvain<G>(
    og_graph: &G,
    weight: Option<&str>,
    resolution: f64,
    threshold: f64,
    seed: Option<bool>,
    is_directed: bool,
) -> f64
where
    G: GraphViewOps,
{
    let mut graph = og_graph.clone();
    let mut nodes_data: HashMap<VertexView<G>, HashSet<VertexView<G>>> = HashMap::new();
    // TODO NODES_DATA Usage in one_level
    let mut partition: Vec<HashSet<VertexView<G>>> =
        graph.vertices().iter().map((|v| HashSet::from([v]))).collect();
    let modularity_result = modularity(&graph, &partition, weight, resolution, is_directed);
    let m = weight_sum(&graph, weight);
    let (mut partition, mut inner_partition, mut improvement) =
        one_level(&graph, m, &partition, resolution, is_directed, seed, weight);
    let mut mod_val = 0.0f64;
    improvement = true;
    while improvement {
        // TODO THE YIELD IN PYTHON
        let new_mod = modularity(&graph, &inner_partition, weight, resolution, is_directed);
        mod_val = new_mod;
        let (graph, nodes_data) = gen_graph(&graph, &inner_partition, &nodes_data, weight);
        let (partition, inner_partition, improvement) =
            one_level(&graph, m, &partition, resolution, is_directed, seed, weight);
    }
    0.0f64
}

fn gen_graph<G>(
    graph: &G,
    partition: &Vec<HashSet<VertexView<G>>>,
    nodes_data: &HashMap<VertexView<G>, HashSet<VertexView<G>>>,
    weight: Option<&str>,
) -> (Graph, HashMap<VertexView<G>, HashSet<VertexView<G>>>)
where
    G: GraphViewOps,
{
    let mut new_g = Graph::new();
    let mut node2com: HashMap<VertexView<G>, usize> = HashMap::new();
    let mut new_nodes_data: HashMap<VertexView<G>, HashSet<VertexView<G>>> = HashMap::new();
    for (i, part) in partition.iter().enumerate() {
        for node in part {
            node2com.insert(node.clone(), i);
            let data = nodes_data.get(node).unwrap_or(&HashSet::new()).clone();
            new_nodes_data.insert(node.clone(), data);
        }
        new_g.add_vertex(0, i as u64, NO_PROPS);
    }

    for e in graph.edges().into_iter() {
        let weight_name = weight.unwrap_or("weight");
        let wt = e.properties().get(weight_name).unwrap_or(Prop::F64(0.0f64));
        let com1: usize = node2com.get(&e.src()).unwrap().clone();
        let com2: usize = node2com.get(&e.dst()).unwrap().clone();
        new_g
            .add_edge(0, com1 as u64, com2 as u64, vec![(weight_name, wt)], None)
            .expect("Error adding node");
    }

    (new_g, new_nodes_data)
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
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
        Direction::Out => v
            .out_edges()
            .map(|e| {
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
        Direction::Both => v
            .edges()
            .map(|e| {
                e.properties()
                    .get(weight_key)
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64()
            })
            .sum::<f64>(),
    }
}

fn one_level<G>(
    graph: &G,
    m: f64,
    mut partition: &Vec<HashSet<VertexView<G>>>,
    resolution: f64,
    is_directed: bool,
    seed: Option<bool>,
    weight_key: Option<&str>,
) -> (Vec<HashSet<VertexView<G>>>, Vec<HashSet<VertexView<G>>>, bool)
where
    G: GraphViewOps,
{
    let mut node2com: HashMap<VertexView<G>, usize> = HashMap::new();
    let mut inner_partition: Vec<HashSet<VertexView<G>>> = vec![];

    for (i, v) in graph.vertices().iter().enumerate() {
        node2com.insert(v.clone(), i);
        inner_partition.push(HashSet::from([v.clone()]));
    }

    let mut in_degrees: HashMap<VertexView<G>, f64> = HashMap::new();
    let mut out_degrees: HashMap<VertexView<G>, f64> = HashMap::new();
    let mut stot_in: Vec<f64> = vec![];
    let mut stot_out: Vec<f64> = vec![];
    let mut degrees: HashMap<VertexView<G>, f64> = HashMap::new();
    let mut stot: Vec<f64> = vec![];
    let mut nbrs: HashMap<VertexView<G>, HashMap<VertexView<G>, f64>> = HashMap::new();
    let mut remove_cost: f64 = 0.0f64;
    let mut gain: f64 = 0.0f64;
    let mut in_degree: f64 = 0.0f64;
    let mut out_degree: f64 = 0.0f64;
    let mut degree: f64 = 0.0f64;
    let mut best_com: usize = 0usize;
    if is_directed {
        in_degrees = graph
            .vertices()
            .iter()
            .map(|v| (v.clone(), degree_sum(&v, weight_key, Direction::In)))
            .collect();
        out_degrees = graph
            .vertices()
            .iter()
            .map(|v| (v.clone(), degree_sum(&v, weight_key, Direction::Out)))
            .collect();
        stot_in = in_degrees.values().map(|&x| x.clone()).collect();
        stot_out = out_degrees.values().map(|&x| x.clone()).collect();
        for u in graph.vertices() {
            let mut neighbors = HashMap::new();
            for out_edge in u.out_edges() {
                let n = out_edge.dst();
                let wt = out_edge
                    .properties()
                    .get(weight_key.unwrap_or(""))
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64();
                *neighbors.entry(n).or_insert(0.0) += wt;
            }
            for in_edge in u.in_edges() {
                let n = in_edge.src();
                let wt = in_edge
                    .properties()
                    .get(weight_key.unwrap_or(""))
                    .unwrap_or(Prop::F64(0.0f64))
                    .unwrap_f64();
                *neighbors.entry(n).or_insert(0.0) += wt;
            }
            nbrs.insert(u, neighbors);
        }
    } else {
        degrees = graph
            .vertices()
            .iter()
            .map(|v| (v.clone(), degree_sum(&v, weight_key, Direction::Both)))
            .collect();
        stot = degrees.values().map(|&x| x.clone()).collect();
        nbrs = graph
            .vertices()
            .iter()
            .map(|u| {
                let neighbour_vals = u
                    .out_edges()
                    .filter(|e| e.dst() != u)
                    .map(|e| {
                        (
                            e.dst(),
                            e.properties()
                                .get(weight_key.unwrap_or(""))
                                .unwrap_or(Prop::F64(0.0f64))
                                .unwrap_f64(),
                        )
                    })
                    .collect::<HashMap<VertexView<G>, f64>>();
                (u, neighbour_vals)
            })
            .collect();
    }
    let mut rand_nodes = graph.vertices().iter().collect_vec();
    let mut nb_moves = 1;
    let mut improvement = false;

    while nb_moves > 0 {
        nb_moves = 0;
        for u in &rand_nodes {
            let mut best_mod = 0.0;
            best_com = *node2com.get(u).unwrap();
            let weights2com = neighbor_weights(&nbrs.get(u).unwrap(), &node2com);
            if is_directed {
                in_degree = *in_degrees.get(u).unwrap_or(&0.0f64);
                out_degree = *out_degrees.get(u).unwrap_or(&0.0f64);
                if let Some(value) = stot_in.get_mut(best_com) {
                    *value -= in_degree;
                }
                if let Some(value) = stot_out.get_mut(best_com) {
                    *value -= out_degree;
                }
                remove_cost = -weights2com.get(&best_com).unwrap_or(&0.0f64).clone() / m
                    + resolution
                        * (out_degree
                            + stot_in.get(best_com).unwrap_or(&0.0f64)
                            + in_degree
                            + stot_out.get(best_com).unwrap_or(&0.0f64))
                        / m.powf(2.0f64);
            } else {
                degree = *degrees.get(u).unwrap_or(&0.0f64);
                if let Some(value) = stot.get_mut(best_com) {
                    *value -= degree;
                }
                remove_cost = -weights2com.get(&best_com).unwrap_or(&0.0f64).clone() / m
                    + resolution * (stot.get(best_com).unwrap_or(&0.0f64) * degree)
                        / (2.0f64 * m.powf(2.0f64));
            }
            for (nbr_com, wt) in weights2com.iter() {
                if is_directed {
                    gain = (remove_cost + wt / m
                        - resolution
                            * (out_degree * stot_in.get(*nbr_com).unwrap_or(&0.0f64)
                                + in_degree * stot_out.get(*nbr_com).unwrap_or(&0.0f64))
                            / m.powf(2.0f64))
                } else {
                    gain = (remove_cost + wt / m
                        - resolution * (stot.get(*nbr_com).unwrap_or(&0.0f64) * degree)
                            / (2.0f64 * m.powf(2.0f64)))
                }
                if gain > best_mod {
                    best_mod = gain;
                    best_com = *nbr_com;
                }
            }
            if is_directed {
                if let Some(value) = stot_in.get_mut(best_com) {
                    *value += in_degree;
                }
                if let Some(value) = stot_out.get_mut(best_com) {
                    *value += out_degree;
                }
            } else {
                if let Some(value) = stot.get_mut(best_com) {
                    *value += degree;
                }
            }
            if best_com != *node2com.get(u).unwrap() {
                // Not sure about this part
                let com: Vec<VertexView<G>> = vec![graph.vertex(u).unwrap()];
                partition[node2com[u].clone()].retain(|x| !com.contains(x));
                inner_partition[node2com[u].clone()].remove(u);
                partition[best_com].extend(com);
                inner_partition[best_com].extend(vec![u.clone()]);
                improvement = true;
                nb_moves += 1;
                node2com.insert(u.clone(), best_com);
            }
        }
    }
    partition = partition.into_iter().filter(|v| !v.is_empty()).collect();
    inner_partition = inner_partition
        .into_iter()
        .filter(|v| !v.is_empty())
        .collect();
    (partition, inner_partition, improvement)
}

fn neighbor_weights<G>(
    nbrs: &HashMap<VertexView<G>, f64>,
    node2com: &HashMap<VertexView<G>, usize>,
) -> HashMap<usize, f64>
where
    G: GraphViewOps,
{
    let mut weights = HashMap::new();
    for (nbr, &wt) in nbrs.iter() {
        let community = *node2com.get(nbr).unwrap_or(&0);
        // let mut res = *weights.entry(*community).or_insert(0.0f64);
        // res = res.add(wt.clone());
        *weights.entry(community).or_insert(0.0) += wt;
    }
    weights
}
