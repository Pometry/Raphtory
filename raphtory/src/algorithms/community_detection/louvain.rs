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

// use type alias' because i keep getting confused
type VID = u64;
type COMM_ID = u64;

pub fn louvain<G>(
    og_graph: &G,
    weight: Option<&str>,
    resolution: f64,
    threshold: f64,
    seed: Option<bool>,
    is_directed: bool,
) -> Option<HashSet<VID>>
where
    G: GraphViewOps,
{
    let mut graph = og_graph.clone();
    let mut nodes_data: HashMap<VID, HashSet<VID>> = HashMap::new();
    let nodes_data: HashMap<VID, HashSet<VID>> = HashMap::new();
    let partition: Vec<HashSet<VID>> = graph
        .vertices()
        .iter()
        .map((|v| HashSet::from([v.id()])))
        .collect_vec();
    // TODO MODULARITY RESULT IS UNUSED? WHY?
    let mut mod_val = modularity(&graph, &partition, weight, resolution, is_directed);
    let m = weight_sum(&graph, weight);
    let (partition, inner_partition, mut improvement) = one_level(
        &graph,
        m,
        partition,
        resolution,
        is_directed,
        seed,
        weight,
        &nodes_data,
    );
    improvement = true;
    while improvement {
        // TODO THE YIELD IN PYTHON
        let new_mod = modularity(&graph, &inner_partition, weight, resolution, is_directed);
        if new_mod - mod_val <= threshold {
            break;
        }
        mod_val = new_mod;
        let (graph, nodes_data): (Graph, HashMap<VID, HashSet<VID>>) =
            gen_graph(&graph, &inner_partition, &nodes_data, weight);
        let (partition, inner_partition, improvement) = one_level(
            &graph,
            m,
            partition.clone(),
            resolution,
            is_directed,
            seed,
            weight,
            &nodes_data,
        );
    }
    partition.last().cloned()
}

fn gen_graph<G>(
    graph: &G,
    partition: &Vec<HashSet<u64>>,
    nodes_data: &HashMap<VID, HashSet<VID>>,
    weight: Option<&str>,
) -> (Graph, HashMap<VID, HashSet<VID>>)
where
    G: GraphViewOps,
{
    let mut new_g = Graph::new();
    let mut node2com: HashMap<VID, COMM_ID> = HashMap::new(); // VID => COMMUNITY_ID
    let mut new_nodes_data: HashMap<VID, HashSet<VID>> = HashMap::new();
    for (i, part) in partition.iter().enumerate() {
        for node in part {
            node2com.insert(node.clone(), i as u64);
            let data = nodes_data.get(node).unwrap_or(&HashSet::new()).clone();
            new_nodes_data.insert(node.clone(), data);
        }
        new_g
            .add_vertex(graph.latest_time().unwrap(), i as u64, NO_PROPS)
            .expect("Error adding node");
    }

    for e in graph.edges().into_iter() {
        let weight_name = weight.unwrap_or("weight");
        let wt = e.properties().get(weight_name).unwrap_or(Prop::F64(0.0f64));
        let com1: COMM_ID = node2com.get(&e.src().id()).unwrap().clone();
        let com2: COMM_ID = node2com.get(&e.dst().id()).unwrap().clone();
        new_g
            .add_edge(
                graph.latest_time().unwrap(),
                com1,
                com2,
                vec![(weight_name, wt)],
                None,
            )
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
    mut partition: Vec<HashSet<VID>>,
    resolution: f64,
    is_directed: bool,
    seed: Option<bool>,
    weight_key: Option<&str>,
    nodes_data: &HashMap<VID, HashSet<VID>>,
) -> (Vec<HashSet<VID>>, Vec<HashSet<VID>>, bool)
where
    G: GraphViewOps,
{
    let mut node2com: HashMap<VID, COMM_ID> = HashMap::new();
    let mut inner_partition: Vec<HashSet<VID>> = vec![];

    for (i, v) in graph.vertices().iter().enumerate() {
        node2com.insert(v.id().clone(), i as u64);
        inner_partition.push(HashSet::from([v.id().clone()]));
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
    let mut best_com: COMM_ID = 0u64;
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
            best_com = *node2com.get(&u.id()).unwrap();
            let weights2com = neighbor_weights(&nbrs.get(u).unwrap(), &node2com);
            if is_directed {
                in_degree = *in_degrees.get(u).unwrap_or(&0.0f64);
                out_degree = *out_degrees.get(u).unwrap_or(&0.0f64);
                if let Some(value) = stot_in.get_mut(best_com as usize) {
                    *value -= in_degree;
                }
                if let Some(value) = stot_out.get_mut(best_com as usize) {
                    *value -= out_degree;
                }
                remove_cost = -weights2com.get(&best_com).unwrap_or(&0.0f64).clone() / m
                    + resolution
                        * (out_degree
                            + stot_in.get(best_com as usize).unwrap_or(&0.0f64)
                            + in_degree
                            + stot_out.get(best_com as usize).unwrap_or(&0.0f64))
                        / m.powf(2.0f64);
            } else {
                degree = *degrees.get(u).unwrap_or(&0.0f64);
                if let Some(value) = stot.get_mut(best_com as usize) {
                    *value -= degree;
                }
                remove_cost = -weights2com.get(&best_com).unwrap_or(&0.0f64).clone() / m
                    + resolution * (stot.get(best_com as usize).unwrap_or(&0.0f64) * degree)
                        / (2.0f64 * m.powf(2.0f64));
            }
            for (nbr_com, wt) in weights2com.iter() {
                if is_directed {
                    gain = (remove_cost + wt / m
                        - resolution
                            * (out_degree * stot_in.get(*nbr_com as usize).unwrap_or(&0.0f64)
                                + in_degree * stot_out.get(*nbr_com as usize).unwrap_or(&0.0f64))
                            / m.powf(2.0f64))
                } else {
                    gain = (remove_cost + wt / m
                        - resolution * (stot.get(*nbr_com as usize).unwrap_or(&0.0f64) * degree)
                            / (2.0f64 * m.powf(2.0f64)))
                }
                if gain > best_mod {
                    best_mod = gain;
                    best_com = *nbr_com;
                }
            }
            if is_directed {
                if let Some(value) = stot_in.get_mut(best_com as usize) {
                    *value += in_degree;
                }
                if let Some(value) = stot_out.get_mut(best_com as usize) {
                    *value += out_degree;
                }
            } else {
                if let Some(value) = stot.get_mut(best_com as usize) {
                    *value += degree;
                }
            }
            if best_com != *node2com.get(&u.id()).unwrap() {
                let temp_vid: VID = u.id();
                let com: HashSet<VID> = nodes_data
                    .get(&temp_vid)
                    .unwrap_or(&HashSet::from([graph.vertex(u).unwrap().id()]))
                    .clone();
                partition[*node2com.get(&temp_vid).unwrap() as usize].retain(|x| !com.contains(x));
                inner_partition[node2com[&temp_vid].clone() as usize].remove(&temp_vid);
                partition[best_com as usize].extend(com);
                inner_partition[best_com as usize].extend(vec![temp_vid]);
                improvement = true;
                nb_moves += 1;
                node2com.insert(u.id().clone(), best_com);
            }
        }
    }
    let new_partition = partition
        .iter()
        .filter(|v| !v.is_empty())
        .cloned()
        .collect();
    inner_partition = inner_partition
        .into_iter()
        .filter(|v| !v.is_empty())
        .collect();
    (new_partition, inner_partition, improvement)
}

fn neighbor_weights<G>(
    nbrs: &HashMap<VertexView<G>, f64>,
    node2com: &HashMap<VID, COMM_ID>,
) -> HashMap<COMM_ID, f64>
where
    G: GraphViewOps,
{
    let mut weights = HashMap::new();
    for (nbr, &wt) in nbrs.iter() {
        let community: COMM_ID = *node2com.get(&nbr.id()).unwrap_or(&0u64);
        // let mut res = *weights.entry(*community).or_insert(0.0f64);
        // res = res.add(wt.clone());
        *weights.entry(community).or_insert(0.0) += wt;
    }
    weights
}
