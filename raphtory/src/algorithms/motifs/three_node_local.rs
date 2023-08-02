use crate::algorithms::algorithm_result::AlgorithmResult;
/// This class regards the counting of the number of three edge, up-to-three node delta-temporal motifs in the graph, using the algorithm of Paranjape et al, Motifs in Temporal Networks (2017).
/// We point the reader to this reference for more information on the algorithm and background, but provide a short summary below.
///
///  ## Motifs included
///
///  ### Stars
///
///  There are three classes (in the order they are outputted) of star motif on three nodes based on the switching behaviour of the edges between the two leaf nodes.
///
///   - PRE: Stars of the form i<->j, i<->j, i<->k (ie two interactions with leaf j followed by one with leaf k)
///   - MID: Stars of the form i<->j, i<->k, i<->j (ie switching interactions from leaf j to leaf k, back to j again)
///   - POST: Stars of the form i<->j, i<->k, i<->k (ie one interaction with leaf j followed by two with leaf k)
///
///  Within each of these classes is 8 motifs depending on the direction of the first to the last edge -- incoming "I" or outgoing "O".
///  These are enumerated in the order III, IIO, IOI, IOO, OII, OIO, OOI, OOO (like binary with "I"-0 and "O"-1).
///
///  ### Two node motifs
///
///  Also included are two node motifs, of which there are 8 when counted from the perspective of each vertex. These are characterised by the direction of each edge, enumerated
///  in the above order. Note that for the global graph counts, each motif is counted in both directions (a single III motif for one vertex is an OOO motif for the other vertex).
///
///  ### Triangles
///
///  There are 8 triangle motifs:
///
///   1. i --> j, k --> j, i --> k
///   2. i --> j, k --> i, j --> k
///   3. i --> j, j --> k, i --> k
///   4. i --> j, i --> k, j --> k
///   5. i --> j, k --> j, k --> i
///   6. i --> j, k --> i, k --> j
///   7. i --> j, j --> k, k --> i
///   8. i --> j, i --> k, k --> j
///
use crate::{algorithms::motifs::three_node_motifs::*, db::api::view::*};
use std::collections::HashMap;

fn star_motif_count<G: GraphViewOps>(graph: &G, v: u64, delta: i64) -> [usize; 24] {
    if let Some(vertex) = graph.vertex(v) {
        let neigh_map: HashMap<u64, usize> = vertex
            .neighbours()
            .iter()
            .enumerate()
            .map(|(num, nb)| (nb.id(), num))
            .into_iter()
            .collect();
        let mut exploded_edges = vertex
            .edges()
            .explode()
            .map(|edge| {
                if edge.src().id() == v {
                    star_event(neigh_map[&edge.dst().id()], 1, edge.time().unwrap())
                } else {
                    star_event(neigh_map[&edge.src().id()], 0, edge.time().unwrap())
                }
            })
            .collect::<Vec<StarEvent>>();
        exploded_edges.sort_by_key(|e| e.time);
        let mut star_count = init_star_count(neigh_map.len());
        star_count.execute(&exploded_edges, delta);
        star_count.return_counts()
    } else {
        [0; 24]
    }
}

fn twonode_motif_count<G: GraphViewOps>(graph: &G, v: u64, delta: i64) -> [usize; 8] {
    let mut counts = [0; 8];
    if let Some(vertex) = graph.vertex(v) {
        for nb in vertex.neighbours().iter() {
            let nb_id = nb.id();
            let out = graph.edge(vertex.id(), nb_id, Layer::All);
            let inc = graph.edge(nb_id, vertex.id(), Layer::All);
            let mut all_exploded = match (out, inc) {
                (Some(o), Some(i)) => o
                    .explode()
                    .chain(i.explode())
                    .map(|e| {
                        two_node_event(if e.src().id() == v { 1 } else { 0 }, e.time().unwrap())
                    })
                    .collect::<Vec<TwoNodeEvent>>(),
                (Some(o), None) => o
                    .explode()
                    .map(|e| two_node_event(1, e.time().unwrap()))
                    .collect::<Vec<TwoNodeEvent>>(),
                (None, Some(i)) => i
                    .explode()
                    .map(|e| two_node_event(0, e.time().unwrap()))
                    .collect::<Vec<TwoNodeEvent>>(),
                (None, None) => Vec::new(),
            };
            all_exploded.sort_by_key(|e| e.time);
            let mut two_node_counter = init_two_node_count();
            two_node_counter.execute(&all_exploded, delta);
            let two_node_result = two_node_counter.return_counts();
            for i in 0..8 {
                counts[i] += two_node_result[i];
            }
        }
    }
    counts
}

fn triangle_motif_count<G: GraphViewOps>(
    graph: &G,
    delta: i64,
) -> AlgorithmResult<u64, Vec<usize>> {
    let mut counts: HashMap<u64, Vec<usize>> = HashMap::new();
    for u in graph.vertices() {
        counts.insert(u.id(), vec![0; 8]);
    }
    for u in graph.vertices() {
        let uid = u.id();
        for v in u.neighbours().iter().filter(|x| x.id() > uid) {
            for nb in u.neighbours().iter().filter(|x| x.id() > v.id()) {
                let mut tri_edges: Vec<TriangleEdge> = Vec::new();
                let out = graph.edge(v.id(), nb.id(), Layer::All);
                let inc = graph.edge(nb.id(), v.id(), Layer::All);
                // The following code checks for triangles
                match (out, inc) {
                    (Some(o), Some(i)) => {
                        tri_edges.append(
                            &mut o
                                .explode()
                                .map(|e| new_triangle_edge(false, 1, 0, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                        tri_edges.append(
                            &mut i
                                .explode()
                                .map(|e| new_triangle_edge(false, 1, 0, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (Some(o), None) => {
                        tri_edges.append(
                            &mut o
                                .explode()
                                .map(|e| new_triangle_edge(false, 1, 0, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (None, Some(i)) => {
                        tri_edges.append(
                            &mut i
                                .explode()
                                .map(|e| new_triangle_edge(false, 1, 0, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>(),
                        );
                    }
                    (None, None) => {
                        continue;
                    }
                }
                if !tri_edges.is_empty() {
                    let uout = graph.edge(uid, nb.id(), Layer::All);
                    let uin = graph.edge(nb.id(), uid, Layer::All);
                    match (uout, uin) {
                        (Some(o), Some(i)) => {
                            tri_edges.append(
                                &mut o
                                    .explode()
                                    .map(|e| new_triangle_edge(false, 0, 0, 1, e.time().unwrap()))
                                    .collect::<Vec<TriangleEdge>>(),
                            );
                            tri_edges.append(
                                &mut i
                                    .explode()
                                    .map(|e| new_triangle_edge(false, 0, 0, 0, e.time().unwrap()))
                                    .collect::<Vec<TriangleEdge>>(),
                            );
                        }
                        (Some(o), None) => {
                            tri_edges.append(
                                &mut o
                                    .explode()
                                    .map(|e| new_triangle_edge(false, 0, 0, 1, e.time().unwrap()))
                                    .collect::<Vec<TriangleEdge>>(),
                            );
                        }
                        (None, Some(i)) => {
                            tri_edges.append(
                                &mut i
                                    .explode()
                                    .map(|e| new_triangle_edge(false, 0, 0, 0, e.time().unwrap()))
                                    .collect::<Vec<TriangleEdge>>(),
                            );
                        }
                        (None, None) => {
                            continue;
                        }
                    }
                    // found triangle at this point!!
                    let u_to_v = match graph.edge(uid, v.id(), Layer::All) {
                        Some(edge) => {
                            let r = edge
                                .explode()
                                .map(|e| new_triangle_edge(true, 1, 0, 1, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>();
                            r.into_iter()
                        }
                        None => vec![].into_iter(),
                    };
                    let v_to_u = match graph.edge(v.id(), uid, Layer::All) {
                        Some(edge) => {
                            let r = edge
                                .explode()
                                .map(|e| new_triangle_edge(true, 0, 0, 0, e.time().unwrap()))
                                .collect::<Vec<TriangleEdge>>();
                            r.into_iter()
                        }
                        None => vec![].into_iter(),
                    };
                    tri_edges.append(&mut u_to_v.collect::<Vec<TriangleEdge>>());
                    tri_edges.append(&mut v_to_u.collect::<Vec<TriangleEdge>>());
                    tri_edges.sort_by_key(|e| e.time);

                    let mut tri_count = init_tri_count(1);
                    tri_count.execute(&tri_edges, delta);
                    let tmp_counts = tri_count.return_counts().iter();
                    for id in [uid, v.id(), nb.id()] {
                        counts.insert(
                            id,
                            counts
                                .get(&id)
                                .unwrap()
                                .iter()
                                .zip(tmp_counts.clone())
                                .map(|(&i1, &i2)| i1 + i2)
                                .collect::<Vec<usize>>(),
                        );
                    }
                }
            }
        }
    }
    AlgorithmResult::new(counts)
}

/// Computes the number of each type of motif that each node participates in.
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `delta` - Maximum time difference between the first and last edge of the
/// motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise
/// milliseconds should be used (if edge times were given as string)
///
/// # Returns
///
/// A dictionary with vertex ids (u64) as keys and a 40 dimensional array of motif counts as a value. The first 24 elements are star counts,
///   the next 8 are two-node motif counts and the final 8 are triangle counts.
///
/// # Notes
///  
/// For this local count, a node is counted as participating in a motif in the following way. For star motifs, only the centre node counts
///    the motif. For two node motifs, both constituent nodes count the motif. For triangles, all three constituent nodes count the motif.
///
///
pub fn local_temporal_three_node_motifs<G: GraphViewOps>(
    graph: &G,
    delta: i64,
) -> AlgorithmResult<u64, Vec<usize>> {
    let mut counts = triangle_motif_count(graph, delta).get_all().to_owned();
    for v in graph.vertices() {
        let vid = v.id();
        let two_nodes = twonode_motif_count(graph, vid, delta).to_vec();
        let tmp_stars = star_motif_count(graph, vid, delta);
        let stars: Vec<usize> = tmp_stars
            .iter()
            .zip(two_nodes.iter().cycle().take(24))
            .map(|(&x1, &x2)| x1 - x2)
            .collect();
        let mut final_cts = Vec::new();
        final_cts.extend(stars.into_iter());
        final_cts.extend(two_nodes.into_iter());
        final_cts.extend(counts.get(&vid).unwrap().into_iter());
        counts.insert(vid, final_cts);
    }
    AlgorithmResult::new(counts)
}

/// Computes the number of each type of motif there is in the graph.
///
/// # Arguments
///
/// * `g` - A reference to the graph
/// * `delta` - Maximum time difference between the first and last edge of the
/// motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise
/// milliseconds should be used (if edge times were given as string)
///
/// # Returns
///
/// A 40 dimensional array with the counts of each motif, given in the same order as described in the class summary. Note that the two-node motif counts are symmetrical so it may be more useful just to consider the first four elements.
///
/// # Notes
///  
/// This is achieved by calling the local motif counting algorithm, summing the resulting arrays and dealing with overcounted motifs: the triangles (by dividing each motif count by three) and two-node motifs (dividing by two).
///
///
pub fn global_temporal_three_node_motifs<G: GraphViewOps>(graph: &G, delta: i64) -> Vec<usize> {
    let counts = local_temporal_three_node_motifs(graph, delta)
        .get_all()
        .to_owned();
    let mut tmp_counts = counts.values().fold(vec![0; 40], |acc, x| {
        acc.iter().zip(x.iter()).map(|(x1, x2)| x1 + x2).collect()
    });
    for ind in 32..40 {
        tmp_counts[ind] /= 3;
    }
    tmp_counts
}

#[cfg(test)]
mod local_motif_test {
    use crate::{
        algorithms::motifs::three_node_local::*,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };

    #[test]
    fn test_init() {
        let graph = Graph::new();

        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, time) in &vs {
            graph.add_edge(*time, *src, *dst, NO_PROPS, None).unwrap();
        }

        // let counts = star_motif_count(&graph, 1, 100);
        let counts = local_temporal_three_node_motifs(&graph, 10);
        // FIXME: Should test this
        let _global_counts = global_temporal_three_node_motifs(&graph, 10);

        let expected: HashMap<u64, Vec<usize>> = HashMap::from([
            (
                1,
                vec![
                    0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 0,
                ],
            ),
            (
                10,
                vec![
                    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
                ],
            ),
            (
                11,
                vec![
                    0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
                ],
            ),
            (
                2,
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                3,
                vec![
                    0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 1, 2, 0,
                ],
            ),
            (
                4,
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 2, 0,
                ],
            ),
            (
                5,
                vec![
                    0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 0, 1, 1, 1,
                ],
            ),
            (
                6,
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
                ],
            ),
            (
                7,
                vec![
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
            (
                8,
                vec![
                    0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 2, 1, 2, 0, 1, 0, 1,
                ],
            ),
            (
                9,
                vec![
                    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
                ],
            ),
        ]);
        for ind in 1..12 {
            assert_eq!(counts.get(&ind).unwrap(), expected.get(&ind).unwrap());
        }
        // print!("{:?}", global_counts);
    }
}
