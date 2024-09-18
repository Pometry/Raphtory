use crate::prelude::{EdgeViewOps, GraphViewOps, NodeViewOps};
use raphtory_api::core::entities::VID;
use std::{
    cmp::{max, min},
    collections::HashSet,
};

use raphtory_api::core::entities::VID;
use rayon::prelude::*;

use crate::prelude::{EdgeViewOps, GraphViewOps, NodeViewOps};

struct SlidingWindows<I> {
    iter: I,
    window_size: usize,
}

impl<I> SlidingWindows<I> {
    fn new(iter: I, window_size: usize) -> Self {
        SlidingWindows { iter, window_size }
    }
}

impl<I> Iterator for SlidingWindows<I>
where
    I: Iterator,
    I::Item: Clone,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut window = Vec::with_capacity(self.window_size);
        for _ in 0..self.window_size {
            if let Some(item) = self.iter.next() {
                window.push(item);
            } else {
                return None;
            }
        }
        Some(window)
    }
}

pub fn temporal_rich_club_coefficient<'a, I, G1, G2>(
    agg_graph: G2,
    views: I,
    k: usize,
    window_size: usize,
) -> f64
where
    I: IntoIterator<Item = G1>,
    G1: GraphViewOps<'a>,
    G2: GraphViewOps<'a>,
{
    // Extract the set of nodes with degree greater than or equal to k
    let s_k: HashSet<VID> = agg_graph
        .nodes()
        .into_iter()
        .filter(|v| v.degree() >= k)
        .map(|v| v.node)
        .collect();

    if s_k.len() <= 1 {
        return 0.0 as f64;
    }

    let temp_rich_club_val = SlidingWindows::new(views.into_iter(), window_size)
        .map(|window| intermediate_rich_club_coef(s_k.clone(), window))
        .reduce(f64::max)
        .unwrap_or(0.0);

    temp_rich_club_val
}

fn intermediate_rich_club_coef<'a, I, G1>(s_k: HashSet<VID>, views: I) -> f64
where
    I: IntoIterator<Item = G1>,
    G1: GraphViewOps<'a>,
{
    // Extract the edges among the top degree nodes which are stable over that subset of snapshots by computing their intersection
    let stable_edges = views
        .into_iter()
        .map(|g| {
            let new_edges: HashSet<UndirEdge> = g
                .subgraph(s_k.clone())
                .edges()
                .into_iter()
                .filter(|e| e.src() != e.dst())
                .map(|e| undir_edge(e.src().node, e.dst().node))
                .collect();
            new_edges
        })
        .into_iter()
        .reduce(|acc_edges, item_edges| acc_edges.intersection(&item_edges).cloned().collect());
    // Compute the density with respect to the possible number of edges between those s_k nodes.
    match stable_edges {
        Some(edges) => {
            let poss_edges = (s_k.len() * (s_k.len() - 1)) / 2;
            return (edges.len() as f64) / (poss_edges as f64);
        }
        None => return 0 as f64,
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct UndirEdge {
    src: VID,
    dst: VID,
}

// So that we can make a set of undirected edges
fn undir_edge<T: Into<VID>>(src: T, dst: T) -> UndirEdge {
    let src_id: VID = src.into();
    let dst_id: VID = dst.into();
    UndirEdge {
        src: min(src_id, dst_id),
        dst: max(src_id, dst_id),
    }
}

#[cfg(test)]
mod rich_club_test {
    use super::*;
    use crate::{
        algorithms::centrality::pagerank::page_rank_tests::assert_eq_f64,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::{TimeOps, NO_PROPS},
    };

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    fn load_sample_graph() -> Graph {
        let edges = vec![
            (1, 1, 2),
            (1, 1, 3),
            (1, 1, 4),
            (1, 2, 3),
            (1, 2, 4),
            (1, 3, 4),
            (1, 4, 5),
            (2, 1, 2),
            (2, 1, 3),
            (2, 1, 4),
            (2, 3, 4),
            (2, 2, 6),
            (3, 1, 2),
            (3, 2, 4),
            (3, 3, 4),
            (3, 1, 4),
            (3, 1, 3),
            (3, 1, 7),
            (4, 1, 2),
            (4, 1, 3),
            (4, 1, 4),
            (4, 2, 8),
            (5, 1, 2),
            (5, 1, 3),
            (5, 1, 4),
            (5, 2, 4),
            (5, 3, 9),
        ];
        load_graph(edges)
    }

    #[test]
    // Using the toy example from the paper
    fn toy_graph_test() {
        let g = load_sample_graph();
        let g_rolling = g.rolling(1, Some(1)).unwrap();

        let rc_coef_1 = temporal_rich_club_coefficient(g.clone(), g_rolling.clone(), 3, 1);
        let rc_coef_3 = temporal_rich_club_coefficient(g.clone(), g_rolling.clone(), 3, 3);
        let rc_coef_5 = temporal_rich_club_coefficient(g.clone(), g_rolling.clone(), 3, 5);
        assert_eq_f64(Some(rc_coef_1), Some(1.0), 3);
        assert_eq_f64(Some(rc_coef_3), Some(0.66666), 3);
        assert_eq_f64(Some(rc_coef_5), Some(0.5), 3);
    }
}
