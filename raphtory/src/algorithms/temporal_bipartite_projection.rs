use itertools::Itertools;
use num_integer::average_floor;
// use num::integer::average_floor;
extern crate num_integer;

use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    db::{
        api::{
            mutation::AdditionOps,
            view::{
                internal::{DynamicGraph, GraphOps},
                *,
            },
        },
        graph::graph::Graph,
    },
    prelude::{EdgeListOps, EdgeViewOps, GraphViewOps, PropUnwrap, VertexViewOps, NO_PROPS},
};

#[derive(Clone)]
struct Visitor {
    name: String,
    time: i64,
}

pub fn temporal_bipartite_projection<G: GraphViewOps>(
    graph: &G,
    delta: i64,
    pivot_type: String,
) -> Graph {
    let new_graph = Graph::new();
    let nodes = graph
        .vertices()
        .iter()
        .filter(|v| v.properties().get("Type").unwrap_str() == pivot_type);
    for v in nodes {
        populate_edges(graph, &new_graph, v, delta)
    }
    new_graph
}

fn populate_edges<G: GraphViewOps, V: Into<VertexRef>>(g: &G, new_graph: &Graph, v: V, delta: i64) {
    if let Some(vertex) = g.vertex(v) {
        // get vector of vertices which need connecting up
        let mut visitors = vertex
            .in_edges()
            .explode()
            .map(|e| Visitor {
                name: e.src().name(),
                time: e.time().unwrap(),
            })
            .collect_vec();
        visitors.sort_by_key(|vis| vis.time);

        let mut start = 0;
        let mut to_process: Vec<Visitor> = vec![];
        for nb in visitors.iter() {
            while visitors[start].time + delta < nb.time {
                to_process.remove(0);
                start += 1
            }
            for node in &to_process {
                let new_time = average_floor(nb.time, node.time);
                new_graph
                    .add_edge(new_time, node.name.clone(), nb.name.clone(), NO_PROPS, None)
                    .unwrap();
            }
            to_process.push(nb.clone());
        }
    } else {
        return;
    }
}

#[cfg(test)]
mod bipartite_graph_tests {
    use itertools::Itertools;

    use super::temporal_bipartite_projection;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::{Prop, NO_PROPS},
    };

    #[test]
    fn small_delta_test() {
        let g = Graph::new();
        let vs = vec![
            (1, "A", "1"),
            (3, "A", "2"),
            (3, "B", "2"),
            (4, "C", "3"),
            (6, "B", "3"),
            (8, "A", "3"),
            (10, "C", "4"),
            (11, "B", "4"),
        ];
        for (t, src, dst) in &vs {
            g.add_vertex(*t, *src, [("Type", Prop::Str("Left".into()))])
                .unwrap();
            g.add_vertex(*t, *dst, [("Type", Prop::Str("Right".into()))])
                .unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 1, "Right".to_string());
        assert!(new_graph.has_edge("A", "B", Layer::All));
        assert_eq!(new_graph.edge("A", "B").unwrap().latest_time(), Some(3));
        assert!(new_graph.has_edge("C", "B", Layer::All));
        assert_eq!(new_graph.edge("C", "B").unwrap().latest_time(), Some(10));
        assert!(!new_graph.has_edge("A", "C", Layer::All));
    }

    #[test]
    fn larger_delta_test() {
        let g = Graph::new();
        let vs = vec![
            (1, "A", "1"),
            (3, "A", "2"),
            (3, "B", "2"),
            (4, "C", "3"),
            (6, "B", "3"),
            (8, "A", "3"),
            (10, "C", "4"),
            (11, "B", "4"),
        ];
        for (t, src, dst) in &vs {
            g.add_vertex(*t, *src, [("Type", Prop::Str("Left".into()))])
                .unwrap();
            g.add_vertex(*t, *dst, [("Type", Prop::Str("Right".into()))])
                .unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 3, "Right".to_string());
        assert!(new_graph.has_edge("A", "B", Layer::All));
        assert_eq!(new_graph.edge("A", "B").unwrap().earliest_time(), Some(3));
        assert_eq!(new_graph.edge("B", "A").unwrap().latest_time(), Some(7));
        assert!(new_graph.has_edge("C", "B", Layer::All));
        assert_eq!(new_graph.edge("C", "B").unwrap().earliest_time(), Some(5));
        assert_eq!(new_graph.edge("C", "B").unwrap().latest_time(), Some(10));
        assert!(!new_graph.has_edge("A", "C", Layer::All));
    }
}
