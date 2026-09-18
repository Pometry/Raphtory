use itertools::Itertools;
use num_integer::average_floor;
use raphtory_api::core::storage::timeindex::AsTime;

extern crate num_integer;
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{mutation::AdditionOps, view::*},
        graph::graph::Graph,
    },
    errors::GraphError,
    prelude::*,
};

#[derive(Clone)]
struct Visitor {
    name: String,
    time: i64,
}

/// Projects a temporal bipartite graph into an undirected temporal graph over the pivot node type. Let G be a bipartite graph with node types A and B. Given delta > 0, the projection graph G' pivoting over type B nodes,
/// will make a connection between nodes n1 and n2 (of type A) at time (t1 + t2)/2 if they respectively have an edge at time t1, t2 with the same node of type B in G, and |t2-t1| < delta.
///
/// # Arguments
/// - `graph`: A directed raphtory graph. Every node must carry a node type.
/// - `delta`: Time period
/// - `pivot_type`: node type to pivot over. If a bipartite graph has types A and B, and B is the pivot type, the new graph will consist of type A nodes.
///
/// # Returns
/// Projected (unipartite) temporal graph.
///
/// # Errors
/// `GraphError::NodeTypeError` if a node of `graph` has no node type, naming that node.
pub fn temporal_bipartite_projection<G: StaticGraphViewOps>(
    graph: &G,
    delta: i64,
    pivot_type: String,
) -> Result<Graph, GraphError> {
    let new_graph = Graph::new();
    let nodes = graph.nodes();
    for v in nodes.iter() {
        let node_type = v.node_type().ok_or_else(|| {
            GraphError::NodeTypeError(format!(
                "node {} has no node type; every node needs a node type to be projected",
                v.name()
            ))
        })?;
        if node_type == pivot_type {
            populate_edges(graph, &new_graph, v, delta)?;
        }
    }
    Ok(new_graph)
}

fn populate_edges<G: StaticGraphViewOps, V: AsNodeRef>(
    g: &G,
    new_graph: &Graph,
    v: V,
    delta: i64,
) -> Result<(), GraphError> {
    if let Some(vertex) = g.node(v) {
        // get vector of vertices which need connecting up
        let mut visitors = vertex
            .edges()
            .explode()
            .iter()
            .map(|e| Visitor {
                name: e.nbr().name(),
                time: e.time().unwrap().t(),
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
                new_graph.add_edge(new_time, node.name.clone(), nb.name.clone(), NO_PROPS, None)?;
            }
            to_process.push(nb.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod bipartite_graph_tests {
    use super::temporal_bipartite_projection;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        errors::GraphError,
        prelude::NO_PROPS,
    };
    use raphtory_api::core::storage::timeindex::AsTime;

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
            g.add_node(*t, *src, NO_PROPS, Some("Left"), None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, Some("Right"), None).unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 1, "Right".to_string()).unwrap();
        assert!(new_graph.has_edge("A", "B"));
        assert_eq!(
            new_graph
                .edge("A", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(3)
        );
        assert!(new_graph.has_edge("C", "B"));
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(10)
        );
        assert!(!new_graph.has_edge("A", "C"));
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
            g.add_node(*t, *src, NO_PROPS, Some("Left"), None).unwrap();
            g.add_node(*t, *dst, NO_PROPS, Some("Right"), None).unwrap();
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
        let new_graph = temporal_bipartite_projection(&g, 3, "Right".to_string()).unwrap();

        assert!(new_graph.has_edge("A", "B"));
        assert_eq!(
            new_graph
                .edge("A", "B")
                .unwrap()
                .earliest_time()
                .map(|t| t.t()),
            Some(3)
        );
        assert_eq!(
            new_graph
                .edge("B", "A")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(7)
        );
        assert!(new_graph.has_edge("C", "B"));
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .earliest_time()
                .map(|t| t.t()),
            Some(5)
        );
        assert_eq!(
            new_graph
                .edge("C", "B")
                .unwrap()
                .latest_time()
                .map(|t| t.t()),
            Some(10)
        );
        assert!(!new_graph.has_edge("A", "C"));
    }

    #[test]
    fn untyped_node_is_an_error_not_a_panic() {
        // add_edge creates its endpoints without a node type
        let g = Graph::new();
        g.add_edge(1, "alice", "laptop", NO_PROPS, None).unwrap();
        g.add_edge(2, "bob", "laptop", NO_PROPS, None).unwrap();

        let err = temporal_bipartite_projection(&g, 5, "Item".to_string()).unwrap_err();
        assert!(matches!(err, GraphError::NodeTypeError(_)), "{err:?}");
        let message = err.to_string();
        assert!(message.contains("has no node type"), "{message}");
        assert!(
            message.contains("alice") || message.contains("bob") || message.contains("laptop"),
            "the error should name the offending node: {message}"
        );
    }

    #[test]
    fn one_untyped_node_among_typed_ones_is_still_an_error() {
        let g = Graph::new();
        g.add_node(1, "A", NO_PROPS, Some("Left"), None).unwrap();
        g.add_node(1, "1", NO_PROPS, Some("Right"), None).unwrap();
        g.add_edge(1, "A", "1", NO_PROPS, None).unwrap();
        // this endpoint is created by add_edge and never given a type
        g.add_edge(2, "B", "1", NO_PROPS, None).unwrap();

        let err = temporal_bipartite_projection(&g, 5, "Right".to_string()).unwrap_err();
        assert!(err.to_string().contains("node B has no node type"), "{err}");
    }

    #[test]
    fn pivot_type_nobody_carries_gives_an_empty_graph() {
        let g = Graph::new();
        g.add_node(1, "A", NO_PROPS, Some("Left"), None).unwrap();
        g.add_node(1, "1", NO_PROPS, Some("Right"), None).unwrap();
        g.add_edge(1, "A", "1", NO_PROPS, None).unwrap();

        let new_graph = temporal_bipartite_projection(&g, 5, "Item".to_string()).unwrap();
        assert_eq!(new_graph.count_nodes(), 0);
        assert_eq!(new_graph.count_edges(), 0);
    }
}
