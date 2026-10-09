use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{mutation::AdditionOps, view::*},
        graph::graph::Graph,
    },
    errors::GraphError,
    prelude::*,
};
use itertools::Itertools;
use num_integer::average_floor;
use raphtory_api::core::storage::timeindex::AsTime;

#[derive(Clone)]
struct Visitor {
    name: String,
    time: i64,
}

/// Projects a temporal bipartite graph into an undirected temporal graph over the pivot node type. Let G be a bipartite graph with node types A and B. Given delta > 0, the projection graph G' pivoting over type B nodes,
/// will make a connection between nodes n1 and n2 (of type A) at time (t1 + t2)/2 if they respectively have an edge at time t1, t2 with the same node of type B in G, and |t2-t1| < delta.
///
/// # Arguments
/// - `graph`: A directed raphtory graph.
/// - `delta`: Time period
/// - `pivot_type`: node type to pivot over. If a bipartite graph has types A and B, and B is the pivot type, the new graph will consist of type A nodes.
///
/// # Returns
/// Projected (unipartite) temporal graph.

pub fn temporal_bipartite_projection<G: StaticGraphViewOps>(
    graph: &G,
    delta: i64,
    pivot_type: &str,
) -> Result<Graph, GraphError> {
    let new_graph = Graph::new();
    let nodes = graph.nodes();
    let pivot_type_id = graph
        .node_meta()
        .get_node_type_id(pivot_type)
        .ok_or_else(|| GraphError::NodeTypeMissingError(pivot_type.to_string()))?;
    for v in nodes.iter() {
        if v.node_type_id() == pivot_type_id {
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
