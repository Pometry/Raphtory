use crate::{
    core::entities::VID,
    db::{
        api::{state::NodeState, view::StaticGraphViewOps},
        graph::node::NodeView,
    },
    prelude::*,
};
use std::collections::{HashMap, HashSet};

fn tarjan<'graph, G>(
    node: NodeView<&'graph G>,
    index: &'graph mut u64,
    stack: &'graph mut Vec<VID>,
    indices: &'graph mut HashMap<VID, u64>,
    lowlink: &'graph mut HashMap<VID, u64>,
    on_stack: &'graph mut HashSet<VID>,
    result: &'graph mut Vec<Vec<VID>>,
) where
    G: StaticGraphViewOps,
{
    *index += 1;
    indices.insert(node.node, *index);
    lowlink.insert(node.node, *index);
    stack.push(node.node);
    on_stack.insert(node.node);

    for neighbor in node.out_neighbours() {
        if !indices.contains_key(&neighbor.node) {
            tarjan(neighbor, index, stack, indices, lowlink, on_stack, result);
            lowlink.insert(node.node, lowlink[&node.node].min(lowlink[&neighbor.node]));
        } else if on_stack.contains(&neighbor.node) {
            lowlink.insert(node.node, lowlink[&node.node].min(indices[&neighbor.node]));
        }
    }

    if indices[&node.node] == lowlink[&node.node] {
        let mut component = Vec::new();
        let mut top = stack.pop().unwrap();
        on_stack.remove(&top);
        component.push(top);
        while top != node.node {
            top = stack.pop().unwrap();
            on_stack.remove(&top);
            component.push(top);
        }
        result.push(component);
    }
}

fn tarjan_scc<G>(graph: &G) -> Vec<Vec<VID>>
where
    G: StaticGraphViewOps,
{
    let mut index = 0;
    let mut stack = Vec::new();
    let mut indices: HashMap<VID, u64> = HashMap::new();
    let mut lowlink: HashMap<VID, u64> = HashMap::new();
    let mut on_stack: HashSet<VID> = HashSet::new();
    let mut result: Vec<Vec<VID>> = Vec::new();

    for node in (&graph).nodes() {
        if !indices.contains_key(&node.node) {
            tarjan(
                node,
                &mut index,
                &mut stack,
                &mut indices,
                &mut lowlink,
                &mut on_stack,
                &mut result,
            );
        }
    }
    result
}

/// Computes the strongly connected components of a graph using Tarjan's Strongly Connected Components algorithm
///
/// Original Paper:
/// https://web.archive.org/web/20170829214726id_/http://www.cs.ucsb.edu/~gilbert/cs240a/old/cs240aSpr2011/slides/TarjanDFS.pdf
///
/// # Arguments
///
/// - `graph` - A reference to the graph
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from each node to its component ID
///
pub fn strongly_connected_components<G>(graph: &G) -> NodeState<'static, usize, G>
where
    G: StaticGraphViewOps,
{
    // TODO: evaluate/improve this early-culling code
    /*
    #[derive(Clone, Debug, Default)]
    struct SCCNode {
        is_scc_node: bool,
    }

    let ctx: Context<G, ComputeStateVec> = graph.into();
    let step1 = ATask::new(move |vv: &mut EvalNodeView<G, SCCNode>| {
        let id = vv.node;
        let mut out_components = HashSet::new();
        let mut to_check_stack = Vec::new();
        vv.out_neighbours().iter().for_each(|node| {
            let id = node.node;
            out_components.insert(id);
            to_check_stack.push(id);
        });

        while let Some(neighbour_id) = to_check_stack.pop() {
            if let Some(neighbour) = vv.graph().node(neighbour_id) {
                neighbour.out_neighbours().iter().for_each(|node| {
                    let id = node.node;
                    if !out_components.contains(&id) {
                        out_components.insert(id);
                        to_check_stack.push(id);
                    }
                });
            }
        }

        let state: &mut SCCNode = vv.get_mut();
        state.is_scc_node = out_components.into_iter().contains(&id);
        Step::Done
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    let local = runner.run(
        vec![Job::new(step1)],
        vec![],
        None,
        |_, _, _, local: Vec<SCCNode>| local,
        threads,
        1,
        None,
        None,
    );
    let sub_graph = graph.subgraph(
        local
            .iter()
            .enumerate()
            .filter(|(_, state)| state.is_scc_node)
            .map(|(vid, _)| VID(vid)),
    );
     */
    let groups = tarjan_scc(graph);

    let mut values = vec![usize::MAX; graph.unfiltered_num_nodes()];

    for (id, group) in groups.into_iter().enumerate() {
        for VID(node) in group {
            values[node] = id;
        }
    }

    NodeState::new_from_eval(graph.clone(), values)
}
