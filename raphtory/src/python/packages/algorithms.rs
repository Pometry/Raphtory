#![allow(non_snake_case)]
use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult,
        centrality::{
            betweenness::betweenness_centrality as betweenness_rs,
            degree_centrality::degree_centrality as degree_centrality_rs, hits::hits as hits_rs,
            pagerank::unweighted_page_rank,
        },
        community_detection::{
            label_propagation::label_propagation as label_propagation_rs,
            louvain::louvain as louvain_rs, modularity::ModularityUnDir,
        },
        components,
        dynamics::temporal::epidemics::{temporal_SEIR as temporal_SEIR_rs, Infected, SeedError},
        layout::fruchterman_reingold::fruchterman_reingold as fruchterman_reingold_rs,
        metrics::{
            balance::balance as balance_rs,
            degree::{
                average_degree as average_degree_rs, max_degree as max_degree_rs,
                max_in_degree as max_in_degree_rs, max_out_degree as max_out_degree_rs,
                min_degree as min_degree_rs, min_in_degree as min_in_degree_rs,
                min_out_degree as min_out_degree_rs,
            },
            directed_graph_density::directed_graph_density as directed_graph_density_rs,
            local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs,
            reciprocity::{
                all_local_reciprocity as all_local_reciprocity_rs,
                global_reciprocity as global_reciprocity_rs,
            },
        },
        motifs::{
            global_temporal_three_node_motifs::{
                global_temporal_three_node_motif as global_temporal_three_node_motif_rs,
                temporal_three_node_motif_multi as global_temporal_three_node_motif_general_rs,
            },
            local_temporal_three_node_motifs::temporal_three_node_motif as local_three_node_rs,
            local_triangle_count::local_triangle_count as local_triangle_count_rs,
        },
        pathing::{
            dijkstra::dijkstra_single_source_shortest_paths as dijkstra_single_source_shortest_paths_rs,
            single_source_shortest_path::single_source_shortest_path as single_source_shortest_path_rs,
            temporal_reachability::temporally_reachable_nodes as temporal_reachability_rs,
        },
    },
    core::{entities::nodes::node_ref::NodeRef, Prop},
    db::{
        api::view::internal::{DelegateCoreOps, DynamicGraph},
        graph::node::NodeView,
    },
    python::{
        graph::{edge::PyDirection, views::graph_view::PyGraphView},
        utils::{PyInputNode, PyTime},
    },
};
use ordered_float::OrderedFloat;
use pyo3::{exceptions::PyGeneratorExit, prelude::*};
use rand::{prelude::StdRng, SeedableRng};
use std::collections::{HashMap, HashSet};

/// Implementations of various graph algorithms that can be run on a graph.
///
/// To run an algorithm simply import the module and call the function with the graph as the argument
///

/// Local triangle count - calculates the number of triangles (a cycle of length 3) a node participates in.
///
/// This function returns the number of pairs of neighbours of a given node which are themselves connected.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph, this can be directed or undirected but will be treated as undirected
///     v (int or str) : node id or name
///
/// Returns:
///     triangles(int) : number of triangles associated with node v
///
#[pyfunction]
pub fn local_triangle_count(g: &PyGraphView, v: NodeRef) -> Option<usize> {
    local_triangle_count_rs(&g.graph, v)
}

/// Weakly connected components -- partitions the graph into node sets which are mutually reachable by an undirected path
///
/// This function assigns a component id to each node such that nodes with the same component id are mutually reachable
/// by an undirected path.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if the labels converge prior to the number of iterations being reached.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping nodes to their component ids.
#[pyfunction]
#[pyo3(signature = (g, iter_count=9223372036854775807))]
pub fn weakly_connected_components(
    g: &PyGraphView,
    iter_count: usize,
) -> AlgorithmResult<DynamicGraph, u64, u64> {
    components::weakly_connected_components(&g.graph, iter_count, None)
}

/// Strongly connected components
///
/// Partitions the graph into node sets which are mutually reachable by an directed path
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///
/// Returns:
///     Vec<Vec<u64>> : List of strongly connected nodes identified by ids
#[pyfunction]
#[pyo3(signature = (g))]
pub fn strongly_connected_components(g: &PyGraphView) -> Vec<Vec<u64>> {
    components::strongly_connected_components(&g.graph, None)
}

/// In components -- Finding the "in-component" of a node in a directed graph involves identifying all nodes that can be reached following only incoming edges.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping each node to an array containing the ids of all nodes within their 'in-component'
#[pyfunction]
#[pyo3(signature = (g))]
pub fn in_components(g: &PyGraphView) -> AlgorithmResult<DynamicGraph, Vec<u64>, Vec<u64>> {
    components::in_components(&g.graph, None)
}

/// Out components -- Finding the "out-component" of a node in a directed graph involves identifying all nodes that can be reached following only outgoing edges.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult object mapping each node to an array containing the ids of all nodes within their 'out-component'
#[pyfunction]
#[pyo3(signature = (g))]
pub fn out_components(g: &PyGraphView) -> AlgorithmResult<DynamicGraph, Vec<u64>, Vec<u64>> {
    components::out_components(&g.graph, None)
}

/// Pagerank -- pagerank centrality value of the nodes in a graph
///
/// This function calculates the Pagerank value of each node in a graph. See https://en.wikipedia.org/wiki/PageRank for more information on PageRank centrality.
/// A default damping factor of 0.85 is used. This is an iterative algorithm which terminates if the sum of the absolute difference in pagerank values between iterations
/// is less than the max diff value given.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph
///     iter_count (int) : Maximum number of iterations to run. Note that this will terminate early if convergence is reached.
///     max_diff (float) : Optional parameter providing an alternative stopping condition. The algorithm will terminate if the sum of the absolute difference in pagerank values between iterations
/// is less than the max diff value given.
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping node names to their pagerank value.
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, max_diff=None))]
pub fn pagerank(
    g: &PyGraphView,
    iter_count: usize,
    max_diff: Option<f64>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    unweighted_page_rank(&g.graph, iter_count, None, max_diff, true)
}

/// Temporally reachable nodes -- the nodes that are reachable by a time respecting path followed out from a set of seed nodes at a starting time.
///
/// This function starts at a set of seed nodes and follows all time respecting paths until either a) a maximum number of hops is reached, b) one of a set of
/// stop nodes is reached, or c) no further time respecting edges exist. A time respecting path is a sequence of nodes v_1, v_2, ... , v_k such that there exists
/// a sequence of edges (v_i, v_i+1, t_i) with t_i < t_i+1 for i = 1, ... , k - 1.
///
/// Arguments:
///     g (Raphtory graph) : directed Raphtory graph
///     max_hops (int) : maximum number of hops to propagate out
///     start_time (int) : time at which to start the path (such that t_1 > start_time for any path starting from these seed nodes)
///     seed_nodes (list(str) or list(int)) : list of node names or ids which should be the starting nodes
///     stop_nodes (list(str) or list(int)) : nodes at which a path shouldn't go any further
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping node names to their pagerank value.
#[pyfunction]
pub fn temporally_reachable_nodes(
    g: &PyGraphView,
    max_hops: usize,
    start_time: i64,
    seed_nodes: Vec<PyInputNode>,
    stop_nodes: Option<Vec<PyInputNode>>,
) -> AlgorithmResult<DynamicGraph, Vec<(i64, String)>, Vec<(i64, String)>> {
    temporal_reachability_rs(&g.graph, None, max_hops, start_time, seed_nodes, stop_nodes)
}

/// Local clustering coefficient - measures the degree to which nodes in a graph tend to cluster together.
///
/// The proportion of pairs of neighbours of a node who are themselves connected.
///
/// Arguments:
///     g (Raphtory graph) : Raphtory graph, can be directed or undirected but will be treated as undirected.
///     v (int or str): node id or name
///
/// Returns:
///     float : the local clustering coefficient of node v in g.
#[pyfunction]
pub fn local_clustering_coefficient(g: &PyGraphView, v: NodeRef) -> Option<f32> {
    local_clustering_coefficient_rs(&g.graph, v)
}

/// Graph density - measures how dense or sparse a graph is.
///
/// The ratio of the number of directed edges in the graph to the total number of possible directed
/// edges (given by N * (N-1) where N is the number of nodes).
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     float : Directed graph density of G.
#[pyfunction]
pub fn directed_graph_density(g: &PyGraphView) -> f32 {
    directed_graph_density_rs(&g.graph)
}

/// The average (undirected) degree of all nodes in the graph.
///
/// Note that this treats the graph as simple and undirected and is equal to twice
/// the number of undirected edges divided by the number of nodes.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph
///
/// Returns:
///     float : the average degree of the nodes in the graph
#[pyfunction]
pub fn average_degree(g: &PyGraphView) -> f64 {
    average_degree_rs(&g.graph)
}

/// The maximum out degree of any node in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest outdegree
#[pyfunction]
pub fn max_out_degree(g: &PyGraphView) -> usize {
    max_out_degree_rs(&g.graph)
}

/// The maximum in degree of any node in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the largest indegree
#[pyfunction]
pub fn max_in_degree(g: &PyGraphView) -> usize {
    max_in_degree_rs(&g.graph)
}

/// The minimum out degree of any node in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the smallest outdegree
#[pyfunction]
pub fn min_out_degree(g: &PyGraphView) -> usize {
    min_out_degree_rs(&g.graph)
}

/// The minimum in degree of any node in the graph.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     int : value of the smallest indegree
#[pyfunction]
pub fn min_in_degree(g: &PyGraphView) -> usize {
    min_in_degree_rs(&g.graph)
}

/// Reciprocity - measure of the symmetry of relationships in a graph, the global reciprocity of
/// the entire graph.
/// This calculates the number of reciprocal connections (edges that go in both directions) in a
/// graph and normalizes it by the total number of directed edges.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     float : reciprocity of the graph between 0 and 1.

#[pyfunction]
pub fn global_reciprocity(g: &PyGraphView) -> f64 {
    global_reciprocity_rs(&g.graph, None)
}

/// Local reciprocity - measure of the symmetry of relationships associated with a node
///
/// This measures the proportion of a node's outgoing edges which are reciprocated with an incoming edge.
///
/// Arguments:
///     g (Raphtory graph) : a directed Raphtory graph
///
/// Returns:
///     AlgorithmResult : AlgorithmResult with string keys and float values mapping each node name to its reciprocity value.
///
#[pyfunction]
pub fn all_local_reciprocity(
    g: &PyGraphView,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    all_local_reciprocity_rs(&g.graph, None)
}

/// Computes the number of connected triplets within a graph
///
/// A connected triplet (also known as a wedge, 2-hop path) is a pair of edges with one node in common. For example, the triangle made up of edges
/// A-B, B-C, C-A is formed of three connected triplets.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph, treated as undirected
///
/// Returns:
///     int : the number of triplets in the graph
#[pyfunction]
pub fn triplet_count(g: &PyGraphView) -> usize {
    crate::algorithms::motifs::triplet_count::triplet_count(&g.graph, None)
}

/// Computes the global clustering coefficient of a graph. The global clustering coefficient is
/// defined as the number of triangles in the graph divided by the number of triplets in the graph.
///
/// Note that this is also known as transitivity and is different to the average clustering coefficient.
///
/// Arguments:
///     g (Raphtory graph) : a Raphtory graph, treated as undirected
///
/// Returns:
///     float : the global clustering coefficient of the graph
///
/// See also:
///     [`Triplet Count`](triplet_count)
#[pyfunction]
pub fn global_clustering_coefficient(g: &PyGraphView) -> f64 {
    crate::algorithms::metrics::clustering_coefficient::clustering_coefficient(&g.graph)
}

/// Computes the number of three edge, up-to-three node delta-temporal motifs in the graph, using the algorithm of Paranjape et al, Motifs in Temporal Networks (2017).
/// We point the reader to this reference for more information on the algorithm and background, but provide a short summary below.
///
///  Motifs included:
///
///  Stars
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
///  Two node motifs:
///
///  Also included are two node motifs, of which there are 8 when counted from the perspective of each node. These are characterised by the direction of each edge, enumerated
///  in the above order. Note that for the global graph counts, each motif is counted in both directions (a single III motif for one node is an OOO motif for the other node).
///
///  Triangles:
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
/// Arguments:
///     g (raphtory graph) : A directed raphtory graph
///     delta (int): Maximum time difference between the first and last edge of the motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     list : A 40 dimensional array with the counts of each motif, given in the same order as described above. Note that the two-node motif counts are symmetrical so it may be more useful just to consider the first four elements.
///
/// Notes:
///     This is achieved by calling the local motif counting algorithm, summing the resulting arrays and dealing with overcounted motifs: the triangles (by dividing each motif count by three) and two-node motifs (dividing by two).
///
#[pyfunction]
pub fn global_temporal_three_node_motif(g: &PyGraphView, delta: i64) -> [usize; 40] {
    global_temporal_three_node_motif_rs(&g.graph, delta, None)
}

/// Computes the global counts of three-edge up-to-three node temporal motifs for a range of timescales. See `global_temporal_three_node_motif` for an interpretation of each row returned.
///
/// Arguments:
///     g (raphtory graph) : A directed raphtory graph
///     deltas(list(int)): A list of delta values to use.
///
/// Returns:
///     list(list(int)) : A list of 40d arrays, each array is the motif count for a particular value of delta, returned in the order that the deltas were given as input.
#[pyfunction]
pub fn global_temporal_three_node_motif_multi(
    g: &PyGraphView,
    deltas: Vec<i64>,
) -> Vec<[usize; 40]> {
    global_temporal_three_node_motif_general_rs(&g.graph, deltas, None)
}

/// Computes the number of each type of motif that each node participates in. See global_temporal_three_node_motifs for a summary of the motifs involved.
///
/// Arguments:
///     g (raphtory graph) : A directed raphtory graph
///     delta (int): Maximum time difference between the first and last edge of the motif. NB if time for edges was given as a UNIX epoch, this should be given in seconds, otherwise milliseconds should be used (if edge times were given as string)
///
/// Returns:
///     dict : A dictionary with node ids as keys and a 40d array of motif counts as values (in the same order as the global motif counts) with the number of each motif that node participates in.
///
/// Notes:
///     For this local count, a node is counted as participating in a motif in the following way. For star motifs, only the centre node counts
///    the motif. For two node motifs, both constituent nodes count the motif. For triangles, all three constituent nodes count the motif.
#[pyfunction]
pub fn local_temporal_three_node_motifs(
    g: &PyGraphView,
    delta: i64,
) -> HashMap<String, Vec<usize>> {
    local_three_node_rs(&g.graph, vec![delta], None)
        .into_iter()
        .map(|(k, v)| (String::from(k), v[0].clone()))
        .collect::<HashMap<String, Vec<usize>>>()
}

/// HITS (Hubs and Authority) Algorithm:
/// AuthScore of a node (A) = Sum of HubScore of all nodes pointing at node (A) from previous iteration /
///     Sum of HubScore of all nodes in the current iteration
///
/// HubScore of a node (A) = Sum of AuthScore of all nodes pointing away from node (A) from previous iteration /
///     Sum of AuthScore of all nodes in the current iteration
///
/// Arguments:
///     g (Raphtory Graph): Graph to run the algorithm on
///     iter_count (int): How many iterations to run the algorithm
///     threads (int): Number of threads to use (optional)
///
/// Returns
///     An AlgorithmResult object containing the mapping from node ID to the hub and authority score of the node
#[pyfunction]
#[pyo3(signature = (g, iter_count=20, threads=None))]
pub fn hits(
    g: &PyGraphView,
    iter_count: usize,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
    hits_rs(&g.graph, iter_count, threads)
}

/// Sums the weights of edges in the graph based on the specified direction.
///
/// This function computes the sum of edge weights based on the direction provided, and can be executed in parallel using a given number of threads.
///
/// Arguments:
///     g (Raphtory Graph): The graph view on which the operation is to be performed.
///     name (str, default = "weight"): The name of the edge property used as the weight. Defaults to "weight" if not provided.
///     direction (`PyDirection`, default = PyDirection("BOTH")): Specifies the direction of the edges to be considered for summation.
///             * PyDirection("OUT"): Only consider outgoing edges.
///             * PyDirection("IN"): Only consider incoming edges.
///             * PyDirection("BOTH"): Consider both outgoing and incoming edges. This is the default.
///     threads (int, default = `None`): The number of threads to be used for parallel execution. Defaults to single-threaded operation if not provided.
///
/// Returns:
///     AlgorithmResult<String, OrderedFloat<f64>>: A result containing a mapping of node names to the computed sum of their associated edge weights.
///
#[pyfunction]
#[pyo3[signature = (g, name="weight".to_string(), direction=PyDirection::new("BOTH"),  threads=None)]]
pub fn balance(
    g: &PyGraphView,
    name: String,
    direction: PyDirection,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    balance_rs(&g.graph, name.clone(), direction.into(), threads)
}

/// Computes the degree centrality of all nodes in the graph. The values are normalized
/// by dividing each result with the maximum possible degree. Graphs with self-loops can have
/// values of centrality greater than 1.
///
/// Arguments:
///     g (Raphtory Graph): The graph view on which the operation is to be performed.
///     threads (`Option<usize>`, default = `None`): The number of threads to be used for parallel execution. Defaults to single-threaded operation if not provided.
///
/// Returns:
///     AlgorithmResult<String, OrderedFloat<f64>>: A result containing a mapping of node names to the computed sum of their associated degree centrality.
#[pyfunction]
#[pyo3[signature = (g, threads=None)]]
pub fn degree_centrality(
    g: &PyGraphView,
    threads: Option<usize>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    degree_centrality_rs(&g.graph, threads)
}

/// Returns the largest degree found in the graph
///
/// Arguments:
///     g (Raphtory Graph): The graph view on which the operation is to be performed.
///
/// Returns:
///     usize: The largest degree
#[pyfunction]
#[pyo3[signature = (g)]]
pub fn max_degree(g: &PyGraphView) -> usize {
    max_degree_rs(&g.graph)
}

/// Returns the smallest degree found in the graph
///
/// Arguments:
///     g (Raphtory Graph): The graph view on which the operation is to be performed.
///
/// Returns:
///     usize: The smallest degree found
#[pyfunction]
#[pyo3[signature = (g)]]
pub fn min_degree(g: &PyGraphView) -> usize {
    min_degree_rs(&g.graph)
}

/// Calculates the single source shortest paths from a given source node.
///
/// Arguments:
///     g (Raphtory Graph): A reference to the graph. Must implement `GraphViewOps`.
///     source (InputNode): The source node. Must implement `InputNode`.
///     cutoff (Int, Optional): An optional cutoff level. The algorithm will stop if this level is reached.
///
/// Returns:
///     Returns an `AlgorithmResult<String, Vec<String>>` containing the shortest paths from the source to all reachable nodes.
///
#[pyfunction]
#[pyo3[signature = (g, source, cutoff=None)]]
pub fn single_source_shortest_path(
    g: &PyGraphView,
    source: PyInputNode,
    cutoff: Option<usize>,
) -> AlgorithmResult<DynamicGraph, Vec<String>, Vec<String>> {
    single_source_shortest_path_rs(&g.graph, source, cutoff)
}

/// Finds the shortest paths from a single source to multiple targets in a graph.
///
/// Arguments:
///     g (Raphtory Graph): The graph to search in.
///     source (InputNode): The source node.
///     targets (List(InputNodes)): A list of target nodes.
///     weight (String, Optional): The name of the weight property for the edges ("weight" is default).
///
/// Returns:
///     Returns a `Dict` where the key is the target node and the value is a tuple containing the total cost and a vector of nodes representing the shortest path.
///
#[pyfunction]
#[pyo3[signature = (g, source, targets, weight="weight".to_string())]]
pub fn dijkstra_single_source_shortest_paths(
    g: &PyGraphView,
    source: PyInputNode,
    targets: Vec<PyInputNode>,
    weight: String,
) -> PyResult<HashMap<String, (Prop, Vec<String>)>> {
    match dijkstra_single_source_shortest_paths_rs(&g.graph, source, targets, weight) {
        Ok(result) => Ok(result),
        Err(err_msg) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(err_msg)),
    }
}

/// Computes the betweenness centrality for nodes in a given graph.
///
/// Arguments:
///     g (Raphtory Graph): A reference to the graph.
///     k (int, optional): Specifies the number of nodes to consider for the centrality computation. Defaults to all nodes if `None`.
///     normalized (boolean, optional): Indicates whether to normalize the centrality values.
///
/// Returns:
///     AlgorithmResult[float]: Returns an `AlgorithmResult` containing the betweenness centrality of each node.
#[pyfunction]
#[pyo3[signature = (g, k=None, normalized=true)]]
pub fn betweenness_centrality(
    g: &PyGraphView,
    k: Option<usize>,
    normalized: Option<bool>,
) -> AlgorithmResult<DynamicGraph, f64, OrderedFloat<f64>> {
    betweenness_rs(&g.graph, k, normalized)
}

/// Computes components using a label propagation algorithm
///
/// Arguments:
///     g (Raphtory Graph): A reference to the graph
///     seed (Array of ints, optional) Array of 32 bytes of u8 which is set as the rng seed
///
/// Returns:
///     A list of sets each containing nodes that have been grouped
///
#[pyfunction]
#[pyo3[signature = (g, seed=None)]]
pub fn label_propagation(
    g: &PyGraphView,
    seed: Option<[u8; 32]>,
) -> PyResult<Vec<HashSet<NodeView<DynamicGraph>>>> {
    match label_propagation_rs(&g.graph, seed) {
        Ok(result) => Ok(result),
        Err(err_msg) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(err_msg)),
    }
}

/// Simulate an SEIR dynamic on the network
///
/// The algorithm uses the event-based sampling strategy from https://doi.org/10.1371/journal.pone.0246961
///
/// Arguments:
///     graph (GraphView): the graph view
///     seeds (int | float | list[Node]): the seeding strategy to use for the initial infection (if `int`, choose fixed number
///            of nodes at random, if `float` infect each node with this probability, if `[Node]`
///            initially infect the specified nodes
///     infection_prob (float): the probability for a contact between infected and susceptible nodes to lead
///                     to a transmission
///     initial_infection (int | str | DateTime): the time of the initial infection
///     recovery_rate (float | None): optional recovery rate (if None, simulates SEI dynamic where nodes never recover)
///                    the actual recovery time is sampled from an exponential distribution with this rate
///     incubation_rate ( float | None): optional incubation rate (if None, simulates SI or SIR dynamics where infected
///                      nodes are infectious at the next time step)
///                      the actual incubation time is sampled from an exponential distribution with
///                      this rate
///     rng_seed (int | None): optional seed for the random number generator
///
/// Returns:
///     AlgorithmResult[Infected]: Returns an `Infected` object for each infected node with attributes
///     
///     `infected`: the time stamp of the infection event
///
///     `active`: the time stamp at which the node actively starts spreading the infection (i.e., the end of the incubation period)
///
///     `recovered`: the time stamp at which the node recovered (i.e., stopped spreading the infection)
///
#[pyfunction(name = "temporal_SEIR")]
pub fn temporal_SEIR(
    graph: &PyGraphView,
    seeds: crate::python::algorithm::epidemics::PySeed,
    infection_prob: f64,
    initial_infection: PyTime,
    recovery_rate: Option<f64>,
    incubation_rate: Option<f64>,
    rng_seed: Option<u64>,
) -> Result<AlgorithmResult<DynamicGraph, Infected>, SeedError> {
    let mut rng = match rng_seed {
        None => StdRng::from_entropy(),
        Some(seed) => StdRng::seed_from_u64(seed),
    };
    temporal_SEIR_rs(
        &graph.graph,
        recovery_rate,
        incubation_rate,
        infection_prob,
        initial_infection,
        seeds,
        &mut rng,
    )
}

/// Louvain algorithm for community detection
///
/// Arguments:
///     graph (GraphView): the graph view
///     resolution (float): the resolution paramter for modularity
///     weight_prop (str | None): the edge property to use for weights (has to be float)
///     tol (None | float): the floating point tolerance for deciding if improvements are significant (default: 1e-8)
#[pyfunction]
#[pyo3[signature=(graph, resolution=1.0, weight_prop=None, tol=None)]]
pub fn louvain(
    graph: &PyGraphView,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
) -> AlgorithmResult<DynamicGraph, usize> {
    louvain_rs::<ModularityUnDir, _>(&graph.graph, resolution, weight_prop, tol)
}

#[pyfunction]
#[pyo3[signature=(graph, iterations=100, width=100.0, height=100.0,repulsion=2.0,attraction=2.0)]]
pub fn fruchterman_reingold(
    graph: &PyGraphView,
    iterations: u64,
    width: f64,
    height: f64,
    repulsion: f64,
    attraction: f64,
) -> HashMap<u64, [f64; 2]> {
    fruchterman_reingold_rs(
        &graph.graph,
        iterations,
        width,
        height,
        repulsion,
        attraction,
    )
}
