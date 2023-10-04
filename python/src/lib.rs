mod graphql;

extern crate core;
use graphql::*;
use pyo3::prelude::*;
use raphtory_core::python::{
    graph::{
        algorithm_result::AlgorithmResult,
        edge::{PyDirection, PyEdge, PyEdges},
        graph::PyGraph,
        graph_with_deletions::PyGraphWithDeletions,
        properties::{PyConstProperties, PyProperties, PyTemporalProp, PyTemporalProperties},
        vertex::{PyVertex, PyVertices},
    },
    packages::{algorithms::*, graph_gen::*, graph_loader::*},
};

macro_rules! add_functions {
    ($module:expr, $($func:ident),* $(,)?) => {
        $(
            $module.add_function(wrap_pyfunction!($func, $module)?)?;
        )*
    };
}

/// Raphtory graph analytics library
#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    //Graph classes
    m.add_class::<PyGraph>()?;
    m.add_class::<PyGraphWithDeletions>()?;
    m.add_class::<PyVertex>()?;
    m.add_class::<PyVertices>()?;
    m.add_class::<PyEdge>()?;
    m.add_class::<PyEdges>()?;
    m.add_class::<PyProperties>()?;
    m.add_class::<PyConstProperties>()?;
    m.add_class::<PyTemporalProperties>()?;
    m.add_class::<PyTemporalProp>()?;
    m.add_class::<PyDirection>()?;

    //GRAPHQL
    let graphql_module = PyModule::new(py, "internal_graphql")?;
    graphql_module.add_function(wrap_pyfunction!(from_map, graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(from_directory, graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(from_map_and_directory, graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(encode_graph, graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(decode_graph, graphql_module)?)?;
    m.add_submodule(graphql_module)?;

    //ALGORITHMS
    m.add_class::<AlgorithmResult>()?;
    let algorithm_module = PyModule::new(py, "algorithms")?;
    add_functions!(
        algorithm_module,
        global_reciprocity,
        all_local_reciprocity,
        triplet_count,
        local_triangle_count,
        average_degree,
        directed_graph_density,
        degree_centrality,
        max_degree,
        min_degree,
        max_out_degree,
        max_in_degree,
        min_out_degree,
        min_in_degree,
        pagerank,
        single_source_shortest_path,
        global_clustering_coefficient,
        temporally_reachable_nodes,
        local_clustering_coefficient,
        weakly_connected_components,
        global_temporal_three_node_motif,
        global_temporal_three_node_motif_multi,
        local_temporal_three_node_motifs,
        hits,
        balance
    );
    m.add_submodule(algorithm_module)?;

    //GRAPH LOADER
    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    graph_loader_module.add_function(wrap_pyfunction!(lotr_graph, graph_loader_module)?)?;
    graph_loader_module.add_function(wrap_pyfunction!(neo4j_movie_graph, graph_loader_module)?)?;
    graph_loader_module.add_function(wrap_pyfunction!(stable_coin_graph, graph_loader_module)?)?;
    graph_loader_module.add_function(wrap_pyfunction!(
        reddit_hyperlink_graph,
        graph_loader_module
    )?)?;
    m.add_submodule(graph_loader_module)?;

    //GRAPH GENERATOR
    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    graph_gen_module.add_function(wrap_pyfunction!(random_attachment, graph_gen_module)?)?;
    graph_gen_module.add_function(wrap_pyfunction!(
        ba_preferential_attachment,
        graph_gen_module
    )?)?;
    m.add_submodule(graph_gen_module)?;

    // TODO: re-enable
    //VECTORS
    // let vectors_module = PyModule::new(py, "vectors")?;
    // vectors_module.add_class::<PyVectorizedGraph>()?;
    // m.add_submodule(vectors_module)?;

    Ok(())
}
