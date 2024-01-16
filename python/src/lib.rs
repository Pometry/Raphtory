mod graphql;

extern crate core;
use graphql::*;
use pyo3::prelude::*;
use raphtory_core::python::{
    graph::{
        algorithm_result::AlgorithmResult,
        edge::{PyDirection, PyEdge, PyMutableEdge},
        edges::PyEdges,
        graph::PyGraph,
        graph_with_deletions::PyGraphWithDeletions,
        index::GraphIndex,
        node::{PyMutableNode, PyNode, PyNodes},
        properties::{PyConstProperties, PyProperties, PyTemporalProp, PyTemporalProperties},
    },
    packages::{
        algorithms::*,
        graph_gen::*,
        graph_loader::*,
        vectors::{PyGraphDocument, PyVectorisedGraph},
    },
};

macro_rules! add_functions {
    ($module:expr, $($func:ident),* $(,)?) => {
        $(
            $module.add_function(wrap_pyfunction!($func, $module)?)?;
        )*
    };
}

macro_rules! add_classes {
    ($module:expr, $($cls:ty),* $(,)?) => {
        $(
            $module.add_class::<$cls>()?;
        )*
    };
}

/// Raphtory graph analytics library
#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    //Graph classes
    add_classes!(
        m,
        PyGraph,
        PyGraphWithDeletions,
        PyNode,
        PyNodes,
        PyMutableNode,
        PyEdge,
        PyEdges,
        PyMutableEdge,
        PyProperties,
        PyConstProperties,
        PyTemporalProperties,
        PyTemporalProp,
        PyDirection,
        AlgorithmResult,
        GraphIndex
    );

    //GRAPHQL
    let graphql_module = PyModule::new(py, "graphql")?;
    graphql_module.add_class::<PyRaphtoryServer>()?;
    graphql_module.add_class::<PyRunningRaphtoryServer>()?;
    graphql_module.add_class::<PyRaphtoryClient>()?;
    m.add_submodule(graphql_module)?;

    //ALGORITHMS
    let algorithm_module = PyModule::new(py, "algorithms")?;
    add_functions!(
        algorithm_module,
        dijkstra_single_source_shortest_paths,
        global_reciprocity,
        betweenness_centrality,
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
        strongly_connected_components,
        in_components,
        out_components,
        global_temporal_three_node_motif,
        global_temporal_three_node_motif_multi,
        local_temporal_three_node_motifs,
        hits,
        balance,
        label_propagation,
        temporal_SEIR,
        louvain,
        fruchterman_reingold,
        cohesive_fruchterman_reingold,
    );
    m.add_submodule(algorithm_module)?;

    // let usecase_algorithm_module = PyModule::new(py, "usecase_algorithms")?;
    // add_functions!(usecase_algorithm_module, one_path_node);
    // m.add_submodule(usecase_algorithm_module)?;

    //GRAPH LOADER
    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    add_functions!(
        graph_loader_module,
        lotr_graph,
        neo4j_movie_graph,
        stable_coin_graph,
        reddit_hyperlink_graph,
        karate_club_graph,
    );
    m.add_submodule(graph_loader_module)?;

    //GRAPH GENERATOR
    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    add_functions!(
        graph_gen_module,
        random_attachment,
        ba_preferential_attachment,
    );
    m.add_submodule(graph_gen_module)?;

    // VECTORS
    let vectors_module = PyModule::new(py, "vectors")?;
    vectors_module.add_class::<PyVectorisedGraph>()?;
    vectors_module.add_class::<PyGraphDocument>()?;
    m.add_submodule(vectors_module)?;

    Ok(())
}
