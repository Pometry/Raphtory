//ALGORITHMS

#[cfg(feature = "storage")]
use crate::python::graph::disk_graph::{PyDiskGraph, PyGraphQuery, PyState};
use crate::{
    add_classes, add_functions,
    python::{
        graph::{
            algorithm_result::AlgorithmResult,
            edge::{PyDirection, PyEdge, PyMutableEdge},
            edges::PyEdges,
            graph::PyGraph,
            graph_with_deletions::PyPersistentGraph,
            index::GraphIndex,
            node::{PyMutableNode, PyNode, PyNodes},
            properties::{PyConstProperties, PyProperties, PyTemporalProp, PyTemporalProperties},
        },
        packages::{
            algorithms::*,
            graph_gen::*,
            graph_loader::*,
            vectors::{generate_property_list, PyVectorisedGraph},
        },
        types::wrappers::document::PyDocument,
    },
};
use pyo3::{prelude::PyModule, PyErr, PyResult, Python};

pub fn add_raphtory_classes(m: &PyModule) -> PyResult<()> {
    //Graph classes
    add_classes!(
        m,
        PyGraph,
        PyPersistentGraph,
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

    #[cfg(feature = "storage")]
    add_classes!(m, PyDiskGraph, PyGraphQuery, PyState);
    return Ok(());
}

pub fn base_algorithm_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
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
        temporal_bipartite_graph_projection,
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

    #[cfg(feature = "storage")]
    add_functions!(algorithm_module, connected_components);
    return Ok(algorithm_module);
}

pub fn base_graph_loader_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    add_functions!(
        graph_loader_module,
        lotr_graph,
        neo4j_movie_graph,
        stable_coin_graph,
        reddit_hyperlink_graph,
        reddit_hyperlink_graph_local,
        karate_club_graph,
    );
    return Ok(graph_loader_module);
}

pub fn base_graph_gen_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    add_functions!(
        graph_gen_module,
        random_attachment,
        ba_preferential_attachment,
    );
    return Ok(graph_gen_module);
}

pub fn base_vectors_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
    let vectors_module = PyModule::new(py, "vectors")?;
    vectors_module.add_class::<PyVectorisedGraph>()?;
    vectors_module.add_class::<PyDocument>()?;
    add_functions!(vectors_module, generate_property_list);
    return Ok(vectors_module);
}
