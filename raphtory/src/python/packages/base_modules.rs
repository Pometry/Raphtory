//ALGORITHMS

#[cfg(feature = "storage")]
use crate::python::graph::disk_graph::PyDiskGraph;
use crate::{
    add_classes, add_functions,
    python::{
        algorithm::max_weight_matching::PyMatching,
        graph::{
            edge::{PyEdge, PyMutableEdge},
            edges::{PyEdges, PyNestedEdges},
            graph::{PyGraph, PyGraphEncoder},
            graph_with_deletions::PyPersistentGraph,
            node::{PyMutableNode, PyNode, PyNodes, PyPathFromGraph, PyPathFromNode},
            properties::{
                PyConstantProperties, PyProperties, PyTemporalProp, PyTemporalProperties,
            },
            views::graph_view::PyGraphView,
        },
        packages::{
            algorithms::*,
            graph_gen::*,
            graph_loader::*,
            vectors::{PyVectorSelection, PyVectorisedGraph},
        },
        types::wrappers::{
            document::PyDocument,
            prop::{PyPropertyFilter, PyPropertyRef},
        },
        utils::PyWindowSet,
    },
};
use pyo3::prelude::*;

pub fn add_raphtory_classes(m: &Bound<PyModule>) -> PyResult<()> {
    //Graph classes
    add_classes!(
        m,
        PyGraphView,
        PyGraph,
        PyPersistentGraph,
        PyGraphEncoder,
        PyNode,
        PyNodes,
        PyPathFromNode,
        PyPathFromGraph,
        PyMutableNode,
        PyEdge,
        PyEdges,
        PyNestedEdges,
        PyMutableEdge,
        PyProperties,
        PyConstantProperties,
        PyTemporalProperties,
        PropertiesView,
        PyTemporalProp,
        PyPropertyRef,
        PyPropertyFilter,
        PyWindowSet,
        PyIndexSpecBuilder,
        PyIndexSpec
    );

    #[cfg(feature = "storage")]
    add_classes!(m, PyDiskGraph);
    Ok(())
}

pub fn base_algorithm_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let algorithm_module = PyModule::new(py, "algorithms")?;
    add_functions!(
        &algorithm_module,
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
        local_clustering_coefficient_batch,
        weakly_connected_components,
        strongly_connected_components,
        in_components,
        in_component,
        out_components,
        out_component,
        fast_rp,
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
        max_weight_matching
    );

    add_classes!(&algorithm_module, PyMatching, PyInfected);
    #[cfg(feature = "storage")]
    add_functions!(&algorithm_module, connected_components);
    Ok(algorithm_module)
}

pub fn base_graph_loader_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    add_functions!(
        &graph_loader_module,
        lotr_graph,
        lotr_graph_with_props,
        neo4j_movie_graph,
        stable_coin_graph,
        reddit_hyperlink_graph,
        reddit_hyperlink_graph_local,
        karate_club_graph,
    );
    Ok(graph_loader_module)
}

pub fn base_graph_gen_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    add_functions!(
        &graph_gen_module,
        random_attachment,
        ba_preferential_attachment,
    );
    Ok(graph_gen_module)
}

pub fn base_vectors_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let vectors_module = PyModule::new(py, "vectors")?;
    vectors_module.add_class::<PyVectorisedGraph>()?;
    vectors_module.add_class::<PyDocument>()?;
    vectors_module.add_class::<PyEmbedding>()?;
    vectors_module.add_class::<PyVectorSelection>()?;
    Ok(vectors_module)
}

pub use crate::python::graph::node_state::base_node_state_module;
use crate::python::{
    algorithm::epidemics::PyInfected,
    graph::{
        index::{PyIndexSpec, PyIndexSpecBuilder},
        properties::PropertiesView,
    },
    types::wrappers::document::PyEmbedding,
};
