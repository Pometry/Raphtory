//ALGORITHMS
use crate::{
    add_classes, add_functions,
    python::{
        algorithm::{epidemics::PyInfected, max_weight_matching::PyMatching},
        graph::{
            edge::{PyEdge, PyMutableEdge},
            edges::{PyEdges, PyNestedEdges},
            graph::{PyGraph, PyGraphEncoder},
            graph_with_deletions::PyPersistentGraph,
            node::{PyMutableNode, PyNode, PyNodes, PyPathFromGraph, PyPathFromNode},
            properties::{
                PropertiesView, PyMetadata, PyPropValueList, PyProperties, PyTemporalProp,
                PyTemporalProperties,
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
            document::{PyDocument, PyEmbedding},
            iterables::{
                ArcStringIterable, ArcStringVecIterable, BoolIterable, GIDGIDIterable, GIDIterable,
                NestedArcStringVecIterable, NestedBoolIterable, NestedGIDGIDIterable,
                NestedGIDIterable, NestedI64VecIterable, NestedOptionArcStringIterable,
                NestedOptionI64Iterable, NestedStringIterable, NestedUsizeIterable,
                NestedUtcDateTimeIterable, NestedVecUtcDateTimeIterable, OptionArcStringIterable,
                OptionI64Iterable, OptionUtcDateTimeIterable, OptionVecUtcDateTimeIterable,
                StringIterable, U64Iterable, UsizeIterable,
            },
        },
        utils::PyWindowSet,
    },
};
use pyo3::prelude::*;

#[cfg(feature = "search")]
use crate::python::graph::index::{PyIndexSpec, PyIndexSpecBuilder};

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
        PyPropValueList,
        PyMetadata,
        PyTemporalProperties,
        PropertiesView,
        PyTemporalProp,
        PyWindowSet,
    );

    #[cfg(feature = "search")]
    add_classes!(m, PyIndexSpecBuilder, PyIndexSpec);

    #[pyfunction]
    /// Return Raphtory version.
    ///
    /// Returns:
    ///     str:
    pub(crate) fn version() -> String {
        String::from(crate::version())
    }

    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}

pub fn base_iterables_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let iterables_module = PyModule::new(py, "iterables")?;
    add_classes!(
        iterables_module,
        NestedUtcDateTimeIterable,
        NestedGIDIterable,
        GIDIterable,
        StringIterable,
        OptionArcStringIterable,
        UsizeIterable,
        OptionI64Iterable,
        NestedOptionArcStringIterable,
        NestedStringIterable,
        NestedOptionI64Iterable,
        NestedI64VecIterable,
        NestedUsizeIterable,
        BoolIterable,
        ArcStringIterable,
        NestedVecUtcDateTimeIterable,
        OptionVecUtcDateTimeIterable,
        GIDGIDIterable,
        NestedGIDGIDIterable,
        NestedBoolIterable,
        U64Iterable,
        OptionUtcDateTimeIterable,
        ArcStringVecIterable,
        NestedArcStringVecIterable,
    );
    Ok(iterables_module)
}

pub fn base_algorithm_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
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
        k_core,
        temporal_SEIR,
        louvain,
        fruchterman_reingold,
        cohesive_fruchterman_reingold,
        max_weight_matching
    );

    add_classes!(&algorithm_module, PyMatching, PyInfected);
    Ok(algorithm_module)
}

pub fn base_graph_loader_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
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

pub fn base_graph_gen_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    add_functions!(
        &graph_gen_module,
        random_attachment,
        ba_preferential_attachment,
    );
    Ok(graph_gen_module)
}

pub fn base_vectors_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let vectors_module = PyModule::new(py, "vectors")?;
    vectors_module.add_class::<PyVectorisedGraph>()?;
    vectors_module.add_class::<PyDocument>()?;
    vectors_module.add_class::<PyEmbedding>()?;
    vectors_module.add_class::<PyVectorSelection>()?;
    Ok(vectors_module)
}

pub use crate::python::graph::node_state::base_node_state_module;
