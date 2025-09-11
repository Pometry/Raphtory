//ALGORITHMS

#[cfg(feature = "storage")]
use crate::python::graph::disk_graph::PyDiskGraph;
use crate::{
    add_classes, add_functions,
    python::{
        algorithm::{epidemics::PyInfected, max_weight_matching::PyMatching},
        filter::node_filter_builders::PyNodeFilterBuilder,
        graph::{
            edge::{PyEdge, PyMutableEdge},
            edges::{PyEdges, PyNestedEdges},
            graph::{PyGraph, PyGraphEncoder},
            graph_with_deletions::PyPersistentGraph,
            history::{
                HistoryDateTimeIterable, HistoryIterable, HistorySecondaryIndexIterable,
                HistoryTimestampIterable, IntervalsIterable, NestedHistoryDateTimeIterable,
                NestedHistoryIterable, NestedHistorySecondaryIndexIterable,
                NestedHistoryTimestampIterable, NestedIntervalsIterable, PyHistory,
                PyHistoryDateTime, PyHistorySecondaryIndex, PyHistoryTimestamp, PyIntervals,
            },
            index::{PyIndexSpec, PyIndexSpecBuilder},
            node::{PyMutableNode, PyNode, PyNodes, PyPathFromGraph, PyPathFromNode},
            properties::{
                MetadataView, PropertiesView, PyMetadata, PyProperties, PyTemporalProp,
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
        types::{
            result_iterable::{
                NestedResultOptionUtcDateTimeIterable, NestedResultUtcDateTimeIterable,
                ResultOptionUtcDateTimeIterable, ResultUtcDateTimeIterable,
            },
            wrappers::{
                document::{PyDocument, PyEmbedding},
                iterables::{
                    I64Iterable, NestedI64Iterable, NestedOptionI64Iterable,
                    NestedOptionTimeIndexEntryIterable, NestedOptionUsizeIterable,
                    NestedTimeIndexEntryIterable, OptionI64Iterable, OptionTimeIndexEntryIterable,
                    OptionUsizeIterable, TimeIndexEntryIterable,
                },
            },
        },
        utils::PyWindowSet,
    },
};
use pyo3::prelude::*;
use raphtory_api::python::timeindex::PyTimeIndexEntry;

pub fn add_raphtory_classes(m: &Bound<PyModule>) -> PyResult<()> {
    //Graph classes
    add_classes!(
        m,
        PyGraphView,
        PyGraph,
        PyPersistentGraph,
        PyGraphEncoder,
        PyNode,
        PyNodeFilterBuilder,
        PyNodes,
        PyPathFromNode,
        PyPathFromGraph,
        PyMutableNode,
        PyEdge,
        PyEdges,
        PyNestedEdges,
        PyMutableEdge,
        PyProperties,
        PyMetadata,
        MetadataView,
        PyTemporalProperties,
        PropertiesView,
        PyTemporalProp,
        PyTimeIndexEntry,
        PyHistory,
        PyHistoryTimestamp,
        PyHistoryDateTime,
        PyHistorySecondaryIndex,
        PyIntervals,
        PyWindowSet,
        PyIndexSpecBuilder,
        PyIndexSpec,
        // do we want to add these and use them in return types?
        HistoryIterable,
        NestedHistoryIterable,
        HistoryTimestampIterable,
        NestedHistoryTimestampIterable,
        HistoryDateTimeIterable,
        NestedHistoryDateTimeIterable,
        HistorySecondaryIndexIterable,
        NestedHistorySecondaryIndexIterable,
        IntervalsIterable,
        NestedIntervalsIterable,
        OptionTimeIndexEntryIterable,
        NestedOptionTimeIndexEntryIterable,
        TimeIndexEntryIterable,
        NestedTimeIndexEntryIterable,
        ResultUtcDateTimeIterable,
        NestedResultUtcDateTimeIterable,
        ResultOptionUtcDateTimeIterable,
        NestedResultOptionUtcDateTimeIterable,
        OptionUsizeIterable,
        NestedOptionUsizeIterable,
        OptionI64Iterable,
        NestedOptionI64Iterable,
        I64Iterable,
        NestedI64Iterable,
    );

    #[pyfunction]
    pub(crate) fn version() -> String {
        String::from(crate::version())
    }

    m.add_function(wrap_pyfunction!(version, m)?)?;

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
        k_core,
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
