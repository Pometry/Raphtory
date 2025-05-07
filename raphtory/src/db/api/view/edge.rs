use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            storage::graph::edges::edge_entry::EdgeStorageEntry,
            view::{
                internal::{
                    CoreGraphOps, EdgeTimeSemanticsOps, GraphTimeSemanticsOps, InternalLayerOps,
                },
                BoxableGraphView, IntoDynBoxed,
            },
        },
        graph::{
            edge::{edge_valid_layer, EdgeView},
            views::layer_graph::LayeredGraph,
        },
    },
    prelude::{GraphViewOps, LayerOps, NodeViewOps, TimeOps},
};
use chrono::{DateTime, Utc};
use ouroboros::self_referencing;
use raphtory_api::{
    core::{
        entities::{LayerIds, GID},
        storage::arc_str::ArcStr,
    },
    iter::BoxedLIter,
};
use std::{iter, marker::PhantomData};

#[self_referencing]
pub struct ExplodedIter<'graph, G: GraphViewOps<'graph>> {
    graph: G,
    #[borrows(graph)]
    #[covariant]
    edge: EdgeStorageEntry<'this>,
    #[borrows(edge, graph)]
    #[covariant]
    iter: BoxedLIter<'this, EdgeRef>,
    marker: PhantomData<&'graph ()>,
}

impl<'graph, G: GraphViewOps<'graph>> Iterator for ExplodedIter<'graph, G> {
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.with_iter(|iter| iter.size_hint())
    }
}

fn exploded<'graph, G: BoxableGraphView + Clone + 'graph>(
    view: G,
    edge_ref: EdgeRef,
) -> BoxedLIter<'graph, EdgeRef> {
    let time_semantics = view.edge_time_semantics();
    ExplodedIterBuilder {
        graph: view,
        edge_builder: |view| view.core_edge(edge_ref.pid()),
        iter_builder: move |edge, graph| {
            time_semantics
                .edge_exploded(edge.as_ref(), graph, graph.layer_ids())
                .map(move |(t, l)| edge_ref.at(t).at_layer(l))
                .into_dyn_boxed()
        },
        marker: Default::default(),
    }
    .build()
    .into_dyn_boxed()
}

fn exploded_layers<'graph, G: BoxableGraphView + Clone + 'graph>(
    view: G,
    edge_ref: EdgeRef,
) -> BoxedLIter<'graph, EdgeRef> {
    let time_semantics = view.edge_time_semantics();
    ExplodedIterBuilder {
        graph: view,
        edge_builder: |view| view.core_edge(edge_ref.pid()),
        iter_builder: move |edge, graph| {
            time_semantics
                .edge_layers(edge.as_ref(), graph, graph.layer_ids())
                .map(move |l| edge_ref.at_layer(l))
                .into_dyn_boxed()
        },
        marker: Default::default(),
    }
    .build()
    .into_dyn_boxed()
}

pub trait BaseEdgeViewOps<'graph>: Clone + TimeOps<'graph> + LayerOps<'graph> {
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type ValueType<T>: 'graph
    where
        T: 'graph;

    type PropType: PropertiesOps + Clone + 'graph;
    type Nodes: NodeViewOps<'graph, Graph = Self::BaseGraph, BaseGraph = Self::BaseGraph> + 'graph;
    type Exploded: EdgeViewOps<'graph, Graph = Self::Graph, BaseGraph = Self::BaseGraph> + 'graph;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O>;

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>>;

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes;

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + Sync + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded;
}

pub trait EdgeViewOps<'graph>: TimeOps<'graph> + LayerOps<'graph> + Clone {
    type ValueType<T>: 'graph
    where
        T: 'graph;
    type PropType: PropertiesOps + Clone + 'graph;
    type Graph: GraphViewOps<'graph>;
    type BaseGraph: GraphViewOps<'graph>;
    type Nodes: NodeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>;

    type Exploded: EdgeViewOps<'graph, BaseGraph = Self::BaseGraph, Graph = Self::Graph>;

    /// List the activation timestamps for the edge
    fn history(&self) -> Self::ValueType<Vec<i64>>;

    fn history_counts(&self) -> Self::ValueType<usize>;

    /// List the activation timestamps for the edge as NaiveDateTime objects if parseable
    fn history_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>>;

    /// List the deletion timestamps for the edge
    fn deletions(&self) -> Self::ValueType<Vec<i64>>;

    /// List the deletion timestamps for the edge as NaiveDateTime objects if parseable
    fn deletions_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>>;

    /// Check that the latest status of the edge is valid (i.e., not deleted)
    fn is_valid(&self) -> Self::ValueType<bool>;

    /// Check that the latest status of the edge is deleted (i.e., not valid)
    fn is_deleted(&self) -> Self::ValueType<bool>;

    /// If the edge is a loop then returns true, else false
    fn is_self_loop(&self) -> Self::ValueType<bool>;

    /// Return a view of the properties of the edge
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>>;

    /// Returns the source node of the edge.
    fn src(&self) -> Self::Nodes;

    /// Returns the destination node of the edge.
    fn dst(&self) -> Self::Nodes;

    /// Returns the node at the other end of the edge (same as `dst()` for out-edges and `src()` for in-edges)
    fn nbr(&self) -> Self::Nodes;

    /// Check if edge is active at a given time point
    fn is_active(&self) -> Self::ValueType<bool>;

    /// Returns the id of the edge.
    fn id(&self) -> Self::ValueType<(GID, GID)>;

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    fn explode(&self) -> Self::Exploded;

    fn explode_layers(&self) -> Self::Exploded;

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Self::ValueType<Option<i64>>;

    fn earliest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>>;

    fn latest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>>;

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Self::ValueType<Option<i64>>;

    /// Gets the time stamp of the edge if it is exploded
    fn time(&self) -> Self::ValueType<Result<i64, GraphError>>;

    fn date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>>;

    /// Gets the layer name for the edge if it is restricted to a single layer
    fn layer_name(&self) -> Self::ValueType<Result<ArcStr, GraphError>>;

    /// Gets the TimeIndexEntry if the edge is exploded
    fn time_and_index(&self) -> Self::ValueType<Result<TimeIndexEntry, GraphError>>;

    /// Gets the name of the layer this edge belongs to
    fn layer_names(&self) -> Self::ValueType<Vec<ArcStr>>;
}

impl<'graph, E: BaseEdgeViewOps<'graph>> EdgeViewOps<'graph> for E {
    type ValueType<T>
        = E::ValueType<T>
    where
        T: 'graph;
    type PropType = E::PropType;
    type Graph = E::Graph;
    type BaseGraph = E::BaseGraph;
    type Nodes = E::Nodes;

    type Exploded = E::Exploded;

    /// list the activation timestamps for the edge
    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                match e.time() {
                    Some(t) => vec![t.t()],
                    None => {
                        let time_semantics = g.edge_time_semantics();
                        let edge = g.core_edge(e.pid());
                        match e.layer() {
                            None => time_semantics
                                .edge_history(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti.t())
                                .collect(),
                            Some(layer) => time_semantics
                                .edge_history(edge.as_ref(), g, &LayerIds::One(layer))
                                .map(|(ti, _)| ti.t())
                                .collect(),
                        }
                    }
                }
            } else {
                vec![]
            }
        })
    }

    fn history_counts(&self) -> Self::ValueType<usize> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                match e.time() {
                    Some(_) => 1,
                    None => match e.layer() {
                        None => g
                            .edge_time_semantics()
                            .edge_exploded_count(g.core_edge(e.pid()).as_ref(), g),
                        Some(layer) => g.edge_time_semantics().edge_exploded_count(
                            g.core_edge(e.pid()).as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer)),
                        ),
                    },
                }
            } else {
                0
            }
        })
    }

    fn history_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                match e.time() {
                    Some(t) => Some(vec![t.dt()?]),
                    None => {
                        let time_semantics = g.edge_time_semantics();
                        let edge = g.core_edge(e.pid());
                        match e.layer() {
                            None => time_semantics
                                .edge_history(edge.as_ref(), g, g.layer_ids())
                                .map(|(ti, _)| ti.dt())
                                .collect(),
                            Some(layer) => time_semantics
                                .edge_history(edge.as_ref(), g, &LayerIds::One(layer))
                                .map(|(ti, _)| ti.dt())
                                .collect(),
                        }
                    }
                }
            } else {
                Some(vec![])
            }
        })
    }

    fn deletions(&self) -> Self::ValueType<Vec<i64>> {
        self.map(|g, e| {
            EdgeView::new(g, e)
                .deletions_hist()
                .map(|(t, _)| t.t())
                .collect()
        })
    }

    fn deletions_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>> {
        self.map(|g, e| {
            EdgeView::new(g, e)
                .deletions_hist()
                .map(|(t, _)| t.dt())
                .collect()
        })
    }

    fn is_valid(&self) -> Self::ValueType<bool> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                let edge = g.core_edge(e.pid());
                match e.time() {
                    None => match e.layer() {
                        None => time_semantics.edge_is_valid(edge.as_ref(), g),
                        Some(layer) => time_semantics.edge_is_valid(
                            edge.as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer)),
                        ),
                    },
                    Some(t) => {
                        let layer = e.layer().expect("exploded edge should have layer");
                        time_semantics.edge_is_valid_exploded(edge.as_ref(), g, t, layer)
                    }
                }
            } else {
                false
            }
        })
    }

    fn is_deleted(&self) -> Self::ValueType<bool> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                let edge = g.core_edge(e.pid());
                match e.time() {
                    None => match e.layer() {
                        None => time_semantics.edge_is_deleted(edge.as_ref(), g),
                        Some(layer) => time_semantics.edge_is_deleted(
                            edge.as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer)),
                        ),
                    },
                    Some(t) => {
                        let layer = e.layer().expect("exploded edge should have layer");
                        time_semantics.edge_is_deleted_exploded(edge.as_ref(), g, t, layer)
                    }
                }
            } else {
                false
            }
        })
    }

    fn is_self_loop(&self) -> Self::ValueType<bool> {
        self.map(|_g, e| e.src() == e.dst())
    }

    /// Return a view of the properties of the edge
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.as_props()
    }

    /// Returns the source node of the edge.
    fn src(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.src())
    }

    /// Returns the destination node of the edge.
    fn dst(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.dst())
    }

    fn nbr(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.remote())
    }

    /// Check if edge is active (i.e. has some update within the current bound)
    fn is_active(&self) -> Self::ValueType<bool> {
        self.map(move |g, e| {
            if edge_valid_layer(g, e) {
                let edge = g.core_edge(e.pid());
                let time_semantics = g.edge_time_semantics();
                match e.time() {
                    None => match e.layer() {
                        None => time_semantics.edge_is_active(edge.as_ref(), g),
                        Some(layer_id) => time_semantics.edge_is_active(
                            edge.as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer_id)),
                        ),
                    },
                    Some(t) => time_semantics.edge_is_active_exploded(
                        edge.as_ref(),
                        g,
                        t,
                        e.layer().expect("exploded edge should have layer"),
                    ),
                }
            } else {
                false
            }
        })
    }

    fn id(&self) -> Self::ValueType<(GID, GID)> {
        self.map(|g, e| (g.node_id(e.src()), g.node_id(e.dst())))
    }

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    fn explode(&self) -> Self::Exploded {
        self.map_exploded(|g, e| {
            if edge_valid_layer(g, e) {
                match e.time() {
                    Some(_) => iter::once(e).into_dyn_boxed(),
                    None => {
                        let view = g.clone();
                        match e.layer() {
                            None => exploded(view, e),
                            Some(layer) => {
                                exploded(LayeredGraph::new(view, LayerIds::One(layer)), e)
                            }
                        }
                    }
                }
            } else {
                iter::empty().into_dyn_boxed()
            }
        })
    }

    fn explode_layers(&self) -> Self::Exploded {
        self.map_exploded(|g, e| {
            if edge_valid_layer(g, e) {
                match e.layer() {
                    Some(_) => Box::new(iter::once(e)),
                    None => {
                        let g = g.clone();
                        exploded_layers(g, e)
                    }
                }
            } else {
                iter::empty().into_dyn_boxed()
            }
        })
    }

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                match e.time() {
                    None => match e.layer() {
                        None => time_semantics.edge_earliest_time(g.core_edge(e.pid()).as_ref(), g),
                        Some(layer) => time_semantics.edge_earliest_time(
                            g.core_edge(e.pid()).as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer)),
                        ),
                    },

                    Some(t) => time_semantics.edge_exploded_earliest_time(
                        g.core_edge(e.pid()).as_ref(),
                        g,
                        t,
                        e.layer().expect("exploded edge should have layer"),
                    ),
                }
            } else {
                None
            }
        })
    }

    fn earliest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                match e.time() {
                    None => time_semantics
                        .edge_earliest_time(g.core_edge(e.pid()).as_ref(), g)?
                        .dt(),
                    Some(t) => time_semantics
                        .edge_exploded_earliest_time(
                            g.core_edge(e.pid()).as_ref(),
                            g,
                            t,
                            e.layer().expect("exploded edge should have layer"),
                        )?
                        .dt(),
                }
            } else {
                None
            }
        })
    }

    fn latest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                match e.time() {
                    None => time_semantics
                        .edge_latest_time(g.core_edge(e.pid()).as_ref(), g)?
                        .dt(),
                    Some(t) => time_semantics
                        .edge_exploded_latest_time(
                            g.core_edge(e.pid()).as_ref(),
                            g,
                            t,
                            e.layer().expect("exploded edge should have layer"),
                        )?
                        .dt(),
                }
            } else {
                None
            }
        })
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let time_semantics = g.edge_time_semantics();
                match e.time() {
                    None => match e.layer() {
                        None => time_semantics.edge_latest_time(g.core_edge(e.pid()).as_ref(), g),

                        Some(layer) => time_semantics.edge_latest_time(
                            g.core_edge(e.pid()).as_ref(),
                            LayeredGraph::new(g, LayerIds::One(layer)),
                        ),
                    },
                    Some(t) => time_semantics.edge_exploded_latest_time(
                        g.core_edge(e.pid()).as_ref(),
                        g,
                        t,
                        e.layer().expect("exploded edge should have layer"),
                    ),
                }
            } else {
                None
            }
        })
    }

    /// Gets the time stamp of the edge if it is exploded
    fn time(&self) -> Self::ValueType<Result<i64, GraphError>> {
        self.map(|_, e| e.time_t().ok_or_else(|| GraphError::TimeAPIError))
    }

    fn date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|_, e| e.time_t()?.dt())
    }

    /// Gets the layer name for the edge if it is restricted to a single layer
    fn layer_name(&self) -> Self::ValueType<Result<ArcStr, GraphError>> {
        self.map(|g, e| {
            e.layer()
                .map(|l_id| g.get_layer_name(l_id))
                .ok_or_else(|| GraphError::LayerNameAPIError)
        })
    }

    /// Gets the TimeIndexEntry if the edge is exploded
    fn time_and_index(&self) -> Self::ValueType<Result<TimeIndexEntry, GraphError>> {
        self.map(|_, e| e.time().ok_or(GraphError::TimeAPIError))
    }

    /// Gets the name of the layer this edge belongs to
    fn layer_names(&self) -> Self::ValueType<Vec<ArcStr>> {
        self.map(|g, e| {
            if edge_valid_layer(g, e) {
                let layer_names = g.edge_meta().layer_meta().get_keys();
                match e.layer() {
                    None => {
                        let time_semantics = g.edge_time_semantics();
                        time_semantics
                            .edge_layers(g.core_edge(e.pid()).as_ref(), g, g.layer_ids())
                            .map(|layer| layer_names[layer].clone())
                            .collect()
                    }
                    Some(l) => {
                        vec![layer_names[l].clone()]
                    }
                }
            } else {
                vec![]
            }
        })
    }
}

#[cfg(test)]
mod test_edge_view {
    use crate::{prelude::*, test_storage, test_utils::test_graph};

    #[test]
    fn test_exploded_edge_properties() {
        let graph = Graph::new();
        let actual_prop_values = vec![0, 1, 2, 3];
        for v in actual_prop_values.iter() {
            graph.add_edge(0, 1, 2, [("test", *v)], None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let prop_values: Vec<_> = graph
                .edge(1, 2)
                .unwrap()
                .explode()
                .properties()
                .flat_map(|p| p.get("test").into_i32())
                .collect();
            assert_eq!(prop_values, actual_prop_values)
        });
    }

    #[test]
    fn test_exploded_edge_properties_window() {
        let graph = Graph::new();
        let actual_prop_values_0 = vec![0, 1, 2, 3];
        for v in actual_prop_values_0.iter() {
            graph.add_edge(0, 1, 2, [("test", *v)], None).unwrap();
        }
        let actual_prop_values_1 = vec![4, 5, 6];
        for v in actual_prop_values_1.iter() {
            graph.add_edge(1, 1, 2, [("test", *v)], None).unwrap();
        }
        test_storage!(&graph, |graph| {
            let prop_values: Vec<_> = graph
                .at(0)
                .edge(1, 2)
                .unwrap()
                .explode()
                .properties()
                .flat_map(|p| p.get("test").into_i32())
                .collect();
            assert_eq!(prop_values, actual_prop_values_0);
            let prop_values: Vec<_> = graph
                .at(1)
                .edge(1, 2)
                .unwrap()
                .explode()
                .properties()
                .flat_map(|p| p.get("test").into_i32())
                .collect();
            assert_eq!(prop_values, actual_prop_values_1)
        });
    }

    #[test]
    fn test_exploded_edge_multilayer() {
        let graph = Graph::new();
        let expected_prop_values = vec![0, 1, 2, 3];
        for v in expected_prop_values.iter() {
            graph
                .add_edge(0, 1, 2, [("test", *v)], Some((v % 2).to_string().as_str()))
                .unwrap();
        }
        // FIXME: Needs support for secondary time index in disk storage (Issue #30)
        test_graph(&graph, |graph| {
            let prop_values: Vec<_> = graph
                .edge(1, 2)
                .unwrap()
                .explode()
                .properties()
                .flat_map(|p| p.get("test").into_i32())
                .collect();
            let actual_layers: Vec<_> = graph
                .edge(1, 2)
                .unwrap()
                .explode()
                .layer_name()
                .flatten()
                .collect();
            let expected_layers: Vec<_> = expected_prop_values
                .iter()
                .map(|v| (v % 2).to_string())
                .collect();
            assert_eq!(prop_values, expected_prop_values);
            assert_eq!(actual_layers, expected_layers);

            assert!(graph.edge(1, 2).unwrap().layer_name().is_err());
            assert!(graph.edges().layer_name().all(|l| l.is_err()));
            assert!(graph
                .edge(1, 2)
                .unwrap()
                .explode()
                .layer_name()
                .all(|l| l.is_ok()));
            assert!(graph
                .edge(1, 2)
                .unwrap()
                .explode_layers()
                .layer_name()
                .all(|l| l.is_ok()));
            assert!(graph.edges().explode().layer_name().all(|l| l.is_ok()));
            assert!(graph
                .edges()
                .explode_layers()
                .layer_name()
                .all(|l| l.is_ok()));

            assert!(graph.edge(1, 2).unwrap().time().is_err());
            assert!(graph.edges().time().all(|l| l.is_err()));
            assert!(graph
                .edge(1, 2)
                .unwrap()
                .explode()
                .time()
                .all(|l| l.is_ok()));
            assert!(graph
                .edge(1, 2)
                .unwrap()
                .explode_layers()
                .time()
                .all(|l| l.is_err()));
            assert!(graph.edges().explode().time().all(|l| l.is_ok()));
            assert!(graph.edges().explode_layers().time().all(|l| l.is_err()));
        });
    }

    #[test]
    fn test_sorting_by_secondary_index() {
        let graph = Graph::new();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        graph.add_edge(0, 1, 2, [("second", true)], None).unwrap();
        graph.add_edge(0, 2, 3, [("second", true)], None).unwrap();

        //FIXME: DiskGraph does not preserve secondary index (see #1780)
        test_graph(&graph, |graph| {
            let mut exploded_edges: Vec<_> = graph.edges().explode().into_iter().collect();
            exploded_edges.sort_by_key(|a| a.time_and_index().unwrap());

            let res: Vec<_> = exploded_edges
                .into_iter()
                .filter_map(|e| {
                    Some((
                        e.src().id().as_u64()?,
                        e.dst().id().as_u64()?,
                        e.properties().get("second").into_bool(),
                    ))
                })
                .collect();
            assert_eq!(
                res,
                vec![
                    (2, 3, None),
                    (1, 2, None),
                    (1, 2, Some(true)),
                    (2, 3, Some(true))
                ]
            )
        });
    }
}
