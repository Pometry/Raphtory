use std::iter;

use chrono::{DateTime, Utc};
use raphtory_api::core::{entities::GID, storage::arc_str::ArcStr};

use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::{errors::GraphError, iter::GenLockedIter},
    },
    db::api::{
        properties::{internal::PropertiesOps, Properties},
        view::{
            internal::{CoreGraphOps, InternalLayerOps, TimeSemantics},
            IntoDynBoxed,
        },
    },
    prelude::{GraphViewOps, LayerOps, NodeViewOps, TimeOps},
};

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
    fn time_and_index(&self) -> Self::ValueType<Option<TimeIndexEntry>>;

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
        self.map(|g, e| g.edge_history(e, g.layer_ids()).map(|ti| ti.t()).collect())
    }

    fn history_counts(&self) -> Self::ValueType<usize> {
        self.map(|g, e| g.edge_exploded_count(g.core_edge(e.pid()).as_ref(), g.layer_ids()))
    }

    fn history_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>> {
        self.map(move |g, e| g.edge_history(e, g.layer_ids()).map(|t| t.dt()).collect())
    }

    fn deletions(&self) -> Self::ValueType<Vec<i64>> {
        self.map(move |g, e| {
            g.edge_deletion_history(e, &g.layer_ids().constrain_from_edge(e))
                .map(|t| t.t())
                .collect()
        })
    }

    fn deletions_date_time(&self) -> Self::ValueType<Option<Vec<DateTime<Utc>>>> {
        self.map(|g, e| {
            g.edge_deletion_history(e, &g.layer_ids().constrain_from_edge(e))
                .into_iter()
                .map(|t| t.dt())
                .collect()
        })
    }

    fn is_valid(&self) -> Self::ValueType<bool> {
        self.map(|g, e| g.edge_is_valid(e, &g.layer_ids().constrain_from_edge(e)))
    }

    fn is_deleted(&self) -> Self::ValueType<bool> {
        self.map(|g, e| !g.edge_is_valid(e, &g.layer_ids().constrain_from_edge(e)))
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
            g.edge_exploded(e, &g.layer_ids().constrain_from_edge(e))
                .next()
                .is_some()
        })
    }

    fn id(&self) -> Self::ValueType<(GID, GID)> {
        self.map(|g, e| (g.node_id(e.src()), g.node_id(e.dst())))
    }

    /// Explodes an edge and returns all instances it had been updated as seperate edges
    fn explode(&self) -> Self::Exploded {
        self.map_exploded(|g, e| match e.time() {
            Some(_) => Box::new(iter::once(e)),
            None => {
                let g = g.clone();
                GenLockedIter::from(g, move |g| g.edge_exploded(e, g.layer_ids())).into_dyn_boxed()
            }
        })
    }

    fn explode_layers(&self) -> Self::Exploded {
        self.map_exploded(|g, e| match e.layer() {
            Some(_) => Box::new(iter::once(e)),
            None => {
                let g = g.clone();
                GenLockedIter::from(g, move |g| g.edge_layers(e, g.layer_ids())).into_dyn_boxed()
            }
        })
    }

    /// Gets the first time an edge was seen
    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, e| g.edge_earliest_time(e, &g.layer_ids().constrain_from_edge(e)))
    }

    fn earliest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, e| {
            g.edge_earliest_time(e, &g.layer_ids().constrain_from_edge(e))?
                .dt()
        })
    }

    fn latest_date_time(&self) -> Self::ValueType<Option<DateTime<Utc>>> {
        self.map(|g, e| {
            g.edge_latest_time(e, &g.layer_ids().constrain_from_edge(e))?
                .dt()
        })
    }

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, e| g.edge_latest_time(e, &g.layer_ids().constrain_from_edge(e)))
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
    fn time_and_index(&self) -> Self::ValueType<Option<TimeIndexEntry>> {
        self.map(|_, e| e.time())
    }

    /// Gets the name of the layer this edge belongs to
    fn layer_names(&self) -> Self::ValueType<Vec<ArcStr>> {
        self.map(|g, e| {
            let layer_names = g.edge_meta().layer_meta().get_keys();
            g.edge_layers(e, &g.layer_ids().constrain_from_edge(e))
                .map(move |ee| {
                    layer_names[ee.layer().expect("exploded edge should have layer")].clone()
                })
                .collect()
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
            exploded_edges.sort_by_key(|a| a.time_and_index());

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
