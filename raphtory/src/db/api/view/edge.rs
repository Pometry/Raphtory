use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        storage::timeindex::EventTime,
    },
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, Metadata, Properties},
            view::{
                history::{DeletionHistory, History},
                internal::{EdgeTimeSemanticsOps, GraphTimeSemanticsOps, GraphView},
                IntoDynBoxed,
            },
        },
        graph::{
            edge::{edge_valid_layer, EdgeView},
            views::layer_graph::LayeredGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps, NodeViewOps, TimeOps},
};
use ouroboros::self_referencing;
use raphtory_api::{
    core::{
        entities::{LayerIds, GID},
        storage::arc_str::ArcStr,
    },
    iter::BoxedLIter,
};
use raphtory_storage::{
    core_ops::CoreGraphOps, graph::edges::edge_entry::EdgeStorageEntry, layer_ops::InternalLayerOps,
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

fn exploded<'graph, G: GraphView + 'graph>(
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

fn exploded_layers<'graph, G: GraphView + 'graph>(
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
    type Graph: GraphViewOps<'graph>;
    type ValueType<T>: 'graph
    where
        T: 'graph;

    type PropType: InternalPropertiesOps + Clone + 'graph;
    type Nodes: NodeViewOps<'graph, Graph = Self::Graph> + 'graph;
    type Exploded: EdgeViewOps<'graph, Graph = Self::Graph> + 'graph;

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O>;

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>>;

    fn as_metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>>;

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
    type PropType: InternalPropertiesOps + Clone + 'graph;
    type Graph: GraphViewOps<'graph>;
    type Nodes: NodeViewOps<'graph, Graph = Self::Graph>;

    type Exploded: EdgeViewOps<'graph, Graph = Self::Graph>;

    /// History object for the edge containing its activation times.
    fn history(&self) -> Self::ValueType<History<'graph, EdgeView<Self::Graph>>>;

    /// History object for the edge containing its deletion times.
    fn deletions(&self)
        -> Self::ValueType<History<'graph, DeletionHistory<EdgeView<Self::Graph>>>>;

    /// Check that the latest status of the edge is valid (i.e., not deleted)
    fn is_valid(&self) -> Self::ValueType<bool>;

    /// Check that the latest status of the edge is deleted (i.e., not valid)
    fn is_deleted(&self) -> Self::ValueType<bool>;

    /// If the edge is a loop then returns true, else false
    fn is_self_loop(&self) -> Self::ValueType<bool>;

    /// Return a view of the properties of the edge
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>>;

    /// Return a view of the metadata of the edge
    fn metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>>;

    /// Returns the source node of the edge.
    ///
    /// Returns:
    ///     Nodes:
    fn src(&self) -> Self::Nodes;

    /// Returns the destination node of the edge.
    ///
    /// Returns:
    ///     Nodes:
    fn dst(&self) -> Self::Nodes;

    /// Returns the node at the other end of the edge (same as `dst()` for out-edges and `src()` for in-edges)
    ///
    /// Returns:
    ///     Nodes:
    fn nbr(&self) -> Self::Nodes;

    /// Check if the edge is active (has some update within the current bound) at a given time point.
    fn is_active(&self) -> Self::ValueType<bool>;

    /// Returns the id of the edge.
    ///
    /// Returns:
    ///     GID:
    fn id(&self) -> Self::ValueType<(GID, GID)>;

    /// Explodes an edge and returns all instances it had been updated as separate edges
    ///
    /// Returns:
    ///     Edges:
    fn explode(&self) -> Self::Exploded;

    /// Returns:
    ///     Edges:
    fn explode_layers(&self) -> Self::Exploded;

    /// Gets the first time an edge was seen
    ///
    /// Returns:
    ///     int:
    fn earliest_time(&self) -> Self::ValueType<Option<EventTime>>;

    /// Gets the latest time an edge was updated
    ///
    /// Returns:
    ///     int:
    fn latest_time(&self) -> Self::ValueType<Option<EventTime>>;

    /// Gets the time stamp of the edge if it is exploded
    ///
    /// Returns:
    ///     int:
    fn time(&self) -> Self::ValueType<Result<EventTime, GraphError>>;

    /// Gets the layer name for the edge if it is restricted to a single layer
    ///
    /// Returns:
    ///     str:
    fn layer_name(&self) -> Self::ValueType<Result<ArcStr, GraphError>>;

    /// Gets the EventTime if the edge is exploded
    fn time_and_event_id(&self) -> Self::ValueType<Result<EventTime, GraphError>>;

    /// Gets the name of the layer this edge belongs to
    ///
    /// Returns:
    ///     str:
    fn layer_names(&self) -> Self::ValueType<Vec<ArcStr>>;
}

impl<'graph, E: BaseEdgeViewOps<'graph>> EdgeViewOps<'graph> for E {
    type ValueType<T>
        = E::ValueType<T>
    where
        T: 'graph;
    type PropType = E::PropType;
    type Graph = E::Graph;
    type Nodes = E::Nodes;

    type Exploded = E::Exploded;

    /// History object for the edge
    fn history(&self) -> Self::ValueType<History<'graph, EdgeView<Self::Graph>>> {
        self.map(|g, e| History::new(EdgeView::new(g.clone(), e)))
    }

    /// Returns:
    ///     History:
    fn deletions(
        &self,
    ) -> Self::ValueType<History<'graph, DeletionHistory<EdgeView<Self::Graph>>>> {
        self.map(|g, e| History::new(DeletionHistory::new(EdgeView::new(g.clone(), e))))
    }

    /// Returns:
    ///     boolean:
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

    /// Returns:
    ///     boolean:
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

    /// Returns true if the source and destination nodes are identical.
    ///
    ///  Returns:
    ///     boolean:
    fn is_self_loop(&self) -> Self::ValueType<bool> {
        self.map(|_g, e| e.src() == e.dst())
    }

    /// Returns a view of the properties of the edge
    ///
    /// Returns:
    ///     properties:
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.as_props()
    }

    /// Returns:
    ///     metadata:
    fn metadata(&self) -> Self::ValueType<Metadata<'graph, Self::PropType>> {
        self.as_metadata()
    }

    /// Returns the source node of the edge.
    ///
    /// Returns:
    ///     Nodes:
    fn src(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.src())
    }

    /// Returns the destination node of the edge.
    ///
    /// Returns:
    ///     Nodes:
    fn dst(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.dst())
    }

    /// Returns:
    ///     Nodes:
    fn nbr(&self) -> Self::Nodes {
        self.map_nodes(|_, e| e.remote())
    }

    /// Check if an edge is active (has some update within the current bound) at a given time point.
    ///
    /// Returns:
    ///     bool:
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
    fn earliest_time(&self) -> Self::ValueType<Option<EventTime>> {
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

    /// Gets the latest time an edge was updated
    fn latest_time(&self) -> Self::ValueType<Option<EventTime>> {
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

    /// Gets the time of the edge if it is exploded
    fn time(&self) -> Self::ValueType<Result<EventTime, GraphError>> {
        self.map(|_, e| e.time().ok_or_else(|| GraphError::TimeAPIError))
    }

    /// Gets the layer name for the edge if it is restricted to a single layer
    fn layer_name(&self) -> Self::ValueType<Result<ArcStr, GraphError>> {
        self.map(|g, e| {
            e.layer()
                .map(|l_id| g.get_layer_name(l_id))
                .ok_or_else(|| GraphError::LayerNameAPIError)
        })
    }

    /// Gets the EventTime if the edge is exploded
    fn time_and_event_id(&self) -> Self::ValueType<Result<EventTime, GraphError>> {
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
