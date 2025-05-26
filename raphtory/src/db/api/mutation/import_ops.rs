use super::time_from_input;
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            properties::internal::TemporalPropertiesOps,
            view::{internal::InternalMaterialize, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphError,
    prelude::{AdditionOps, EdgeViewOps, GraphViewOps, NodeViewOps},
};
use raphtory_api::core::{
    entities::GID,
    storage::{arc_str::OptionAsStr, timeindex::AsTime},
};
use raphtory_storage::mutation::{
    addition_ops::InternalAdditionOps, deletion_ops::InternalDeletionOps,
    property_addition_ops::InternalPropertyAdditionOps,
};
use std::{borrow::Borrow, fmt::Debug};

pub trait ImportOps: Sized {
    /// Imports a single node into the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if the imported node already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        node: &NodeView<'a, GHH, GH>,
        merge: bool,
    ) -> Result<NodeView<'static, Self, Self>, GraphError>;

    /// Imports a single node into the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `new_id` - The new node id.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if the imported node already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        node: &NodeView<'a, GHH, GH>,
        new_id: V,
        merge: bool,
    ) -> Result<NodeView<'static, Self, Self>, GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if any of the imported nodes already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the nodes were successfully imported, and `Err` otherwise.
    fn import_nodes<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH, GH>>>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `new_ids` - A list of node IDs to use for the imported nodes.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if any of the imported nodes already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the nodes were successfully imported, and `Err` otherwise.
    fn import_nodes_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH, GH>>>,
        new_ids: impl IntoIterator<Item = V>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if the imported edge already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edge was successfully imported, and `Err` otherwise.
    fn import_edge<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edge: &EdgeView<GHH, GH>,
        merge: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `new_id` - The ID of the new edge. It's a tuple of the source and destination node ids.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if the imported edge already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edge was successfully imported, and `Err` otherwise.
    fn import_edge_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        edge: &EdgeView<GHH, GH>,
        new_id: (V, V),
        merge: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if any of the imported edges already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edges were successfully imported, and `Err` otherwise.
    fn import_edges<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `new_ids` - The IDs of the new edges. It's a vector of tuples of the source and destination node ids.
    /// * `merge` - An optional boolean flag.
    ///             If `merge` is `false`, the function will return an error if any of the imported edges already exists in the graph.
    ///             If `merge` is `true`, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edges were successfully imported, and `Err` otherwise.
    fn import_edges_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        new_ids: impl IntoIterator<Item = (V, V)>,
        merge: bool,
    ) -> Result<(), GraphError>;
}

impl<
        G: StaticGraphViewOps
            + InternalAdditionOps<Error = GraphError>
            + InternalDeletionOps<Error = GraphError>
            + InternalPropertyAdditionOps<Error = GraphError>
            + InternalMaterialize,
    > ImportOps for G
{
    fn import_node<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        node: &NodeView<'a, GHH, GH>,
        merge: bool,
    ) -> Result<NodeView<'static, G, G>, GraphError> {
        import_node_internal(&self, node, node.id(), merge)
    }

    fn import_node_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        node: &NodeView<'a, GHH, GH>,
        new_id: V,
        merge: bool,
    ) -> Result<NodeView<'static, Self, Self>, GraphError> {
        import_node_internal(&self, node, new_id, merge)
    }

    fn import_nodes<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH, GH>>>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let nodes: Vec<_> = nodes.into_iter().collect();
        let new_ids: Vec<GID> = nodes.iter().map(|n| n.borrow().id()).collect();
        check_existing_nodes(self, &new_ids, merge)?;
        for node in &nodes {
            self.import_node(node.borrow(), merge)?;
        }
        Ok(())
    }

    fn import_nodes_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH, GH>>>,
        new_ids: impl IntoIterator<Item = V>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let new_ids: Vec<V> = new_ids.into_iter().collect();
        check_existing_nodes(self, &new_ids, merge)?;
        for (node, new_node_id) in nodes.into_iter().zip(new_ids.into_iter()) {
            self.import_node_as(node.borrow(), new_node_id, merge)?;
        }
        Ok(())
    }

    fn import_edge<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edge: &EdgeView<GHH, GH>,
        merge: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        import_edge_internal(&self, edge, edge.src().id(), edge.dst().id(), merge)
    }

    fn import_edge_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        edge: &EdgeView<GHH, GH>,
        new_id: (V, V),
        merge: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        import_edge_internal(&self, edge, new_id.0, new_id.1, merge)
    }

    fn import_edges<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let edges: Vec<_> = edges.into_iter().collect();
        let new_ids: Vec<(GID, GID)> = edges.iter().map(|e| e.borrow().id()).collect();
        check_existing_edges(self, &new_ids, merge)?;
        for edge in edges {
            self.import_edge(edge.borrow(), merge)?;
        }
        Ok(())
    }

    fn import_edges_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        new_ids: impl IntoIterator<Item = (V, V)>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let new_ids: Vec<(V, V)> = new_ids.into_iter().collect();
        check_existing_edges(self, &new_ids, merge)?;
        for (new_id, edge) in new_ids.into_iter().zip(edges) {
            self.import_edge_as(edge.borrow(), new_id, merge)?;
        }
        Ok(())
    }
}

fn import_node_internal<
    'a,
    G: StaticGraphViewOps
        + InternalAdditionOps<Error = GraphError>
        + InternalDeletionOps<Error = GraphError>
        + InternalPropertyAdditionOps<Error = GraphError>
        + InternalMaterialize,
    GHH: GraphViewOps<'a>,
    GH: GraphViewOps<'a>,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    node: &NodeView<'a, GHH, GH>,
    id: V,
    merge: bool,
) -> Result<NodeView<'static, G, G>, GraphError>
where
    GraphError: From<<G as InternalAdditionOps>::Error>,
{
    let id = id.as_node_ref();
    if !merge {
        if let Some(existing_node) = graph.node(id) {
            return Err(GraphError::NodeExistsError(existing_node.id()));
        }
    }

    let node_internal = match node.node_type().as_str() {
        None => graph.resolve_node(id)?.inner(),
        Some(node_type) => {
            let (node_internal, _) = graph.resolve_node_and_type(id, node_type)?.inner();
            node_internal.inner()
        }
    };
    let keys = node.temporal_prop_keys().collect::<Vec<_>>();

    for (t, row) in node.rows() {
        let t = time_from_input(graph, t)?;

        let props = row
            .into_iter()
            .zip(&keys)
            .map(|((_, prop), key)| {
                let prop_id = graph.resolve_node_property(key, prop.dtype(), false);
                prop_id.map(|prop_id| (prop_id.inner(), prop))
            })
            .collect::<Result<Vec<_>, _>>()?;
        graph.internal_add_node(t, node_internal, &props)?;
    }

    graph
        .node(node_internal)
        .expect("node added")
        .add_constant_properties(node.properties().constant())?;

    Ok(graph.node(node_internal).unwrap())
}

fn import_edge_internal<
    'a,
    G: StaticGraphViewOps
        + InternalAdditionOps<Error = GraphError>
        + InternalDeletionOps<Error = GraphError>
        + InternalPropertyAdditionOps<Error = GraphError>
        + InternalMaterialize,
    GHH: GraphViewOps<'a>,
    GH: GraphViewOps<'a>,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    edge: &EdgeView<GHH, GH>,
    src_id: V,
    dst_id: V,
    merge: bool,
) -> Result<EdgeView<G, G>, GraphError>
where
    GraphError: From<<G as InternalAdditionOps>::Error>,
    GraphError: From<<G as InternalDeletionOps>::Error>,
    GraphError: From<<G as InternalPropertyAdditionOps>::Error>,
{
    let src_id = src_id.as_node_ref();
    let dst_id = dst_id.as_node_ref();
    if !merge && graph.has_edge(&src_id, &dst_id) {
        if let Some(existing_edge) = graph.edge(src_id, dst_id) {
            return Err(GraphError::EdgeExistsError(
                existing_edge.src().id(),
                existing_edge.dst().id(),
            ));
        }
    }

    // Add edges first to ensure associated nodes are present
    for ee in edge.explode_layers() {
        let layer_id = ee.edge.layer().expect("exploded layers");
        let layer_name = graph.get_layer_name(layer_id);

        for ee in ee.explode() {
            graph.add_edge(
                ee.time().expect("exploded edge"),
                &src_id,
                &dst_id,
                ee.properties().temporal().collect_properties(),
                Some(&layer_name),
            )?;
        }

        for (t, _) in edge.deletions_hist() {
            let ti = time_from_input(graph, t.t())?;
            let src_node = graph.resolve_node(src_id)?.inner();
            let dst_node = graph.resolve_node(dst_id)?.inner();
            let layer = graph.resolve_layer(Some(&layer_name))?.inner();
            graph.internal_delete_edge(ti, src_node, dst_node, layer)?;
        }

        graph
            .edge(&src_id, &dst_id)
            .expect("edge added")
            .add_constant_properties(ee.properties().constant(), Some(&layer_name))?;
    }

    Ok(graph.edge(&src_id, &dst_id).unwrap())
}

fn check_existing_nodes<
    G: StaticGraphViewOps
        + InternalAdditionOps
        + InternalDeletionOps
        + InternalPropertyAdditionOps
        + InternalMaterialize,
    V: AsNodeRef,
>(
    graph: &G,
    ids: &[V],
    merge: bool,
) -> Result<(), GraphError> {
    if !merge {
        let mut existing_nodes = vec![];
        for id in ids {
            if let Some(node) = graph.node(id) {
                existing_nodes.push(node.id());
            }
        }
        if !existing_nodes.is_empty() {
            return Err(GraphError::NodesExistError(existing_nodes));
        }
    }
    Ok(())
}

fn check_existing_edges<
    G: StaticGraphViewOps
        + InternalAdditionOps
        + InternalDeletionOps
        + InternalPropertyAdditionOps
        + InternalMaterialize,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    new_ids: &[(V, V)],
    merge: bool,
) -> Result<(), GraphError> {
    if !merge {
        let mut existing_edges = vec![];
        for (src, dst) in new_ids {
            if let Some(existing_edge) = graph.edge(src, dst) {
                existing_edges.push((existing_edge.src().id(), existing_edge.dst().id()));
            }
        }
        if !existing_edges.is_empty() {
            return Err(GraphError::EdgesExistError(existing_edges));
        }
    }
    Ok(())
}
