use raphtory_api::core::entities::VID;
use std::{borrow::Borrow, fmt::Debug};

use crate::{
    core::{
        entities::{
            nodes::node_ref::{AsNodeRef, NodeRef},
            LayerIds,
        },
        utils::errors::{
            GraphError,
            GraphError::{EdgeExistsError, NodeExistsError},
        },
    },
    db::{
        api::{
            mutation::internal::{
                InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
            },
            view::{internal::InternalMaterialize, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{AdditionOps, EdgeViewOps, GraphViewOps, NodeViewOps},
};
use raphtory_api::core::storage::{arc_str::OptionAsStr, timeindex::AsTime};

use super::time_from_input;

pub trait ImportOps:
    StaticGraphViewOps
    + InternalAdditionOps
    + InternalDeletionOps
    + InternalPropertyAdditionOps
    + InternalMaterialize
{
    /// Imports a single node into the graph.
    ///
    /// This function takes a reference to a node and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if the node already exists in the graph.
    /// If `force` is `true`, the function will overwrite the existing node in the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing node.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        node: &NodeView<GHH, GH>,
        force: bool,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    /// Imports a single node into the graph.
    ///
    /// This function takes a reference to a node and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if the node already exists in the graph.
    /// If `force` is `true`, the function will overwrite the existing node in the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `new_id` - The new node id.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing node.
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
        node: &NodeView<GHH, GH>,
        new_id: V,
        force: bool,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// This function takes a vector of references to nodes and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if any of the nodes already exist in the graph.
    /// If `force` is `true`, the function will overwrite the existing nodes in the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing nodes.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the nodes were successfully imported, and `Err` otherwise.
    fn import_nodes<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<GHH, GH>>>,
        force: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// This function takes a vector of references to nodes and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if any of the nodes already exist in the graph.
    /// If `force` is `true`, the function will overwrite the existing nodes in the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `new_ids` - A list of node IDs to use for the imported nodes.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing nodes.
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
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<GHH, GH>>>,
        new_ids: impl IntoIterator<Item = V>,
        force: bool,
    ) -> Result<(), GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// This function takes a reference to an edge and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if the edge already exists in the graph.
    /// If `force` is `true`, the function will overwrite the existing edge in the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edge.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edge was successfully imported, and `Err` otherwise.
    fn import_edge<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// This function takes a reference to an edge and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if the edge already exists in the graph.
    /// If `force` is `true`, the function will overwrite the existing edge in the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `new_id` - The ID of the new edge. It's a tuple of the source and destination node ids.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edge.
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
        force: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// This function takes a vector of references to edges and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if any of the edges already exist in the graph.
    /// If `force` is `true`, the function will overwrite the existing edges in the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edges.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edges were successfully imported, and `Err` otherwise.
    fn import_edges<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        force: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// This function takes a vector of references to edges and an optional boolean flag `force`.
    /// If `force` is `false`, the function will return an error if any of the edges already exist in the graph.
    /// If `force` is `true`, the function will overwrite the existing edges in the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `new_ids` - The IDs of the new edges. It's a vector of tuples of the source and destination node ids.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edges.
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
        force: bool,
    ) -> Result<(), GraphError>;
}

impl<
        G: StaticGraphViewOps
            + InternalAdditionOps
            + InternalDeletionOps
            + InternalPropertyAdditionOps
            + InternalMaterialize,
    > ImportOps for G
{
    fn import_node<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        node: &NodeView<GHH, GH>,
        force: bool,
    ) -> Result<NodeView<G, G>, GraphError> {
        import_node_internal(&self, node, node.id(), force)
    }

    fn import_node_as<
        'a,
        GHH: GraphViewOps<'a>,
        GH: GraphViewOps<'a>,
        V: AsNodeRef + Clone + Debug,
    >(
        &self,
        node: &NodeView<GHH, GH>,
        new_id: V,
        force: bool,
    ) -> Result<NodeView<Self, Self>, GraphError> {
        import_node_internal(&self, node, new_id, force)
    }

    fn import_nodes<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<GHH, GH>>>,
        force: bool,
    ) -> Result<(), GraphError> {
        for node in nodes {
            self.import_node(node.borrow(), force)?;
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
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<GHH, GH>>>,
        new_ids: impl IntoIterator<Item = V>,
        force: bool,
    ) -> Result<(), GraphError> {
        let new_ids: Vec<V> = new_ids.into_iter().collect();
        if !force {
            let mut existing_nodes = vec![];
            for new_id in &new_ids {
                if let Some(node) = self.node(new_id) {
                    existing_nodes.push(node.id());
                }
            }
            if !existing_nodes.is_empty() {
                return Err(GraphError::NodesExistError(existing_nodes));
            }
        }

        for (node, new_node_id) in nodes.into_iter().zip(new_ids.into_iter()) {
            self.import_node_as(node.borrow(), new_node_id, force)?;
        }

        Ok(())
    }

    fn import_edge<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        import_edge_internal(&self, edge, edge.src().id(), edge.dst().id(), force)
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
        force: bool,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        import_edge_internal(&self, edge, new_id.0, new_id.1, force)
    }

    fn import_edges<'a, GHH: GraphViewOps<'a>, GH: GraphViewOps<'a>>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH, GH>>>,
        force: bool,
    ) -> Result<(), GraphError> {
        for edge in edges {
            self.import_edge(edge.borrow(), force)?;
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
        force: bool,
    ) -> Result<(), GraphError> {
        let new_ids: Vec<(V, V)> = new_ids.into_iter().collect();

        if !force {
            let mut existing_edges = vec![];
            for (src, dst) in &new_ids {
                if let Some(existing_edge) = self.edge(src, dst) {
                    existing_edges.push((existing_edge.src().id(), existing_edge.dst().id()));
                }
            }

            if !existing_edges.is_empty() {
                return Err(GraphError::EdgesExistError(existing_edges));
            }
        }

        for (new_id, edge) in new_ids.into_iter().zip(edges) {
            self.import_edge_as(edge.borrow(), new_id, force)?;
        }

        Ok(())
    }
}

fn import_node_internal<
    'a,
    G: StaticGraphViewOps
        + InternalAdditionOps
        + InternalDeletionOps
        + InternalPropertyAdditionOps
        + InternalMaterialize,
    GHH: GraphViewOps<'a>,
    GH: GraphViewOps<'a>,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    node: &NodeView<GHH, GH>,
    id: V,
    force: bool,
) -> Result<NodeView<G, G>, GraphError> {
    if !force {
        if let Some(existing_node) = graph.node(&id) {
            return Err(NodeExistsError(existing_node.id()));
        }
    }

    let node_internal = match node.node_type().as_str() {
        None => graph.resolve_node(&id)?.inner(),
        Some(node_type) => {
            let (node_internal, _) = graph.resolve_node_and_type(&id, node_type)?.inner();
            node_internal.inner()
        }
    };

    for h in node.history() {
        let t = time_from_input(graph, h)?;
        graph.internal_add_node(t, node_internal, &[])?;
    }

    for (name, prop_view) in node.properties().temporal().iter() {
        let old_prop_id = node
            .graph
            .node_meta()
            .temporal_prop_meta()
            .get_id(&name)
            .unwrap();
        let dtype = node
            .graph
            .node_meta()
            .temporal_prop_meta()
            .get_dtype(old_prop_id)
            .unwrap();
        let new_prop_id = graph.resolve_node_property(&name, dtype, false)?.inner();
        for (h, prop) in prop_view.iter() {
            let t = time_from_input(graph, h)?;
            graph.internal_add_node(t, node_internal, &[(new_prop_id, prop)])?;
        }
    }

    graph
        .node(&id)
        .expect("node added")
        .add_constant_properties(node.properties().constant())?;

    Ok(graph.node(&id).unwrap())
}

fn import_edge_internal<
    'a,
    G: StaticGraphViewOps
        + InternalAdditionOps
        + InternalDeletionOps
        + InternalPropertyAdditionOps
        + InternalMaterialize,
    GHH: GraphViewOps<'a>,
    GH: GraphViewOps<'a>,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    edge: &EdgeView<GHH, GH>,
    src_id: V,
    dst_id: V,
    force: bool,
) -> Result<EdgeView<G, G>, GraphError> {
    // Preserve all layers even if they are empty (except the default layer)
    for layer in edge.graph.unique_layers().skip(1) {
        graph.resolve_layer(Some(&layer))?;
    }

    if !force && graph.has_edge(&src_id, &dst_id) {
        if let Some(existing_edge) = graph.edge(&src_id, &dst_id) {
            return Err(EdgeExistsError(
                existing_edge.src().id(),
                existing_edge.dst().id(),
            ));
        }
    }

    // Add edges first to ensure associated nodes are present
    for ee in edge.explode_layers() {
        let layer_id = ee.edge.layer().expect("exploded layers");
        let layer_ids = LayerIds::One(layer_id);
        let layer_name = graph.get_layer_name(layer_id);
        let layer_name: Option<&str> = if layer_id == 0 {
            None
        } else {
            Some(&layer_name)
        };

        for ee in ee.explode() {
            graph.add_edge(
                ee.time().expect("exploded edge"),
                &src_id,
                &dst_id,
                ee.properties().temporal().collect_properties(),
                layer_name,
            )?;
        }

        if graph.include_deletions() {
            for t in edge.graph.edge_deletion_history(edge.edge, &layer_ids) {
                let ti = time_from_input(graph, t.t())?;
                let src_node = graph.resolve_node(&src_id)?.inner();
                let dst_node = graph.resolve_node(&dst_id)?.inner();
                let layer = graph.resolve_layer(layer_name)?.inner();
                graph.internal_delete_edge(ti, src_node, dst_node, layer)?;
            }
        }

        graph
            .edge(&src_id, &dst_id)
            .expect("edge added")
            .add_constant_properties(ee.properties().constant(), layer_name)?;
    }

    Ok(graph.edge(&src_id, &dst_id).unwrap())
}
