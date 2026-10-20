use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            mutation::time_from_input_session,
            view::{
                internal::{GraphView, InternalMaterialize},
                StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphError,
    prelude::{
        AdditionOps, DeletionOps, EdgeViewOps, GraphViewOps, NodeViewOps, PropertiesOps,
        PropertyAdditionOps,
    },
};
use raphtory_api::core::{
    entities::{
        properties::{meta::STATIC_GRAPH_LAYER_ID, prop::unify_types},
        GID,
    },
    storage::{arc_str::OptionAsStr, timeindex::AsTime},
};
use raphtory_storage::{
    graph::graph::GraphStorage,
    mutation::{
        addition_ops::InternalAdditionOps, property_addition_ops::InternalPropertyAdditionOps,
    },
};
use std::{borrow::Borrow, fmt::Debug};

pub trait ImportOps: Sized {
    /// Imports a single node into the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if the imported node already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node<'a, GHH: GraphView>(
        &self,
        node: &NodeView<'a, GHH>,
        merge: bool,
    ) -> Result<NodeView<'static, Self>, GraphError>;

    /// Imports a single node into the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `new_id` - The new node id.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if the imported node already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported node and the existing node (in the graph).
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node_as<'a, GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        node: &NodeView<'a, GHH>,
        new_id: V,
        merge: bool,
    ) -> Result<NodeView<'static, Self>, GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if any of the imported nodes already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the nodes were successfully imported.
    ///     Err(GraphError) if the operation fails.
    ///
    fn import_nodes<'a, GHH: GraphView>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH>>>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// Arguments:
    ///     nodes: A list of nodes to be imported.
    ///     new_ids: A list of node IDs to use for the imported nodes.
    ///     merge: An optional boolean flag.
    ///         If `merge` is `false`, the function will return an error if any of the imported nodes already exists in the graph.
    ///         If `merge` is `true`, the function merges the histories of the imported nodes and the existing nodes (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the nodes were successfully imported.
    ///     Err(GraphError) if the operation fails.
    fn import_nodes_as<'a, GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH>>>,
        new_ids: impl IntoIterator<Item = V>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if the imported edge already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the edge were successfully imported.
    ///     Err(GraphError) if the operation fails.
    fn import_edge<GHH: GraphView>(
        &self,
        edge: &EdgeView<GHH>,
        merge: bool,
    ) -> Result<EdgeView<Self>, GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `new_id` - The ID of the new edge. It's a tuple of the source and destination node ids.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if the imported edge already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported edge and the existing edge (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the edge were successfully imported.
    ///     Err(GraphError) if the operation fails.
    fn import_edge_as<GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        edge: &EdgeView<GHH>,
        new_id: (V, V),
        merge: bool,
    ) -> Result<EdgeView<Self>, GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if any of the imported edges already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the edges were successfully imported.
    ///     Err(GraphError) if the operation fails.
    fn import_edges<GHH: GraphView>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH>>>,
        merge: bool,
    ) -> Result<(), GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `new_ids` - The IDs of the new edges. It's a vector of tuples of the source and destination node ids.
    /// * `merge` - An optional boolean flag.
    ///   If `merge` is `false`, the function will return an error if any of the imported edges already exists in the graph.
    ///   If `merge` is `true`, the function merges the histories of the imported edges and the existing edges (in the graph).
    ///
    /// Returns:
    ///     Ok(()) if the edges were successfully imported.
    ///     Err(GraphError) if the operation fails.
    fn import_edges_as<GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH>>>,
        new_ids: impl IntoIterator<Item = (V, V)>,
        merge: bool,
    ) -> Result<(), GraphError>;
}

impl<
        G: StaticGraphViewOps + AdditionOps + DeletionOps + PropertyAdditionOps + InternalMaterialize,
    > ImportOps for G
{
    fn import_node<'a, GHH: GraphView>(
        &self,
        node: &NodeView<'a, GHH>,
        merge: bool,
    ) -> Result<NodeView<'static, G>, GraphError> {
        let mut source = None;
        let source = lock_source(self, node, &mut source)?;
        import_node_internal(self, node, node.id(), merge, source)
    }

    fn import_node_as<'a, GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        node: &NodeView<'a, GHH>,
        new_id: V,
        merge: bool,
    ) -> Result<NodeView<'static, Self>, GraphError> {
        let mut source = None;
        let source = lock_source(self, node, &mut source)?;
        import_node_internal(self, node, new_id, merge, source)
    }

    fn import_nodes<'a, GHH: GraphView>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH>>>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let nodes: Vec<_> = nodes.into_iter().collect();
        let new_ids: Vec<GID> = nodes.iter().map(|n| n.borrow().id()).collect();
        check_existing_nodes(self, &new_ids, merge)?;
        let mut source = None;
        for node in &nodes {
            let node = node.borrow();
            let source = lock_source(self, node, &mut source)?;
            import_node_internal(self, node, node.id(), merge, source)?;
        }
        Ok(())
    }

    fn import_nodes_as<'a, GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        nodes: impl IntoIterator<Item = impl Borrow<NodeView<'a, GHH>>>,
        new_ids: impl IntoIterator<Item = V>,
        merge: bool,
    ) -> Result<(), GraphError> {
        let new_ids: Vec<V> = new_ids.into_iter().collect();
        check_existing_nodes(self, &new_ids, merge)?;
        let mut source = None;
        for (node, new_node_id) in nodes.into_iter().zip(new_ids) {
            let node = node.borrow();
            let source = lock_source(self, node, &mut source)?;
            import_node_internal(self, node, new_node_id, merge, source)?;
        }
        Ok(())
    }

    fn import_edge<GHH: GraphView>(
        &self,
        edge: &EdgeView<GHH>,
        merge: bool,
    ) -> Result<EdgeView<Self>, GraphError> {
        import_edge_internal(self, edge, edge.src().id(), edge.dst().id(), merge)
    }

    fn import_edge_as<GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        edge: &EdgeView<GHH>,
        new_id: (V, V),
        merge: bool,
    ) -> Result<EdgeView<Self>, GraphError> {
        import_edge_internal(self, edge, new_id.0, new_id.1, merge)
    }

    fn import_edges<GHH: GraphView>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH>>>,
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

    fn import_edges_as<GHH: GraphView, V: AsNodeRef + Clone + Debug>(
        &self,
        edges: impl IntoIterator<Item = impl Borrow<EdgeView<GHH>>>,
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
    G: StaticGraphViewOps + AdditionOps + DeletionOps + PropertyAdditionOps + InternalMaterialize,
    GHH: GraphView,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    node: &NodeView<'a, GHH>,
    id: V,
    merge: bool,
    source: &GraphStorage,
) -> Result<NodeView<'static, G>, GraphError> {
    let id = id.as_node_ref();
    let exists = match graph.node(id) {
        Some(existing_node) if !merge => {
            return Err(GraphError::NodeExistsError(existing_node.id()));
        }
        existing_node => existing_node.is_some(),
    };

    // Collect upfront: we must not hold a read guard on the source while writing when
    // importing into the same graph, and a new node needs at least one update to exist.
    let mut rows = node.rows_gs(source).peekable();
    if rows.peek().is_none() && !exists {
        return Err(GraphError::ImportNodeWithoutUpdates(node.id()));
    }

    let src_meta = node.graph.node_meta();
    let node_type = node.node_type();
    for (t, layer_id, row) in rows {
        let layer =
            (layer_id != STATIC_GRAPH_LAYER_ID).then(|| src_meta.get_layer_name_by_id(layer_id));
        let props = row
            .into_iter()
            .map(|(prop_id, prop)| (src_meta.get_prop_name(prop_id, false), prop));
        graph.add_node(t, id, props, node_type.as_str(), layer.as_str())?;
    }

    let imported = graph.node(id).expect("node added");
    imported.add_metadata(node.metadata().iter_filtered())?;
    Ok(imported)
}

/// Returns the storage to read `node` from, read-locked so the node cannot change while we
/// import it. The lock is taken once per distinct source graph, at which point we also check
/// that its node property types unify with ours, so the imports that follow cannot fail on them.
/// Importing from the target graph itself is not locked, as our own writes would block on it.
fn lock_source<'s, G: StaticGraphViewOps, GHH: GraphView>(
    graph: &G,
    node: &NodeView<'_, GHH>,
    source: &'s mut Option<GraphStorage>,
) -> Result<&'s GraphStorage, GraphError> {
    let node_storage = node.graph.core_graph();
    if !source.as_ref().is_some_and(|s| s.ptr_eq(node_storage)) {
        check_node_prop_types(graph, &node.graph)?;
        *source = Some(if graph.core_graph().ptr_eq(node_storage) {
            node_storage.clone()
        } else {
            node_storage.lock()
        });
    }
    Ok(source.as_ref().expect("source set"))
}

fn check_node_prop_types<G: StaticGraphViewOps, GHH: GraphView>(
    graph: &G,
    source: &GHH,
) -> Result<(), GraphError> {
    let src_meta = source.node_meta();
    let meta = graph.node_meta();
    for (prop_ids, is_static) in [
        (source.node_visible_temporal_prop_ids(), false),
        (source.node_visible_metadata_ids(), true),
    ] {
        let src_mapper = if is_static {
            src_meta.metadata_mapper()
        } else {
            src_meta.temporal_prop_mapper()
        };
        for prop_id in prop_ids {
            let Some(src_dtype) = src_mapper.get_dtype(prop_id) else {
                continue;
            };
            let name = src_meta.get_prop_name(prop_id, is_static);
            if let Some((_, dtype)) = meta.get_prop_id_and_type(&name, is_static) {
                unify_types(&dtype, &src_dtype, &mut false)
                    .map_err(|err| err.with_name(name.to_string()))?;
            }
        }
    }
    Ok(())
}

fn import_edge_internal<
    G: StaticGraphViewOps + AdditionOps + DeletionOps + PropertyAdditionOps + InternalMaterialize,
    GHH: GraphView,
    V: AsNodeRef + Clone + Debug,
>(
    graph: &G,
    edge: &EdgeView<GHH>,
    src_id: V,
    dst_id: V,
    merge: bool,
) -> Result<EdgeView<G>, GraphError> {
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
    let session = graph.write_session().map_err(|err| err.into())?;
    for ee in edge.explode_layers() {
        let layer_name = ee.layer_name().expect("exploded layers");

        for ee in ee.explode() {
            graph.add_edge(
                ee.time().expect("exploded edge").t(),
                &src_id,
                &dst_id,
                ee.properties().temporal().collect_properties(),
                Some(&layer_name),
            )?;
        }

        for (t, _) in ee.deletions_hist() {
            let ti = time_from_input_session(&session, t.t())?;
            graph.delete_edge(ti.t(), src_id, dst_id, Some(&layer_name))?;
        }

        graph
            .edge(&src_id, &dst_id)
            .expect("edge added")
            .add_metadata(ee.metadata().iter_filtered(), Some(&layer_name))?;
    }

    Ok(graph.edge(&src_id, &dst_id).unwrap())
}

fn check_existing_nodes<
    G: StaticGraphViewOps + InternalAdditionOps + InternalPropertyAdditionOps + InternalMaterialize,
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
    G: StaticGraphViewOps + InternalAdditionOps + InternalPropertyAdditionOps + InternalMaterialize,
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
