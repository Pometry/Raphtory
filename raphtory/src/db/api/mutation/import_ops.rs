use crate::{
    core::{
        entities::LayerIds,
        storage::timeindex::TimeIndexEntry,
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
            view::{
                internal::{InternalMaterialize},
                IntoDynamic, StaticGraphViewOps,
            },
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{AdditionOps, EdgeViewOps, NodeViewOps, NO_PROPS},
};

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
    /// If `force` is `Some(false)` or `None`, the function will return an error if the node already exists in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing node in the graph.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the node to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing node.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the node was successfully imported, and `Err` otherwise.
    fn import_node<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: &NodeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<NodeView<Self, Self>, GraphError>;

    /// Imports multiple nodes into the graph.
    ///
    /// This function takes a vector of references to nodes and an optional boolean flag `force`.
    /// If `force` is `Some(false)` or `None`, the function will return an error if any of the nodes already exist in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing nodes in the graph.
    ///
    /// # Arguments
    ///
    /// * `nodes` - A vector of references to the nodes to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing nodes.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the nodes were successfully imported, and `Err` otherwise.
    fn import_nodes<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: Vec<&NodeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<NodeView<Self, Self>>, GraphError>;

    /// Imports a single edge into the graph.
    ///
    /// This function takes a reference to an edge and an optional boolean flag `force`.
    /// If `force` is `Some(false)` or `None`, the function will return an error if the edge already exists in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing edge in the graph.
    ///
    /// # Arguments
    ///
    /// * `edge` - A reference to the edge to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edge.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edge was successfully imported, and `Err` otherwise.
    fn import_edge<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<EdgeView<Self, Self>, GraphError>;

    /// Imports multiple edges into the graph.
    ///
    /// This function takes a vector of references to edges and an optional boolean flag `force`.
    /// If `force` is `Some(false)` or `None`, the function will return an error if any of the edges already exist in the graph.
    /// If `force` is `Some(true)`, the function will overwrite the existing edges in the graph.
    ///
    /// # Arguments
    ///
    /// * `edges` - A vector of references to the edges to be imported.
    /// * `force` - An optional boolean flag. If `Some(true)`, the function will overwrite the existing edges.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` if the edges were successfully imported, and `Err` otherwise.
    fn import_edges<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edges: Vec<&EdgeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<EdgeView<Self, Self>>, GraphError>;
}

impl<
        G: StaticGraphViewOps
            + InternalAdditionOps
            + InternalDeletionOps
            + InternalPropertyAdditionOps
            + InternalMaterialize,
    > ImportOps for G
{
    fn import_node<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        node: &NodeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<NodeView<G, G>, GraphError> {
        if force == Some(false) || force.is_none() {
            if self.node(node.id()).is_some() {
                return Err(NodeExistsError(node.id()));
            }
        }

        for h in node.history() {
            self.add_node(h, node.name(), NO_PROPS)?;
        }
        for (name, prop_view) in node.properties().temporal().iter() {
            for (t, prop) in prop_view.iter() {
                self.add_node(t, node.name(), [(name.clone(), prop)])?;
            }
        }
        self.node(node.id())
            .expect("node added")
            .add_constant_properties(node.properties().constant())?;

        Ok(self.node(node.id()).unwrap())
    }

    fn import_nodes<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        nodes: Vec<&NodeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<NodeView<G, G>>, GraphError> {
        let mut added_nodes = vec![];
        for node in nodes {
            let res = self.import_node(node, force);
            added_nodes.push(res.unwrap())
        }
        Ok(added_nodes)
    }

    fn import_edge<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edge: &EdgeView<GHH, GH>,
        force: Option<bool>,
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        // make sure we preserve all layers even if they are empty
        // skip default layer
        for layer in edge.graph.unique_layers().skip(1) {
            self.resolve_layer(Some(&layer));
        }
        if force == Some(false) || force.is_none() {
            if self.has_edge(edge.src().name(), edge.dst().name()) {
                return Err(EdgeExistsError(edge.src().id(), edge.dst().id()));
            }
        }
        // Add edges first so we definitely have all associated nodes (important in case of persistent edges)
        // FIXME: this needs to be verified
        for ee in edge.explode_layers() {
            let layer_id = *ee.edge.layer().expect("exploded layers");
            let layer_ids = LayerIds::One(layer_id);
            let layer_name = self.get_layer_name(layer_id);
            let layer_name: Option<&str> = if layer_id == 0 {
                None
            } else {
                Some(&layer_name)
            };
            for ee in ee.explode() {
                self.add_edge(
                    ee.time().expect("exploded edge"),
                    ee.src().name(),
                    ee.dst().name(),
                    ee.properties().temporal().collect_properties(),
                    layer_name,
                )?;
            }

            if self.include_deletions() {
                for t in edge.graph.edge_deletion_history(edge.edge, layer_ids) {
                    let ti = TimeIndexEntry::from_input(self, t)?;
                    let src_id = self.resolve_node(edge.src().id(), Some(&edge.src().name()));
                    let dst_id = self.resolve_node(edge.dst().id(), Some(&edge.dst().name()));
                    let layer = self.resolve_layer(layer_name);
                    self.internal_delete_edge(ti, src_id, dst_id, layer)?;
                }
            }

            self.edge(ee.src().id(), ee.dst().id())
                .expect("edge added")
                .add_constant_properties(ee.properties().constant(), layer_name)?;
        }
        Ok(self.edge(edge.src().name(), edge.dst().name()).unwrap())
    }

    fn import_edges<GHH: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>(
        &self,
        edges: Vec<&EdgeView<GHH, GH>>,
        force: Option<bool>,
    ) -> Result<Vec<EdgeView<Self, Self>>, GraphError> {
        let mut added_edges = vec![];
        for edge in edges {
            let res = self.import_edge(edge, force);
            added_edges.push(res.unwrap())
        }
        Ok(added_edges)
    }
}
