use crate::{
    core::entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef},
    db::{
        api::{
            mutation::time_from_input_session,
            view::{graph::GraphViewOps, node::NodeViewOps, StaticGraphViewOps},
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::{
    entities::properties::{
        meta::{DEFAULT_NODE_TYPE_ID, STATIC_GRAPH_LAYER_ID},
        prop::Prop,
    },
    utils::time::{IntoTimeWithFormat, TryIntoInputTime},
};
use raphtory_storage::{
    durability_ops::DurabilityOps,
    mutation::addition_ops::{
        EdgeWriteLock, GraphMutationOps, InternalAdditionOps, NodeWriteLock, PropertyMutationOps,
        SetLsn,
    },
};
use storage::wal::{GraphWalOps, WalOps};

pub trait AdditionOps: StaticGraphViewOps + InternalAdditionOps {
    // TODO: Probably add vector reference here like add
    /// Add a node to the graph
    ///
    /// # Arguments
    ///
    /// * `t` - The time
    /// * `v` - The node (can be a string or integer)
    /// * `props` - The properties of the node
    /// * `node_type` - The optional string which will be used as a node type
    ///
    /// Returns:
    ///
    /// A result containing the node id
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    /// let g = Graph::new();
    /// let v = g.add_node(0, "Alice", NO_PROPS, None, None);
    /// let v = g.add_node(0, 5, NO_PROPS, None, None);
    /// ```
    fn add_node<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(
        &self,
        t: T,
        v: V,
        props: PII,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<NodeView<'static, Self>, GraphError>;

    /// Add a node to the graph, returning an error if the node already exists.
    fn create_node<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: T,
        v: V,
        props: PII,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<NodeView<'static, Self>, GraphError>;

    fn add_node_with_custom_time_format<
        V: AsNodeRef,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: &str,
        fmt: &str,
        v: V,
        props: PII,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<NodeView<'static, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_node(time, v, props, node_type, layer)
    }

    // TODO: Node.name which gets ._id property else numba as string
    /// Adds an edge between the source and destination nodes with the given timestamp and properties.
    ///
    /// # Arguments
    ///
    /// * `t` - The timestamp of the edge.
    /// * `src` - An instance of `T` that implements the `InputNode` trait representing the source node.
    /// * `dst` - An instance of `T` that implements the `InputNode` trait representing the destination node.
    /// * `props` - A vector of tuples containing the property name and value pairs to add to the edge.
    ///
    /// # Example
    ///
    /// ```
    /// use raphtory::prelude::*;
    ///
    /// let graph = Graph::new();
    /// graph.add_node(1, "Alice", NO_PROPS, None, None).unwrap();
    /// graph.add_node(2, "Bob", NO_PROPS, None, None).unwrap();
    /// graph.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
    /// ```
    fn add_edge<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PII,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError>;

    fn add_edge_with_custom_time_format<
        V: AsNodeRef,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        props: PII,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }

    fn flush(&self) -> Result<(), GraphError>;

    fn vacuum(&self) -> Result<(), GraphError>;
}

impl<G: InternalAdditionOps + StaticGraphViewOps> AdditionOps for G {
    fn add_node<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PII: IntoIterator<Item = (PN, P)>,
    >(
        &self,
        t: T,
        v: V,
        props: PII,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<NodeView<'static, G>, GraphError> {
        let fail_if_exists = false;
        add_node_impl(self, t, v, props, node_type, fail_if_exists, layer)
    }

    fn create_node<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: T,
        v: V,
        props: PII,
        node_type: Option<&str>,
        layer: Option<&str>,
    ) -> Result<NodeView<'static, G>, GraphError> {
        let fail_if_exists = true;
        add_node_impl(self, t, v, props, node_type, fail_if_exists, layer)
    }

    fn add_edge<
        V: AsNodeRef,
        T: TryIntoInputTime,
        PN: AsRef<str>,
        P: Into<Prop>,
        PI: ExactSizeIterator<Item = (PN, P)>,
        PII: IntoIterator<Item = (PN, P), IntoIter = PI>,
    >(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PII,
        layer: Option<&str>,
    ) -> Result<EdgeView<G>, GraphError> {
        let mutable_graph = self.mutable()?;
        let transaction_manager = mutable_graph.transaction_manager()?;
        let wal = mutable_graph.wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let src = src.as_node_ref();
        let dst = dst.as_node_ref();

        let prop_writer = mutable_graph
            .validate_props(false, self.edge_meta(), props)
            .map_err(into_graph_err)?;

        let ti = time_from_input_session(mutable_graph, t)?;
        let src_gid = src.as_gid_ref();
        let dst_gid = dst.as_gid_ref();

        // Hold all locks for src node, dst node and edge until writer goes out of scope.
        let mut writer = mutable_graph
            .atomic_add_edge(src, dst, None)
            .map_err(into_graph_err)?;
        let (prop_names, props) = prop_writer.into_props_and_names();
        let layer_id = mutable_graph.resolve_layer(layer).inner();
        mutable_graph
            .edge_meta()
            .temporal_prop_mapper()
            .mark_props_in_layer(layer_id, props.iter().map(|(id, _)| *id));

        let src_id = writer.src().inner();
        let dst_id = writer.dst().inner();
        let edge_id = writer.eid().inner();

        let props_for_wal = prop_names
            .iter()
            .zip(props.iter())
            .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
            .collect::<Vec<_>>();

        // NOTE: We log edge id after it is inserted into the edge segment.
        // This is fine as long as we hold onto the edge segment lock through writer
        // for the entire operation.
        let lsn = wal.log_add_edge(
            transaction_id,
            ti,
            src_gid,
            src_id,
            dst_gid,
            dst_id,
            edge_id,
            props_for_wal,
            layer,
            layer_id,
        )?;

        mutable_graph
            .edge_meta()
            .temporal_prop_mapper()
            .mark_props_in_layer(layer_id, props.iter().map(|(id, _)| *id));
        writer.update_properties(ti, layer_id, props);

        // Update the src, dst and edge segments with the lsn of the wal entry.
        writer.set_lsn(lsn);

        transaction_manager.end_transaction(transaction_id);

        // Segment locks can be released before flush to allow
        // other operations to proceed.
        drop(writer);

        // Flush the wal entry to disk.
        // Any error here is fatal.
        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(edge_id, src_id, dst_id).at_layer(layer_id),
        ))
    }

    fn flush(&self) -> Result<(), GraphError> {
        self.core_graph().flush().map_err(into_graph_err)
    }

    fn vacuum(&self) -> Result<(), GraphError> {
        self.core_graph().vacuum().map_err(into_graph_err)?;
        Ok(())
    }
}

fn add_node_impl<
    G: InternalAdditionOps + StaticGraphViewOps,
    V: AsNodeRef,
    T: TryIntoInputTime,
    PN: AsRef<str>,
    P: Into<Prop>,
    PII: IntoIterator<Item = (PN, P)>,
>(
    graph: &G,
    t: T,
    v: V,
    props: PII,
    node_type: Option<&str>,
    fail_if_exists: bool,
    layer: Option<&str>,
) -> Result<NodeView<'static, G>, GraphError> {
    let mutable_graph = graph.mutable()?;
    let transaction_manager = mutable_graph.transaction_manager()?;
    let wal = mutable_graph.wal()?;
    let transaction_id = transaction_manager.begin_transaction();
    let node_ref = v.as_node_ref();
    let node_gid = node_ref.as_gid_ref();
    let ti = time_from_input_session(&mutable_graph, t)?;
    let validated_properties =
        mutable_graph.validate_props(false, mutable_graph.node_meta(), props)?;
    let mut writer = mutable_graph
        .atomic_add_node(node_ref, node_type)
        .map_err(into_graph_err)?;

    let node_exists = !writer.node().is_new();
    let node_id = writer.node().inner();

    if fail_if_exists && node_exists {
        drop(writer);
        let node_id = graph.node(node_id).unwrap().id();
        return Err(GraphError::NodeExistsError(node_id));
    }

    let node_type_id = match node_type {
        None => DEFAULT_NODE_TYPE_ID,
        Some(_) => writer.get_type(),
    };

    // We don't care about logging the default node type.
    let node_type_and_id =
        node_type.zip(Some(node_type_id).filter(|&id| id != DEFAULT_NODE_TYPE_ID));

    let (prop_names, props) = validated_properties.into_props_and_names();
    let layer_id = match layer {
        Some(layer) => mutable_graph.resolve_layer(Some(layer)).inner(),
        None => STATIC_GRAPH_LAYER_ID,
    };
    mutable_graph
        .node_meta()
        .temporal_prop_mapper()
        .mark_props_in_layer(layer_id, props.iter().map(|(id, _)| *id));

    let props_for_wal = prop_names
        .iter()
        .zip(props.iter())
        .map(|(prop_name, (prop_id, prop))| (prop_name.as_ref(), *prop_id, prop.clone()))
        .collect::<Vec<_>>();

    // Create a wal entry to mark operation as durable.
    let lsn = wal.log_add_node(
        transaction_id,
        ti,
        node_gid,
        node_id,
        node_type_and_id,
        props_for_wal,
        layer,
        layer_id,
    )?;

    writer.update_properties(ti, layer_id, props);
    // Update node segment with the lsn of the wal entry.
    writer.set_lsn(lsn);
    transaction_manager.end_transaction(transaction_id);

    // Segment lock can be released before flush to allow
    // other operations to proceed.
    drop(writer);

    // Flush the wal entry to disk.
    // Any error here is fatal.
    if let Err(e) = wal.flush(lsn) {
        return Err(GraphError::FatalWriteError(e));
    }

    Ok(NodeView::new_internal(graph.clone(), node_id))
}
