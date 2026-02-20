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
    entities::properties::{meta::DEFAULT_NODE_TYPE_ID, prop::Prop},
    utils::time::{IntoTimeWithFormat, TryIntoInputTime},
};
use raphtory_storage::mutation::{
    addition_ops::{EdgeWriteLock, InternalAdditionOps},
    durability_ops::DurabilityOps,
    MutationError,
};
use storage::wal::{GraphWalOps, WalOps};

pub trait AdditionOps: StaticGraphViewOps + InternalAdditionOps<Error: Into<GraphError>> {
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
    /// let v = g.add_node(0, "Alice", NO_PROPS, None);
    /// let v = g.add_node(0, 5, NO_PROPS, None);
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
    ) -> Result<NodeView<'static, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_node(time, v, props, node_type)
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
    /// graph.add_node(1, "Alice", NO_PROPS, None).unwrap();
    /// graph.add_node(2, "Bob", NO_PROPS, None).unwrap();
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

    fn flush(&self) -> Result<(), Self::Error>;
}

impl<G: InternalAdditionOps<Error: Into<GraphError>> + StaticGraphViewOps> AdditionOps for G {
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
    ) -> Result<NodeView<'static, G>, GraphError> {
        let error_if_exists = false;
        add_node_impl(self, t, v, props, node_type, error_if_exists)
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
    ) -> Result<NodeView<'static, G>, GraphError> {
        let error_if_exists = true;
        add_node_impl(self, t, v, props, node_type, error_if_exists)
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
        let transaction_manager = self.core_graph().transaction_manager()?;
        let wal = self.core_graph().wal()?;
        let transaction_id = transaction_manager.begin_transaction();
        let session = self.write_session().map_err(|err| err.into())?;
        let src = src.as_node_ref();
        let dst = dst.as_node_ref();

        self.validate_gids(
            [src, dst]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
        )
        .map_err(into_graph_err)?;

        let props_with_status = self
            .validate_props_with_status(
                false,
                self.edge_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;

        let props_for_wal = props_with_status
            .iter()
            .map(|maybe_new| {
                let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
                (prop_name.as_ref(), *prop_id, prop.clone())
            })
            .collect::<Vec<_>>();

        let ti = time_from_input_session(&session, t)?;
        let src_gid = src.as_gid_ref().left();
        let dst_gid = dst.as_gid_ref().left();

        // FIXME: We are logging node -> node id mappings AFTER they are inserted into the
        // resolver. Make sure resolver mapping CANNOT get to disk before Wal.
        let src_id = self.resolve_node(src).map_err(into_graph_err)?;
        let dst_id = self.resolve_node(dst).map_err(into_graph_err)?;
        let layer_id = self.resolve_layer(layer).map_err(into_graph_err)?;

        let src_id = src_id.inner();
        let dst_id = dst_id.inner();
        let layer_id = layer_id.inner();

        // Hold all locks for src node, dst node and edge until add_edge_op goes out of scope.
        let mut writers = self
            .atomic_add_edge(src_id, dst_id, None, layer_id)
            .map_err(into_graph_err)?;

        // NOTE: We log edge id after it is inserted into the edge segment.
        // This is fine as long as we hold onto the edge segment lock through add_edge_op
        // for the entire operation.
        let edge_id = writers.internal_add_static_edge(src_id, dst_id);

        // All names, ids and values have been generated for this operation.
        // Create a wal entry to mark it as durable.
        let lsn = wal.log_add_edge(
            transaction_id,
            ti,
            src_gid,
            src_id,
            dst_gid,
            dst_id,
            edge_id.inner(),
            layer,
            layer_id,
            props_for_wal,
        )?;

        let props = props_with_status
            .into_iter()
            .map(|maybe_new| {
                let (_, prop_id, prop) = maybe_new.inner();
                (prop_id, prop)
            })
            .collect::<Vec<_>>();

        let edge_id = writers.internal_add_edge(
            ti,
            src_id,
            dst_id,
            edge_id.map(|eid| eid.with_layer(layer_id)),
            props,
        );

        writers.store_src_node_info(src_id, src_gid);
        writers.store_dst_node_info(dst_id, dst_gid);

        // Update the src, dst and edge segments with the lsn of the wal entry.
        writers.set_lsn(lsn);

        transaction_manager.end_transaction(transaction_id);

        // Segment locks can be released before flush to allow
        // other operations to proceed.
        drop(writers);

        // Flush the wal entry to disk.
        // Any error here is fatal.
        if let Err(e) = wal.flush(lsn) {
            return Err(GraphError::FatalWriteError(e));
        }

        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(edge_id.inner().edge, src_id, dst_id).at_layer(layer_id),
        ))
    }

    fn flush(&self) -> Result<(), Self::Error> {
        self.core_graph()
            .flush()
            .map_err(|err| MutationError::from(err).into())
    }
}

fn add_node_impl<
    G: InternalAdditionOps<Error: Into<GraphError>> + StaticGraphViewOps,
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
    error_if_exists: bool,
) -> Result<NodeView<'static, G>, GraphError> {
    let transaction_manager = graph.core_graph().transaction_manager()?;
    let wal = graph.core_graph().wal()?;
    let transaction_id = transaction_manager.begin_transaction();
    let session = graph.write_session().map_err(|err| err.into())?;
    let node_ref = v.as_node_ref();

    graph
        .validate_gids(
            [node_ref]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
        )
        .map_err(into_graph_err)?;

    let props_with_status = graph
        .validate_props_with_status(
            false,
            graph.node_meta(),
            props.into_iter().map(|(k, v)| (k, v.into())),
        )
        .map_err(into_graph_err)?;

    let node_gid = node_ref.as_gid_ref().left();
    let ti = time_from_input_session(&session, t)?;

    let (node_id, node_type_id) = graph
        .resolve_and_update_node_and_type(node_ref, node_type)
        .map_err(into_graph_err)?
        .inner();

    let is_new = node_id.is_new();
    let node_id = node_id.inner();
    let node_type_id = node_type_id.inner();

    if error_if_exists && !is_new {
        let node_id = graph.node(node_id).unwrap().id();
        return Err(GraphError::NodeExistsError(node_id));
    }

    // We don't care about logging the default node type.
    let node_type_and_id = Some(node_type_id)
        .filter(|&id| id != DEFAULT_NODE_TYPE_ID)
        .and_then(|id| node_type.map(|name| (name, id)));

    let props = props_with_status
        .iter()
        .map(|maybe_new| {
            let (_, prop_id, prop) = maybe_new.as_ref().inner();
            (*prop_id, prop.clone())
        })
        .collect::<Vec<_>>();

    let mut writer = graph
        .internal_add_node(ti, node_id, props)
        .map_err(into_graph_err)?;

    let props_for_wal = props_with_status
        .iter()
        .map(|maybe_new| {
            let (prop_name, prop_id, prop) = maybe_new.as_ref().inner();
            (prop_name.as_ref(), *prop_id, prop.clone())
        })
        .collect::<Vec<_>>();

    // Create a wal entry to mark operation as durable.
    let lsn = wal.log_add_node(
        transaction_id,
        ti,
        node_gid,
        node_id,
        node_type_and_id,
        props_for_wal,
    )?;

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
