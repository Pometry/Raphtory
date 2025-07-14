use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef},
        utils::time::IntoTimeWithFormat,
    },
    db::{
        api::{
            mutation::{time_from_input_session, TryIntoInputTime},
            view::StaticGraphViewOps,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::{into_graph_err, GraphError},
    prelude::{GraphViewOps, NodeViewOps},
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::dict_mapper::MaybeNew::{self, Existing, New},
};
use raphtory_storage::mutation::addition_ops::{
    EdgeWriteLock, InternalAdditionOps, SessionAdditionOps,
};
use storage::wal::{WalOps, WalEntryBuilder};
use storage::WalEntry;

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
    ) -> Result<NodeView<'static, Self, Self>, GraphError>;

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
    ) -> Result<NodeView<'static, Self, Self>, GraphError>;

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
    ) -> Result<NodeView<'static, Self, Self>, GraphError> {
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
    ) -> Result<EdgeView<Self, Self>, GraphError>;

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
    ) -> Result<EdgeView<Self, Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.add_edge(time, src, dst, props, layer)
    }
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
    ) -> Result<NodeView<'static, G, G>, GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        self.validate_gids(
            [v.as_node_ref()]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
        )
        .map_err(into_graph_err)?;

        let props = self
            .validate_props(
                false,
                self.node_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;
        let ti = time_from_input_session(&session, t)?;
        let (node_id, node_type) = match node_type {
            None => self
                .resolve_node(v.as_node_ref())
                .map_err(into_graph_err)?
                .map(|node_id| (node_id, None))
                .inner(),
            Some(node_type) => {
                let node_id = self
                    .resolve_node_and_type(v.as_node_ref(), node_type)
                    .map_err(into_graph_err)?;
                node_id
                    .map(|(node_id, node_type)| (node_id.inner(), Some(node_type.inner())))
                    .inner()
            }
        };

        self.internal_add_node(
            ti,
            node_id,
            v.as_node_ref().as_gid_ref().left(),
            node_type,
            props,
        )
        .map_err(into_graph_err)?;

        Ok(NodeView::new_internal(self.clone(), node_id))
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
    ) -> Result<NodeView<'static, G, G>, GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        self.validate_gids(
            [v.as_node_ref()]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
        )
        .map_err(into_graph_err)?;

        let props = self
            .validate_props(
                false,
                self.node_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;
        let ti = time_from_input_session(&session, t)?;
        let node_id = match node_type {
            None => self
                .resolve_node(v.as_node_ref())
                .map_err(into_graph_err)?
                .map(|node_id| (node_id, None)),
            Some(node_type) => {
                let node_id = self
                    .resolve_node_and_type(v.as_node_ref(), node_type)
                    .map_err(into_graph_err)?;
                node_id.map(|(node_id, node_type)| (node_id.inner(), Some(node_type.inner())))
            }
        };

        let is_new = node_id.is_new();
        let (node_id, node_type) = node_id.inner();

        if !is_new {
            let node_id = self.node(node_id).unwrap().id();
            return Err(GraphError::NodeExistsError(node_id));
        }

        self.internal_add_node(
            ti,
            node_id,
            v.as_node_ref().as_gid_ref().left(),
            node_type,
            props,
        )
        .map_err(into_graph_err)?;

        Ok(NodeView::new_internal(self.clone(), node_id))
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
    ) -> Result<EdgeView<G, G>, GraphError> {
        // Start & log the transaction
        let txn_id = self.transaction_manager().begin();
        let wal_entry = WalEntry::begin_txn(txn_id);
        self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();

        let session = self.write_session().map_err(|err| err.into())?;

        self.validate_gids(
            [src.as_node_ref(), dst.as_node_ref()]
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

        // Log any new prop name -> prop id mappings
        let wal_entry = WalEntry::add_new_temporal_prop_ids(&props_with_status);
        self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();

        let props = props_with_status
            .into_iter()
            .map(|maybe_new| {
                let (_, prop_id, prop) = maybe_new.inner();
                (prop_id, prop)
            })
            .collect::<Vec<_>>();

        let ti = time_from_input_session(&session, t)?;
        let src_id = self.resolve_node(src.as_node_ref()).map_err(into_graph_err)?;
        let dst_id = self.resolve_node(dst.as_node_ref()).map_err(into_graph_err)?;
        let layer_id = self.resolve_layer(layer).map_err(into_graph_err)?;

        // Log any layer -> layer id mappings
        match (layer, layer_id) {
            // Only log if layer is specified & is a new layer
            (Some(layer), New(layer_id)) => {
                let wal_entry = WalEntry::add_layer_id(layer, layer_id);
                self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();
            }
            _ => {}
        }

        let layer_id = layer_id.inner();

        // Log any node -> node id mappings
        // FIXME: We are logging node -> node id mappings AFTER they are inserted into the
        // resolver. Make sure resolver mapping CANNOT get to disk before Wal.
        match (src_id, src.as_node_ref().as_gid_ref().left()) {
            (New(src_id), Some(gid)) => {
                let wal_entry = WalEntry::add_node_id(gid.into(), src_id);
                self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();
            }
            _ => {}
        }

        match (dst_id, dst.as_node_ref().as_gid_ref().left()) {
            (New(dst_id), Some(gid)) => {
                let wal_entry = WalEntry::add_node_id(gid.into(), dst_id);
                self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();
            }
            _ => {}
        }

        let src_id = src_id.inner();
        let dst_id = dst_id.inner();

        let mut add_edge_op = self
            .atomic_add_edge(src_id, dst_id, None, layer_id)
            .map_err(into_graph_err)?;

        // Log edge addition
        let c_props = &[];
        let wal_entry = WalEntry::add_edge(ti, src_id, dst_id, layer_id, &props, c_props);
        self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();

        let edge_id = add_edge_op.internal_add_static_edge(src_id, dst_id, 0);
        let edge_id = add_edge_op.internal_add_edge(
            ti,
            src_id,
            dst_id,
            edge_id.map(|eid| eid.with_layer(layer_id)),
            0,
            props,
        );

        // Log edge -> edge id mappings
        // NOTE: We log edge id mappings after they are inserted into edge segments.
        // This is fine as long as we hold onto segment locks for the entire operation.
        if let New(edge_id) = edge_id {
            let wal_entry = WalEntry::add_edge_id(src_id, dst_id, edge_id.into());
            self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();
        }

        add_edge_op.store_src_node_info(src_id, src.as_node_ref().as_gid_ref().left());
        add_edge_op.store_dst_node_info(dst_id, dst.as_node_ref().as_gid_ref().left());

        // Log transaction commit
        let wal_entry = WalEntry::commit_txn(txn_id);
        self.wal().append(&wal_entry.to_bytes().unwrap()).unwrap();

        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(edge_id.inner().edge, src_id, dst_id).at_layer(layer_id),
        ))
    }
}
