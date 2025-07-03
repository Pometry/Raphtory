use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_ref::AsNodeRef},
        utils::time::IntoTimeWithFormat,
    },
    db::{
        api::{
            mutation::{time_from_input_session, CollectProperties, TryIntoInputTime},
            state::ops::node,
            view::StaticGraphViewOps,
        },
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::{into_graph_err, GraphError},
    prelude::{GraphViewOps, NodeViewOps},
};
use raphtory_api::core::{
    entities::properties::prop::Prop,
    storage::dict_mapper::MaybeNew::{Existing, New},
};
use raphtory_storage::mutation::addition_ops::{
    AtomicEdgeAddition, InternalAdditionOps, SessionAdditionOps,
};

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
        let session = self.write_session().map_err(|err| err.into())?;
        self.validate_gids(
            [src.as_node_ref(), dst.as_node_ref()]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
        )
        .map_err(into_graph_err)?;
        let props = self
            .validate_props(
                false,
                self.edge_meta(),
                props.into_iter().map(|(k, v)| (k, v.into())),
            )
            .map_err(into_graph_err)?;

        let ti = time_from_input_session(&session, t)?;
        let src_id = self
            .resolve_node(src.as_node_ref())
            .map_err(into_graph_err)?
            .inner();
        let dst_id = self
            .resolve_node(dst.as_node_ref())
            .map_err(into_graph_err)?
            .inner();
        let layer_id = self.resolve_layer(layer).map_err(into_graph_err)?.inner();

        let mut add_edge_op = self
            .atomic_add_edge(src_id, dst_id, None, layer_id)
            .map_err(into_graph_err)?;
        let edge_id = add_edge_op.internal_add_edge(ti, src_id, dst_id, 0, layer_id, props);

        add_edge_op.store_node_id_as_prop(src.as_node_ref(), src_id);
        add_edge_op.store_node_id_as_prop(dst.as_node_ref(), dst_id);

        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(edge_id.inner().edge, src_id, dst_id).at_layer(layer_id),
        ))
    }
}
