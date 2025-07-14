use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::time::IntoTimeWithFormat},
    db::{
        api::{
            mutation::{time_from_input_session, TryIntoInputTime},
            view::StaticGraphViewOps,
        },
        graph::edge::EdgeView,
    },
    errors::{into_graph_err, GraphError},
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use raphtory_storage::mutation::{
    addition_ops::{EdgeWriteLock, InternalAdditionOps},
    deletion_ops::InternalDeletionOps,
};

pub trait DeletionOps:
    InternalDeletionOps<Error: Into<GraphError>>
    + InternalAdditionOps<Error: Into<GraphError>>
    + StaticGraphViewOps
    + Sized
{
    fn delete_edge<V: AsNodeRef, T: TryIntoInputTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError> {
        let session = self.write_session().map_err(|err| err.into())?;
        self.validate_gids(
            [src.as_node_ref(), dst.as_node_ref()]
                .iter()
                .filter_map(|node_ref| node_ref.as_gid_ref().left()),
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

        let edge_id = add_edge_op.internal_delete_edge(ti, src_id, dst_id, 0, layer_id);
        println!("ADDED EDGE {edge_id:?} as {src_id:?} -> {dst_id:?}");

        add_edge_op.store_src_node_info(src_id, src.as_node_ref().as_gid_ref().left());
        add_edge_op.store_dst_node_info(dst_id, dst.as_node_ref().as_gid_ref().left());

        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(edge_id.inner().edge, src_id, dst_id).at_layer(layer_id),
        ))
    }

    fn delete_edge_with_custom_time_format<V: AsNodeRef>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.delete_edge(time, src, dst, layer)
    }
}

impl<
        T: InternalDeletionOps<Error: Into<GraphError>>
            + InternalAdditionOps<Error: Into<GraphError>>
            + StaticGraphViewOps
            + Sized,
    > DeletionOps for T
{
}
