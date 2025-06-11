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
    addition_ops::InternalAdditionOps, deletion_ops::InternalDeletionOps,
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
        let ti = time_from_input_session(&session, t).map_err(into_graph_err)?;
        let src_id = self
            .resolve_node(src.as_node_ref())
            .map_err(into_graph_err)?
            .inner();
        let dst_id = self
            .resolve_node(dst.as_node_ref())
            .map_err(into_graph_err)?
            .inner();
        let layer = self.resolve_layer(layer).map_err(into_graph_err)?.inner();
        let eid = self
            .internal_delete_edge(ti, src_id, dst_id, layer)
            .map_err(into_graph_err)?
            .inner();
        Ok(EdgeView::new(
            self.clone(),
            EdgeRef::new_outgoing(eid, src_id, dst_id).at_layer(layer),
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
