use super::time_from_input;
use crate::{
    core::{
        entities::nodes::node_ref::AsNodeRef,
        utils::{errors::GraphError, time::IntoTimeWithFormat},
    },
    db::{
        api::{
            mutation::{
                internal::{InternalAdditionOps, InternalDeletionOps},
                TryIntoInputTime,
            },
            view::StaticGraphViewOps,
        },
        graph::edge::EdgeView,
    },
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;

pub trait DeletionOps:
    InternalDeletionOps + InternalAdditionOps + StaticGraphViewOps + Sized
{
    fn delete_edge<V: AsNodeRef, T: TryIntoInputTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<EdgeView<Self>, GraphError> {
        let ti = time_from_input(self, t)?;
        let src_id = self.resolve_node(src)?.inner();
        let dst_id = self.resolve_node(dst)?.inner();
        let layer = self.resolve_layer(layer)?.inner();
        let eid = self
            .internal_delete_edge(ti, src_id, dst_id, layer)?
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
