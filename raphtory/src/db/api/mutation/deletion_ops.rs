use crate::{
    core::{
        entities::vertices::input_vertex::InputVertex,
        storage::timeindex::TimeIndexEntry,
        utils::{errors::GraphError, time::IntoTimeWithFormat},
    },
    db::api::mutation::{
        internal::{InternalAdditionOps, InternalDeletionOps},
        TryIntoInputTime,
    },
};

pub trait DeletionOps {
    fn delete_edge<V: InputVertex, T: TryIntoInputTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;

    fn delete_edge_with_custom_time_format<V: InputVertex>(
        &self,
        t: &str,
        fmt: &str,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let time: i64 = t.parse_time(fmt)?;
        self.delete_edge(time, src, dst, layer)
    }
}

impl<G: InternalDeletionOps + InternalAdditionOps> DeletionOps for G {
    fn delete_edge<V: InputVertex, T: TryIntoInputTime>(
        &self,
        t: T,
        src: V,
        dst: V,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let ti = TimeIndexEntry::from_input(self, t)?;
        let src_id = self.resolve_vertex(src.id(), src.id_str());
        let dst_id = self.resolve_vertex(dst.id(), src.id_str());
        let layer = self.resolve_layer(layer);
        self.internal_delete_edge(ti, src_id, dst_id, layer)
    }
}
