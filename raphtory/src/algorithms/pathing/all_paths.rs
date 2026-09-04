use crate::{
    db::{api::view::internal::GraphView, graph::nodes::Nodes},
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;

#[derive(thiserror::Error, Debug)]
pub enum AllPathsError {
    #[error("Source node is not part of the graph view")]
    SrcNodeMissing,
    #[error("Destination node is not part of the graph view")]
    DstNodeMissing,
}

pub fn all_directed_path<'graph, G: GraphView + 'graph>(
    view: &G,
    src: impl AsNodeRef,
    dst: impl AsNodeRef,
    max_depth: usize,
) -> Result<Vec<Nodes<'graph, G>>, AllPathsError> {
    let src = (&view).node(src).ok_or(AllPathsError::SrcNodeMissing)?;
    let dst = (&view).node(dst).ok_or(AllPathsError::DstNodeMissing)?;
    todo!()
}
