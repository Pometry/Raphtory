use crate::model::{
    algorithms::{filtered_view, GqlExecutableAlgorithm},
    graph::{filtering::GqlViewFilter, node_id::GqlNodeId},
};
use raphtory::{
    algorithms::motifs::local_triangle_count::local_triangle_count, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Local triangle count of a single node, see [`local_triangle_count`].
pub(crate) struct GqlLocalTriangleCount;

pub(crate) struct GqlLocalTriangleCountArgs {
    pub(crate) node: GqlNodeId,
    pub(crate) filter: Option<GqlViewFilter>,
}

impl GqlExecutableAlgorithm for GqlLocalTriangleCount {
    type Args = GqlLocalTriangleCountArgs;
    type Output = Option<usize>;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let view = filtered_view(graph, args.filter)?;
        Ok(local_triangle_count(&view, args.node))
    }
}
