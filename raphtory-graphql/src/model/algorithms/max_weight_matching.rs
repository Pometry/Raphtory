use crate::model::{algorithms::GqlExecutableAlgorithm, graph::matching::GqlMatching};
use raphtory::{
    algorithms::bipartite::max_weight_matching::max_weight_matching, db::api::view::DynamicGraph,
    errors::GraphError,
};

/// Maximum weight matching, see [`max_weight_matching`].
pub(crate) struct GqlMaxWeightMatching;

pub(crate) struct GqlMaxWeightMatchingArgs {
    pub(crate) weight_prop: Option<String>,
    pub(crate) max_cardinality: bool,
    pub(crate) verify_optimum: bool,
}

impl GqlExecutableAlgorithm for GqlMaxWeightMatching {
    type Args = GqlMaxWeightMatchingArgs;
    type Output = GqlMatching;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let matching = max_weight_matching(
            graph,
            args.weight_prop.as_deref(),
            args.max_cardinality,
            args.verify_optimum,
        );
        Ok(matching.into())
    }
}
