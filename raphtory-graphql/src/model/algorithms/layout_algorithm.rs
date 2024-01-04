use async_graphql::dynamic::{FieldValue, ResolverContext, TypeRef};
use itertools::Itertools;
use async_graphql::FieldResult;
use dynamic_graphql::SimpleObject;
use async_graphql::futures_util::future::BoxFuture;
use dynamic_graphql::internal::TypeName;
use raphtory::algorithms::layout::fruchterman_reingold::fruchterman_reingold;
use raphtory::prelude::{GraphViewOps, NodeViewOps};
use crate::model::algorithms::algorithm::Algorithm;
use crate::model::algorithms::graph_algorithms::GraphAlgorithms;


#[derive(SimpleObject)]
pub(crate) struct NodeLocation {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) x: f64,
    pub(crate) y: f64
}

pub(crate) struct Layout;

impl<'a> Algorithm<'a, GraphAlgorithms> for Layout {
    type OutputType = NodeLocation;
    fn output_type() -> TypeRef {
        TypeRef::named_nn_list_nn(NodeLocation::get_type_name())
    }
    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("iterations", TypeRef::named_nn(TypeRef::INT)),
            ("width", TypeRef::named_nn(TypeRef::FLOAT)),
            ("height", TypeRef::named_nn(TypeRef::FLOAT)),
            ("repulsion", TypeRef::named_nn(TypeRef::FLOAT)),
            ("attraction", TypeRef::named_nn(TypeRef::FLOAT)),
        ]
    }
    fn apply_algo<'b>(
        entry_point: &GraphAlgorithms,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let algo_res = fruchterman_reingold(
            &entry_point.graph,
            ctx.args.try_get("iterations").unwrap().u64().unwrap(),
            ctx.args.try_get("width").unwrap().f64().unwrap(),
            ctx.args.try_get("height").unwrap().f64().unwrap(),
            ctx.args.try_get("repulsion").unwrap().f64().unwrap(),
            ctx.args.try_get("attraction").unwrap().f64().unwrap()
        );

        let complete_loc = entry_point.graph.nodes().iter().filter_map(
            |node| Option::from({
                let pos = algo_res.get(&node.id()).unwrap();
                FieldValue::owned_any(NodeLocation {
                    id: node.id().to_string(),
                    name: node.name().clone(),
                    x: *pos.get(0).unwrap(),
                    y: *pos.get(1).unwrap()
                })
            })
        ).collect_vec();

        // let node_locations = algo_res.iter().map(
        //     |(nodeid, position)| {
        //         entry_point.graph.node(*nodeid).map(|raphtory_node| {
        //             let id = raphtory_node.id().to_string();
        //             FieldValue::owned_any(NodeLocation {
        //                 id,
        //                 name: raphtory_node.name().clone(),
        //                 x: *position.get(0).unwrap(),
        //                 y: *position.get(1).unwrap()
        //             })
        //         })
        //     })
        //     .collect_vec();
        Box::pin(async move {
            Ok(Some(FieldValue::list(complete_loc)))
        })
    }
}