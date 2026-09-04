use raphtory::{
    db::{
        api::view::Filter,
        graph::views::filter::model::{node_expr::DynCreateOp, DynCreateFilter},
    },
    prelude::*,
};
use std::sync::Arc;

#[test]
fn edge_endpoint_id_validation() {
    let g = Graph::new();
    g.add_edge(0, "a", "b", NO_PROPS, None).unwrap();

    // typed
    let f = EntityExprFilterOps::eq(EdgeFilter::src().id(), Prop::I64(3));
    match g.filter(f) {
        Ok(_) => println!("typed: NO RAISE"),
        Err(e) => println!("typed: raised {e}"),
    }

    // dyn (python path)
    let id_expr: Arc<dyn DynCreateOp> = Arc::new(EdgeFilter::src().id());
    let cmp = EntityExprFilterOps::eq(id_expr, Prop::I64(3));
    let dynf: Arc<dyn DynCreateFilter> = Arc::new(cmp);
    match g.filter(dynf) {
        Ok(_) => println!("dyn: NO RAISE"),
        Err(e) => println!("dyn: raised {e}"),
    }
}
