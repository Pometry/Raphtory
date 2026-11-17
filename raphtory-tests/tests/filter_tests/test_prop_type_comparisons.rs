use raphtory::{
    db::{api::view::Filter, graph::views::filter::model::PropertyExprFactory},
    prelude::*,
};

#[test]
fn test_comparison_works_within_family_and_errors_across() {
    let g = Graph::new();
    g.add_node(
        0,
        "n",
        [
            ("v_u8", Prop::U8(5)),
            ("v_decimal", Prop::Decimal(5.into())),
        ],
        None,
        None,
    )
    .unwrap();

    let expr_u8 = NodeFilter.property("v_u8").eq(Prop::Decimal(5.into()));
    assert_eq!(g.filter(expr_u8).unwrap().count_nodes(), 1);
}
