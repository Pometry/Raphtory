use raphtory::{
    db::{
        api::view::Filter,
        graph::{
            assertions::assert_graph_equal,
            views::filter::model::{graph_filter::GraphFilter, ViewWrapOps},
        },
    },
    prelude::*,
};
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;

#[test]
fn test_id_filter_not_in_is_equivalent_to_exclude_nodes() {
    let g = PersistentGraph::new();
    g.add_node(1, 5, NO_PROPS, None, Some("a")).unwrap();
    g.add_node(2, 2, NO_PROPS, None, Some("a")).unwrap();
    g.add_node(3, 0, NO_PROPS, None, None).unwrap();
    g.add_node(4, 1, NO_PROPS, None, Some("a")).unwrap();
    g.add_node(5, 3, NO_PROPS, None, Some("a")).unwrap();

    g.add_edge(6, 3, 6, NO_PROPS, Some("b")).unwrap();

    let expected_g = PersistentGraph::new();
    expected_g
        .add_node(1, 5, NO_PROPS, None, Some("a"))
        .unwrap();
    expected_g.resolve_layer(Some("b")).unwrap();

    assert_graph_equal(&g.exclude_nodes([0, 1, 2, 3]), &expected_g);
    assert_eq!(g.at(7).exclude_nodes([0, 1, 2, 3]).count_nodes(), 1);
    assert_graph_equal(&g.latest().exclude_nodes([0, 1, 2, 3]), &expected_g.at(6));

    assert_graph_equal(
        &g.latest().exclude_nodes([0, 1, 2, 3]),
        &g.filter(GraphFilter.latest())
            .unwrap()
            .filter(NodeFilter::id().is_not_in([0, 1, 2, 3]))
            .unwrap(),
    );
}
