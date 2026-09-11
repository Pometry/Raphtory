use raphtory::{
    db::api::{
        state::{ops::filter::NodeTypeFilterOp, NodeOp},
        view::internal::NodeList,
    },
    prelude::*,
};
use raphtory_storage::core_ops::CoreGraphOps;
use storage::api::node_type_index::NodeTypeIndexOps;

#[test]
fn type_filter_domain_uses_index() {
    let g = Graph::new();
    g.add_node(1, "a", NO_PROPS, Some("Person"), None).unwrap();
    g.add_node(1, "b", NO_PROPS, Some("Person"), None).unwrap();
    g.add_node(1, "c", NO_PROPS, Some("Company"), None).unwrap();

    let person_id = g.node_meta().get_node_type_id("Person").unwrap();
    let company_id = g.node_meta().get_node_type_id("Company").unwrap();

    let storage = g.core_graph();
    let empty_op = NodeTypeFilterOp::from_values(["Person"], &g);

    assert!(
        matches!(empty_op.domain(storage), NodeList::All),
        "empty index should fall back to a full scan"
    );

    let a = g.node("a").unwrap().node;
    let b = g.node("b").unwrap().node;
    let c = g.node("c").unwrap().node;

    // TODO: Remove manually updating the index here once it's wired up to add_node.
    storage.node_type_index().head().insert(person_id, a);
    storage.node_type_index().head().insert(person_id, b);
    storage.node_type_index().head().insert(company_id, c);

    let op = NodeTypeFilterOp::from_values(["Person"], &g);

    match op.domain(storage) {
        NodeList::List { elems } => {
            assert_eq!(elems.len(), 2);
            assert!(elems.contains(&a));
            assert!(elems.contains(&b));
            assert!(!elems.contains(&c));
        }
        NodeList::All => panic!("expected index-backed list domain"),
    }

    let both = NodeTypeFilterOp::from_values(["Person", "Company"], &g);

    match both.domain(storage) {
        NodeList::List { elems } => {
            assert_eq!(elems.len(), 3);
            assert!(elems.contains(&a));
            assert!(elems.contains(&b));
            assert!(elems.contains(&c));
        }
        NodeList::All => panic!("expected index-backed list domain"),
    }
}
