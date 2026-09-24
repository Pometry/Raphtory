use raphtory::{
    db::{
        api::{
            state::{ops::filter::NodeTypeFilterOp, NodeOp},
            view::{
                filter_ops::Filter,
                internal::{ListOps, NodeList},
            },
        },
        graph::views::filter::{
            model::{
                node_filter::{ops::NodeFilterOps, NodeFilter},
                property_filter::ops::PropertyFilterOps,
                ComposableFilter, PropertyFilterFactory,
            },
            CreateFilter,
        },
    },
    prelude::*,
};
use raphtory_storage::core_ops::CoreGraphOps;
use std::fmt::Debug;
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

    let domain = op.domain(storage);
    assert!(domain.dynamically_trusted());
    match domain {
        list @ NodeList::NodeTypeIdx { .. } => {
            let elems = list.into_index(storage);
            assert_eq!(elems.len(), 2);
            assert!(elems.contains(&a));
            assert!(elems.contains(&b));
            assert!(!elems.contains(&c));
        }
        other => panic!("expected index-backed node type domain, got {other:?}"),
    }

    let both = NodeTypeFilterOp::from_values(["Person", "Company"], &g);

    match both.domain(storage) {
        list @ NodeList::NodeTypeIdx { .. } => {
            let elems = list.into_index(storage);
            assert_eq!(elems.len(), 3);
            assert!(elems.contains(&a));
            assert!(elems.contains(&b));
            assert!(elems.contains(&c));
        }
        other => panic!("expected index-backed node type domain, got {other:?}"),
    }
}

/// Builds the same small typed graph twice: once with the node type index
/// populated (so type filters take the `NodeList::NodeTypeIdx` path) and once
/// without (full-scan fallback), so every query can be checked against both.
fn typed_graphs() -> (Graph, Graph) {
    let build = |index: bool| {
        let g = Graph::new();
        let nodes = [
            ("a", "Person", 1i64),
            ("b", "Person", 2),
            ("c", "Company", 3),
            ("d", "Company", 4),
            ("e", "City", 5),
        ];
        for (name, node_type, p) in nodes {
            g.add_node(1, name, [("p", p)], Some(node_type), None)
                .unwrap();
        }
        // untyped node: the loader does not index the default type
        g.add_node(1, "f", [("p", 6i64)], None, None).unwrap();
        for (src, dst) in [
            ("a", "b"),
            ("a", "c"),
            ("b", "d"),
            ("c", "e"),
            ("d", "a"),
            ("f", "a"),
        ] {
            g.add_edge(2, src, dst, NO_PROPS, None).unwrap();
        }
        if index {
            // TODO: Remove manually updating the index here once it's wired up to add_node.
            let storage = g.core_graph();
            for node in g.nodes() {
                let type_id = storage.node_type_id(node.node);
                if type_id != 0 {
                    storage.node_type_index().head().insert(type_id, node.node);
                }
            }
        }
        g
    };
    (build(true), build(false))
}

fn sorted_names<'graph, G: GraphViewOps<'graph>>(g: &G) -> Vec<String> {
    let mut names = g.nodes().name().collect::<Vec<_>>();
    names.sort();
    names
}

fn sorted_edges<'graph, G: GraphViewOps<'graph>>(g: &G) -> Vec<(String, String)> {
    let mut edges = g
        .edges()
        .iter()
        .map(|e| (e.src().name(), e.dst().name()))
        .collect::<Vec<_>>();
    edges.sort();
    edges
}

#[test]
fn type_index_subgraph_matches_scan() {
    let (indexed, scanned) = typed_graphs();

    for types in [
        vec!["Person"],
        vec!["Company", "Person"],
        vec!["City"],
        vec![],
    ] {
        let i = indexed.subgraph_node_types(types.clone());
        let s = scanned.subgraph_node_types(types.clone());
        assert!(
            matches!(i.node_list(), NodeList::NodeTypeIdx { .. }),
            "expected the indexed subgraph to use the node type index"
        );
        assert_eq!(sorted_names(&i), sorted_names(&s), "{types:?}");
        assert_eq!(i.count_nodes(), s.count_nodes(), "{types:?}");
        assert_eq!(i.count_edges(), s.count_edges(), "{types:?}");
        assert_eq!(sorted_edges(&i), sorted_edges(&s), "{types:?}");
        assert_eq!(i.nodes().len(), s.nodes().len(), "{types:?}");
    }

    let persons = indexed.subgraph_node_types(["Person"]);
    assert_eq!(sorted_names(&persons), ["a", "b"]);
    assert_eq!(sorted_edges(&persons), [("a".into(), "b".into())]);
}

fn check_filter_matches_scan<F: CreateFilter + Clone + Debug>(
    indexed: &Graph,
    scanned: &Graph,
    filter: F,
) {
    let i = indexed.filter(filter.clone()).unwrap();
    let s = scanned.filter(filter.clone()).unwrap();
    assert_eq!(sorted_names(&i), sorted_names(&s), "{filter:?}");
    assert_eq!(i.count_nodes(), s.count_nodes(), "{filter:?}");
    assert_eq!(sorted_edges(&i), sorted_edges(&s), "{filter:?}");

    let mut i_nodes = indexed
        .nodes()
        .filter(filter.clone())
        .unwrap()
        .name()
        .collect::<Vec<_>>();
    let mut s_nodes = scanned
        .nodes()
        .filter(filter.clone())
        .unwrap()
        .name()
        .collect::<Vec<_>>();
    i_nodes.sort();
    s_nodes.sort();
    assert_eq!(i_nodes, s_nodes, "{filter:?}");
}

#[test]
fn type_index_combined_filters_match_scan() {
    let (indexed, scanned) = typed_graphs();

    check_filter_matches_scan(&indexed, &scanned, NodeFilter::node_type().eq("Person"));
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::node_type()
            .eq("Person")
            .and(NodeFilter.property("p").gt(1i64)),
    );
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::node_type()
            .eq("Person")
            .or(NodeFilter.property("p").gt(3i64)),
    );
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::node_type()
            .is_in(["Person", "City"])
            .or(NodeFilter::name().eq("c")),
    );
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::node_type()
            .is_in(["Person", "Company"])
            .and(NodeFilter::node_type().is_in(["Company", "City"])),
    );
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::name()
            .is_in(["a", "c", "e"])
            .and(NodeFilter::node_type().is_in(["Person", "City"])),
    );

    check_filter_matches_scan(&indexed, &scanned, NodeFilter::node_type().ne("Person"));
    check_filter_matches_scan(
        &indexed,
        &scanned,
        NodeFilter::node_type().is_in(["_default", "City"]),
    );

    // type filter stacked on a property filtered view must not claim exactness
    let i = indexed
        .filter(NodeFilter.property("p").gt(1i64))
        .unwrap()
        .subgraph_node_types(["Person"]);
    assert_eq!(sorted_names(&i), ["b"]);
    assert_eq!(i.count_nodes(), 1);
}
