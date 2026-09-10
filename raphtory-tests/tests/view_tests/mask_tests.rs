use raphtory::prelude::*;

#[test]
fn test_edge_mask() {
    let g = Graph::new();
    g.add_edge(0, 0, 1, NO_PROPS, Some("0")).unwrap();
    g.add_edge(1, 1, 2, NO_PROPS, Some("1")).unwrap();

    let masked = g.exclude_edges([(0, 1)]);
    assert!(masked.has_edge(1, 2));
    assert!(!masked.has_edge(0, 1));
    assert!(!masked.valid_layers("0").has_edge(1, 2))
}
