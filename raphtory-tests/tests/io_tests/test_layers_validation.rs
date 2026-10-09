use raphtory::prelude::{AdditionOps, DeletionOps, Graph, GraphViewOps, NO_PROPS};

const FUNKY: &[&str] = &["/../../escape", "/.", "..", r"C:\\"];

#[test]
fn test_validation_on_addition_ops() {
    let g = Graph::new();
    for layer in FUNKY {
        assert!(g.add_node(0, 0, NO_PROPS, None, Some(layer)).is_err());
        assert!(g.add_edge(0, 0, 0, NO_PROPS, Some(layer)).is_err());
        assert!(g.delete_edge(0, 0, 0, Some(layer)).is_err())
    }
    assert!(g.is_empty());
}

#[test]
fn test_valid_layers() {
    let g = Graph::new();
    g.add_node(0, 0, NO_PROPS, None, Some("IS_PART_OF"))
        .unwrap();
}
