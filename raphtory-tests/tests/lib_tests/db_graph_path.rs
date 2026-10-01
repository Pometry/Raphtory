#[cfg(test)]
mod test {
    use raphtory_api::core::entities::GID;

    use raphtory::prelude::*;

    #[test]
    fn test_node_view_ops() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();

        let n = Vec::from_iter(g.node(1).unwrap().neighbours().id());
        assert_eq!(n, [GID::U64(2)])
    }
}
