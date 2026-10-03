#[cfg(test)]
mod test {
    use itertools::Itertools;
    use raphtory::{
        db::{
            api::{
                mutation::AdditionOps,
                view::{internal::BoxableGraphView, *},
            },
            graph::graph::Graph,
        },
        prelude::{NodeStateOps, NO_PROPS},
    };
    use std::sync::Arc;

    #[test]
    fn test_boxing() {
        // this tests that a boxed graph actually compiles
        let g = Graph::new();
        g.add_node(0, 1u64, NO_PROPS, None, None).unwrap();
        let boxed: Arc<dyn BoxableGraphView> = Arc::new(g);
        assert_eq!(
            boxed
                .nodes()
                .id()
                .iter_values()
                .filter_map(|v| v.as_u64())
                .collect_vec(),
            vec![1]
        );
    }
}
