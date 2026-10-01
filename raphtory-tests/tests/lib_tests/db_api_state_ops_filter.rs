#[cfg(test)]
mod test {
    use raphtory::db::api::state::ops::filter::{AndOp, OrOp};
    use raphtory::{
        db::api::{
            state::ops::{Const, NodeFilterOp, NodeOp},
            view::internal::NodeList,
        },
        prelude::Graph,
    };
    use raphtory_api::core::entities::VID;
    use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage};

    #[test]
    fn test_const() {
        let c = Const(true);
        assert!(!c.is_filtered());
    }

    /// A stub op with a configurable domain and `const_value` / `const_value_in_domain`, so the
    /// combinators can be exercised over non-trivial domains without building a real filter.
    #[derive(Clone)]
    struct Stub {
        cv: Option<bool>,
        cvid: Option<bool>,
        domain: NodeList,
    }

    impl NodeOp for Stub {
        type Output = bool;

        fn domain(&self, _storage: &GraphStorage) -> NodeList {
            self.domain.clone()
        }

        fn apply(&self, _storage: &GraphStorage, _node: VID) -> bool {
            true
        }

        fn const_value(&self) -> Option<bool> {
            self.cv
        }

        fn const_value_in_domain(&self, _storage: &GraphStorage) -> Option<bool> {
            self.cvid
        }
    }

    fn list(vids: impl IntoIterator<Item = usize>) -> NodeList {
        NodeList::List {
            elems: vids.into_iter().map(VID).collect(),
        }
    }

    /// Constant-true over a bounded domain but not globally — the profile of `name.is_in([...])`.
    fn member(vids: impl IntoIterator<Item = usize>) -> Stub {
        Stub {
            cv: None,
            cvid: Some(true),
            domain: list(vids),
        }
    }

    /// Domain-all and not constant over it — the profile of `node_type.is_in([...])`.
    fn wide() -> Stub {
        Stub {
            cv: None,
            cvid: None,
            domain: NodeList::All,
        }
    }

    /// Not constant, but with a bounded domain — the (currently hypothetical) shape the superset
    /// case exists for.
    fn bounded_wide(vids: impl IntoIterator<Item = usize>) -> Stub {
        Stub {
            cv: None,
            cvid: None,
            domain: list(vids),
        }
    }

    #[test]
    fn or_const_value_in_domain() {
        let g = Graph::new();
        let s = g.core_graph();

        // Both branches constant-true over their domains: the union is covered.
        let both = OrOp {
            left: member([0, 1]),
            right: member([2, 3]),
        };
        assert_eq!(both.const_value_in_domain(s), Some(true));

        // A non-constant branch with domain `All` can match anywhere, so it is never covered.
        let widened = OrOp {
            left: wide(),
            right: member([0, 1]),
        };
        assert_eq!(widened.const_value_in_domain(s), None);

        let nested = OrOp {
            left: wide(),
            right: OrOp {
                left: member([0, 1]),
                right: member([2, 3]),
            },
        };
        assert_eq!(nested.const_value_in_domain(s), None);

        // A constant-true branch whose domain covers the other branch's is trusted even when that
        // other branch is not constant (`true || false == true`)...
        let covered = OrOp {
            left: member([0, 1, 2]),
            right: bounded_wide([0, 1]),
        };
        assert_eq!(covered.const_value_in_domain(s), Some(true));
        // ...but not once the non-constant branch reaches past that domain.
        let uncovered = OrOp {
            left: member([0, 1, 2]),
            right: bounded_wide([0, 3]),
        };
        assert_eq!(uncovered.const_value_in_domain(s), None);

        // A globally-true branch has domain `All`, so it covers anything OR'd with it.
        let global = Stub {
            cv: Some(true),
            cvid: Some(true),
            domain: NodeList::All,
        };
        let global_or = OrOp {
            left: global,
            right: wide(),
        };
        assert_eq!(global_or.const_value(), Some(true));
        assert_eq!(global_or.const_value_in_domain(s), Some(true));
    }

    #[test]
    fn and_const_value_in_domain_is_the_conjunction() {
        let g = Graph::new();
        let s = g.core_graph();
        let both = AndOp {
            left: member([0, 1]),
            right: member([0, 1]),
        };
        assert_eq!(both.const_value_in_domain(s), Some(true));
        let mixed = AndOp {
            left: member([0, 1]),
            right: wide(),
        };
        assert_eq!(mixed.const_value_in_domain(s), None);
    }
}
