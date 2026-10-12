//! What lets a node filter start somewhere smaller than every node.
//!
//! A yes/no node tree reports how it can be narrowed through
//! `Expr::pushdown`: the nodes it names by id, or a test a property index can
//! answer with a candidate set. The tree knows its own shape and says so,
//! the way `NodeOp::const_value` lets an op say it is constant.

use crate::db::{
    api::{
        state::Index,
        view::internal::{GraphView, InnerFilterOps, NodeList},
    },
    graph::views::filter::model::{
        filter_operator::{BinaryOp, StringOp},
        node_expr::ops::gid_for_id_lookup,
    },
};
use raphtory_api::core::entities::{
    properties::{
        meta::NODE_ID_PROP_ID,
        prop::{prop_hashable::HashableProp, Prop},
    },
    GID,
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::graph::graph::{NodePropPredicate, NodePropSemantics};
use std::collections::HashSet;

/// What lets a node predicate start somewhere smaller than every node.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Pushdown {
    /// `id == v`, `name == v`, `id in [..]` or `name in [..]` on the bare
    /// field: those nodes. The name is the node's external id, so it names
    /// its node as the id does.
    Ids(Vec<Prop>),
    /// A test a property index can answer with a candidate set.
    Index(IndexQuery),
}

impl Pushdown {
    /// The nodes a node filter starts from, resolved once against `graph`.
    /// `None` when no index serves the test: the filter then scans every
    /// node. Whatever comes back is a superset of the matches, and `apply`
    /// still runs on each node.
    pub(crate) fn domain<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        match self {
            Pushdown::Ids(ids) => {
                let id_type = graph.id_type();
                let elems = ids
                    .iter()
                    .map(|v| gid_for_id_lookup(id_type, v))
                    .collect::<Option<Vec<GID>>>()?
                    .into_iter()
                    .filter_map(|gid| graph.internalise_node(gid.as_node_ref()))
                    .collect();
                Some(NodeList::List { elems })
            }
            Pushdown::Index(query) => query.candidates(graph),
        }
    }
}

/// A term a node index covers, as the expression reading it reports itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexTerm {
    /// The bare node id, which names its nodes outright.
    Id,
    /// The node name: its external id, which the id index answers patterns on.
    Name,
    /// A property or metadata entry; `ever` when it is the property's history,
    /// so that any value ever held may match.
    Property {
        name: String,
        metadata: bool,
        ever: bool,
    },
}

impl IndexTerm {
    /// The latest update of a history is the property's latest value.
    pub(crate) fn latest_value(self) -> Option<IndexTerm> {
        match self {
            IndexTerm::Property {
                name,
                metadata,
                ever: true,
            } => Some(IndexTerm::Property {
                name,
                metadata,
                ever: false,
            }),
            _ => None,
        }
    }
}

/// A property test in the shape the storage's index answers: one term on one
/// side, one constant on the other, no view on the term.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexQuery {
    term: IndexTerm,
    test: IndexTest,
}

impl IndexQuery {
    /// A comparison of `term` with `value`. `!=` has no candidate set, and the
    /// id index answers patterns on the name only.
    pub(crate) fn cmp(term: IndexTerm, op: BinaryOp, value: Prop) -> Option<Self> {
        if matches!(term, IndexTerm::Id | IndexTerm::Name) {
            return None;
        }
        let test = match op {
            BinaryOp::Eq => IndexTest::Eq(value),
            BinaryOp::Lt => IndexTest::Lt(value),
            BinaryOp::Le => IndexTest::Le(value),
            BinaryOp::Gt => IndexTest::Gt(value),
            BinaryOp::Ge => IndexTest::Ge(value),
            BinaryOp::Ne => return None,
        };
        Some(IndexQuery { term, test })
    }

    /// A string test of `term` against a string constant.
    pub(crate) fn string(term: IndexTerm, op: StringOp, value: Prop) -> Option<Self> {
        if matches!(term, IndexTerm::Id) {
            return None;
        }
        let Prop::Str(s) = value else {
            return None;
        };
        let test = match op {
            StringOp::StartsWith => IndexTest::StartsWith(s.to_string()),
            StringOp::EndsWith => IndexTest::EndsWith(s.to_string()),
            StringOp::Contains => IndexTest::Contains(s.to_string()),
            StringOp::NotContains | StringOp::FuzzySearch { .. } => return None,
        };
        Some(IndexQuery { term, test })
    }

    /// Membership of `term` in `values`.
    pub(crate) fn members(term: IndexTerm, values: &[Prop]) -> Option<Self> {
        if matches!(term, IndexTerm::Id | IndexTerm::Name) {
            return None;
        }
        let values = values.iter().cloned().map(HashableProp).collect();
        Some(IndexQuery {
            term,
            test: IndexTest::In(values),
        })
    }

    /// Whether the term is a property's history. Such a test narrows only under
    /// `any()`, where any value ever held may match; a latest-value test only
    /// outside it.
    pub(crate) fn is_history(&self) -> bool {
        matches!(self.term, IndexTerm::Property { ever: true, .. })
    }

    #[cfg(test)]
    pub(crate) fn term(&self) -> &IndexTerm {
        &self.term
    }

    #[cfg(test)]
    pub(crate) fn test(&self) -> &IndexTest {
        &self.test
    }

    /// The candidates the graph's index has for this test, or `None` when no
    /// index can serve it. A restricted view's latest value can differ from the
    /// global one, so under a window or layer the query asks for every value
    /// ever held, a superset, and drops the index's exactness claim.
    fn candidates<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        let plain_view = !graph.window_filtered() && !graph.is_layer_filtered();
        let (prop_id, metadata, semantics, exact_allowed) = match &self.term {
            IndexTerm::Property {
                name,
                metadata,
                ever,
            } => {
                let prop_id = graph.node_meta().get_prop_id(name, *metadata)?;
                let (semantics, exact) = match (*ever, plain_view) {
                    (true, plain) => (NodePropSemantics::Ever, plain),
                    (false, true) => (NodePropSemantics::Latest, true),
                    (false, false) => (NodePropSemantics::Ever, false),
                };
                (prop_id, *metadata, semantics, exact)
            }
            IndexTerm::Name => (NODE_ID_PROP_ID, true, NodePropSemantics::Latest, false),
            // The ids a predicate names are resolved outright, not through the index.
            IndexTerm::Id => return None,
        };
        let mut candidates = graph.core_graph().node_prop_candidates(
            prop_id,
            metadata,
            &self.test.predicate(),
            semantics,
        )?;
        candidates.exact &= exact_allowed;
        // index candidates come ascending and deduplicated, as `from_sorted` needs
        Some(NodeList::List {
            elems: Index::from_sorted(candidates.vids, candidates.exact),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IndexTest {
    Eq(Prop),
    Lt(Prop),
    Le(Prop),
    Gt(Prop),
    Ge(Prop),
    In(HashSet<HashableProp>),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
}

impl IndexTest {
    fn predicate(&self) -> NodePropPredicate<'_> {
        match self {
            IndexTest::Eq(v) => NodePropPredicate::Eq(v),
            IndexTest::Lt(v) => NodePropPredicate::Lt(v),
            IndexTest::Le(v) => NodePropPredicate::Le(v),
            IndexTest::Gt(v) => NodePropPredicate::Gt(v),
            IndexTest::Ge(v) => NodePropPredicate::Ge(v),
            IndexTest::In(values) => NodePropPredicate::In(values),
            IndexTest::StartsWith(s) => NodePropPredicate::StartsWith(s),
            IndexTest::EndsWith(s) => NodePropPredicate::EndsWith(s),
            IndexTest::Contains(s) => NodePropPredicate::Contains(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            api::{
                state::NodeOp,
                view::internal::{CoreGraphOps, DynGraphArc},
            },
            graph::views::filter::{
                model::{
                    expr::{Expr, NodeLeaf, OpaqueFilter, OpaqueOp},
                    node_filter::{NodeFilter, NodeFilterFactory},
                    EntityExprFilterOps, PropertyExprFactory, ViewWrapOps,
                },
                CreateFilter,
            },
        },
        errors::GraphError,
        prelude::{AdditionOps, EntityAggOps, Graph, GraphViewOps, NO_PROPS},
    };
    use raphtory_api::core::entities::{
        properties::prop::{IntoProp, PropType},
        VID,
    };
    use std::sync::{Arc, Mutex};

    /// `name == "x"` names its node, the way `id == x` does: the compiled
    /// filter starts from that node alone instead of scanning every node.
    #[test]
    fn a_name_equality_starts_from_its_node() {
        let g = Graph::new();
        for name in ["alice", "bob", "carol"] {
            g.add_node(0, name, NO_PROPS, None, None).unwrap();
        }
        let bob = g.node("bob").unwrap().node;
        let alice = g.node("alice").unwrap().node;

        let op = NodeFilter
            .name()
            .eq("bob")
            .create_node_filter(g.clone())
            .unwrap();
        let NodeList::List { elems } = op.domain(g.core_graph()) else {
            panic!("name equality scanned every node");
        };
        assert!(elems.index(&bob).is_some());
        assert!(elems.index(&alice).is_none());
        assert!(op.apply(g.core_graph(), bob));
        assert!(!op.apply(g.core_graph(), alice));
    }

    /// A value that records the address of the erased graph it is compiled
    /// against.
    #[derive(Clone, Default)]
    struct GraphProbe {
        seen: Arc<Mutex<Vec<usize>>>,
    }

    impl OpaqueOp for GraphProbe {
        fn node_value_op<'g>(
            &self,
            graph: DynGraphArc<'g>,
        ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
            self.seen
                .lock()
                .unwrap()
                .push(Arc::as_ptr(&graph) as *const () as usize);
            Expr::<NodeLeaf>::Const(Prop::I64(2)).value_op(graph)
        }

        fn prop_type(&self) -> PropType {
            PropType::Empty
        }

        fn nullable(&self) -> bool {
            true
        }
    }

    /// `probe > 1` as a node filter: the filter, the comparison and the
    /// opaque term each hand the graph on; the term must receive the very
    /// `Arc` the caller passed in, not a fresh box around it per level.
    #[test]
    fn erased_levels_share_one_graph_arc() {
        let g = Graph::new();
        g.add_node(0, "n", [("a", Prop::I64(2))], None, None)
            .unwrap();
        let probe = GraphProbe::default();
        let tree = Expr::Term(NodeLeaf::Opaque(OpaqueFilter::new(probe.clone()))).gt(1i64);

        let base: DynGraphArc<'static> = Arc::new(g.clone());
        let op = tree.create_node_filter(base.clone()).unwrap();
        assert!(op.apply(g.core_graph(), VID(0)));

        assert_eq!(
            *probe.seen.lock().unwrap(),
            vec![Arc::as_ptr(&base) as *const () as usize]
        );
    }

    fn property(name: &str, ever: bool) -> IndexTerm {
        IndexTerm::Property {
            name: name.to_owned(),
            metadata: false,
            ever,
        }
    }

    fn index_of(pushdown: Option<Pushdown>) -> IndexQuery {
        match pushdown {
            Some(Pushdown::Index(query)) => query,
            other => panic!("expected an index query, got {other:?}"),
        }
    }

    #[test]
    fn plain_property_tests_reach_the_index_from_either_side() {
        let gt = index_of(NodeFilter.property("score").gt(4i64).pushdown());
        assert_eq!(gt.term(), &property("score", false));
        assert_eq!(gt.test(), &IndexTest::Gt(4i64.into_prop()));
        let flipped = Expr::<NodeLeaf>::Const(Prop::I64(4)).lt(NodeFilter.property("score"));
        assert_eq!(
            index_of(flipped.pushdown()).test(),
            &IndexTest::Gt(4i64.into_prop())
        );
        let contains = index_of(NodeFilter.property("name").contains("acme").pushdown());
        assert_eq!(contains.term(), &property("name", false));
        assert_eq!(contains.test(), &IndexTest::Contains("acme".to_owned()));
        let members = index_of(
            NodeFilter
                .property("tag")
                .is_in(["a".into_prop(), "b".into_prop()])
                .pushdown(),
        );
        assert_eq!(members.term(), &property("tag", false));
        let metadata = index_of(NodeFilter.metadata("kind").eq("x").pushdown());
        assert_eq!(
            metadata.term(),
            &IndexTerm::Property {
                name: "kind".to_owned(),
                metadata: true,
                ever: false
            }
        );
    }

    #[test]
    fn a_history_under_any_asks_for_every_value_ever_held() {
        let any = NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .any()
            .pushdown();
        assert_eq!(index_of(any).term(), &property("score", true));
        // The latest update of a history is the property's latest value.
        let latest = NodeFilter
            .property("score")
            .temporal()
            .latest()
            .eq(4i64)
            .pushdown();
        assert_eq!(index_of(latest).term(), &property("score", false));
        // A history outside any() has no single value the index can test.
        assert!(NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .pushdown()
            .is_none());
        // all() must see every value, so it scans.
        assert!(NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .all()
            .pushdown()
            .is_none());
    }

    #[test]
    fn what_the_index_cannot_answer_scans() {
        assert!(NodeFilter
            .latest()
            .property("score")
            .eq(4i64)
            .pushdown()
            .is_none());
        assert!(NodeFilter.property("score").ne(4i64).pushdown().is_none());
        assert!(NodeFilter
            .property("tag")
            .is_not_in(["a".into_prop()])
            .pushdown()
            .is_none());
        // A pattern on the name goes to the id index; equality resolves the node outright.
        let prefix = index_of(NodeFilter.name().starts_with("bo").pushdown());
        assert_eq!(prefix.term(), &IndexTerm::Name);
        assert_eq!(prefix.test(), &IndexTest::StartsWith("bo".to_owned()));
        assert!(NodeFilter
            .property("a")
            .eq(NodeFilter.property("b"))
            .pushdown()
            .is_none());
    }

    #[test]
    fn the_ids_a_predicate_names_are_resolved_outright() {
        assert_eq!(
            NodeFilter.id().eq(1u64).pushdown(),
            Some(Pushdown::Ids(vec![1u64.into_prop()]))
        );
        assert_eq!(
            NodeFilter.id().is_in([1u64, 2u64]).pushdown(),
            Some(Pushdown::Ids(vec![1u64.into_prop(), 2u64.into_prop()]))
        );
        // The name is the node's external id, so equality on it resolves the node too.
        assert_eq!(
            NodeFilter.name().eq("bob").pushdown(),
            Some(Pushdown::Ids(vec!["bob".into_prop()]))
        );
        assert_eq!(
            NodeFilter.name().is_in(["bob", "carol"]).pushdown(),
            Some(Pushdown::Ids(vec!["bob".into_prop(), "carol".into_prop()]))
        );
        // Only equality names nodes; an ordering on the id is a scan.
        assert!(NodeFilter.id().gt(1u64).pushdown().is_none());
        assert!(NodeFilter.name().ne("bob").pushdown().is_none());
        // A view written on a field is ignored, so the id stays resolvable outright.
        assert_eq!(
            NodeFilter.latest().id().eq(1u64).pushdown(),
            Some(Pushdown::Ids(vec![1u64.into_prop()]))
        );
        assert_eq!(
            NodeFilter.latest().name().eq("bob").pushdown(),
            Some(Pushdown::Ids(vec!["bob".into_prop()]))
        );
    }
}
