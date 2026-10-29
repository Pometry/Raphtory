use raphtory::{
    db::{
        api::view::{Filter, IntoDynamic},
        graph::views::filter::model::{
            edge_filter::EdgeFilter,
            expr::NodeExpr,
            filter_operator::{BinaryOp, UnaryOp},
            node_filter::{NodeFilter, NodeFilterFactory},
            EntityAggOps, EntityExprFilterOps, PropertyExprFactory,
        },
    },
    prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
};
use raphtory_api::core::entities::properties::prop::Prop;

/// n: flag=true, score=3, ints=[1, 2], nested_ints=[[1], [2]],
/// nested_strs=[["a"], ["b"]] · m: no properties
fn graph() -> Graph {
    let g = Graph::new();
    let ints = || Prop::list([1i64, 2]).unwrap();
    let nested_ints =
        Prop::list([Prop::list([1i64]).unwrap(), Prop::list([2i64]).unwrap()]).unwrap();
    let nested_strs = Prop::list([
        Prop::list([Prop::str("a")]).unwrap(),
        Prop::list([Prop::str("b")]).unwrap(),
    ])
    .unwrap();
    g.add_node(
        0,
        "n",
        [
            ("flag", Prop::Bool(true)),
            ("score", Prop::I64(3)),
            ("ints", ints()),
            ("nested_ints", nested_ints),
            ("nested_strs", nested_strs),
        ],
        None,
        None,
    )
    .unwrap();
    g.add_node(0, "m", NO_PROPS, None, None).unwrap();
    g
}

fn refused(g: &Graph, expr: NodeExpr) -> String {
    match g.filter(expr.clone()) {
        Ok(_) => panic!("expected {expr:?} to be refused"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn any_and_all_collapse_only_an_elementwise_bool() {
    let g = graph();
    let prop = |name: &str| NodeFilter.property(name);
    // A List<Bool> collapses to one answer; a List<List<Bool>> needs one
    // any()/all() per level.
    assert!(g.filter(prop("ints").gt(1i64).any()).is_ok());
    assert!(g.filter(prop("nested_ints").gt(1i64).any().all()).is_ok());
    let needs = "Invalid filter: any()/all() need one boolean answer per element, which \
                 comparing a list or temporal property gives; ";
    assert_eq!(
        refused(&g, prop("flag").eq(true).any()),
        format!("{needs}this expression gives a single boolean answer, so drop the any()/all()")
    );
    // A list whose innermost type is not a boolean is not an element-wise answer.
    assert_eq!(
        refused(&g, prop("nested_strs").any()),
        format!(
            "{needs}this expression gives the list itself (List<List<Str>>), so compare it \
             first and put any()/all() after the comparison"
        )
    );
    assert_eq!(
        refused(&g, prop("ints").any()),
        format!(
            "{needs}this expression gives the list itself (List<I64>), so compare it first \
             and put any()/all() after the comparison"
        )
    );
    assert_eq!(
        refused(&g, prop("score").any()),
        format!("{needs}this expression gives a single I64, so compare it without any()/all()")
    );
}

#[test]
fn a_filter_needs_one_bool_and_says_when_to_add_a_qualifier() {
    let g = graph();
    let prop = |name: &str| NodeFilter.property(name);
    assert!(g.filter(NodeExpr::from(prop("flag"))).is_ok());
    let per_element = |pt: &str| {
        format!(
            "Invalid filter: a filter needs one boolean answer, but comparing a list or \
             temporal property gives one per element ({pt}); add any() or all() to say \
             whether any or every element must match"
        )
    };
    assert_eq!(
        refused(&g, prop("ints").gt(1i64)),
        per_element("List<Bool>")
    );
    assert_eq!(
        refused(&g, prop("nested_ints").gt(1i64)),
        per_element("List<List<Bool>>")
    );
    // A list of strings is not an element-wise boolean, so the hint does not apply.
    assert_eq!(
        refused(&g, prop("nested_strs").into()),
        "Invalid filter: a filter needs a boolean answer, but this expression has type \
         List<List<Str>>"
    );
}

/// A history is a list before any graph is at hand; the graph supplies its
/// element type, at whatever depth. A list-valued property's history is a list
/// of lists, so `len()` answers once per update.
#[test]
fn a_history_takes_its_element_type_from_the_graph() {
    let g = graph();
    let history = |name: &str| NodeFilter.property(name).temporal();
    let per_element = "Invalid filter: a filter needs one boolean answer, but comparing a \
                       list or temporal property gives one per element (List<Bool>); add any() \
                       or all() to say whether any or every element must match";
    assert_eq!(refused(&g, history("score").gt(1i64)), per_element);
    assert_eq!(refused(&g, history("ints").len().ge(2u64)), per_element);
    // Each update of `ints` is a list, so a list constant is compared with
    // each update, not with the whole history.
    let ints = || Prop::list([1i64, 2]).unwrap();
    assert_eq!(refused(&g, history("ints").eq(ints())), per_element);
    assert!(g.filter(history("ints").eq(ints()).any()).is_ok());
    let kept = g
        .filter(history("ints").len().ge(2u64).any())
        .unwrap()
        .into_dynamic();
    let kept: Vec<String> = kept.nodes().iter().map(|n| n.name()).collect();
    assert_eq!(kept, ["n"]);
    assert_eq!(
        refused(&g, history("score").sum().gt("a")),
        "Invalid filter: value a of type Str cannot be compared with I64"
    );
}

/// With the left side's type still unknown, an ordering against a value no
/// type can order (a map, at any list depth) is refused where it is written;
/// a value some type could order defers to filter time.
#[test]
fn an_ordering_against_an_unorderable_value_is_refused_before_any_graph() {
    let prop = || NodeExpr::from(NodeFilter.property("p"));
    let map = || Prop::map([("k", 1i64)]);
    let refused = |value: Prop| {
        prop()
            .check_value(BinaryOp::Gt, &value)
            .unwrap_err()
            .to_string()
    };
    assert_eq!(
        refused(map()),
        "Invalid filter: operator > is not valid for map properties"
    );
    assert_eq!(
        refused(Prop::list([map()]).unwrap()),
        "Invalid filter: operator > is not valid for map properties"
    );
    assert!(prop().check_value(BinaryOp::Gt, &5i64.into()).is_ok());
    assert!(prop().check_value(BinaryOp::Gt, &true.into()).is_ok());
    assert!(prop()
        .check_value(BinaryOp::Gt, &Prop::list([1i64, 2]).unwrap())
        .is_ok());
    assert!(prop().check_value(BinaryOp::Eq, &map()).is_ok());
}

#[test]
fn the_static_checks_answer_before_any_graph() {
    let prop = || NodeExpr::from(NodeFilter.property("p"));
    // A name is a Str, known before any graph: a number is refused at once.
    let err = NodeFilter
        .name()
        .check_value(BinaryOp::Eq, &5i64.into())
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid filter: value 5 of type I64 cannot be compared with Str"
    );
    // A property's type only a graph knows, so the check waits for it.
    assert!(prop().check_value(BinaryOp::Eq, &5i64.into()).is_ok());

    // A degree always has a value; a property may be missing.
    let err = NodeFilter
        .degree()
        .check_nullable(UnaryOp::IsNone)
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid filter: IS_NONE() is not valid on an expression that always has a value"
    );
    assert!(prop().check_nullable(UnaryOp::IsNone).is_ok());

    // Comparing a name is one boolean, so any()/all() after it is refused; a
    // history compared with a value waits for the graph to know its elements.
    let err = NodeFilter
        .name()
        .eq("x")
        .check_qualifiable()
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("this expression gives a single boolean answer"),
        "{err}"
    );
    assert!(NodeFilter
        .property("p")
        .temporal()
        .gt(1i64)
        .check_qualifiable()
        .is_ok());

    // A string operator takes only a string operand; on a degree, known to be a
    // number, it is refused whatever the operand.
    let err = prop().check_string(&5i64.into()).unwrap_err().to_string();
    assert_eq!(
        err,
        "Invalid filter: value 5 of type I64 cannot be compared with Str"
    );
    assert!(prop().check_string(&"x".into()).is_ok());
    let err = NodeExpr::from(NodeFilter.name())
        .check_string(&5i64.into())
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid filter: value 5 of type I64 cannot be compared with Str"
    );
    let err = NodeFilter
        .degree()
        .check_string(&"x".into())
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid filter: string operator requires a Str property, but the property type is U64"
    );
}

/// ```text
/// time    1          2
/// x       l=[1,2]    other=1
/// a→b     l=[1,2]    other=1
/// ```
/// `all()` goes after the comparison it qualifies. Before it, on
/// the list `last()` picks, it is refused; after it, x and a→b are kept though
/// their later update lacks `l`.
#[test]
fn all_after_last_follows_the_comparison() {
    let g = Graph::new();
    let l = || Prop::list([1i64, 2]).unwrap();
    g.add_node(1, "x", [("l", l())], None, None).unwrap();
    g.add_node(2, "x", [("other", Prop::I64(1))], None, None)
        .unwrap();
    g.add_edge(1, "a", "b", [("l", l())], None).unwrap();
    g.add_edge(2, "a", "b", [("other", Prop::I64(1))], None)
        .unwrap();
    let last = || NodeFilter.property("l").temporal().last();
    assert!(refused(&g, last().all().gt(0i64)).ends_with(
        "this expression gives the list itself (List<I64>), so compare it first and put \
         any()/all() after the comparison"
    ));
    let nodes = g.filter(last().gt(0i64).all()).unwrap();
    assert_eq!(nodes.nodes().name().collect::<Vec<_>>(), ["x"]);
    let edges = g
        .filter(EdgeFilter.property("l").temporal().last().gt(0i64).all())
        .unwrap();
    let ids: Vec<(String, String)> = edges
        .edges()
        .iter()
        .map(|e| (e.src().name(), e.dst().name()))
        .collect();
    assert_eq!(ids, [("a".to_string(), "b".to_string())]);
}
