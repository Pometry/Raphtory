use super::*;
use crate::{
    db::api::view::Filter,
    prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps},
};
use raphtory_api::core::{
    entities::properties::prop::IntoProp, storage::timeindex::EventTime, Direction,
};

/// alice.score 3@0 7@2 9@6 · bob.score 5@1 2@7 · carol none · dave.score 1@2 1@3
/// eve.scores [1,2]@0 [5,5]@1
/// alice→bob [knows] w=1@1 w=2@4 · bob→carol [works] w=1@2 · carol→dave [knows] w=3@6
fn graph() -> Graph {
    let g = Graph::new();
    for (t, name, score) in [
        (0, "alice", 3.0),
        (2, "alice", 7.0),
        (6, "alice", 9.0),
        (1, "bob", 5.0),
        (7, "bob", 2.0),
        (2, "dave", 1.0),
        (3, "dave", 1.0),
    ] {
        g.add_node(t, name, [("score", score.into_prop())], None, None)
            .unwrap();
    }
    g.add_node(0, "carol", [("tag", "x".into_prop())], None, None)
        .unwrap();
    for (t, scores) in [(0, vec![1i64, 2]), (1, vec![5, 5])] {
        let list = Prop::List(scores.into_iter().map(Prop::I64).collect::<Vec<_>>().into());
        g.add_node(t, "eve", [("scores", list)], None, None)
            .unwrap();
    }
    for (t, src, dst, layer, w) in [
        (1, "alice", "bob", "knows", 1i64),
        (4, "alice", "bob", "knows", 2),
        (2, "bob", "carol", "works", 1),
        (6, "carol", "dave", "knows", 3),
    ] {
        g.add_edge(t, src, dst, [("w", w.into_prop())], Some(layer))
            .unwrap();
    }
    g
}

fn nodes(g: &Graph, filter: &FilterExpr) -> Vec<String> {
    let mut names: Vec<String> = g
        .filter(filter.clone())
        .unwrap()
        .nodes()
        .iter()
        .map(|n| n.name())
        .collect();
    names.sort();
    names
}

fn edges(g: &Graph, filter: &FilterExpr) -> Vec<String> {
    let mut ids: Vec<String> = g
        .filter(filter.clone())
        .unwrap()
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    ids.sort();
    ids
}

fn error(g: &Graph, filter: &FilterExpr) -> String {
    match g.filter(filter.clone()) {
        Ok(_) => panic!("expected {filter} to be refused"),
        Err(e) => e.to_string(),
    }
}

fn c(v: impl Into<Prop>) -> NodeExpr {
    Expr::Const(v.into())
}

fn prop(name: &str) -> NodeExpr {
    Expr::Read(NodeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: false,
    })
}

fn history(name: &str) -> NodeExpr {
    Expr::Read(NodeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: true,
    })
}

fn field(field: Field) -> NodeExpr {
    Expr::Read(NodeLeaf::Field {
        views: vec![],
        field,
    })
}

fn degree(direction: Direction) -> NodeExpr {
    Expr::Read(NodeLeaf::Degree {
        views: vec![],
        direction,
    })
}

fn cmp<L>(op: CmpOp, l: Expr<L>, r: Expr<L>) -> Expr<L> {
    Expr::Cmp(op, Box::new(l), Box::new(r))
}

fn node(e: NodeExpr) -> FilterExpr {
    FilterExpr::Node(e)
}

fn edge_prop(name: &str) -> EdgeExpr {
    Expr::Read(EdgeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: false,
    })
}

fn src(e: NodeExpr) -> EdgeExpr {
    Expr::Read(EdgeLeaf::Src(Box::new(e)))
}

fn dst(e: NodeExpr) -> EdgeExpr {
    Expr::Read(EdgeLeaf::Dst(Box::new(e)))
}

fn window(start: i64, end: i64) -> ViewOp {
    ViewOp::Window {
        start: EventTime::start(start),
        end: EventTime::start(end),
    }
}

#[test]
fn a_constant_comparison_reads_the_latest_value() {
    let g = graph();
    let f = node(cmp(CmpOp::Gt, prop("score"), c(4.0)));
    assert_eq!(nodes(&g, &f), ["alice"]);
    assert_eq!(f.to_string(), "NODE(score > 4)");
    // The constant may stand on either side.
    let f = node(cmp(CmpOp::Lt, c(4.0), prop("score")));
    assert_eq!(nodes(&g, &f), ["alice"]);
}

#[test]
fn views_scope_the_read_not_the_result() {
    let g = graph();
    let windowed = Expr::Read(NodeLeaf::Property {
        views: vec![window(0, 5)],
        name: "score".into(),
        temporal: false,
    });
    // inside [0,5): alice's latest score is 7, bob's is 5
    let f = node(cmp(CmpOp::Gt, windowed, c(4.0)));
    assert_eq!(nodes(&g, &f), ["alice", "bob"]);
    assert_eq!(f.to_string(), "NODE(WINDOW[0..5](score) > 4)");
}

#[test]
fn both_sides_may_be_expressions() {
    let g = graph();
    let f = node(cmp(
        CmpOp::Gt,
        degree(Direction::BOTH),
        degree(Direction::IN),
    ));
    assert_eq!(nodes(&g, &f), ["alice", "bob", "carol"]);
    // A degree compares by value with a fractional constant.
    let f = node(cmp(CmpOp::Ge, degree(Direction::BOTH), c(1.5)));
    assert_eq!(nodes(&g, &f), ["bob", "carol"]);
}

#[test]
fn string_and_set_tests() {
    let g = graph();
    let starts = node(Expr::Str(
        StrOp::StartsWith,
        Box::new(field(Field::Name)),
        Box::new(c("a")),
    ));
    assert_eq!(nodes(&g, &starts), ["alice"]);
    let members = node(Expr::In {
        expr: Box::new(field(Field::Name)),
        values: vec!["alice".into(), "dave".into(), 7i64.into()],
        negated: false,
    });
    assert_eq!(nodes(&g, &members), ["alice", "dave"]);
    let others = node(Expr::In {
        expr: Box::new(field(Field::Name)),
        values: vec!["alice".into(), "dave".into()],
        negated: true,
    });
    assert_eq!(nodes(&g, &others), ["bob", "carol", "eve"]);
    // Naming ids outright narrows the scan to those nodes and still answers.
    let by_id = node(cmp(CmpOp::Eq, field(Field::Id), c("bob")));
    assert_eq!(nodes(&g, &by_id), ["bob"]);
    let by_ids = node(Expr::In {
        expr: Box::new(field(Field::Id)),
        values: vec!["bob".into(), "eve".into()],
        negated: false,
    });
    assert_eq!(nodes(&g, &by_ids), ["bob", "eve"]);
}

#[test]
fn presence_and_combinators() {
    let g = graph();
    let has_score = node(Expr::IsSome(Box::new(prop("score"))));
    assert_eq!(nodes(&g, &has_score), ["alice", "bob", "dave"]);
    let no_score = node(Expr::IsNone(Box::new(prop("score"))));
    assert_eq!(nodes(&g, &no_score), ["carol", "eve"]);
    // Inside one entity's expression.
    let both = node(Expr::And(vec![
        cmp(CmpOp::Gt, prop("score"), c(1.5)),
        cmp(CmpOp::Lt, prop("score"), c(8.0)),
    ]));
    assert_eq!(nodes(&g, &both), ["bob"]);
    let either = node(Expr::Or(vec![
        cmp(CmpOp::Gt, prop("score"), c(8.0)),
        Expr::IsNone(Box::new(prop("score"))),
    ]));
    assert_eq!(nodes(&g, &either), ["alice", "carol", "eve"]);
    let not = node(Expr::Not(Box::new(cmp(CmpOp::Gt, prop("score"), c(1.5)))));
    assert_eq!(nodes(&g, &not), ["carol", "dave", "eve"]);
    // And across filters.
    let f = FilterExpr::And(vec![
        node(cmp(CmpOp::Gt, prop("score"), c(1.5))),
        FilterExpr::Not(Box::new(node(cmp(CmpOp::Gt, prop("score"), c(8.0))))),
    ]);
    assert_eq!(nodes(&g, &f), ["bob"]);
}

#[test]
fn qualifiers_follow_the_comparison() {
    let g = graph();
    // alice 3,7,9 · bob 5,2 · dave 1,1
    let any_high = node(Expr::Any(Box::new(cmp(
        CmpOp::Gt,
        history("score"),
        c(8.0),
    ))));
    assert_eq!(nodes(&g, &any_high), ["alice"]);
    let all_over_two = node(Expr::All(Box::new(cmp(
        CmpOp::Gt,
        history("score"),
        c(2.5),
    ))));
    assert_eq!(nodes(&g, &all_over_two), ["alice"]);
    let any_member = node(Expr::Any(Box::new(Expr::In {
        expr: Box::new(history("score")),
        values: vec![1.0.into(), 5.0.into()],
        negated: false,
    })));
    assert_eq!(nodes(&g, &any_member), ["bob", "dave"]);
    // An element-wise result is a list of answers, not a filter, until it is
    // collapsed.
    let bare = node(cmp(CmpOp::Gt, history("score"), c(8.0)));
    assert!(
        error(&g, &bare).contains("List<Bool>"),
        "{}",
        error(&g, &bare)
    );
    // any()/all() over a plain yes/no has nothing to collapse.
    let scalar = node(Expr::Any(Box::new(cmp(CmpOp::Gt, prop("score"), c(8.0)))));
    assert!(error(&g, &scalar).contains("any()/all()"));
    assert_eq!(any_high.to_string(), "NODE(ANY(TEMPORAL(score) > 8))");
}

#[test]
fn aggregates_over_history_and_over_lists_of_lists() {
    let g = graph();
    let sum = Expr::Agg(Agg::Sum, Box::new(history("score")));
    let f = node(cmp(CmpOp::Ge, sum, c(10.0)));
    assert_eq!(nodes(&g, &f), ["alice"]);
    let len = Expr::Agg(Agg::Len, Box::new(history("score")));
    let f = node(cmp(CmpOp::Eq, len, c(2u64)));
    assert_eq!(nodes(&g, &f), ["bob", "dave"]);
    // eve.scores is a list per update: the history is a list of lists, the
    // sum is one number per update, and the comparison is one answer per
    // update that any()/all() collapse.
    let sums = Expr::Agg(Agg::Sum, Box::new(history("scores")));
    let any_big = node(Expr::Any(Box::new(cmp(CmpOp::Ge, sums.clone(), c(5i64)))));
    assert_eq!(nodes(&g, &any_big), ["eve"]);
    let all_big = node(Expr::All(Box::new(cmp(CmpOp::Ge, sums, c(5i64)))));
    assert_eq!(nodes(&g, &all_big), Vec::<String>::new());
}

#[test]
fn edges_endpoints_and_structure() {
    let g = graph();
    let heavy = FilterExpr::Edge(cmp(CmpOp::Gt, edge_prop("w"), Expr::Const(1i64.into())));
    assert_eq!(edges(&g, &heavy), ["alice->bob", "carol->dave"]);
    let from_alice = FilterExpr::Edge(cmp(
        CmpOp::Eq,
        src(field(Field::Name)),
        Expr::Const("alice".into()),
    ));
    assert_eq!(edges(&g, &from_alice), ["alice->bob"]);
    // Two endpoint values compare at the edge level.
    let downhill = FilterExpr::Edge(cmp(CmpOp::Gt, src(prop("score")), dst(prop("score"))));
    assert_eq!(edges(&g, &downhill), ["alice->bob"]);
    // A node predicate through an endpoint is an edge predicate.
    let into_scored = FilterExpr::Edge(dst(Expr::IsSome(Box::new(prop("score")))));
    assert_eq!(edges(&g, &into_scored), ["alice->bob", "carol->dave"]);
    let works = FilterExpr::Edge(Expr::Read(EdgeLeaf::IsActive {
        views: vec![ViewOp::Layers(vec!["works".into()])],
    }));
    assert_eq!(edges(&g, &works), ["bob->carol"]);
    assert_eq!(works.to_string(), "EDGE(LAYER[works](IS_ACTIVE))");
    let exploded = FilterExpr::ExplodedEdge(cmp(
        CmpOp::Eq,
        Expr::Read(ExplodedEdgeLeaf::Property {
            views: vec![],
            name: "w".into(),
            temporal: false,
        }),
        Expr::Const(1i64.into()),
    ));
    assert_eq!(edges(&g, &exploded), ["alice->bob", "bob->carol"]);
    assert!(heavy.tests_edges() && !node(c(true)).tests_edges());
}

#[test]
fn type_clashes_are_refused_when_the_filter_is_built() {
    let g = graph();
    let cases: Vec<(FilterExpr, &str)> = vec![
        (
            node(cmp(CmpOp::Gt, prop("score"), c("x"))),
            "cannot be compared with F64",
        ),
        (node(prop("score")), "needs a yes/no answer"),
        (
            node(Expr::IsSome(Box::new(degree(Direction::BOTH)))),
            "always has a value",
        ),
        (
            node(Expr::Str(
                StrOp::Contains,
                Box::new(prop("score")),
                Box::new(c("x")),
            )),
            "string operator requires a Str property",
        ),
        (
            node(Expr::Str(
                StrOp::Contains,
                Box::new(field(Field::Name)),
                Box::new(c(3i64)),
            )),
            "cannot be compared with Str",
        ),
        (node(cmp(CmpOp::Gt, prop("scores"), c(1i64))), "List<Bool>"),
        (
            node(Expr::And(vec![prop("score"), c(true)])),
            "and needs a yes/no answer",
        ),
        (node(Expr::And(vec![])), "needs at least one operand"),
    ];
    for (filter, expected) in cases {
        let msg = error(&g, &filter);
        assert!(msg.contains(expected), "{filter}: {msg}");
    }
}

#[test]
fn a_view_leg_restricts_the_whole_filter() {
    let g = graph();
    let score_gt_4 = node(cmp(CmpOp::Gt, prop("score"), c(4.0)));
    let win = FilterExpr::View(vec![window(0, 7)]);
    // The view applies first and the predicate runs inside it.
    let f = FilterExpr::And(vec![win.clone(), score_gt_4.clone()]);
    assert_eq!(nodes(&g, &f), ["alice", "bob"]);
    // A view alone is the result.
    let f = FilterExpr::View(vec![window(0, 5), ViewOp::Latest]);
    assert_eq!(edges(&g, &f), ["alice->bob"]);
    assert_eq!(f.to_string(), "VIEW(WINDOW[0..5] . LATEST)");
    assert!(FilterExpr::View(vec![]).compile().is_err());
    // Under `or` or `not` a view has no meaning the engine can give it.
    assert!(FilterExpr::Or(vec![win.clone(), score_gt_4.clone()])
        .compile()
        .is_err());
    assert!(FilterExpr::Not(Box::new(win.clone())).compile().is_err());
    assert!(f.has_view() && !score_gt_4.has_view());
}

#[test]
fn trees_round_trip_through_json_with_the_read_as_the_key() {
    let f = FilterExpr::And(vec![
        node(Expr::Any(Box::new(cmp(
            CmpOp::Gt,
            Expr::Read(NodeLeaf::Property {
                views: vec![window(0, 5)],
                name: "score".into(),
                temporal: true,
            }),
            c(4.0),
        )))),
        FilterExpr::Edge(cmp(
            CmpOp::Eq,
            src(field(Field::Name)),
            Expr::Const("alice".into()),
        )),
    ]);
    let json = serde_json::to_string(&f).unwrap();
    assert!(
        json.contains(r#""property":{"views":[{"window":"#),
        "{json}"
    );
    assert!(
        json.contains(r#""src":{"field":{"field":"name"}}"#),
        "{json}"
    );
    assert!(!json.contains("read"), "{json}");
    let back: FilterExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(back, f);
}

#[test]
fn an_opaque_filter_refuses_to_serialise() {
    let f = FilterExpr::Opaque(OpaqueFilter(node(c(true)).compile().unwrap()));
    let err = serde_json::to_string(&f).unwrap_err().to_string();
    assert!(err.contains(OPAQUE_FILTER_ERROR));
}
