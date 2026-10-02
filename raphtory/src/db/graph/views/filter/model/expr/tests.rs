use super::*;
use crate::{
    db::{
        api::view::{Filter, Select},
        graph::views::filter::{
            model::{
                edge_filter::EdgeFilter,
                node_filter::{NodeFilter, NodeFilterFactory},
                windowed_filter::Windowed,
                ComposableFilter, DynCreateFilter, EdgeViewFilterOps, EntityExprFilterOps,
                PropertyExprFactory, ViewWrapOps,
            },
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, TimeOps, NO_PROPS},
};
use raphtory_api::core::{
    entities::properties::prop::IntoProp, storage::timeindex::EventTime, Direction,
};
use std::sync::Arc;

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
    Expr::Term(NodeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: false,
    })
}

fn history(name: &str) -> NodeExpr {
    Expr::Term(NodeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: true,
    })
}

fn field(field: Field) -> NodeExpr {
    Expr::Term(NodeLeaf::Field {
        views: vec![],
        field,
    })
}

fn degree(direction: Direction) -> NodeExpr {
    Expr::Term(NodeLeaf::Degree {
        views: vec![],
        direction,
    })
}

fn cmp<L>(op: BinaryOp, l: Expr<L>, r: Expr<L>) -> Expr<L> {
    Expr::Cmp(op, Box::new(l), Box::new(r))
}

fn node(e: NodeExpr) -> FilterExpr {
    FilterExpr::Node(e)
}

fn edge_prop(name: &str) -> EdgeExpr {
    Expr::Term(EdgeLeaf::Property {
        views: vec![],
        name: name.into(),
        temporal: false,
    })
}

fn src(e: NodeExpr) -> EdgeExpr {
    Expr::Term(EdgeLeaf::Src(Box::new(e)))
}

fn dst(e: NodeExpr) -> EdgeExpr {
    Expr::Term(EdgeLeaf::Dst(Box::new(e)))
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
    let f = node(cmp(BinaryOp::Gt, prop("score"), c(4.0)));
    assert_eq!(nodes(&g, &f), ["alice"]);
    assert_eq!(f.to_string(), "NODE(score > 4)");
    // The constant may stand on either side.
    let f = node(cmp(BinaryOp::Lt, c(4.0), prop("score")));
    assert_eq!(nodes(&g, &f), ["alice"]);
}

#[test]
fn views_scope_the_term_not_the_result() {
    let g = graph();
    let windowed = Expr::Term(NodeLeaf::Property {
        views: vec![window(0, 5)],
        name: "score".into(),
        temporal: false,
    });
    // inside [0,5): alice's latest score is 7, bob's is 5
    let f = node(cmp(BinaryOp::Gt, windowed, c(4.0)));
    assert_eq!(nodes(&g, &f), ["alice", "bob"]);
    assert_eq!(f.to_string(), "NODE(WINDOW[0..5](score) > 4)");
}

#[test]
fn both_sides_may_be_expressions() {
    let g = graph();
    let f = node(cmp(
        BinaryOp::Gt,
        degree(Direction::BOTH),
        degree(Direction::IN),
    ));
    assert_eq!(nodes(&g, &f), ["alice", "bob", "carol"]);
    // A degree compares by value with a fractional constant.
    let f = node(cmp(BinaryOp::Ge, degree(Direction::BOTH), c(1.5)));
    assert_eq!(nodes(&g, &f), ["bob", "carol"]);
}

#[test]
fn string_and_set_tests() {
    let g = graph();
    let starts = node(Expr::Str(
        StringOp::StartsWith,
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
    let by_id = node(cmp(BinaryOp::Eq, field(Field::Id), c("bob")));
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
        cmp(BinaryOp::Gt, prop("score"), c(1.5)),
        cmp(BinaryOp::Lt, prop("score"), c(8.0)),
    ]));
    assert_eq!(nodes(&g, &both), ["bob"]);
    let either = node(Expr::Or(vec![
        cmp(BinaryOp::Gt, prop("score"), c(8.0)),
        Expr::IsNone(Box::new(prop("score"))),
    ]));
    assert_eq!(nodes(&g, &either), ["alice", "carol", "eve"]);
    let not = node(Expr::Not(Box::new(cmp(
        BinaryOp::Gt,
        prop("score"),
        c(1.5),
    ))));
    assert_eq!(nodes(&g, &not), ["carol", "dave", "eve"]);
    // And across filters.
    let f = FilterExpr::And(vec![
        node(cmp(BinaryOp::Gt, prop("score"), c(1.5))),
        FilterExpr::Not(Box::new(node(cmp(BinaryOp::Gt, prop("score"), c(8.0))))),
    ]);
    assert_eq!(nodes(&g, &f), ["bob"]);
}

#[test]
fn qualifiers_follow_the_comparison() {
    let g = graph();
    // alice 3,7,9 · bob 5,2 · dave 1,1
    let any_high = node(Expr::Any(Box::new(cmp(
        BinaryOp::Gt,
        history("score"),
        c(8.0),
    ))));
    assert_eq!(nodes(&g, &any_high), ["alice"]);
    let all_over_two = node(Expr::All(Box::new(cmp(
        BinaryOp::Gt,
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
    let bare = node(cmp(BinaryOp::Gt, history("score"), c(8.0)));
    assert!(
        error(&g, &bare).contains("List<Bool>"),
        "{}",
        error(&g, &bare)
    );
    // any()/all() over a plain yes/no has nothing to collapse.
    let scalar = node(Expr::Any(Box::new(cmp(
        BinaryOp::Gt,
        prop("score"),
        c(8.0),
    ))));
    assert!(error(&g, &scalar).contains("any()/all()"));
    assert_eq!(any_high.to_string(), "NODE(ANY(TEMPORAL(score) > 8))");
}

#[test]
fn aggregates_over_history_and_over_lists_of_lists() {
    let g = graph();
    let sum = Expr::Agg(Agg::Sum, Box::new(history("score")));
    let f = node(cmp(BinaryOp::Ge, sum, c(10.0)));
    assert_eq!(nodes(&g, &f), ["alice"]);
    let len = Expr::Agg(Agg::Len, Box::new(history("score")));
    let f = node(cmp(BinaryOp::Eq, len, c(2u64)));
    assert_eq!(nodes(&g, &f), ["bob", "dave"]);
    // eve.scores is a list per update: the history is a list of lists, the
    // sum is one number per update, and the comparison is one answer per
    // update that any()/all() collapse.
    let sums = Expr::Agg(Agg::Sum, Box::new(history("scores")));
    let any_big = node(Expr::Any(Box::new(cmp(
        BinaryOp::Ge,
        sums.clone(),
        c(5i64),
    ))));
    assert_eq!(nodes(&g, &any_big), ["eve"]);
    let all_big = node(Expr::All(Box::new(cmp(BinaryOp::Ge, sums, c(5i64)))));
    assert_eq!(nodes(&g, &all_big), Vec::<String>::new());
}

#[test]
fn edges_endpoints_and_structure() {
    let g = graph();
    let heavy = FilterExpr::Edge(cmp(BinaryOp::Gt, edge_prop("w"), Expr::Const(1i64.into())));
    assert_eq!(edges(&g, &heavy), ["alice->bob", "carol->dave"]);
    let from_alice = FilterExpr::Edge(cmp(
        BinaryOp::Eq,
        src(field(Field::Name)),
        Expr::Const("alice".into()),
    ));
    assert_eq!(edges(&g, &from_alice), ["alice->bob"]);
    // Two endpoint values compare at the edge level.
    let downhill = FilterExpr::Edge(cmp(BinaryOp::Gt, src(prop("score")), dst(prop("score"))));
    assert_eq!(edges(&g, &downhill), ["alice->bob"]);
    // A node predicate through an endpoint is an edge predicate.
    let into_scored = FilterExpr::Edge(dst(Expr::IsSome(Box::new(prop("score")))));
    assert_eq!(edges(&g, &into_scored), ["alice->bob", "carol->dave"]);
    let works = FilterExpr::Edge(Expr::Term(EdgeLeaf::IsActive {
        views: vec![ViewOp::Layers(vec!["works".into()])],
    }));
    assert_eq!(edges(&g, &works), ["bob->carol"]);
    assert_eq!(works.to_string(), "EDGE(LAYER[works](IS_ACTIVE))");
    let exploded = FilterExpr::ExplodedEdge(cmp(
        BinaryOp::Eq,
        Expr::Term(ExplodedEdgeLeaf::Property {
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
            node(cmp(BinaryOp::Gt, prop("score"), c("x"))),
            "cannot be compared with F64",
        ),
        (node(prop("score")), "needs a yes/no answer"),
        (
            node(Expr::IsSome(Box::new(degree(Direction::BOTH)))),
            "always has a value",
        ),
        (
            node(Expr::Str(
                StringOp::Contains,
                Box::new(prop("score")),
                Box::new(c("x")),
            )),
            "string operator requires a Str property",
        ),
        (
            node(Expr::Str(
                StringOp::Contains,
                Box::new(field(Field::Name)),
                Box::new(c(3i64)),
            )),
            "cannot be compared with Str",
        ),
        (
            node(cmp(BinaryOp::Gt, prop("scores"), c(1i64))),
            "List<Bool>",
        ),
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
    let score_gt_4 = node(cmp(BinaryOp::Gt, prop("score"), c(4.0)));
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
fn trees_round_trip_through_json_with_the_term_as_the_key() {
    let f = FilterExpr::And(vec![
        node(Expr::Any(Box::new(cmp(
            BinaryOp::Gt,
            Expr::Term(NodeLeaf::Property {
                views: vec![window(0, 5)],
                name: "score".into(),
                temporal: true,
            }),
            c(4.0),
        )))),
        FilterExpr::Edge(cmp(
            BinaryOp::Eq,
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
    assert!(!json.contains("term"), "{json}");
    let back: FilterExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(back, f);
}

#[test]
fn an_opaque_filter_refuses_to_serialise() {
    let f = FilterExpr::Opaque(OpaqueFilter::new(NodeFilter.property("tag").is_some()));
    let err = serde_json::to_string(&f).unwrap_err().to_string();
    assert!(err.contains(OPAQUE_FILTER_ERROR));
}

#[test]
fn before_and_at_agree_with_the_graph_views() {
    // alice→bob @2 (first event at 2) · carol→dave @2 (second event at 2) · eve→fay @5
    let g = Graph::new();
    g.add_edge(2, "alice", "bob", NO_PROPS, None).unwrap();
    g.add_edge(2, "carol", "dave", NO_PROPS, None).unwrap();
    g.add_edge(5, "eve", "fay", NO_PROPS, None).unwrap();
    fn edge_names<'graph, G: GraphViewOps<'graph>>(g: &G) -> Vec<String> {
        let mut ids: Vec<String> = g
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        ids.sort();
        ids
    }
    let view = |op: ViewOp| FilterExpr::View(vec![op]);
    let typed = |f: Windowed<EdgeFilter>| Arc::new(f.is_active()) as Arc<dyn DynCreateFilter>;
    fn applied(g: &Graph, filter: Arc<dyn DynCreateFilter>) -> Vec<String> {
        let mut ids: Vec<String> = g
            .filter(filter)
            .unwrap()
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        ids.sort();
        ids
    }
    let none: [&str; 0] = [];
    let at_two = ["alice->bob", "carol->dave"];

    // `before(t)` excludes every event at `t`, like the graph view does.
    assert_eq!(edge_names(&g.before(2)), none);
    assert_eq!(edges(&g, &view(ViewOp::Before(EventTime::start(2)))), none);
    assert_eq!(applied(&g, typed(EdgeFilter.before(2))), none);
    assert_eq!(edge_names(&g.before(3)), at_two);
    assert_eq!(
        edges(&g, &view(ViewOp::Before(EventTime::start(3)))),
        at_two
    );

    // `at(t)` covers the whole timestamp, even when handed a time that sits
    // between two events at `t`.
    let mid_two = EventTime::start(2).set_event_id(1);
    assert_eq!(edge_names(&g.at(mid_two)), at_two);
    assert_eq!(edges(&g, &view(ViewOp::At(mid_two))), at_two);
    assert_eq!(applied(&g, typed(EdgeFilter.at(mid_two))), at_two);

    // A window bound that carries an event id is honoured, as the graph view does.
    let from_second_event = EventTime::start(2).set_event_id(1);
    assert_eq!(
        edge_names(&g.window(from_second_event, EventTime::start(3))),
        ["carol->dave"]
    );
    assert_eq!(
        edges(
            &g,
            &view(ViewOp::Window {
                start: from_second_event,
                end: EventTime::start(3),
            }),
        ),
        ["carol->dave"]
    );
    assert_eq!(
        applied(&g, typed(EdgeFilter.window(from_second_event, 3))),
        ["carol->dave"]
    );

    // `after(t)` excludes `t` and everything before it.
    assert_eq!(edge_names(&g.after(2)), ["eve->fay"]);
    assert_eq!(
        edges(&g, &view(ViewOp::After(EventTime::start(2)))),
        ["eve->fay"]
    );
}

/// Aggregates reduce the innermost list, so on a list-valued history they
/// answer per update; `earliest` and `latest` pick an update as it is.
#[test]
fn aggregates_reduce_inside_each_update_and_earliest_picks_one() {
    let g = graph();
    let scores = |agg: Agg| Expr::Agg(agg, Box::new(history("scores")));
    // eve.scores is [1, 2] at 0 and [5, 5] at 1.
    let eve = ["eve"];
    let none: [&str; 0] = [];
    assert_eq!(
        nodes(
            &g,
            &node(Expr::Any(Box::new(cmp(
                BinaryOp::Eq,
                scores(Agg::First),
                c(1i64)
            ))))
        ),
        eve
    );
    assert_eq!(
        nodes(
            &g,
            &node(Expr::All(Box::new(cmp(
                BinaryOp::Eq,
                scores(Agg::Last),
                c(5i64)
            ))))
        ),
        none
    );
    assert_eq!(
        nodes(
            &g,
            &node(Expr::Any(Box::new(cmp(
                BinaryOp::Eq,
                scores(Agg::Len),
                c(2i64)
            ))))
        ),
        eve
    );
    let list = |items: &[i64]| {
        Prop::List(
            items
                .iter()
                .map(|v| Prop::I64(*v))
                .collect::<Vec<_>>()
                .into(),
        )
    };
    assert_eq!(
        nodes(
            &g,
            &node(cmp(
                BinaryOp::Eq,
                scores(Agg::Earliest),
                Expr::Const(list(&[1, 2]))
            ))
        ),
        eve
    );
    assert_eq!(
        nodes(
            &g,
            &node(cmp(
                BinaryOp::Eq,
                scores(Agg::Latest),
                Expr::Const(list(&[5, 5]))
            ))
        ),
        eve
    );
    // On a scalar history the two readings agree.
    let score = |agg: Agg| Expr::Agg(agg, Box::new(history("score")));
    assert_eq!(
        nodes(&g, &node(cmp(BinaryOp::Eq, score(Agg::Earliest), c(3.0)))),
        nodes(&g, &node(cmp(BinaryOp::Eq, score(Agg::First), c(3.0))))
    );
    // An update is only a thing on a temporal history.
    let msg = error(
        &g,
        &node(cmp(
            BinaryOp::Eq,
            Expr::Agg(Agg::Latest, Box::new(prop("scores"))),
            c(1i64),
        )),
    );
    assert!(
        msg.contains("earliest() and latest() pick an update"),
        "{msg}"
    );
}

/// A filter answers two questions, which nodes stay and which edges stay;
/// `and`, `or` and `not` combine the direct answers question by question. A
/// negated node predicate keeps the nodes that fail it and the edges between
/// them; an `or` with a leg that leaves a question open leaves it open; the
/// per-edge form of every filter agrees with its graph; and a chain of
/// filters gives the `and`'s answer.
#[test]
fn filters_answer_the_node_and_edge_questions_separately() {
    use crate::db::api::view::{DynamicGraph, IntoDynamic};
    let g = graph();
    let score_gt = |v: f64| node(cmp(BinaryOp::Gt, prop("score"), c(v)));
    let score_lt = |v: f64| node(cmp(BinaryOp::Lt, prop("score"), c(v)));
    let ec = |v: i64| -> EdgeExpr { Expr::Const(v.into()) };
    let w_gt = |v: i64| FilterExpr::Edge(cmp(BinaryOp::Gt, edge_prop("w"), ec(v)));
    let not = |f: FilterExpr| FilterExpr::Not(Box::new(f));
    let names = |v: &DynamicGraph| {
        let mut n: Vec<String> = v.nodes().iter().map(|n| n.name()).collect();
        n.sort();
        n
    };
    let eids = |v: &DynamicGraph| {
        let mut e: Vec<String> = v
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        e.sort();
        e
    };
    let all_nodes = ["alice", "bob", "carol", "dave", "eve"];
    let all_edges = ["alice->bob", "bob->carol", "carol->dave"];
    // alice.score 9 · bob 2 · dave 1 · carol, eve none · alice→bob w=2 · bob→carol w=1 · carol→dave w=3
    let cases: Vec<(&str, FilterExpr, &[&str], &[&str])> = vec![
        (
            "not(edge)",
            not(w_gt(2)),
            &all_nodes,
            &["alice->bob", "bob->carol"],
        ),
        (
            "not(node)",
            not(score_gt(4.0)),
            &["bob", "carol", "dave", "eve"],
            &["bob->carol", "carol->dave"],
        ),
        ("not(not(node))", not(not(score_gt(4.0))), &["alice"], &[]),
        (
            "not(and(node, node)) = or(not, not)",
            not(FilterExpr::And(vec![score_gt(1.5), score_lt(8.0)])),
            &["alice", "carol", "dave", "eve"],
            &["carol->dave"],
        ),
        (
            "not(or(node, node)) = and(not, not)",
            not(FilterExpr::Or(vec![score_gt(4.0), score_lt(1.5)])),
            &["bob", "carol", "eve"],
            &["bob->carol"],
        ),
        (
            "not(and(node, edge)): each answer negated",
            not(FilterExpr::And(vec![score_gt(4.0), w_gt(2)])),
            &["bob", "carol", "dave", "eve"],
            &["bob->carol"],
        ),
        (
            "or(node, node): an edge whose ends pass different legs",
            FilterExpr::Or(vec![score_gt(8.0), score_lt(3.0)]),
            &["alice", "bob", "dave"],
            &["alice->bob"],
        ),
        (
            "or(node, edge): both questions left open",
            FilterExpr::Or(vec![score_gt(4.0), w_gt(2)]),
            &all_nodes,
            &all_edges,
        ),
        (
            "not(or(node, edge)): still open",
            not(FilterExpr::Or(vec![score_gt(4.0), w_gt(2)])),
            &all_nodes,
            &all_edges,
        ),
        (
            "and(node, or(node, edge)): the open or drops out",
            FilterExpr::And(vec![
                score_gt(1.5),
                FilterExpr::Or(vec![score_gt(4.0), w_gt(2)]),
            ]),
            &["alice", "bob"],
            &["alice->bob"],
        ),
    ];
    let base = g.filter(score_gt(1.5)).unwrap().into_dynamic();
    assert_eq!(names(&base), ["alice", "bob"]);
    assert_eq!(eids(&base), ["alice->bob"]);
    for (label, f, want_nodes, want_edges) in cases {
        let alone = g.filter(f.clone()).unwrap().into_dynamic();
        assert_eq!(names(&alone), want_nodes, "{label}: nodes");
        assert_eq!(eids(&alone), want_edges, "{label}: edges");
        let mut selected: Vec<String> = g
            .edges()
            .select(f.clone())
            .unwrap()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        selected.sort();
        assert_eq!(selected, want_edges, "{label}: edges.select");
        // Chained after the base, the answer is the `and`'s: the base's
        // entities that the filter keeps.
        let chained = base.filter(f.clone()).unwrap().into_dynamic();
        let anded = g
            .filter(FilterExpr::And(vec![score_gt(1.5), f.clone()]))
            .unwrap()
            .into_dynamic();
        assert_eq!(names(&chained), names(&anded), "{label}: chained nodes");
        assert_eq!(eids(&chained), eids(&anded), "{label}: chained edges");
        let want_chained_nodes: Vec<&str> = want_nodes
            .iter()
            .copied()
            .filter(|n| ["alice", "bob"].contains(n))
            .collect();
        assert_eq!(
            names(&chained),
            want_chained_nodes,
            "{label}: chained nodes"
        );
        let want_chained_edges: Vec<&str> = want_edges
            .iter()
            .copied()
            .filter(|e| *e == "alice->bob")
            .collect();
        assert_eq!(eids(&chained), want_chained_edges, "{label}: chained edges");
    }
}

/// `not` over an exploded-edge predicate negates each instance.
#[test]
fn not_over_an_exploded_predicate_keeps_the_other_instances() {
    let g = graph();
    // alice→bob w=1@1 w=2@4 · bob→carol w=1@2 · carol→dave w=3@6
    let w_gt_1 = FilterExpr::ExplodedEdge(cmp(
        BinaryOp::Gt,
        Expr::Term(ExplodedEdgeLeaf::Property {
            views: vec![],
            name: "w".into(),
            temporal: false,
        }),
        Expr::Const(1i64.into()),
    ));
    let not = FilterExpr::Not(Box::new(w_gt_1.clone()));
    assert_eq!(edges(&g, &w_gt_1), ["alice->bob", "carol->dave"]);
    assert_eq!(edges(&g, &not), ["alice->bob", "bob->carol"]);
    let instances = |f: &FilterExpr| {
        let mut i: Vec<(String, i64)> = g
            .filter(f.clone())
            .unwrap()
            .edges()
            .explode()
            .iter()
            .map(|e| (e.src().name(), e.time().unwrap().0))
            .collect();
        i.sort();
        i
    };
    assert_eq!(
        instances(&w_gt_1),
        [("alice".to_string(), 4), ("carol".to_string(), 6)]
    );
    assert_eq!(
        instances(&not),
        [("alice".to_string(), 1), ("bob".to_string(), 2)]
    );
}

/// A view under `not` is refused, before and after the push-down.
#[test]
fn a_view_under_not_is_refused_inside_a_composite_too() {
    let g = graph();
    let win = FilterExpr::View(vec![window(0, 5)]);
    let pred = node(cmp(BinaryOp::Gt, prop("score"), c(1.5)));
    let f = FilterExpr::Not(Box::new(FilterExpr::And(vec![win, pred])));
    assert!(error(&g, &f).contains("view"));
}

/// A node collection refuses a filter that tests edges anywhere in it, even
/// one that compiles to "every node".
#[test]
fn a_node_collection_refuses_a_filter_that_tests_edges() {
    let g = graph();
    let score_gt = |v: f64| node(cmp(BinaryOp::Gt, prop("score"), c(v)));
    let w_gt_2 = FilterExpr::Edge(cmp(BinaryOp::Gt, edge_prop("w"), Expr::Const(2i64.into())));
    let refused = |f: FilterExpr| {
        matches!(
            g.nodes().select(f).map(|_| ()),
            Err(GraphError::NotNodeFilter)
        )
    };
    assert!(refused(FilterExpr::Or(vec![score_gt(4.0), w_gt_2.clone()])));
    assert!(refused(FilterExpr::And(vec![
        score_gt(4.0),
        w_gt_2.clone()
    ])));
    assert!(refused(FilterExpr::Not(Box::new(FilterExpr::And(vec![
        score_gt(4.0),
        w_gt_2
    ])))));
    let mut both: Vec<String> = g
        .nodes()
        .select(FilterExpr::Or(vec![score_gt(4.0), score_gt(1.5)]))
        .unwrap()
        .iter()
        .map(|n| n.name())
        .collect();
    both.sort();
    assert_eq!(both, ["alice", "bob"]);
}

/// a→b w=1@1 · b→c w=2@2
fn chain() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "a", "b", [("w", 1i64.into_prop())], None)
        .unwrap();
    g.add_edge(2, "b", "c", [("w", 2i64.into_prop())], None)
        .unwrap();
    g
}

fn edge_ids<'graph, G: GraphViewOps<'graph>>(g: &G) -> Vec<String> {
    let mut ids: Vec<String> = g
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    ids.sort();
    ids
}

fn selected_edges<F: CreateFilter + Clone>(g: &Graph, filter: &F) -> Vec<String> {
    let mut ids: Vec<String> = g
        .edges()
        .select(filter.clone())
        .unwrap()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    ids.sort();
    ids
}

/// A typed `and`, `or` and `not` answer the node and edge questions
/// separately, like the tree they build: the edge b→c is kept by
/// `name == "b" | name == "c"` because each end passes one leg.
#[test]
fn typed_combinators_answer_the_node_and_edge_questions_separately() {
    let g = chain();
    let name_is = |n: &'static str| NodeFilter.name().eq(n);
    let w_gt_1 = || EdgeFilter.property("w").gt(1i64);

    let or = name_is("b").or(name_is("c"));
    assert_eq!(selected_edges(&g, &or), ["b->c"], "or: edges.select");
    assert_eq!(
        edge_ids(&g.filter(or.clone()).unwrap()),
        ["b->c"],
        "or: filter"
    );

    let or_node_edge = name_is("b").or(w_gt_1());
    assert_eq!(
        selected_edges(&g, &or_node_edge),
        ["a->b", "b->c"],
        "or(node, edge): edges.select"
    );
    assert_eq!(
        edge_ids(&g.filter(or_node_edge.clone()).unwrap()),
        ["a->b", "b->c"],
        "or(node, edge): filter"
    );

    let and = name_is("a").not().and(w_gt_1());
    assert_eq!(selected_edges(&g, &and), ["b->c"], "and: edges.select");
    assert_eq!(
        edge_ids(&g.filter(and.clone()).unwrap()),
        ["b->c"],
        "and: filter"
    );

    let not_or = name_is("b").or(name_is("c")).not();
    assert!(
        selected_edges(&g, &not_or).is_empty(),
        "not(or): edges.select"
    );
    assert!(
        edge_ids(&g.filter(not_or.clone()).unwrap()).is_empty(),
        "not(or): filter"
    );
    let names: Vec<String> = g.nodes().select(not_or).unwrap().name().collect();
    assert_eq!(names, ["a"], "not(or): nodes.select");
}

/// A field term under a view reads the node in that view: a node the
/// view does not hold has no name, id or type there, as it has no properties.
#[test]
fn a_field_term_under_a_view_is_none_for_a_node_outside_it() {
    // early@1 · late@7
    let g = Graph::new();
    g.add_node(1, "early", NO_PROPS, None, None).unwrap();
    g.add_node(7, "late", NO_PROPS, Some("kind"), None).unwrap();
    let filtered = |f: &dyn Fn() -> Arc<dyn DynCreateFilter>| {
        let mut n: Vec<String> = g.filter(f()).unwrap().nodes().name().collect();
        n.sort();
        n
    };
    let selected = |f: &dyn Fn() -> Arc<dyn DynCreateFilter>| {
        let mut n: Vec<String> = g.nodes().select(f()).unwrap().name().collect();
        n.sort();
        n
    };
    let win = || NodeFilter.window(0, 5);
    let cases: Vec<(&str, Box<dyn Fn() -> Arc<dyn DynCreateFilter>>, &[&str])> = vec![
        (
            "window name == late",
            Box::new(move || Arc::new(win().name().eq("late"))),
            &[],
        ),
        (
            "window name == early",
            Box::new(move || Arc::new(win().name().eq("early"))),
            &["early"],
        ),
        (
            "window id == late",
            Box::new(move || Arc::new(win().id().eq("late"))),
            &[],
        ),
        (
            "window node_type == kind",
            Box::new(move || Arc::new(win().node_type().eq("kind"))),
            &[],
        ),
        (
            "window node_type is_none",
            Box::new(move || Arc::new(win().node_type().is_none())),
            &["late"],
        ),
        (
            "name == late",
            Box::new(|| Arc::new(NodeFilter.name().eq("late"))),
            &["late"],
        ),
        (
            "tree: window name == late",
            Box::new(|| {
                Arc::new(node(cmp(
                    BinaryOp::Eq,
                    Expr::Term(NodeLeaf::Field {
                        views: vec![window(0, 5)],
                        field: Field::Name,
                    }),
                    c("late"),
                )))
            }),
            &[],
        ),
    ];
    for (label, f, want) in cases {
        assert_eq!(filtered(&*f), want, "{label}: filter");
        assert_eq!(selected(&*f), want, "{label}: nodes.select");
    }
}
