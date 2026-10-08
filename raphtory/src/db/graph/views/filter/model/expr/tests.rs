use super::*;
use crate::{
    db::{
        api::view::{DynamicGraph, Filter, IntoDynamic, Select},
        graph::{
            edges::Edges,
            views::{
                deletion_graph::PersistentGraph,
                filter::{
                    model::{
                        edge_filter::EdgeFilter,
                        exploded_edge_filter::ExplodedEdgeFilter,
                        graph_filter::GraphFilter,
                        node_expr::Compiled,
                        node_filter::{NodeFilter, NodeFilterFactory},
                        ComposableFilter, EdgeViewFilterOps, EntityAggOps, EntityExprFilterOps,
                        PropertyExprFactory, ViewWrapOps,
                    },
                    CreateFilter,
                },
            },
        },
    },
    errors::GraphError,
    prelude::{
        AdditionOps, DeletionOps, EdgeViewOps, Graph, GraphViewOps, LayerOps, NodeViewOps, TimeOps,
        NO_PROPS,
    },
};
use raphtory_api::core::{
    entities::{properties::prop::IntoProp, GID},
    storage::timeindex::{AsTime, EventTime},
    Direction,
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
        let list = Prop::list(scores).unwrap();
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
    assert!(FilterExpr::View(vec![]).split().is_err());
    // Under `or` or `not` a view has no meaning the engine can give it.
    assert!(FilterExpr::Or(vec![win.clone(), score_gt_4.clone()])
        .split()
        .is_err());
    assert!(FilterExpr::Not(Box::new(win.clone())).split().is_err());
    assert!(f.has_view() && !score_gt_4.has_view());
}

/// On a node or edge collection a view leg is one more test, "exists in the
/// view", and the other legs read the collection's own graph; only a filtered
/// graph is seen through the view first. So `window(0,5) & window(5,8).score
/// > 6` selects alice, whose score is 9 at 6, while the same filter applied to
/// the graph windows it to [0,5) first and the inner window finds nothing.
#[test]
fn a_view_leg_in_a_select_is_an_existence_test() {
    fn selected<'graph, G: GraphViewOps<'graph>>(edges: Edges<'graph, G>) -> Vec<String> {
        let mut ids: Vec<String> = edges
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect();
        ids.sort();
        ids
    }
    let g = graph();
    let in_window = |start, end, e: NodeExpr| {
        let mut e = e;
        e.push_view(window(start, end));
        e
    };
    let score_after_5 = node(cmp(BinaryOp::Gt, in_window(5, 8, prop("score")), c(6.0)));
    let f = FilterExpr::And(vec![
        FilterExpr::View(vec![window(0, 5)]),
        FilterExpr::View(vec![ViewOp::Layers(vec!["knows".into()])]),
        score_after_5,
    ]);
    let mut names: Vec<String> = g.nodes().select(f.clone()).unwrap().name().collect();
    names.sort();
    assert_eq!(names, ["alice"]);
    assert!(nodes(&g, &f).is_empty());

    // the same on edges: alice→bob exists in [0,3) and its weight in [3,5) is 2
    let mut w = edge_prop("w");
    w.push_view(window(3, 5));
    let f = FilterExpr::And(vec![
        FilterExpr::View(vec![window(0, 3)]),
        FilterExpr::Edge(cmp(BinaryOp::Gt, w, Expr::Const(1i64.into()))),
    ]);
    assert_eq!(
        selected(g.edges().select(f.clone()).unwrap()),
        ["alice->bob"]
    );
    assert!(edges(&g, &f).is_empty());

    // the test is existence, not activity: on a persistent graph an edge that
    // is alive in the window exists there without an update in it
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.add_edge(7, "c", "d", NO_PROPS, None).unwrap();
    let in_window = FilterExpr::View(vec![window(5, 10)]);
    assert_eq!(
        selected(g.edges().select(in_window).unwrap()),
        ["a->b", "c->d"]
    );
    let active = EdgeFilter.window(5, 10).is_active();
    assert_eq!(selected(g.edges().select(active).unwrap()), ["c->d"]);
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
fn numeric_aggregates_refuse_values_that_are_not_numbers() {
    let g = Graph::new();
    g.add_node(
        1,
        "n",
        [("text", Prop::str("a")), ("flag", Prop::Bool(true))],
        None,
        None,
    )
    .unwrap();
    g.add_node(
        2,
        "n",
        [("text", Prop::str("b")), ("flag", Prop::Bool(false))],
        None,
        None,
    )
    .unwrap();
    let text_sum = g
        .filter(NodeFilter.property("text").temporal().sum().is_some())
        .err()
        .unwrap()
        .to_string();
    assert!(text_sum.contains("sum() requires numeric values, but the elements are Str"));
    let flag_avg = g
        .filter(NodeFilter.property("flag").temporal().avg().gt(0.5))
        .err()
        .unwrap()
        .to_string();
    assert!(flag_avg.contains("avg() requires numeric values, but the elements are Bool"));
    let text_min = g
        .filter(NodeFilter.property("text").temporal().min().eq("a"))
        .err()
        .unwrap()
        .to_string();
    assert!(text_min.contains("min() requires numeric values, but the elements are Str"));
    // a numeric history still sums
    g.add_node(1, "m", [("n", Prop::I64(2))], None, None)
        .unwrap();
    g.add_node(2, "m", [("n", Prop::I64(3))], None, None)
        .unwrap();
    let summed = g
        .filter(NodeFilter.property("n").temporal().sum().eq(5i64))
        .unwrap();
    assert_eq!(summed.nodes().name().collect::<Vec<_>>(), vec!["m"]);
}

#[test]
fn an_opaque_filter_refuses_to_serialise() {
    let f = OpaqueFilter::new(NodeFilter.property("tag").is_some().compiled()).into_filter();
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
    let typed = |f: Chain<EdgeLeaf>| FilterExpr::from(f.is_active());
    fn applied(g: &Graph, filter: FilterExpr) -> Vec<String> {
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
    let list = |items: &[i64]| Prop::list(items.iter().copied()).unwrap();
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
            // the union of what each keeps: every node, and the edges either leg
            // keeps (none between alice and herself, and carol->dave by weight)
            "or(node, edge): the union of what each leg keeps",
            FilterExpr::Or(vec![score_gt(4.0), w_gt(2)]),
            &all_nodes,
            &["carol->dave"],
        ),
        (
            "not(or(node, edge)): still open",
            not(FilterExpr::Or(vec![score_gt(4.0), w_gt(2)])),
            &all_nodes,
            &all_edges,
        ),
        (
            // the or keeps carol->dave only, which the node leg's ends then drop
            "and(node, or(node, edge)): the or's edges count",
            FilterExpr::And(vec![
                score_gt(1.5),
                FilterExpr::Or(vec![score_gt(4.0), w_gt(2)]),
            ]),
            &["alice", "bob"],
            &[],
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

    // the union of what each leg keeps: no edge has both ends at b, so only
    // the edge leg's b->c
    let or_node_edge = name_is("b").or(w_gt_1());
    assert_eq!(
        selected_edges(&g, &or_node_edge),
        ["b->c"],
        "or(node, edge): edges.select"
    );
    assert_eq!(
        edge_ids(&g.filter(or_node_edge.clone()).unwrap()),
        ["b->c"],
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

/// An `or` between a node predicate and an edge predicate is the union of what
/// each keeps: every node, since an edge predicate keeps every node, and the
/// edges either leg keeps, where the node leg keeps the edges with both ends
/// inside its nodes. It is not "everything".
#[test]
fn a_mixed_or_is_the_union_of_what_its_legs_keep() {
    // a→b w=3 · b→c w=7 · c→d w=1 · a→a w=1; every node added on its own too,
    // so a node with every edge hidden still exists (#2821)
    let g = Graph::new();
    for name in ["a", "b", "c", "d"] {
        g.add_node(0, name, NO_PROPS, None, None).unwrap();
    }
    g.add_edge(1, "a", "b", [("w", 3i64)], None).unwrap();
    g.add_edge(2, "b", "c", [("w", 7i64)], None).unwrap();
    g.add_edge(3, "c", "d", [("w", 1i64)], None).unwrap();
    g.add_edge(4, "a", "a", [("w", 1i64)], None).unwrap();
    let heavy = EdgeFilter.property("w").ge(5i64);
    let named_a = NodeFilter.name().eq("a");
    let union = g
        .filter(FilterExpr::Or(vec![
            named_a.clone().into(),
            heavy.clone().into(),
        ]))
        .unwrap();
    let mut nodes: Vec<String> = union.nodes().name().collect();
    nodes.sort();
    assert_eq!(nodes, ["a", "b", "c", "d"]);
    let mut edges: Vec<(String, String)> = union
        .edges()
        .iter()
        .map(|e| (e.src().name(), e.dst().name()))
        .collect();
    edges.sort();
    // b→c passes the edge leg; a→a has both ends inside the node leg's nodes
    assert_eq!(edges, [("a".into(), "a".into()), ("b".into(), "c".into())]);
    // the edge leg alone keeps every node and one edge
    assert_eq!(g.filter(heavy).unwrap().edges().len(), 1);

    // with an exploded-edge leg the union is decided update by update: b→c's
    // update by weight, a→a's because both ends are inside the node leg
    let heavy_update = ExplodedEdgeFilter.property("w").eq(7i64);
    let union = g
        .filter(FilterExpr::Or(vec![named_a.into(), heavy_update.into()]))
        .unwrap();
    let mut updates: Vec<(String, String, i64)> = union
        .edges()
        .explode()
        .iter()
        .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
        .collect();
    updates.sort();
    assert_eq!(
        updates,
        [("a".into(), "a".into(), 4), ("b".into(), "c".into(), 2)]
    );
}

/// An `and`, `or` or view leg with nothing under it is refused wherever it
/// sits: an empty `and` is not "everything", `not` of it is not either, and an
/// empty view leg beside a real one is not nothing.
#[test]
fn empty_legs_are_refused() {
    let g = Graph::new();
    g.add_node(1, "a", NO_PROPS, None, None).unwrap();
    let refused = |filter: FilterExpr, message: &str| {
        let err = g.filter(filter).err().expect("refused").to_string();
        assert!(err.contains(message), "{err}");
    };
    refused(FilterExpr::And(vec![]), "`and` needs at least one operand");
    refused(FilterExpr::Or(vec![]), "`or` needs at least one operand");
    refused(
        FilterExpr::Not(Box::new(FilterExpr::And(vec![]))),
        "`and` needs at least one operand",
    );
    refused(
        FilterExpr::And(vec![
            FilterExpr::View(vec![]),
            FilterExpr::View(vec![window(0, 5)]),
        ]),
        "a view filter needs at least one view",
    );
}

/// A name, id or type has no time axis and does not depend on the node set,
/// so a view written on a field term is kept for display and ignored when the
/// filter runs: the term reads the field whether or not the view holds the node.
#[test]
fn a_view_on_a_field_term_is_ignored() {
    // early@1 · late@7
    let g = Graph::new();
    g.add_node(1, "early", NO_PROPS, None, None).unwrap();
    g.add_node(7, "late", NO_PROPS, Some("kind"), None).unwrap();
    let filtered = |f: &dyn Fn() -> FilterExpr| {
        let mut n: Vec<String> = g.filter(f()).unwrap().nodes().name().collect();
        n.sort();
        n
    };
    let selected = |f: &dyn Fn() -> FilterExpr| {
        let mut n: Vec<String> = g.nodes().select(f()).unwrap().name().collect();
        n.sort();
        n
    };
    let win = || NodeFilter.window(0, 5);
    let cases: Vec<(&str, Box<dyn Fn() -> FilterExpr>, &[&str])> = vec![
        (
            "window name == late",
            Box::new(move || FilterExpr::from(win().name().eq("late"))),
            &["late"],
        ),
        (
            "window name == early",
            Box::new(move || FilterExpr::from(win().name().eq("early"))),
            &["early"],
        ),
        (
            "window id == late",
            Box::new(move || FilterExpr::from(win().id().eq("late"))),
            &["late"],
        ),
        (
            "window node_type == kind",
            Box::new(move || FilterExpr::from(win().node_type().eq("kind"))),
            &["late"],
        ),
        (
            "window node_type == _default",
            Box::new(move || FilterExpr::from(win().node_type().eq("_default"))),
            &["early"],
        ),
        (
            "name == late",
            Box::new(|| FilterExpr::from(NodeFilter.name().eq("late"))),
            &["late"],
        ),
        (
            "exclude_nodes name == late",
            Box::new(|| FilterExpr::from(NodeFilter.exclude_nodes(["late"]).name().eq("late"))),
            &["late"],
        ),
        (
            "subgraph name == early",
            Box::new(|| FilterExpr::from(NodeFilter.subgraph(["late"]).name().eq("early"))),
            &["early"],
        ),
        (
            "subgraph_node_types name == early",
            Box::new(|| {
                FilterExpr::from(NodeFilter.subgraph_node_types(["kind"]).name().eq("early"))
            }),
            &["early"],
        ),
        (
            "tree: window name == late",
            Box::new(|| {
                node(cmp(
                    BinaryOp::Eq,
                    Expr::Term(NodeLeaf::Field {
                        views: vec![window(0, 5)],
                        field: Field::Name,
                    }),
                    c("late"),
                ))
            }),
            &["late"],
        ),
    ];
    for (label, f, want) in cases {
        assert_eq!(filtered(&*f), want, "{label}: filter");
        assert_eq!(selected(&*f), want, "{label}: nodes.select");
    }
}

/// a→b [x] @1 · b→c [y] @3 · c→d [_default] @5 · d→a [x] @7 · e alone @8
///
/// ```text
///            1    3    5    7    8
/// a→b [x]    ●
/// b→c [y]         ●
/// c→d [def]            ●
/// d→a [x]                   ●
/// e                              ●
/// ```
fn layered_graph() -> Graph {
    let g = Graph::new();
    for (t, src, dst, layer) in [
        (1, "a", "b", Some("x")),
        (3, "b", "c", Some("y")),
        (5, "c", "d", None),
        (7, "d", "a", Some("x")),
    ] {
        g.add_edge(t, src, dst, NO_PROPS, layer).unwrap();
    }
    g.add_node(8, "e", NO_PROPS, None, None).unwrap();
    g
}

fn names_of(g: &DynamicGraph) -> Vec<String> {
    let mut names: Vec<String> = g.nodes().iter().map(|n| n.name()).collect();
    names.sort();
    names
}

fn edge_ids_of(g: &DynamicGraph) -> Vec<String> {
    let mut ids: Vec<String> = g
        .edges()
        .iter()
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    ids.sort();
    ids
}

/// The view ops `ops` read the way `core` does: as a graph-level view, through
/// a node term and through an edge term.
fn agrees_with_core(
    g: &DynamicGraph,
    ops: &[ViewOp],
    core: impl Fn(&DynamicGraph) -> Result<DynamicGraph, GraphError>,
) {
    let expected = core(g).unwrap();
    let label = format!("{:?}", ops);

    let viewed = g
        .filter(FilterExpr::View(ops.to_vec()))
        .unwrap()
        .into_dynamic();
    assert_eq!(names_of(&viewed), names_of(&expected), "nodes {label}");
    assert_eq!(
        edge_ids_of(&viewed),
        edge_ids_of(&expected),
        "edges {label}"
    );

    let degree = node(cmp(
        BinaryOp::Gt,
        Expr::Term(NodeLeaf::Degree {
            views: ops.to_vec(),
            direction: Direction::BOTH,
        }),
        c(0u64),
    ));
    let mut with_degree: Vec<String> = g
        .nodes()
        .iter()
        .filter(|n| {
            expected
                .node(n.name())
                .is_some_and(|in_view| in_view.degree() > 0)
        })
        .map(|n| n.name())
        .collect();
    with_degree.sort();
    let filtered = g.filter(degree).unwrap().into_dynamic();
    assert_eq!(names_of(&filtered), with_degree, "node term {label}");

    let node_active = node(Expr::Term(NodeLeaf::IsActive {
        views: ops.to_vec(),
    }));
    let mut active_nodes: Vec<String> = g
        .nodes()
        .iter()
        .filter(|n| {
            expected
                .node(n.name())
                .is_some_and(|in_view| in_view.is_active())
        })
        .map(|n| n.name())
        .collect();
    active_nodes.sort();
    let filtered = g.filter(node_active).unwrap().into_dynamic();
    assert_eq!(names_of(&filtered), active_nodes, "node is_active {label}");

    let active = FilterExpr::Edge(Expr::Term(EdgeLeaf::IsActive {
        views: ops.to_vec(),
    }));
    let mut active_in_view: Vec<String> = g
        .edges()
        .iter()
        .filter(|e| {
            expected
                .edge(e.src().name(), e.dst().name())
                .is_some_and(|in_view| in_view.is_active())
        })
        .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
        .collect();
    active_in_view.sort();
    let filtered = g.filter(active).unwrap().into_dynamic();
    assert_eq!(edge_ids_of(&filtered), active_in_view, "edge term {label}");
}

fn t(time: i64) -> EventTime {
    EventTime::start(time)
}

#[test]
fn shrink_start_and_shrink_end_agree_with_the_graph_views() {
    let events = layered_graph();
    for g in [
        events.clone().into_dynamic(),
        events.persistent_graph().into_dynamic(),
    ] {
        // On an unwindowed graph the shrink sets the one bound.
        agrees_with_core(&g, &[ViewOp::ShrinkStart(t(3))], |g| {
            Ok(g.shrink_start(3).into_dynamic())
        });
        agrees_with_core(&g, &[ViewOp::ShrinkEnd(t(5))], |g| {
            Ok(g.shrink_end(5).into_dynamic())
        });
        // Inside a window it narrows the window.
        agrees_with_core(&g, &[window(1, 9), ViewOp::ShrinkStart(t(3))], |g| {
            Ok(g.window(1, 9).shrink_start(3).into_dynamic())
        });
        agrees_with_core(&g, &[window(1, 9), ViewOp::ShrinkEnd(t(6))], |g| {
            Ok(g.window(1, 9).shrink_end(6).into_dynamic())
        });
        // It never widens: a bound outside the window changes nothing.
        agrees_with_core(&g, &[window(3, 6), ViewOp::ShrinkStart(t(1))], |g| {
            Ok(g.window(3, 6).shrink_start(1).into_dynamic())
        });
        agrees_with_core(&g, &[window(3, 6), ViewOp::ShrinkEnd(t(9))], |g| {
            Ok(g.window(3, 6).shrink_end(9).into_dynamic())
        });
        // A shrink past the other bound leaves an empty window.
        agrees_with_core(&g, &[window(3, 6), ViewOp::ShrinkStart(t(7))], |g| {
            Ok(g.window(3, 6).shrink_start(7).into_dynamic())
        });
        // And a window after a shrink intersects with it.
        agrees_with_core(&g, &[ViewOp::ShrinkStart(t(3)), window(1, 6)], |g| {
            Ok(g.shrink_start(3).window(1, 6).into_dynamic())
        });
    }
    // `window(1, 9).shrink_start(3)` is `window(3, 9)`.
    let g = events.into_dynamic();
    agrees_with_core(&g, &[window(1, 9), ViewOp::ShrinkStart(t(3))], |g| {
        Ok(g.window(3, 9).into_dynamic())
    });
}

#[test]
fn default_layer_and_exclude_layers_agree_with_the_graph_views() {
    let g = layered_graph().into_dynamic();
    let names = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    agrees_with_core(&g, &[ViewOp::DefaultLayer], |g| {
        Ok(g.default_layer().into_dynamic())
    });
    agrees_with_core(&g, &[ViewOp::ExcludeLayers(names(&["x"]))], |g| {
        Ok(g.exclude_layers("x")?.into_dynamic())
    });
    agrees_with_core(&g, &[ViewOp::ExcludeLayers(names(&["x", "y"]))], |g| {
        Ok(g.exclude_layers(vec!["x", "y"])?.into_dynamic())
    });
    agrees_with_core(&g, &[ViewOp::ExcludeLayers(Vec::new())], |g| {
        Ok(g.exclude_layers(Vec::<String>::new())?.into_dynamic())
    });
    // After a layer selection: exclusion works inside it, and the default
    // layer outside it is nothing.
    agrees_with_core(
        &g,
        &[
            ViewOp::Layers(names(&["x", "y"])),
            ViewOp::ExcludeLayers(names(&["y"])),
        ],
        |g| {
            Ok(g.layers(vec!["x", "y"])?
                .exclude_layers("y")?
                .into_dynamic())
        },
    );
    agrees_with_core(
        &g,
        &[ViewOp::Layers(names(&["x"])), ViewOp::DefaultLayer],
        |g| Ok(g.layers("x")?.default_layer().into_dynamic()),
    );
    agrees_with_core(
        &g,
        &[ViewOp::ExcludeLayers(names(&["x"])), ViewOp::DefaultLayer],
        |g| Ok(g.exclude_layers("x")?.default_layer().into_dynamic()),
    );
    // Layers and time compose in either order.
    agrees_with_core(
        &g,
        &[
            ViewOp::ExcludeLayers(names(&["y"])),
            ViewOp::ShrinkEnd(t(6)),
        ],
        |g| Ok(g.exclude_layers("y")?.shrink_end(6).into_dynamic()),
    );
    // A graph with no default layer has nothing in it, where naming the
    // layer would be refused.
    let no_default = Graph::new();
    no_default
        .add_edge(1, "a", "b", NO_PROPS, Some("x"))
        .unwrap();
    agrees_with_core(&no_default.into_dynamic(), &[ViewOp::DefaultLayer], |g| {
        Ok(g.default_layer().into_dynamic())
    });
    // An unknown layer is refused, as the graph view refuses it.
    assert!(g.exclude_layers("nope").is_err());
    let unknown = FilterExpr::View(vec![ViewOp::ExcludeLayers(names(&["nope"]))]);
    assert!(g.filter(unknown).is_err());
}

#[test]
fn the_new_views_display_and_round_trip_through_json() {
    let ops = vec![
        ViewOp::DefaultLayer,
        ViewOp::ExcludeLayers(vec!["a".into(), "b".into()]),
        ViewOp::ShrinkStart(t(3)),
        ViewOp::ShrinkEnd(t(9)),
    ];
    let f = FilterExpr::View(ops.clone());
    assert_eq!(
        f.to_string(),
        "VIEW(DEFAULT_LAYER . EXCLUDE_LAYER[a, b] . SHRINK_START[3] . SHRINK_END[9])"
    );
    let json = serde_json::to_string(&f).unwrap();
    assert!(json.contains(r#""default_layer""#), "{json}");
    assert!(json.contains(r#""exclude_layers":["a","b"]"#), "{json}");
    assert!(json.contains(r#""shrink_start":"#), "{json}");
    let back: FilterExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(back, f);
    // The builder spells the same ops.
    let built: FilterExpr = GraphFilter
        .default_layer()
        .exclude_layers(["a", "b"])
        .shrink_start(3)
        .shrink_end(9)
        .into();
    assert_eq!(built, f);
    let one: FilterExpr = GraphFilter.exclude_layer("a").into();
    assert_eq!(
        one,
        FilterExpr::View(vec![ViewOp::ExcludeLayers(vec!["a".into()])])
    );
}

/// a→b @1 · b→c @3 · c→d @5, deleted @6 · z alone @9; a, b are `person`,
/// c, d are `org`, z is `bot`
///
/// ```text
///            1    3    5    6    9
/// a→b        ●
/// b→c             ●
/// c→d                  ●    ✕
/// z                              ●
/// ```
fn node_set_graph() -> Graph {
    let g = Graph::new();
    for (t, src, dst) in [(1, "a", "b"), (3, "b", "c"), (5, "c", "d")] {
        g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
    }
    g.delete_edge(6, "c", "d", None).unwrap();
    for (t, name, node_type) in [
        (1, "a", "person"),
        (1, "b", "person"),
        (3, "c", "org"),
        (5, "d", "org"),
        (9, "z", "bot"),
    ] {
        g.add_node(t, name, NO_PROPS, Some(node_type), None)
            .unwrap();
    }
    g
}

fn ids(names: &[&str]) -> Vec<GID> {
    names.iter().map(|n| GID::Str(n.to_string())).collect()
}

fn type_names(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

#[test]
fn node_set_views_agree_with_the_graph_views() {
    let events = node_set_graph();
    for g in [
        events.clone().into_dynamic(),
        events.persistent_graph().into_dynamic(),
    ] {
        agrees_with_core(&g, &[ViewOp::ExcludeNodes(ids(&["b", "z"]))], |g| {
            Ok(g.exclude_nodes(["b", "z"]).into_dynamic())
        });
        // An id the view does not hold changes nothing.
        agrees_with_core(&g, &[ViewOp::ExcludeNodes(ids(&["nope"]))], |g| {
            Ok(g.exclude_nodes(["nope"]).into_dynamic())
        });
        agrees_with_core(&g, &[ViewOp::Subgraph(ids(&["a", "b", "c"]))], |g| {
            Ok(g.subgraph(["a", "b", "c"]).into_dynamic())
        });
        agrees_with_core(&g, &[ViewOp::Subgraph(ids(&["a", "nope"]))], |g| {
            Ok(g.subgraph(["a", "nope"]).into_dynamic())
        });
        agrees_with_core(&g, &[ViewOp::Subgraph(Vec::new())], |g| {
            Ok(g.subgraph(Vec::<String>::new()).into_dynamic())
        });
        agrees_with_core(
            &g,
            &[ViewOp::SubgraphNodeTypes(type_names(&["org"]))],
            |g| Ok(g.subgraph_node_types(["org"]).into_dynamic()),
        );
        agrees_with_core(
            &g,
            &[ViewOp::SubgraphNodeTypes(type_names(&["person", "bot"]))],
            |g| Ok(g.subgraph_node_types(["person", "bot"]).into_dynamic()),
        );
        agrees_with_core(&g, &[ViewOp::Valid], |g| Ok(g.valid().into_dynamic()));
        // They compose with time and with each other, in list order.
        agrees_with_core(
            &g,
            &[window(0, 4), ViewOp::ExcludeNodes(ids(&["a"]))],
            |g| Ok(g.window(0, 4).exclude_nodes(["a"]).into_dynamic()),
        );
        agrees_with_core(
            &g,
            &[ViewOp::ExcludeNodes(ids(&["a"])), window(0, 4)],
            |g| Ok(g.exclude_nodes(["a"]).window(0, 4).into_dynamic()),
        );
        agrees_with_core(
            &g,
            &[ViewOp::Subgraph(ids(&["c", "d", "z"])), ViewOp::Valid],
            |g| Ok(g.subgraph(["c", "d", "z"]).valid().into_dynamic()),
        );
        agrees_with_core(
            &g,
            &[
                ViewOp::SubgraphNodeTypes(type_names(&["person", "org"])),
                ViewOp::ExcludeNodes(ids(&["d"])),
            ],
            |g| {
                Ok(g.subgraph_node_types(["person", "org"])
                    .exclude_nodes(["d"])
                    .into_dynamic())
            },
        );
    }
}

#[test]
fn exclude_nodes_before_latest_is_not_latest_before_exclude_nodes() {
    // The newest event, z@9, belongs to the excluded node: excluding first
    // leaves c→d@5 as the newest, the other way round leaves z's moment with
    // z gone.
    let g = node_set_graph().into_dynamic();
    let excluded_first = [ViewOp::ExcludeNodes(ids(&["z"])), ViewOp::Latest];
    let latest_first = [ViewOp::Latest, ViewOp::ExcludeNodes(ids(&["z"]))];
    agrees_with_core(&g, &excluded_first, |g| {
        Ok(g.exclude_nodes(["z"]).latest().into_dynamic())
    });
    agrees_with_core(&g, &latest_first, |g| {
        Ok(g.latest().exclude_nodes(["z"]).into_dynamic())
    });
    let view = |ops: &[ViewOp]| {
        g.filter(FilterExpr::View(ops.to_vec()))
            .unwrap()
            .into_dynamic()
    };
    let (a, b) = (view(&excluded_first), view(&latest_first));
    assert_ne!(
        (names_of(&a), edge_ids_of(&a)),
        (names_of(&b), edge_ids_of(&b))
    );
}

#[test]
fn the_node_set_views_display_and_round_trip_through_json() {
    let ops = vec![
        ViewOp::ExcludeNodes(vec![GID::Str("a".into()), GID::U64(7)]),
        ViewOp::Subgraph(vec![GID::U64(1), GID::Str("b".into())]),
        ViewOp::SubgraphNodeTypes(type_names(&["person", "org"])),
        ViewOp::Valid,
    ];
    let f = FilterExpr::View(ops.clone());
    assert_eq!(
        f.to_string(),
        "VIEW(EXCLUDE_NODES[a, 7] . SUBGRAPH[1, b] . SUBGRAPH_NODE_TYPES[person, org] . VALID)"
    );
    let json = serde_json::to_string(&f).unwrap();
    assert!(
        json.contains(r#""exclude_nodes":[{"Str":"a"},{"U64":7}]"#),
        "{json}"
    );
    assert!(
        json.contains(r#""subgraph":[{"U64":1},{"Str":"b"}]"#),
        "{json}"
    );
    assert!(
        json.contains(r#""subgraph_node_types":["person","org"]"#),
        "{json}"
    );
    assert!(json.contains(r#""valid""#), "{json}");
    let back: FilterExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(back, f);
    // The builder spells the same ops.
    let built: FilterExpr = GraphFilter
        .exclude_nodes([GID::Str("a".into()), GID::U64(7)])
        .subgraph([GID::U64(1), GID::Str("b".into())])
        .subgraph_node_types(["person", "org"])
        .valid()
        .into();
    assert_eq!(built, f);
}
