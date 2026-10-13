use dynamic_graphql::{App, Request, ResolvedObject, ResolvedObjectFields, Variables};
use itertools::Itertools;
use raphtory::{
    algorithms::alternating_mask::alternating_mask,
    db::{
        api::view::Filter,
        graph::views::filter::model::{
            expr::{
                self, Agg, EdgeLeaf, ExplodedEdgeLeaf, Field, FilterExpr as F, Leaf, NodeLeaf,
                ViewOp, OPAQUE_FILTER_ERROR,
            },
            filter_operator::{BinaryOp, StringOp},
            node_filter::{NodeFilter, NodeFilterFactory},
        },
    },
    prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer, GID},
    storage::timeindex::EventTime,
    Direction,
};
use raphtory_graphql::model::graph::filter_expr_input::{GqlFilter, GqlViewOp, ViewKind};
use serde_json::json;
/// A schema with one field that parses a filter the way every resolver
/// does and answers the tree it became, as JSON.
#[derive(ResolvedObject)]
#[graphql(root)]
struct Echo;

#[ResolvedObjectFields]
impl Echo {
    /// The tree `expr` parses to, as JSON.
    async fn tree(expr: GqlFilter) -> dynamic_graphql::Result<String> {
        Ok(serde_json::to_string(&F::try_from(expr)?)?)
    }
}

#[derive(App)]
struct EchoApp(Echo);

async fn run(request: Request) -> Result<String, String> {
    let schema = EchoApp::create_schema().finish().unwrap();
    let res = schema.execute(request).await;
    if !res.errors.is_empty() {
        return Err(res.errors.iter().map(|e| e.message.as_str()).join("; "));
    }
    let data = res.data.into_json().unwrap();
    Ok(data["tree"].as_str().unwrap().to_string())
}

/// The tree a filter sent as a GraphQL variable becomes, as JSON.
async fn from_variable(filter: serde_json::Value) -> Result<String, String> {
    run(Request::new("query($f: FilterExpr!) { tree(expr: $f) }")
        .variables(Variables::from_json(json!({ "f": filter }))))
    .await
}

/// The tree a filter written as a GraphQL literal becomes, as JSON.
async fn from_literal(filter: &str) -> Result<String, String> {
    run(Request::new(format!("{{ tree(expr: {filter}) }}"))).await
}

/// The tree a filter stored as JSON (a grant) becomes.
fn from_json(filter: serde_json::Value) -> Result<F, String> {
    let wire: GqlFilter = serde_json::from_value(filter).map_err(|e| e.to_string())?;
    F::try_from(wire).map_err(|e| e.to_string())
}

fn tree_json(tree: &F) -> String {
    serde_json::to_string(tree).unwrap()
}

/// The tree goes to the wire type, to JSON, and back by serde and by
/// GraphQL, and is the same tree every way.
async fn assert_round_trip(tree: F) {
    let wire = GqlFilter::try_from(&tree).unwrap();
    let json = serde_json::to_value(&wire).unwrap();
    assert_eq!(from_json(json.clone()).unwrap(), tree);
    assert_eq!(from_variable(json).await.unwrap(), tree_json(&tree));
}

/// How one view is sent: the single view of a view leg holding only it.
fn sent_view(op: &ViewOp) -> serde_json::Value {
    let leg = GqlFilter::try_from(&F::View(vec![op.clone()])).unwrap();
    let mut json = serde_json::to_value(leg).unwrap();
    let views = json["view"].as_array_mut().unwrap();
    assert_eq!(views.len(), 1, "{views:?}");
    views.remove(0)
}

fn t(at: i64) -> EventTime {
    EventTime::from(at)
}

fn every_view() -> Vec<ViewOp> {
    vec![
        ViewOp::Window {
            start: t(0),
            end: t(5),
        },
        ViewOp::At(t(1)),
        ViewOp::After(t(2)),
        ViewOp::Before(t(9)),
        ViewOp::Latest,
        ViewOp::SnapshotAt(t(4)),
        ViewOp::SnapshotLatest,
        ViewOp::Layers("work".into()),
        ViewOp::DefaultLayer,
        ViewOp::ExcludeLayers(["a", "b"].into()),
        ViewOp::ValidLayers(["work", "nope"].into()),
        ViewOp::ExcludeValidLayers(["a", "nope"].into()),
        ViewOp::ShrinkStart(t(3)),
        ViewOp::ShrinkEnd(t(8)),
        ViewOp::ExcludeNodes(vec![GID::Str("a".into()), GID::U64(7)]),
        ViewOp::Subgraph(vec![GID::U64(1), GID::Str("b".into())]),
        ViewOp::SubgraphNodeTypes(vec!["person".into()]),
        ViewOp::Valid,
    ]
}

/// Every variant of the tree over one entity: each leaf given (as a
/// presence test), and every operator around the first one.
fn every_variant<L: Leaf>(leaves: Vec<L>) -> expr::Expr<L> {
    let term = || Box::new(expr::Expr::Term(leaves[0].clone()));
    let one = || Box::new(expr::Expr::Const(Prop::I64(1)));
    let text = || Box::new(expr::Expr::Const(Prop::str("al")));
    let mut all: Vec<expr::Expr<L>> = leaves
        .iter()
        .map(|leaf| expr::Expr::IsSome(Box::new(expr::Expr::Term(leaf.clone()))))
        .collect();
    all.push(expr::Expr::Const(Prop::Bool(true)));
    for agg in [
        Agg::Sum,
        Agg::Avg,
        Agg::Min,
        Agg::Max,
        Agg::First,
        Agg::Last,
        Agg::Len,
        Agg::Earliest,
        Agg::Latest,
    ] {
        all.push(expr::Expr::Cmp(
            BinaryOp::Ge,
            Box::new(expr::Expr::Agg(agg, term())),
            one(),
        ));
    }
    for op in [
        BinaryOp::Eq,
        BinaryOp::Ne,
        BinaryOp::Lt,
        BinaryOp::Le,
        BinaryOp::Gt,
        BinaryOp::Ge,
    ] {
        all.push(expr::Expr::Cmp(op, term(), one()));
    }
    for op in [
        StringOp::StartsWith,
        StringOp::EndsWith,
        StringOp::Contains,
        StringOp::NotContains,
        StringOp::FuzzySearch {
            levenshtein_distance: 2,
            prefix_match: true,
        },
    ] {
        all.push(expr::Expr::Str(op, term(), text()));
    }
    for negated in [false, true] {
        all.push(expr::Expr::In {
            expr: term(),
            values: vec![Prop::str("alice"), Prop::I64(3)],
            negated,
        });
    }
    all.push(expr::Expr::IsNone(term()));
    let gt = || Box::new(expr::Expr::Cmp(BinaryOp::Gt, term(), one()));
    all.push(expr::Expr::Any(gt()));
    all.push(expr::Expr::All(gt()));
    all.push(expr::Expr::Or(vec![*gt(), expr::Expr::Not(gt())]));
    expr::Expr::And(all)
}

fn node_leaves() -> Vec<NodeLeaf> {
    let field = |field| NodeLeaf::Field {
        views: Vec::new(),
        field,
    };
    let degree = |direction| NodeLeaf::Degree {
        views: Vec::new(),
        direction,
    };
    vec![
        NodeLeaf::Property {
            views: every_view(),
            name: "score".into(),
            temporal: false,
        },
        NodeLeaf::Property {
            views: Vec::new(),
            name: "score".into(),
            temporal: true,
        },
        NodeLeaf::Metadata {
            views: vec![ViewOp::Latest],
            name: "region".into(),
        },
        field(Field::Name),
        field(Field::Id),
        field(Field::NodeType),
        degree(Direction::BOTH),
        degree(Direction::IN),
        degree(Direction::OUT),
        NodeLeaf::IsActive {
            views: vec![ViewOp::Layers("work".into())],
        },
    ]
}

#[tokio::test]
async fn every_node_variant_round_trips() {
    assert_round_trip(F::Node(every_variant(node_leaves()))).await;
}

#[tokio::test]
async fn every_edge_variant_round_trips() {
    let views = || vec![ViewOp::At(t(2)), ViewOp::DefaultLayer];
    let endpoint = || Box::new(every_variant(node_leaves()));
    let leaves = vec![
        EdgeLeaf::Property {
            views: every_view(),
            name: "w".into(),
            temporal: false,
        },
        EdgeLeaf::Property {
            views: Vec::new(),
            name: "w".into(),
            temporal: true,
        },
        EdgeLeaf::Metadata {
            views: Vec::new(),
            name: "kind".into(),
        },
        EdgeLeaf::IsActive { views: views() },
        EdgeLeaf::IsValid { views: Vec::new() },
        EdgeLeaf::IsDeleted { views: views() },
        EdgeLeaf::IsSelfLoop { views: Vec::new() },
        EdgeLeaf::Src(endpoint()),
        EdgeLeaf::Dst(endpoint()),
    ];
    assert_round_trip(F::Edge(every_variant(leaves))).await;
}

#[tokio::test]
async fn every_exploded_edge_variant_round_trips() {
    let leaves = vec![
        ExplodedEdgeLeaf::Property {
            views: every_view(),
            name: "w".into(),
            temporal: false,
        },
        ExplodedEdgeLeaf::Property {
            views: Vec::new(),
            name: "w".into(),
            temporal: true,
        },
        ExplodedEdgeLeaf::Metadata {
            views: Vec::new(),
            name: "kind".into(),
        },
        ExplodedEdgeLeaf::IsActive {
            views: vec![ViewOp::Valid],
        },
        ExplodedEdgeLeaf::IsValid { views: Vec::new() },
        ExplodedEdgeLeaf::IsDeleted { views: Vec::new() },
        ExplodedEdgeLeaf::IsSelfLoop { views: Vec::new() },
    ];
    assert_round_trip(F::ExplodedEdge(every_variant(leaves))).await;
}

#[tokio::test]
async fn every_filter_variant_round_trips() {
    let score = || {
        F::Node(expr::Expr::IsSome(Box::new(expr::Expr::Term(
            NodeLeaf::Property {
                views: Vec::new(),
                name: "score".into(),
                temporal: false,
            },
        ))))
    };
    assert_round_trip(F::And(vec![
        F::View(every_view()),
        F::Or(vec![score(), score()]),
        F::Not(Box::new(score())),
    ]))
    .await;
}

#[tokio::test]
async fn the_wire_json_is_the_graphql_spelling() {
    let tree = F::Edge(expr::Expr::Cmp(
        BinaryOp::Eq,
        Box::new(expr::Expr::Term(EdgeLeaf::Src(Box::new(expr::Expr::Term(
            NodeLeaf::Field {
                views: Vec::new(),
                field: Field::Name,
            },
        ))))),
        Box::new(expr::Expr::Const(Prop::str("alice"))),
    ));
    let wire = GqlFilter::try_from(&tree).unwrap();
    assert_eq!(
        serde_json::to_string(&wire).unwrap(),
        r#"{"edge":{"cmp":{"op":"EQ","lhs":{"read":{"src":{"expr":{"read":{"field":{"name":"NAME"}}}}}},"rhs":{"const":{"str":"alice"}}}}}"#
    );
    let literal = r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { expr: { read: { field: { name: NAME } } } } } }, rhs: { const: { str: "alice" } } } } }"#;
    assert_eq!(from_literal(literal).await.unwrap(), tree_json(&tree));
}

#[tokio::test]
async fn the_documented_examples_parse() {
    for example in [
        r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score" } } }, rhs: { const: { i64: 4 } } } } }"#,
        r#"{ node: { cmp: { op: EQ, lhs: { read: { metadata: { name: "region" } } }, rhs: { const: { str: "eu" } } } } }"#,
        r#"{ node: { cmp: { op: EQ, lhs: { read: { field: { name: NODE_TYPE } } }, rhs: { const: { str: "user" } } } } }"#,
        r#"{ node: { cmp: { op: GT, lhs: { read: { field: { name: DEGREE } } }, rhs: { read: { field: { name: IN_DEGREE } } } } } }"#,
        r#"{ node: { read: { field: { name: IS_ACTIVE } } } }"#,
        r#"{ node: { str: { op: STARTS_WITH, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "al" } } } } }"#,
        r#"{ node: { str: { op: FUZZY, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alise" } }, levenshteinDistance: 1, prefixMatch: false } } }"#,
        r#"{ node: { isIn: { expr: { read: { field: { name: NAME } } }, values: { list: [ { str: "alice" }, { str: "carol" } ] } } } }"#,
        r#"{ node: { presence: { op: IS_NONE, expr: { read: { property: { name: "score" } } } } } }"#,
        r#"{ node: { cmp: { op: GE, lhs: { agg: { op: SUM, expr: { read: { temporalProperty: { name: "score" } } } } }, rhs: { const: { i64: 19 } } } } }"#,
        r#"{ node: { qualified: { op: ANY, expr: { cmp: { op: GT, lhs: { read: { temporalProperty: { name: "score" } } }, rhs: { const: { i64: 8 } } } } } } }"#,
        r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score", views: [ { window: { start: 0, end: 2 } } ] } } }, rhs: { const: { i64: 4 } } } } }"#,
        r#"{ node: { cmp: { op: EQ, lhs: { read: { property: { name: "score", views: [ { window: { start: 0, end: 4 } }, { kind: LATEST } ] } } }, rhs: { const: { i64: 2 } } } } }"#,
        r#"{ node: { read: { field: { name: IS_ACTIVE, views: [ { excludeNodes: ["bob"] }, { kind: LATEST } ] } } } }"#,
        r#"{ edge: { cmp: { op: EQ, lhs: { read: { src: { expr: { read: { field: { name: NAME } } } } } }, rhs: { const: { str: "alice" } } } } }"#,
        r#"{ edge: { cmp: { op: GT, lhs: { read: { src: { expr: { read: { property: { name: "score", views: [ { at: 2 } ] } } } } } }, rhs: { read: { dst: { expr: { read: { property: { name: "score", views: [ { at: 1 } ] } } } } } } } } }"#,
        r#"{ edge: { read: { field: { name: IS_VALID, views: [ { layers: ["work"] } ] } } } }"#,
        r#"{ explodedEdge: { cmp: { op: GT, lhs: { read: { property: { name: "w" } } }, rhs: { const: { f64: 5.0 } } } } }"#,
        r#"{ node: { cmp: { op: GT, lhs: { read: { property: { name: "score", views: [{ window: { start: 0, end: 5 } }] } } }, rhs: { const: { i64: 4 } } } } }"#,
        r#"{ edge: { read: { src: { expr: { cmp: { op: EQ, lhs: { read: { field: { name: NAME } } }, rhs: { const: { str: "alice" } } } } } } } }"#,
        r#"{ view: [ { window: { start: 1, end: 5 } }, { excludeLayers: ["friends"] } ] }"#,
        r#"{ view: [ { subgraph: ["alice", "bob"] }, { kind: VALID } ] }"#,
        r#"{ and: [ { view: [ { window: { start: 1, end: 5 } } ] }, { node: { cmp: { op: GT, lhs: { read: { property: { name: "score" } } }, rhs: { const: { i64: 4 } } } } } ] }"#,
        r#"{ node: { and: [ { presence: { op: IS_SOME, expr: { read: { property: { name: "score" } } } } }, { cmp: { op: EQ, lhs: { read: { field: { name: NODE_TYPE } } }, rhs: { const: { str: "user" } } } } ] } }"#,
    ] {
        if let Err(err) = from_literal(example).await {
            panic!("{example}: {err}");
        }
    }
}

#[tokio::test]
async fn a_read_names_exactly_one_term() {
    for read in [
        // Two terms.
        json!({ "property": { "name": "a" }, "metadata": { "name": "b" } }),
        json!({ "property": { "name": "a" }, "metadata": { "name": "b" }, "field": { "name": "NAME" } }),
        // No term.
        json!({}),
        json!({ "views": [{ "kind": "LATEST" }] }),
        // The flat string form.
        json!({ "property": "a" }),
        // The `term` wrapper.
        json!({ "term": { "property": "a" } }),
        json!({ "term": { "property": { "name": "a" } } }),
    ] {
        let filter = json!({ "node": { "read": read } });
        assert!(from_variable(filter.clone()).await.is_err(), "{filter}");
        assert!(from_json(filter.clone()).is_err(), "{filter}");
    }
    assert!(from_literal(
        r#"{ edge: { read: { field: { name: IS_VALID }, src: { expr: { read: { field: { name: NAME } } } } } } }"#,
    )
    .await
    .is_err());
}

#[tokio::test]
async fn only_fuzzy_takes_its_arguments() {
    let test = |extra: serde_json::Value, op: &str| {
        let mut s = json!({
            "op": op,
            "lhs": { "read": { "field": { "name": "NAME" } } },
            "rhs": { "const": { "str": "al" } },
        });
        s.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({ "node": { "str": s } })
    };
    for (filter, message) in [
        (
            test(json!({}), "FUZZY"),
            "FUZZY requires `levenshteinDistance` and `prefixMatch`",
        ),
        (
            test(json!({ "levenshteinDistance": 1 }), "FUZZY"),
            "FUZZY requires `levenshteinDistance` and `prefixMatch`",
        ),
        (
            test(json!({ "levenshteinDistance": 1 }), "CONTAINS"),
            "only FUZZY takes `levenshteinDistance` and `prefixMatch`",
        ),
        (
            test(
                json!({ "levenshteinDistance": 1, "prefixMatch": true }),
                "CONTAINS",
            ),
            "only FUZZY takes `levenshteinDistance` and `prefixMatch`",
        ),
    ] {
        let err = from_variable(filter.clone()).await.unwrap_err();
        assert!(err.contains(message), "{err}");
        let err = from_json(filter).unwrap_err();
        assert!(err.contains(message), "{err}");
    }
    let fuzzy = test(
        json!({ "levenshteinDistance": 1, "prefixMatch": false }),
        "FUZZY",
    );
    assert!(from_variable(fuzzy).await.is_ok());
}

#[tokio::test]
async fn a_view_without_argument_is_a_kind() {
    for (kind, op, name) in [
        (ViewKind::Latest, ViewOp::Latest, "LATEST"),
        (
            ViewKind::SnapshotLatest,
            ViewOp::SnapshotLatest,
            "SNAPSHOT_LATEST",
        ),
        (ViewKind::Valid, ViewOp::Valid, "VALID"),
        (
            ViewKind::DefaultLayer,
            ViewOp::DefaultLayer,
            "DEFAULT_LAYER",
        ),
    ] {
        assert_eq!(ViewOp::from(GqlViewOp::Kind(kind)), op);
        let json = sent_view(&op);
        assert_eq!(json, json!({ "kind": name }));
        let back: GqlViewOp = serde_json::from_value(json).unwrap();
        assert_eq!(ViewOp::from(back), op);
    }
    for old in ["latest", "snapshotLatest", "valid", "defaultLayer"] {
        let view = json!([{ old: true }]);
        assert!(serde_json::from_value::<Vec<GqlViewOp>>(view.clone()).is_err());
        let err = from_variable(json!({ "view": view })).await.unwrap_err();
        assert!(err.contains(old), "{err}");
    }
}

#[test]
fn layer_views_carry_the_names_as_a_list() {
    let one = ViewOp::from(GqlViewOp::ExcludeLayers(vec!["a".into()]));
    assert_eq!(one, ViewOp::ExcludeLayers("a".into()));
    let json = sent_view(&one);
    assert_eq!(json, json!({ "excludeLayers": ["a"] }));
    // No name is no layer, and back.
    let none = ViewOp::from(GqlViewOp::Layers(Vec::new()));
    assert_eq!(none, ViewOp::Layers(Layer::None));
    let json = sent_view(&none);
    assert_eq!(json, json!({ "layers": [] }));
}

/// a→b @1 on `x`, b→c @2 on `y`, each with `w`
///
/// ```text
///            1    2
/// a→b  (x)   ●
/// b→c  (y)        ●
/// ```
#[tokio::test]
async fn every_layer_is_sent_as_the_view_it_means() {
    let graph = Graph::new();
    graph
        .add_edge(1, "a", "b", [("w", Prop::I64(1))], Some("x"))
        .unwrap();
    graph
        .add_edge(2, "b", "c", [("w", Prop::I64(2))], Some("y"))
        .unwrap();
    let w = |views: Vec<ViewOp>| {
        F::Edge(expr::Expr::IsSome(Box::new(expr::Expr::Term(
            EdgeLeaf::Property {
                views,
                name: "w".into(),
                temporal: false,
            },
        ))))
    };
    let every = || ViewOp::Layers(Layer::All);
    let none = || ViewOp::ExcludeLayers(Layer::All);
    let at_2 = || ViewOp::After(t(2));
    for tree in [
        w(vec![every()]),
        w(vec![none()]),
        w(vec![ViewOp::ValidLayers(Layer::All), at_2()]),
        w(vec![ViewOp::ExcludeValidLayers(Layer::All)]),
        F::View(vec![every()]),
        F::View(vec![none()]),
        F::View(vec![every(), at_2()]),
        F::And(vec![F::View(vec![every()]), w(vec![every()])]),
        F::And(vec![F::View(vec![none()]), w(Vec::new())]),
    ] {
        let json = serde_json::to_value(GqlFilter::try_from(&tree).unwrap()).unwrap();
        assert!(!json.to_string().contains("null"), "{json}");
        let back = from_json(json.clone()).unwrap();
        assert_eq!(from_variable(json).await.unwrap(), tree_json(&back));
        let expected = graph.filter(tree).unwrap();
        let actual = graph.filter(back).unwrap();
        assert_eq!(
            actual.nodes().name().collect_vec(),
            expected.nodes().name().collect_vec()
        );
        assert_eq!(
            actual.edges().id().collect_vec(),
            expected.edges().id().collect_vec()
        );
    }
    // A view leg whose views all restrict nothing is sent as excluding
    // no layer: an empty leg would be refused.
    let json = serde_json::to_value(GqlFilter::try_from(&F::View(vec![every()])).unwrap());
    assert_eq!(json.unwrap(), json!({ "view": [{ "excludeLayers": [] }] }));
    // Excluding every layer is no layer.
    let json = serde_json::to_value(GqlFilter::try_from(&F::View(vec![none()])).unwrap());
    assert_eq!(json.unwrap(), json!({ "view": [{ "layers": [] }] }));
    // A leg with no view at all is still sent empty, and still refused.
    let empty = GqlFilter::try_from(&F::View(Vec::new())).unwrap();
    assert_eq!(serde_json::to_value(empty).unwrap(), json!({ "view": [] }));
}

/// An empty view leg, `and` or `or` is refused while it is decoded, with the
/// message the filter itself refuses it with, by GraphQL and by serde.
#[tokio::test]
async fn empty_legs_are_refused_when_decoded() {
    for (filter, message) in [
        (
            json!({ "view": [] }),
            "Invalid filter: a view filter needs at least one view",
        ),
        (
            json!({ "and": [] }),
            "Invalid filter: `and` needs at least one operand",
        ),
        (
            json!({ "or": [] }),
            "Invalid filter: `or` needs at least one operand",
        ),
        (
            json!({ "not": { "and": [] } }),
            "Invalid filter: `and` needs at least one operand",
        ),
        (
            json!({ "and": [{ "view": [] }, { "view": [{ "kind": "LATEST" }] }] }),
            "Invalid filter: a view filter needs at least one view",
        ),
    ] {
        assert_eq!(
            from_variable(filter.clone()).await.unwrap_err(),
            message,
            "{filter}"
        );
        assert_eq!(from_json(filter.clone()).unwrap_err(), message, "{filter}");
    }
}

#[test]
fn node_ids_take_the_scalar_spelling_and_keep_their_type() {
    // The wire form is the variables a client sends, so a node id is the
    // `NodeId` scalar: a JSON string or a JSON integer, never the tagged
    // serde form of `GID`.
    let op = ViewOp::ExcludeNodes(vec![GID::Str("7".into()), GID::U64(7)]);
    let json = sent_view(&op);
    assert_eq!(json, json!({ "excludeNodes": ["7", 7] }));
    let back: GqlViewOp = serde_json::from_value(json).unwrap();
    assert_eq!(ViewOp::from(back), op);
    assert!(serde_json::from_value::<GqlViewOp>(json!({ "subgraph": [-1] })).is_err());
    assert!(serde_json::from_value::<GqlViewOp>(json!({ "subgraph": [{ "U64": 1 }] })).is_err());
}

#[test]
fn an_opaque_filter_has_no_wire_form() {
    let graph = Graph::new();
    graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 2, NO_PROPS, None, None).unwrap();
    let mask = alternating_mask(&graph);
    let opaque = NodeFilter::by_column(&mask, "bool_col").unwrap();
    let err = GqlFilter::try_from(&opaque).unwrap_err();
    assert!(err.to_string().contains(OPAQUE_FILTER_ERROR), "{err}");
}
