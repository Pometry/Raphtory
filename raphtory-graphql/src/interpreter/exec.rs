//! The push-based executor.
//!
//! Walks a [`Plan`] depth-first, pushing/popping typed [`Value`]s on a reused
//! stack and writing leaves straight into the [`Sink`]. The only growable
//! structure is the `Vec<Value>` stack (depth = query nesting, already capped by
//! `schema.max_query_depth`); nothing is allocated per result.

use super::{
    plan::{IterKind, LeafKind, Nav, Op, Plan},
    sink::Sink,
    value::Value,
};
use crate::model::graph::{history::GqlHistory, node::GqlNode, nodes::GqlNodes};
use raphtory::{
    db::api::view::DynamicGraph,
    prelude::{GraphViewOps, NodeViewOps, TimeOps},
};
use raphtory_api::core::{entities::GID, storage::timeindex::AsTime};

/// Execute a plan against an already-loaded root graph, writing the full
/// `{"data": {"<root_key>": …}}` document into `sink`.
///
/// The graph is pre-loaded (the only genuinely async step) and seeded as the
/// bottom of the stack; everything below runs synchronously.
pub fn execute(plan: &Plan, graph: DynamicGraph, sink: &mut Sink) {
    sink.begin_object(); // {
    sink.begin_field("data"); // "data":
    sink.begin_object(); // {
    sink.begin_field(&plan.root_key); // "graph":
    sink.begin_object(); // {

    let mut stack: Vec<Value> = Vec::with_capacity(8);
    stack.push(Value::Graph(graph));
    for op in plan.children.iter() {
        exec(op, &mut stack, sink);
    }
    stack.pop();

    sink.end_object(); // graph
    sink.end_object(); // data
    sink.end_object(); // root
}

fn exec(op: &Op, stack: &mut Vec<Value>, sink: &mut Sink) {
    match op {
        Op::Navigate {
            key,
            nav,
            nullable,
            children,
        } => {
            // `apply` returns an owned Value, so the borrow of the stack ends
            // before we push.
            let produced = nav.apply(stack.last().expect("non-empty stack"));
            match produced {
                Some(v) => {
                    sink.begin_field(key);
                    sink.begin_object();
                    stack.push(v);
                    for child in children.iter() {
                        exec(child, stack, sink);
                    }
                    stack.pop();
                    sink.end_object();
                }
                None if *nullable => sink.field_null(key),
                None => {
                    // A non-null field resolved to nothing. The POC subset never
                    // hits this (validation + the single nullable field, `node`,
                    // cover it); abort rather than emit malformed JSON.
                    debug_assert!(false, "non-null field resolved to None");
                }
            }
        }

        Op::List {
            key,
            iter,
            children,
        } => {
            sink.begin_field(key);
            sink.begin_array();
            // Clone the iterable handle out of the stack (a cheap Arc bump) into
            // a local, so the item iterator borrows the local — not `stack` —
            // and we're free to push items as we go.
            match iter {
                IterKind::NodesList => {
                    let nodes = match stack.last().expect("non-empty stack") {
                        Value::Nodes(n) => n.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for node in nodes.iter() {
                        sink.begin_object();
                        stack.push(Value::Node(node));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
                IterKind::HistoryList => {
                    let history = match stack.last().expect("non-empty stack") {
                        Value::History(h) => h.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for et in history.history.iter() {
                        sink.begin_object();
                        stack.push(Value::EventTime(et));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
            }
            sink.end_array();
        }

        Op::Leaf { key, leaf } => {
            sink.begin_field(key);
            leaf.write(stack.last().expect("non-empty stack"), sink);
        }
    }
}

impl Nav {
    /// Apply this navigation to a receiver, producing the next receiver (or
    /// `None` for a nullable field that resolved to nothing).
    fn apply(&self, recv: &Value) -> Option<Value> {
        match (self, recv) {
            (Nav::Nodes, Value::Graph(g)) => Some(Value::Nodes(GqlNodes::new(g.nodes()))),
            (Nav::Node(id), Value::Graph(g)) => g.node(id).map(|n| Value::Node(GqlNode::from(n))),
            (Nav::History, Value::Node(n)) => {
                Some(Value::History(GqlHistory::from(n.vv.history())))
            }
            (Nav::After(t), Value::Node(n)) => Some(Value::Node(GqlNode::from(n.vv.after(*t)))),
            (Nav::Before(t), Value::Node(n)) => Some(Value::Node(GqlNode::from(n.vv.before(*t)))),
            (Nav::Window { start, end }, Value::Node(n)) => {
                Some(Value::Node(GqlNode::from(n.vv.window(*start, *end))))
            }
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

impl LeafKind {
    fn write(&self, recv: &Value, sink: &mut Sink) {
        match (self, recv) {
            (LeafKind::Id, Value::Node(n)) => match n.vv.id() {
                GID::Str(s) => sink.write_str(&s),
                GID::U64(u) => sink.write_u64(u),
            },
            (LeafKind::Name, Value::Node(n)) => sink.write_str(&n.vv.name()),
            (LeafKind::Timestamp, Value::EventTime(t)) => sink.write_i64(t.t()),
            (LeafKind::EventId, Value::EventTime(t)) => sink.write_u64(t.i() as u64),
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interpreter::{
        plan::{IterKind, LeafKind, Nav, Op, Plan},
        sink::test_collect_json,
    };
    use raphtory::{
        db::api::view::IntoDynamic,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };
    use serde_json::{json, Value as Json};

    fn boxed(ops: Vec<Op>) -> Box<[Op]> {
        ops.into_boxed_slice()
    }

    /// graph { node(name:"ben") { history { list { timestamp eventId } } } }
    fn history_plan(node: &str) -> Plan {
        Plan {
            root_key: "graph".into(),
            children: boxed(vec![Op::Navigate {
                key: "node".into(),
                nav: Nav::Node(node.into()),
                nullable: true,
                children: boxed(vec![Op::Navigate {
                    key: "history".into(),
                    nav: Nav::History,
                    nullable: false,
                    children: boxed(vec![Op::List {
                        key: "list".into(),
                        iter: IterKind::HistoryList,
                        children: boxed(vec![
                            Op::Leaf {
                                key: "timestamp".into(),
                                leaf: LeafKind::Timestamp,
                            },
                            Op::Leaf {
                                key: "eventId".into(),
                                leaf: LeafKind::EventId,
                            },
                        ]),
                    }]),
                }]),
            }]),
        }
    }

    async fn run(plan: Plan, graph: DynamicGraph) -> Json {
        test_collect_json(move |sink| execute(&plan, graph, sink)).await
    }

    #[tokio::test]
    async fn node_history_list() {
        let g = Graph::new();
        // node "ben" gets three updates; event ids are assigned in global
        // insertion order (0, 1, 2).
        g.add_node(1, "ben", NO_PROPS, None, None).unwrap();
        g.add_node(2, "ben", NO_PROPS, None, None).unwrap();
        g.add_node(3, "ben", NO_PROPS, None, None).unwrap();

        let out = run(history_plan("ben"), g.into_dynamic()).await;
        assert_eq!(
            out,
            json!({"data": {"graph": {"node": {"history": {"list": [
                {"timestamp": 1, "eventId": 0},
                {"timestamp": 2, "eventId": 1},
                {"timestamp": 3, "eventId": 2},
            ]}}}}})
        );
    }

    #[tokio::test]
    async fn missing_node_is_null() {
        let g = Graph::new();
        g.add_node(1, "ben", NO_PROPS, None, None).unwrap();

        let out = run(history_plan("nope"), g.into_dynamic()).await;
        assert_eq!(out, json!({"data": {"graph": {"node": null}}}));
    }

    /// Differential test: the interpreter's `data` must equal what the live
    /// async-graphql endpoint returns for the same query on the same graph.
    #[tokio::test]
    async fn matches_async_graphql_endpoint() {
        use crate::{
            client::raphtory_client::RaphtoryGraphQLClient, url_encode::url_encode_graph,
            GraphServer,
        };
        use raphtory::db::api::storage::storage::Config;
        use std::{collections::HashMap, time::Duration};
        use tempfile::TempDir;
        use url::Url;

        // a graph with a few updates on "ben"
        let g = Graph::new();
        g.add_node(1, "ben", NO_PROPS, None, None).unwrap();
        g.add_node(5, "ben", NO_PROPS, None, None).unwrap();
        g.add_edge(7, "ben", "hamza", NO_PROPS, None).unwrap();
        g.add_node(9, "ben", NO_PROPS, None, None).unwrap();

        // start the real server (old engine)
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default())
            .await
            .unwrap();
        let port = 43932;
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        // send the graph via the existing mutation path
        let url = Url::parse(&format!("http://localhost:{port}/")).unwrap();
        let client = RaphtoryGraphQLClient::new(url, None);
        let encoded = url_encode_graph(g.materialize().unwrap()).unwrap();
        client.send_graph("g", &encoded, true).await.unwrap();

        let query = r#"{ graph(path: "g") { node(name: "ben") {
            history { list { timestamp eventId } } } } }"#;
        let expected = client.query(query, HashMap::new()).await.unwrap();
        let expected = serde_json::to_value(expected).unwrap(); // {"graph": {...}}

        // run the interpreter over the same in-memory graph
        let out = run(history_plan("ben"), g.into_dynamic()).await;
        assert_eq!(out["data"], expected);
    }
}
