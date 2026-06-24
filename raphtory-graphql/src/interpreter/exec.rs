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
use crate::model::graph::{
    edge::GqlEdge,
    edges::GqlEdges,
    history::GqlHistory,
    node::GqlNode,
    nodes::GqlNodes,
    path_from_node::GqlPathFromNode,
    property::{prop_to_gql, GqlMetadata, GqlProperties},
    timeindex::GqlEventTime,
};
use raphtory::{
    db::api::{
        properties::dyn_props::DynProperties,
        view::{DynamicGraph, IntoDynamic},
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, NodeViewOps, TimeOps},
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
                IterKind::NeighboursList => {
                    let path = match stack.last().expect("non-empty stack") {
                        Value::Path(p) => p.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for node in path.iter() {
                        sink.begin_object();
                        stack.push(Value::Node(node));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
                IterKind::EdgesList => {
                    let edges = match stack.last().expect("non-empty stack") {
                        Value::Edges(e) => e.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for edge in edges.iter() {
                        sink.begin_object();
                        stack.push(Value::Edge(edge));
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
                        stack.push(Value::EventTime(GqlEventTime::from(et)));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
                IterKind::PropertiesValues(keys) => {
                    let props = match stack.last().expect("non-empty stack") {
                        Value::Properties(p) => p.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for prop in props.collect_values(keys.as_deref()) {
                        sink.begin_object();
                        stack.push(Value::Property(prop));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
                IterKind::MetadataValues(keys) => {
                    let meta = match stack.last().expect("non-empty stack") {
                        Value::Metadata(m) => m.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for prop in meta.collect_values(keys.as_deref()) {
                        sink.begin_object();
                        stack.push(Value::Property(prop));
                        for child in children.iter() {
                            exec(child, stack, sink);
                        }
                        stack.pop();
                        sink.end_object();
                    }
                }
                IterKind::TemporalValues(keys) => {
                    let temporal = match stack.last().expect("non-empty stack") {
                        Value::TemporalProperties(t) => t.clone(),
                        _ => unreachable!("plan/type mismatch"),
                    };
                    for tp in temporal.collect_values(keys.as_deref()) {
                        sink.begin_object();
                        stack.push(Value::TemporalProperty(tp));
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
            (Nav::Edges, Value::Graph(g)) => Some(Value::Edges(GqlEdges::new(g.edges()))),
            (Nav::Edge { src, dst }, Value::Graph(g)) => {
                g.edge(src, dst).map(|e| Value::Edge(GqlEdge::from(e)))
            }
            (Nav::Src, Value::Edge(e)) => Some(Value::Node(GqlNode::from(e.ee.src()))),
            (Nav::Dst, Value::Edge(e)) => Some(Value::Node(GqlNode::from(e.ee.dst()))),
            (Nav::History, Value::Node(n)) => {
                Some(Value::History(GqlHistory::from(n.vv.history())))
            }
            (Nav::History, Value::Edge(e)) => {
                Some(Value::History(GqlHistory::from(e.ee.history())))
            }
            (Nav::Neighbours, Value::Node(n)) => {
                Some(Value::Path(GqlPathFromNode::new(n.vv.neighbours())))
            }
            (Nav::History, Value::TemporalProperty(tp)) => {
                Some(Value::History(tp.history_handle()))
            }
            (Nav::Properties, Value::Node(n)) => {
                let dp: DynProperties = n.vv.properties().into();
                Some(Value::Properties(GqlProperties::from(dp)))
            }
            (Nav::Properties, Value::Edge(e)) => {
                Some(Value::Properties(GqlProperties::from(e.ee.properties())))
            }
            (Nav::Metadata, Value::Node(n)) => {
                Some(Value::Metadata(GqlMetadata::from(n.vv.metadata())))
            }
            (Nav::Metadata, Value::Edge(e)) => {
                Some(Value::Metadata(GqlMetadata::from(e.ee.metadata())))
            }
            (Nav::Temporal, Value::Properties(p)) => {
                Some(Value::TemporalProperties(p.temporal_view()))
            }
            (Nav::Timestamps, Value::History(h)) => {
                Some(Value::HistoryTimestamp(h.timestamps_view()))
            }
            (Nav::EventIds, Value::History(h)) => Some(Value::HistoryEventId(h.event_id_view())),
            (Nav::DateTimes(fmt), Value::History(h)) => Some(Value::HistoryDateTime(
                h.datetimes_view(fmt.as_ref().map(|s| s.to_string())),
            )),
            (Nav::Layer(name), Value::Graph(g)) => Some(Value::Graph(
                g.valid_layers(name.to_string()).into_dynamic(),
            )),
            (Nav::Layer(name), Value::Node(n)) => Some(Value::Node(GqlNode::from(
                n.vv.valid_layers(name.to_string()),
            ))),
            (Nav::Layer(name), Value::Edge(e)) => Some(Value::Edge(GqlEdge::from(
                e.ee.valid_layers(name.to_string()),
            ))),
            (Nav::After(t), Value::Node(n)) => Some(Value::Node(GqlNode::from(n.vv.after(*t)))),
            (Nav::After(t), Value::Edge(e)) => Some(Value::Edge(GqlEdge::from(e.ee.after(*t)))),
            (Nav::Before(t), Value::Node(n)) => Some(Value::Node(GqlNode::from(n.vv.before(*t)))),
            (Nav::Before(t), Value::Edge(e)) => Some(Value::Edge(GqlEdge::from(e.ee.before(*t)))),
            (Nav::Window { start, end }, Value::Graph(g)) => {
                Some(Value::Graph(g.window(*start, *end).into_dynamic()))
            }
            (Nav::Window { start, end }, Value::Node(n)) => {
                Some(Value::Node(GqlNode::from(n.vv.window(*start, *end))))
            }
            (Nav::Window { start, end }, Value::Edge(e)) => {
                Some(Value::Edge(GqlEdge::from(e.ee.window(*start, *end))))
            }
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

impl LeafKind {
    fn write(&self, recv: &Value, sink: &mut Sink) {
        match (self, recv) {
            (LeafKind::Id, Value::Node(n)) => write_gid(sink, n.vv.id()),
            (LeafKind::Name, Value::Node(n)) => sink.write_str(&n.vv.name()),
            (LeafKind::EdgeId, Value::Edge(e)) => {
                let (src, dst) = e.ee.id();
                sink.begin_array();
                write_gid(sink, src);
                write_gid(sink, dst);
                sink.end_array();
            }
            (LeafKind::Timestamp, Value::EventTime(t)) => match t.inner {
                Some(et) => sink.write_i64(et.t()),
                None => sink.write_null(),
            },
            (LeafKind::EventId, Value::EventTime(t)) => match t.inner {
                Some(et) => sink.write_u64(et.i() as u64),
                None => sink.write_null(),
            },
            (LeafKind::Key, Value::Property(p)) => sink.write_str(&p.key),
            (LeafKind::Key, Value::TemporalProperty(tp)) => sink.write_str(&tp.key),
            (LeafKind::AsString, Value::Property(p)) => sink.write_str(&p.prop.to_string()),
            (LeafKind::Value, Value::Property(p)) => {
                // Serialize the typed property value via the same path async-graphql
                // uses for the `PropertyOutput` scalar, so formatting matches exactly.
                // let json =
                //     serde_json::to_vec(&prop_to_gql(&p.prop)).unwrap_or_else(|_| b"null".to_vec());
                // serde_json::to_writer(sink, &prop_to_gql(&p.prop))
                //     .unwrap_or_else(|_| panic!("should not panic! :O"));
                // sink.write_raw_json(&json);
                sink.write_json(&prop_to_gql(&p.prop));
            }
            (LeafKind::TimestampList, Value::HistoryTimestamp(h)) => {
                sink.begin_array();
                for ts in h.iter_values() {
                    sink.write_i64(ts);
                }
                sink.end_array();
            }
            (LeafKind::EventIdList, Value::HistoryEventId(h)) => {
                sink.begin_array();
                for id in h.iter_values() {
                    sink.write_u64(id);
                }
                sink.end_array();
            }
            (LeafKind::DateTimeList, Value::HistoryDateTime(h)) => {
                sink.begin_array();
                for dt in h.iter_formatted() {
                    match dt {
                        Some(s) => sink.write_str(&s),
                        None => sink.write_null(),
                    }
                }
                sink.end_array();
            }
            (LeafKind::DateTime(fmt), Value::EventTime(t)) => match t.inner {
                None => sink.write_null(),
                Some(et) => match et.dt() {
                    Ok(dt) => sink.write_str(&dt.format(fmt).to_string()),
                    Err(_) => sink.write_null(),
                },
            },
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

/// Write a node id as the schema's `NodeId` scalar: a JSON string for
/// string-indexed graphs, a JSON number for integer-indexed graphs.
fn write_gid(sink: &mut Sink, gid: GID) {
    match gid {
        GID::Str(s) => sink.write_str(&s),
        GID::U64(u) => sink.write_u64(u),
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
