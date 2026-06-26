//! The push-based executor.
//!
//! Walks a [`Plan`] depth-first, pushing/popping typed [`Value`]s on a reused
//! stack and writing leaves straight into the [`Sink`]. The only growable
//! structure is the `Vec<Value>` stack (depth = query nesting, already capped by
//! `schema.max_query_depth`); nothing is allocated per result.

use super::{
    plan::{IterKind, LeafKind, Nav, Op, Plan, ViewKind},
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
    algorithms::components::{in_component, out_component},
    db::{
        api::{
            properties::dyn_props::DynProperties,
            view::{DynamicGraph, Filter, IntoDynamic},
        },
        graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
        },
    },
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, NodeStateOps, NodeViewOps, TimeOps},
};

/// `expect` message for a runtime filter/select failure. Filters are structurally
/// validated at plan time (`TryInto<Composite*Filter>`), so a failure here is the
/// Q-C "impossible mid-stream error" path: the panic drops the `Sink`, closing
/// the channel and truncating the (already-streaming) response.
const FILTER_FAILED: &str = "filter application failed at runtime (validated at plan time)";
use raphtory_api::core::{entities::GID, storage::timeindex::AsTime};

/// Clone the top-of-stack receiver out into a local (a cheap Arc bump) so the
/// item iterator borrows the local, not `stack` — leaving us free to push items
/// as we iterate. The variant is guaranteed by SDL validation + planning.
macro_rules! clone_top {
    ($stack:expr, $variant:path) => {
        match $stack.last().expect("non-empty stack") {
            $variant(v) => v.clone(),
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    };
}

/// Emit each item of `items` as a JSON object running `children` against it.
/// `items` must not borrow `stack` (clone the receiver out first via
/// [`clone_top!`]).
fn emit_items(
    items: impl Iterator<Item = Value>,
    children: &[Op],
    stack: &mut Vec<Value>,
    sink: &mut Sink,
) {
    for item in items {
        sink.begin_object();
        stack.push(item);
        for child in children.iter() {
            exec(child, stack, sink);
        }
        stack.pop();
        sink.end_object();
    }
}

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
            match iter {
                IterKind::NodesList => {
                    let nodes = clone_top!(stack, Value::Nodes);
                    emit_items(nodes.iter().map(Value::Node), children, stack, sink);
                }
                IterKind::NodesPage(p) => {
                    let nodes = clone_top!(stack, Value::Nodes);
                    let items = nodes.iter().map(Value::Node).skip(p.start()).take(p.limit);
                    emit_items(items, children, stack, sink);
                }
                IterKind::EdgesList => {
                    let edges = clone_top!(stack, Value::Edges);
                    emit_items(edges.iter().map(Value::Edge), children, stack, sink);
                }
                IterKind::EdgesPage(p) => {
                    let edges = clone_top!(stack, Value::Edges);
                    let items = edges.iter().map(Value::Edge).skip(p.start()).take(p.limit);
                    emit_items(items, children, stack, sink);
                }
                IterKind::NeighboursList => {
                    let path = clone_top!(stack, Value::Path);
                    emit_items(path.iter().map(Value::Node), children, stack, sink);
                }
                IterKind::NeighboursPage(p) => {
                    let path = clone_top!(stack, Value::Path);
                    let items = path.iter().map(Value::Node).skip(p.start()).take(p.limit);
                    emit_items(items, children, stack, sink);
                }
                IterKind::HistoryList => {
                    let history = clone_top!(stack, Value::History);
                    let items = history
                        .history
                        .iter()
                        .map(|et| Value::EventTime(GqlEventTime::from(et)));
                    emit_items(items, children, stack, sink);
                }
                IterKind::PropertiesValues(keys) => {
                    let props = clone_top!(stack, Value::Properties);
                    let items = props
                        .collect_values(keys.as_deref())
                        .into_iter()
                        .map(Value::Property);
                    emit_items(items, children, stack, sink);
                }
                IterKind::MetadataValues(keys) => {
                    let meta = clone_top!(stack, Value::Metadata);
                    let items = meta
                        .collect_values(keys.as_deref())
                        .into_iter()
                        .map(Value::Property);
                    emit_items(items, children, stack, sink);
                }
                IterKind::TemporalValues(keys) => {
                    let temporal = clone_top!(stack, Value::TemporalProperties);
                    let items = temporal
                        .collect_values(keys.as_deref())
                        .into_iter()
                        .map(Value::TemporalProperty);
                    emit_items(items, children, stack, sink);
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
            (Nav::Nodes(filt), Value::Graph(g)) => {
                Some(Value::Nodes(opt_select_nodes(GqlNodes::new(g.nodes()), filt)))
            }
            (Nav::Node(id), Value::Graph(g)) => g.node(id).map(|n| Value::Node(GqlNode::from(n))),
            (Nav::Edges(filt), Value::Graph(g)) => {
                Some(Value::Edges(opt_select_edges(GqlEdges::new(g.edges()), filt)))
            }
            (Nav::Edges(filt), Value::Node(n)) => {
                Some(Value::Edges(opt_select_edges(GqlEdges::new(n.vv.edges()), filt)))
            }
            (Nav::Edge { src, dst }, Value::Graph(g)) => {
                g.edge(src, dst).map(|e| Value::Edge(GqlEdge::from(e)))
            }
            (Nav::Src, Value::Edge(e)) => Some(Value::Node(GqlNode::from(e.ee.src()))),
            (Nav::Dst, Value::Edge(e)) => Some(Value::Node(GqlNode::from(e.ee.dst()))),
            (Nav::Nbr, Value::Edge(e)) => Some(Value::Node(GqlNode::from(e.ee.nbr()))),
            (Nav::Explode, Value::Edge(e)) => Some(Value::Edges(GqlEdges::new(e.ee.explode()))),
            (Nav::ExplodeLayers, Value::Edge(e)) => {
                Some(Value::Edges(GqlEdges::new(e.ee.explode_layers())))
            }
            (Nav::Deletions, Value::Edge(e)) => {
                Some(Value::History(GqlHistory::from(e.ee.deletions())))
            }
            (Nav::InEdges(filt), Value::Node(n)) => {
                Some(Value::Edges(opt_select_edges(GqlEdges::new(n.vv.in_edges()), filt)))
            }
            (Nav::OutEdges(filt), Value::Node(n)) => {
                Some(Value::Edges(opt_select_edges(GqlEdges::new(n.vv.out_edges()), filt)))
            }
            (Nav::Neighbours(filt), Value::Node(n)) => Some(Value::Path(opt_select_path(
                GqlPathFromNode::new(n.vv.neighbours()),
                filt,
            ))),
            (Nav::InNeighbours(filt), Value::Node(n)) => Some(Value::Path(opt_select_path(
                GqlPathFromNode::new(n.vv.in_neighbours()),
                filt,
            ))),
            (Nav::OutNeighbours(filt), Value::Node(n)) => Some(Value::Path(opt_select_path(
                GqlPathFromNode::new(n.vv.out_neighbours()),
                filt,
            ))),
            (Nav::InComponent, Value::Node(n)) => {
                Some(Value::Nodes(GqlNodes::new(in_component(n.vv.clone()).nodes())))
            }
            (Nav::OutComponent, Value::Node(n)) => {
                Some(Value::Nodes(GqlNodes::new(out_component(n.vv.clone()).nodes())))
            }
            // Filters pushed into raphtory (parsed + structurally validated at plan time).
            (Nav::FilterNodes(f), Value::Graph(g)) => Some(Value::Graph(
                g.filter(f.clone()).expect(FILTER_FAILED).into_dynamic(),
            )),
            (Nav::FilterEdges(f), Value::Graph(g)) => Some(Value::Graph(
                g.filter(f.clone()).expect(FILTER_FAILED).into_dynamic(),
            )),
            (Nav::ApplyNodeFilter { filter, .. }, Value::Node(n)) => {
                Some(Value::Node(n.filter_view(filter.clone()).expect(FILTER_FAILED)))
            }
            (Nav::ApplyNodeFilter { filter, select }, Value::Nodes(ns)) => {
                Some(Value::Nodes(apply_node_filter(ns, filter, *select)))
            }
            (Nav::ApplyNodeFilter { filter, select }, Value::Path(p)) => {
                let out = if *select {
                    p.select_view(filter.clone())
                } else {
                    p.filter_view(filter.clone())
                };
                Some(Value::Path(out.expect(FILTER_FAILED)))
            }
            (Nav::ApplyEdgeFilter { filter, select }, Value::Edges(es)) => {
                let out = if *select {
                    es.select_view(filter.clone())
                } else {
                    es.filter_view(filter.clone())
                };
                Some(Value::Edges(out.expect(FILTER_FAILED)))
            }
            (Nav::EarliestTime, Value::Graph(g)) => Some(Value::EventTime(g.earliest_time().into())),
            (Nav::EarliestTime, Value::Node(n)) => {
                Some(Value::EventTime(n.vv.earliest_time().into()))
            }
            (Nav::EarliestTime, Value::Edge(e)) => {
                Some(Value::EventTime(e.ee.earliest_time().into()))
            }
            (Nav::LatestTime, Value::Graph(g)) => Some(Value::EventTime(g.latest_time().into())),
            (Nav::LatestTime, Value::Node(n)) => Some(Value::EventTime(n.vv.latest_time().into())),
            (Nav::LatestTime, Value::Edge(e)) => Some(Value::EventTime(e.ee.latest_time().into())),
            (Nav::Start, Value::Graph(g)) => Some(Value::EventTime(g.start().into())),
            (Nav::Start, Value::Node(n)) => Some(Value::EventTime(n.vv.start().into())),
            (Nav::Start, Value::Edge(e)) => Some(Value::EventTime(e.ee.start().into())),
            (Nav::End, Value::Graph(g)) => Some(Value::EventTime(g.end().into())),
            (Nav::End, Value::Node(n)) => Some(Value::EventTime(n.vv.end().into())),
            (Nav::End, Value::Edge(e)) => Some(Value::EventTime(e.ee.end().into())),
            (Nav::FirstUpdate, Value::Node(n)) => {
                Some(Value::EventTime(n.vv.history().earliest_time().into()))
            }
            (Nav::FirstUpdate, Value::Edge(e)) => {
                Some(Value::EventTime(e.ee.history().earliest_time().into()))
            }
            (Nav::LastUpdate, Value::Node(n)) => {
                Some(Value::EventTime(n.vv.history().latest_time().into()))
            }
            (Nav::LastUpdate, Value::Edge(e)) => {
                Some(Value::EventTime(e.ee.history().latest_time().into()))
            }
            (Nav::History, Value::Node(n)) => {
                Some(Value::History(GqlHistory::from(n.vv.history())))
            }
            (Nav::History, Value::Edge(e)) => {
                Some(Value::History(GqlHistory::from(e.ee.history())))
            }
            (Nav::SortedNodes(sort_bys), Value::Nodes(n)) => {
                Some(Value::Nodes(n.sorted_view(sort_bys.clone())))
            }
            (Nav::SortedEdges(sort_bys), Value::Edges(e)) => {
                Some(Value::Edges(e.sorted_view(sort_bys.clone())))
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
            // Same-type view transforms — dispatched per receiver type.
            (Nav::View(vk), Value::Graph(g)) => Some(Value::Graph(view_graph(g, vk))),
            (Nav::View(vk), Value::Node(n)) => Some(Value::Node(view_node(n, vk))),
            (Nav::View(vk), Value::Edge(e)) => Some(Value::Edge(view_edge(e, vk))),
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

impl LeafKind {
    fn write(&self, recv: &Value, sink: &mut Sink) {
        match (self, recv) {
            (LeafKind::Count, Value::Nodes(n)) => sink.write_u64(n.nn.len() as u64),
            (LeafKind::Count, Value::Edges(e)) => sink.write_u64(e.ee.len() as u64),
            (LeafKind::Count, Value::Path(p)) => sink.write_u64(p.nn.len() as u64),
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
            (LeafKind::NodeType, Value::Node(n)) => match n.vv.node_type() {
                Some(t) => sink.write_str(&t.to_string()),
                None => sink.write_null(),
            },
            (LeafKind::Degree, Value::Node(n)) => sink.write_u64(n.vv.degree() as u64),
            (LeafKind::InDegree, Value::Node(n)) => sink.write_u64(n.vv.in_degree() as u64),
            (LeafKind::OutDegree, Value::Node(n)) => sink.write_u64(n.vv.out_degree() as u64),
            (LeafKind::EdgeHistoryCount, Value::Node(n)) => {
                sink.write_u64(n.vv.edge_history_count() as u64)
            }
            (LeafKind::IsActive, Value::Node(n)) => sink.write_bool(n.vv.is_active()),
            (LeafKind::IsActive, Value::Edge(e)) => sink.write_bool(e.ee.is_active()),
            (LeafKind::IsValid, Value::Edge(e)) => sink.write_bool(e.ee.is_valid()),
            (LeafKind::IsDeleted, Value::Edge(e)) => sink.write_bool(e.ee.is_deleted()),
            (LeafKind::IsSelfLoop, Value::Edge(e)) => sink.write_bool(e.ee.is_self_loop()),
            (LeafKind::LayerNames, Value::Edge(e)) => {
                sink.begin_array();
                for name in e.ee.layer_names() {
                    sink.write_str(&name.to_string());
                }
                sink.end_array();
            }
            (LeafKind::UniqueLayers, Value::Graph(g)) => {
                sink.begin_array();
                for name in g.unique_layers() {
                    sink.write_str(&name.to_string());
                }
                sink.end_array();
            }
            (LeafKind::CountNodes, Value::Graph(g)) => sink.write_u64(g.count_nodes() as u64),
            (LeafKind::CountEdges, Value::Graph(g)) => sink.write_u64(g.count_edges() as u64),
            (LeafKind::CountTemporalEdges, Value::Graph(g)) => {
                sink.write_u64(g.count_temporal_edges() as u64)
            }
            (LeafKind::HasNode(id), Value::Graph(g)) => sink.write_bool(g.has_node(id.clone())),
            (LeafKind::HasEdge { src, dst, layer }, Value::Graph(g)) => {
                let exists = match layer {
                    Some(name) => g
                        .layers(name.to_string())
                        .map(|l| l.has_edge(src.clone(), dst.clone()))
                        .unwrap_or(false),
                    None => g.has_edge(src.clone(), dst.clone()),
                };
                sink.write_bool(exists);
            }
            _ => unreachable!("plan/type mismatch — validation should prevent this"),
        }
    }
}

/// Apply a same-type view transform to a graph. Mirrors `GqlGraph`'s resolvers;
/// every result is re-wrapped as a `DynamicGraph`.
fn view_graph(g: &DynamicGraph, vk: &ViewKind) -> DynamicGraph {
    match vk {
        ViewKind::Window { start, end } => g.window(*start, *end).into_dynamic(),
        ViewKind::At(t) => g.at(*t).into_dynamic(),
        ViewKind::Before(t) => g.before(*t).into_dynamic(),
        ViewKind::After(t) => g.after(*t).into_dynamic(),
        ViewKind::Latest => g.latest().into_dynamic(),
        ViewKind::SnapshotAt(t) => g.snapshot_at(*t).into_dynamic(),
        ViewKind::SnapshotLatest => g.snapshot_latest().into_dynamic(),
        ViewKind::ShrinkWindow { start, end } => g.shrink_window(*start, *end).into_dynamic(),
        ViewKind::ShrinkStart(t) => g.shrink_start(*t).into_dynamic(),
        ViewKind::ShrinkEnd(t) => g.shrink_end(*t).into_dynamic(),
        ViewKind::DefaultLayer => g.default_layer().into_dynamic(),
        ViewKind::Layer(name) => g.valid_layers(name.to_string()).into_dynamic(),
        ViewKind::Layers(names) => g.valid_layers(names.to_vec()).into_dynamic(),
        ViewKind::ExcludeLayer(name) => g.exclude_valid_layers(name.to_string()).into_dynamic(),
        ViewKind::ExcludeLayers(names) => g.exclude_valid_layers(names.to_vec()).into_dynamic(),
        ViewKind::Valid => g.valid().into_dynamic(),
        ViewKind::Subgraph(nodes) => g.subgraph(nodes.clone()).into_dynamic(),
        ViewKind::SubgraphNodeTypes(types) => g.subgraph_node_types(types.to_vec()).into_dynamic(),
        ViewKind::ExcludeNodes(nodes) => g.exclude_nodes(nodes.clone()).into_dynamic(),
    }
}

/// Apply a same-type view transform to a node. Graph-only variants are
/// `unreachable!` — the planner never emits them for a `Node`.
fn view_node(n: &GqlNode, vk: &ViewKind) -> GqlNode {
    match vk {
        ViewKind::Window { start, end } => GqlNode::from(n.vv.window(*start, *end)),
        ViewKind::At(t) => GqlNode::from(n.vv.at(*t)),
        ViewKind::Before(t) => GqlNode::from(n.vv.before(*t)),
        ViewKind::After(t) => GqlNode::from(n.vv.after(*t)),
        ViewKind::Latest => GqlNode::from(n.vv.latest()),
        ViewKind::SnapshotAt(t) => GqlNode::from(n.vv.snapshot_at(*t)),
        ViewKind::SnapshotLatest => GqlNode::from(n.vv.snapshot_latest()),
        ViewKind::ShrinkWindow { start, end } => GqlNode::from(n.vv.shrink_window(*start, *end)),
        ViewKind::ShrinkStart(t) => GqlNode::from(n.vv.shrink_start(*t)),
        ViewKind::ShrinkEnd(t) => GqlNode::from(n.vv.shrink_end(*t)),
        ViewKind::DefaultLayer => GqlNode::from(n.vv.default_layer()),
        ViewKind::Layer(name) => GqlNode::from(n.vv.valid_layers(name.to_string())),
        ViewKind::Layers(names) => GqlNode::from(n.vv.valid_layers(names.to_vec())),
        ViewKind::ExcludeLayer(name) => GqlNode::from(n.vv.exclude_valid_layers(name.to_string())),
        ViewKind::ExcludeLayers(names) => GqlNode::from(n.vv.exclude_valid_layers(names.to_vec())),
        _ => unreachable!("not a node view op — validation should prevent this"),
    }
}

/// Apply a same-type view transform to an edge. Graph-only variants are
/// `unreachable!` — the planner never emits them for an `Edge`.
fn view_edge(e: &GqlEdge, vk: &ViewKind) -> GqlEdge {
    match vk {
        ViewKind::Window { start, end } => GqlEdge::from(e.ee.window(*start, *end)),
        ViewKind::At(t) => GqlEdge::from(e.ee.at(*t)),
        ViewKind::Before(t) => GqlEdge::from(e.ee.before(*t)),
        ViewKind::After(t) => GqlEdge::from(e.ee.after(*t)),
        ViewKind::Latest => GqlEdge::from(e.ee.latest()),
        ViewKind::SnapshotAt(t) => GqlEdge::from(e.ee.snapshot_at(*t)),
        ViewKind::SnapshotLatest => GqlEdge::from(e.ee.snapshot_latest()),
        ViewKind::ShrinkWindow { start, end } => GqlEdge::from(e.ee.shrink_window(*start, *end)),
        ViewKind::ShrinkStart(t) => GqlEdge::from(e.ee.shrink_start(*t)),
        ViewKind::ShrinkEnd(t) => GqlEdge::from(e.ee.shrink_end(*t)),
        ViewKind::DefaultLayer => GqlEdge::from(e.ee.default_layer()),
        ViewKind::Layer(name) => GqlEdge::from(e.ee.valid_layers(name.to_string())),
        ViewKind::Layers(names) => GqlEdge::from(e.ee.valid_layers(names.to_vec())),
        ViewKind::ExcludeLayer(name) => GqlEdge::from(e.ee.exclude_valid_layers(name.to_string())),
        ViewKind::ExcludeLayers(names) => GqlEdge::from(e.ee.exclude_valid_layers(names.to_vec())),
        _ => unreachable!("not an edge view op — validation should prevent this"),
    }
}

/// Apply an optional `select:` filter (pushed into raphtory) to a freshly
/// produced node collection.
fn opt_select_nodes(nodes: GqlNodes, filt: &Option<CompositeNodeFilter>) -> GqlNodes {
    match filt {
        Some(f) => nodes.select_view(f.clone()).expect(FILTER_FAILED),
        None => nodes,
    }
}

fn opt_select_edges(edges: GqlEdges, filt: &Option<CompositeEdgeFilter>) -> GqlEdges {
    match filt {
        Some(f) => edges.select_view(f.clone()).expect(FILTER_FAILED),
        None => edges,
    }
}

fn opt_select_path(path: GqlPathFromNode, filt: &Option<CompositeNodeFilter>) -> GqlPathFromNode {
    match filt {
        Some(f) => path.select_view(f.clone()).expect(FILTER_FAILED),
        None => path,
    }
}

/// `nodes.filter(expr:)` (sticky) or `nodes.select(expr:)` (one-hop).
fn apply_node_filter(ns: &GqlNodes, filter: &CompositeNodeFilter, select: bool) -> GqlNodes {
    let out = if select {
        ns.select_view(filter.clone())
    } else {
        ns.filter_view(filter.clone())
    };
    out.expect(FILTER_FAILED)
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
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
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
