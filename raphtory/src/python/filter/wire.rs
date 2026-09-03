//! Wire-side recording for python filter construction.
//!
//! A python filter object does two jobs: run locally (the compiled
//! expression) and travel to a server (the GraphQL wire JSON). Expressions
//! cannot be faithfully reverse-engineered into JSON, so each construction
//! step records its wire fragment alongside the expression it builds. The
//! remote client reads the finished [`FilterTree`]; constructions with no
//! wire equivalent (an expression on both sides of a comparison) simply
//! carry no tree and are rejected at the remote boundary with a clear error.

use crate::db::graph::views::filter::model::{
    degree_filter::DegreeFilter,
    edge_filter::{CompositeEdgeFilter, EdgeFilter, Endpoint},
    exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
    filter::{FieldFilterValue, Filter},
    latest_filter::Latest,
    layered_filter::Layered,
    node_filter::{CompositeNodeFilter, NodeFilter},
    property_filter::{Op, PropertyFilter, PropertyFilterValue, PropertyRef},
    snapshot_filter::{SnapshotAt, SnapshotLatest},
    windowed_filter::Windowed,
    FilterOperator, FilterTree,
};
use raphtory_api::core::{storage::timeindex::EventTime, Direction};

/// Which entity's tree a finished predicate belongs to.
#[derive(Clone, Copy, Debug)]
pub(crate) enum WireEntity {
    Node,
    Edge,
    ExplodedEdge,
}

/// A view restriction recorded from a factory chain.
#[derive(Clone, Debug)]
pub(crate) enum WireView {
    Window(EventTime, EventTime),
    Latest,
    SnapshotAt(EventTime),
    SnapshotLatest,
    Layers(Vec<String>),
}

/// What the value expression selects, before its predicate.
#[derive(Clone, Debug)]
pub(crate) enum WireTarget {
    /// A built-in field (node id / name / type), by its wire field name.
    Field(&'static str),
    /// A property, metadata or temporal-property column.
    Prop(PropertyRef),
    /// A node degree in a direction.
    Degree(Direction),
}

/// The recorded lhs of a comparison: entity + target + op chain + views, and
/// an optional endpoint when the chain went through `src()`/`dst()`.
#[derive(Clone, Debug)]
pub(crate) struct WireLhs {
    pub entity: WireEntity,
    pub endpoint: Option<Endpoint>,
    pub target: WireTarget,
    pub ops: Vec<Op>,
    pub views: Vec<WireView>,
}

impl WireLhs {
    pub(crate) fn with_op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    /// `.temporal()` switches a property target to its temporal column.
    pub(crate) fn temporal(mut self) -> Option<Self> {
        match self.target {
            WireTarget::Prop(PropertyRef::Property(name)) => {
                self.target = WireTarget::Prop(PropertyRef::TemporalProperty(name));
                Some(self)
            }
            _ => None,
        }
    }

    /// Finish the lhs with a predicate, producing the wire tree.
    pub(crate) fn finish(self, operator: FilterOperator, value: WireValue) -> Option<FilterTree> {
        let node_leaf = |lhs: &WireLhs| -> Option<CompositeNodeFilter> {
            Some(match (&lhs.target, &value) {
                (WireTarget::Field(name), WireValue::Field(v)) => {
                    CompositeNodeFilter::Node(Filter {
                        field_name: name.to_string(),
                        field_value: v.clone(),
                        operator: operator.clone(),
                    })
                }
                (WireTarget::Prop(prop_ref), WireValue::Prop(v)) => {
                    CompositeNodeFilter::Property(PropertyFilter {
                        prop_ref: prop_ref.clone(),
                        prop_value: v.clone(),
                        operator: operator.clone(),
                        ops: lhs.ops.clone(),
                        entity: NodeFilter,
                    })
                }
                (WireTarget::Degree(dir), WireValue::Prop(v)) => {
                    if !lhs.ops.is_empty() {
                        return None;
                    }
                    CompositeNodeFilter::Degree(DegreeFilter {
                        direction: *dir,
                        operator: operator.clone(),
                        value: v.clone(),
                        ops: Vec::new(),
                    })
                }
                _ => return None,
            })
        };

        match (self.entity, &self.endpoint) {
            (WireEntity::Node, Some(_)) => None,
            (WireEntity::Node, None) => {
                let leaf = node_leaf(&self)?;
                Some(FilterTree::Node(wrap_node_views(leaf, &self.views)))
            }
            (WireEntity::Edge, Some(endpoint)) => {
                let leaf = node_leaf(&self)?;
                let ep = match endpoint {
                    Endpoint::Src => CompositeEdgeFilter::Src(leaf),
                    Endpoint::Dst => CompositeEdgeFilter::Dst(leaf),
                };
                Some(FilterTree::Edge(wrap_edge_views(ep, &self.views)))
            }
            (WireEntity::Edge, None) => {
                let leaf = match (&self.target, &value) {
                    (WireTarget::Prop(prop_ref), WireValue::Prop(v)) => {
                        CompositeEdgeFilter::Property(PropertyFilter {
                            prop_ref: prop_ref.clone(),
                            prop_value: v.clone(),
                            operator,
                            ops: self.ops.clone(),
                            entity: EdgeFilter,
                        })
                    }
                    _ => return None,
                };
                Some(FilterTree::Edge(wrap_edge_views(leaf, &self.views)))
            }
            (WireEntity::ExplodedEdge, endpoint) => {
                let leaf = match (endpoint, &self.target, &value) {
                    (Some(ep), _, _) => {
                        let n = node_leaf(&self)?;
                        match ep {
                            Endpoint::Src => CompositeExplodedEdgeFilter::Src(n),
                            Endpoint::Dst => CompositeExplodedEdgeFilter::Dst(n),
                        }
                    }
                    (None, WireTarget::Prop(prop_ref), WireValue::Prop(v)) => {
                        CompositeExplodedEdgeFilter::Property(PropertyFilter {
                            prop_ref: prop_ref.clone(),
                            prop_value: v.clone(),
                            operator,
                            ops: self.ops.clone(),
                            entity: ExplodedEdgeFilter,
                        })
                    }
                    _ => return None,
                };
                Some(FilterTree::ExplodedEdge(wrap_exploded_views(
                    leaf,
                    &self.views,
                )))
            }
        }
    }
}

/// The rhs of a wire predicate.
#[derive(Clone, Debug)]
pub(crate) enum WireValue {
    Field(FieldFilterValue),
    Prop(PropertyFilterValue),
}

macro_rules! wrap_views_fn {
    ($name:ident, $composite:ident) => {
        pub(crate) fn $name(leaf: $composite, views: &[WireView]) -> $composite {
            // Views recorded factory-first wrap outside-in: the first view a
            // user applied is the outermost restriction.
            views.iter().rev().fold(leaf, |acc, view| match view {
                WireView::Window(start, end) => {
                    $composite::Windowed(Box::new(Windowed::new(*start, *end, acc)))
                }
                WireView::Latest => $composite::Latest(Box::new(Latest::new(acc))),
                WireView::SnapshotAt(t) => {
                    $composite::SnapshotAt(Box::new(SnapshotAt::new(*t, acc)))
                }
                WireView::SnapshotLatest => {
                    $composite::SnapshotLatest(Box::new(SnapshotLatest::new(acc)))
                }
                WireView::Layers(names) => {
                    $composite::Layered(Box::new(Layered::from_layers(names.clone(), acc)))
                }
            })
        }
    };
}

wrap_views_fn!(wrap_node_views, CompositeNodeFilter);
wrap_views_fn!(wrap_edge_views, CompositeEdgeFilter);
wrap_views_fn!(wrap_exploded_views, CompositeExplodedEdgeFilter);
