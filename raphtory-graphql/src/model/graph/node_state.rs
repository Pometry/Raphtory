use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        node::GqlNode,
        node_id::GqlNodeId,
        nodes::GqlNodes,
        property::GqlPropertyOutputVal,
    },
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result, SimpleObject, Union};
use raphtory::{
    db::{
        api::{
            state::{
                GenericNodeState, Index, NodeStateOutput, NodeStateValue, OutputTypedNodeState,
                PropMap, TypedNodeState,
            },
            view::{BoxableGraphView, DynamicGraph},
        },
        graph::node::NodeView,
    },
    prelude::{NodeStateOps, Prop},
};
use raphtory_api::core::entities::{properties::prop::PropUnwrap, VID};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// A mapping from the nodes of a graph to the values computed for them by an algorithm.
///
/// The output is columnar: every column of the underlying node state is exposed as
/// a `NodeStateColumn` whose `values` are row-aligned with `nodes`.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "NodeState")]
pub struct GqlNodeState {
    pub(crate) node_state: OutputTypedNodeState<'static, DynamicGraph>,
}

impl<V, T> From<TypedNodeState<'static, V, DynamicGraph, T>> for GqlNodeState
where
    V: NodeStateValue + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn from(node_state: TypedNodeState<'static, V, DynamicGraph, T>) -> Self {
        Self {
            node_state: node_state.to_output_nodestate(),
        }
    }
}

/// A single cell of a node state column: either a plain property value, a
/// node, or a collection of nodes.
#[derive(Union, Clone)]
#[graphql(name = "NodeStateValue")]
pub(crate) enum GqlNodeStateValue {
    Prop(GqlNodeStateProp),
    Node(GqlNode),
    Nodes(GqlNodes),
}

/// A plain property value of a node state cell.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateProp")]
pub struct GqlNodeStateProp {
    /// The property value; null if the node has no value in this column.
    prop: Option<GqlPropertyOutputVal>,
}

impl From<NodeStateOutput<'static, Arc<dyn BoxableGraphView>>> for GqlNodeStateValue {
    fn from(value: NodeStateOutput<'static, Arc<dyn BoxableGraphView>>) -> Self {
        match value {
            NodeStateOutput::Prop(prop) => GqlNodeStateValue::Prop(GqlNodeStateProp {
                prop: prop.map(|p| GqlPropertyOutputVal(p.into())),
            }),
            NodeStateOutput::Node(node) => GqlNodeStateValue::Node(node.into()),
            NodeStateOutput::Nodes(nodes) => GqlNodeStateValue::Nodes(GqlNodes::new(nodes)),
        }
    }
}

/// One column of a node state: the values of a single output field of the
/// algorithm. Row-aligned with `NodeState.nodes`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateColumn")]
pub struct GqlNodeStateColumn {
    /// Name of the column.
    name: String,
    /// The values of this column; `values[i]` belongs to `NodeState.nodes[i]`.
    values: Vec<GqlNodeStateValue>,
}

/// One column's value for a single node.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateEntry")]
pub struct GqlNodeStateEntry {
    /// Name of the column.
    column_name: String,
    /// The node's value in this column.
    value: GqlNodeStateValue,
}

/// A `(node, value)` pair, e.g. the result of a column aggregate.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateItem")]
pub struct GqlNodeStateItem {
    /// The node.
    node: GqlNode,
    /// The node's value.
    value: GqlPropertyOutputVal,
}

/// A node's full row in the node state: one entry per column.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateRow")]
pub struct GqlNodeStateRow {
    /// The node this row belongs to.
    node: GqlNode,
    /// The row's values, one entry per column.
    entries: Vec<GqlNodeStateEntry>,
}

/// The nodes sharing one value of a column, as returned by `groupBy`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateGroup")]
pub struct GqlNodeStateGroup {
    /// The value shared by the nodes in this group; null if their cell is empty.
    value: Option<GqlPropertyOutputVal>,
    /// The nodes holding that value.
    nodes: GqlNodes,
}

/// A node's full row in the node state without the column names: `values[i]`
/// belongs to the column `NodeState.columnNames[i]`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateHeadlessRow")]
pub struct GqlNodeStateHeadlessRow {
    /// The node this row belongs to.
    node: GqlNode,
    /// The row's values, in `columnNames` order.
    values: Vec<GqlNodeStateValue>,
}

/// Function for total order over rows by `column`'s value: empty cells always lose.
/// `nulls_last` puts them last (for min); `false` puts them first (for max) and incomparable pairs tie.
fn column_cmp<'a>(
    column: &'a str,
    nulls_last: bool,
) -> impl Fn(&PropMap, &PropMap) -> Ordering + Sync + 'a {
    move |a, b| {
        let a = a.get(column).and_then(|v| v.as_ref());
        let b = b.get(column).and_then(|v| v.as_ref());
        match (a, b) {
            (Some(a), Some(b)) => a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => {
                if nulls_last {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Some(_), None) => {
                if nulls_last {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

/// A column value used as a `group_by` key. `Prop` is only `PartialEq`, so both
/// equality and hashing go through its debug representation, which distinguishes
/// variants as well as values. Computed once per row rather than per comparison.
/// Note that this groups `NaN` with itself, unlike `PartialEq` on floats.
#[derive(Clone, Debug)]
struct GroupKey {
    value: Option<Prop>,
    repr: String,
}

impl GroupKey {
    fn new(value: Option<Prop>) -> Self {
        let repr = format!("{value:?}");
        Self { value, repr }
    }
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.repr == other.repr
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.repr.hash(state);
    }
}

impl GqlNodeState {
    /// A slice of `limit` rows starting at `start`, as a node state of its own.
    fn slice(&self, start: usize, limit: usize) -> GqlNodeState {
        let state = &self.node_state.state;
        let start = start.min(self.node_state.len());
        let limit = limit.min(self.node_state.len() - start);
        let values = state.values_ref().slice(start, limit);
        let keys: Index<VID> = state.keys_ref().iter().skip(start).take(limit).collect();
        GqlNodeState {
            node_state: GenericNodeState::new(
                state.base_graph.clone(),
                values,
                keys,
                Some(state.node_cols.clone()),
            )
            .to_output_nodestate(),
        }
    }

    /// Whether `column` exists and holds plain property values rather than nodes.
    fn is_prop_column(&self, column: &str) -> bool {
        !self.node_state.state.node_cols.contains_key(column)
            && self
                .node_state
                .state
                .values_ref()
                .schema()
                .index_of(column)
                .is_ok()
    }

    /// Iterator over the non-empty values of a plain-prop column.
    /// None if the column does not exist or contains nodes.
    fn column_value_iter<'a>(&'a self, column: &'a str) -> Option<impl Iterator<Item = Prop> + 'a> {
        self.is_prop_column(column).then(|| {
            self.node_state
                .iter()
                .filter_map(move |(_, mut row)| Some(row.swap_remove(column)??.into()))
        })
    }

    /// Checks that `column` is a plain-prop column with at least one non-empty, comparable value;
    /// used as the guard for the `*_item_by` aggregates. None if not comparable.
    fn check_comparable(&self, column: &str) -> Option<()> {
        self.column_value_iter(column)?
            .next()
            .filter(|first| first.dtype().has_cmp())
            .map(|_| ())
    }

    /// Wraps a `(node, row)` item into the output item; None if the cell is empty or nonexistent.
    fn item_from_row(
        &self,
        column: &str,
        item: (NodeView<'_, &DynamicGraph>, PropMap),
    ) -> Option<GqlNodeStateItem> {
        let (node, mut row) = item;
        let value: Prop = row.swap_remove(column)??.into();
        Some(GqlNodeStateItem {
            node: node.cloned().into(),
            value: GqlPropertyOutputVal(value),
        })
    }
}

// Paging: `page` is the bounded reader; `rows`/`headlessRows`/`columns` are the
// unbounded bulk endpoints and are gated by `check_list_allowed` like every
// other `list`-shaped field. Columnar paging is tracked in #2722.

// Not exposed: `merge` (takes a second NodeState, which cannot be a query
// argument) — see #2722 for the remote NodeState subsystem.
// and `to_parquet`/`from_parquet` (avoid server-side filesystem access).
#[ResolvedObjectFields]
impl GqlNodeState {
    /// Returns the number of nodes with a value in this node state.
    pub async fn count(&self) -> usize {
        self.node_state.len()
    }

    /// The nodes with a value in this node state, in row order. Aligned with `values`.
    pub async fn nodes(&self) -> GqlNodes {
        GqlNodes::new(self.node_state.nodes())
    }

    /// The column names of this node state in order.
    pub async fn column_names(&self) -> Vec<String> {
        self.node_state
            .state
            .values_ref()
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    /// All rows of the node state keyed by node, with one entry per column.
    /// Unbounded: honours the same list guard as the other bulk endpoints, so
    /// `disable_lists` cannot be bypassed through node state. Use `page` when
    /// lists are disabled.
    pub async fn rows(&self, ctx: &Context<'_>) -> Result<Vec<GqlNodeStateRow>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            self_clone
                .node_state
                .iter()
                .map(|(node, row)| GqlNodeStateRow {
                    node: node.cloned().into(),
                    entries: self_clone
                        .node_state
                        .convert(row)
                        .into_iter()
                        .map(|(name, value)| GqlNodeStateEntry {
                            column_name: name,
                            value: value.into(),
                        })
                        .collect(),
                })
                .collect()
        })
        .await)
    }

    /// All rows of the node state keyed by node, without the column names: the `values` of each row are
    /// in `columnNames` order.
    pub async fn headless_rows(&self, ctx: &Context<'_>) -> Result<Vec<GqlNodeStateHeadlessRow>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            self_clone
                .node_state
                .iter()
                .map(|(node, row)| GqlNodeStateHeadlessRow {
                    node: node.cloned().into(),
                    values: self_clone
                        .node_state
                        .convert(row)
                        .into_iter()
                        .map(|(_, value)| value.into())
                        .collect(),
                })
                .collect()
        })
        .await)
    }

    /// Returns the values for a node, one entry per column; null if the node has no value in this NodeState.
    pub async fn get(
        &self,
        #[graphql(desc = "Node id.")] node: GqlNodeId,
    ) -> Option<Vec<GqlNodeStateEntry>> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let row = self_clone.node_state.get_by_node(node)?;
            let transformed = self_clone.node_state.convert(row);
            Some(
                transformed
                    .into_iter()
                    .map(|(name, value)| GqlNodeStateEntry {
                        column_name: name,
                        value: value.into(),
                    })
                    .collect(),
            )
        })
        .await
    }

    /// Minimum `(node, value)` of a column. Null if the column does not exist, is empty,
    /// or its values are not comparable (e.g. contains nodes).
    pub async fn min(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            let item = self_clone
                .node_state
                .min_item_by(column_cmp(&column, true))?;
            self_clone.item_from_row(&column, item)
        })
        .await
    }

    /// Maximum `(node, value)` of a column. Null if the column does not exist, is empty,
    /// or its values are not comparable (e.g. contains nodes).
    pub async fn max(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            let item = self_clone
                .node_state
                .max_item_by(column_cmp(&column, false))?;
            self_clone.item_from_row(&column, item)
        })
        .await
    }

    /// Sum of a column's values, skipping empty cells. Null if the column does not exist, is empty,
    /// or is not additive (e.g. contains nodes).
    pub async fn sum(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let mut values = self_clone.column_value_iter(&column)?;
            let mut acc = values.next()?;
            if !acc.dtype().has_add() {
                return None;
            }
            for value in values {
                acc = acc.add(value)?;
            }
            Some(GqlPropertyOutputVal(acc))
        })
        .await
    }

    /// Mean of a column's values as a float, skipping empty cells. Null if the column does not exist,
    /// is empty, or has any non-numeric value.
    pub async fn mean(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlPropertyOutputVal> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let mut values = self_clone.column_value_iter(&column)?;
            let mut sum = values.next()?.as_f64()?;
            let mut count = 1usize;
            for value in values {
                sum += value.as_f64()?;
                count += 1;
            }
            Some(GqlPropertyOutputVal(Prop::F64(sum / count as f64)))
        })
        .await
    }

    /// Median `(node, value)` of a column (upper median on even lengths). Null if the column
    /// does not exist, is empty, or is not comparable (e.g. contains nodes).
    pub async fn median(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<GqlNodeStateItem> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            let item = self_clone
                .node_state
                .median_item_by(column_cmp(&column, true))?;
            self_clone.item_from_row(&column, item)
        })
        .await
    }

    /// Returns the `k` rows with the largest values in a column. Empty cells rank
    /// lowest, so they are only included if fewer than `k` rows have a value.
    /// Null if the column does not exist, is empty, or is not comparable.
    pub async fn top_k(
        &self,
        #[graphql(desc = "Column name.")] column: String,
        #[graphql(desc = "Number of rows to return.")] k: usize,
    ) -> Option<GqlNodeState> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            Some(GqlNodeState {
                node_state: self_clone
                    .node_state
                    .top_k_by(column_cmp(&column, false), k),
            })
        })
        .await
    }

    /// Returns the `k` rows with the smallest values in a column. Empty cells rank
    /// highest, so they are only included if fewer than `k` rows have a value.
    /// Null if the column does not exist, is empty, or is not comparable.
    pub async fn bottom_k(
        &self,
        #[graphql(desc = "Column name.")] column: String,
        #[graphql(desc = "Number of rows to return.")] k: usize,
    ) -> Option<GqlNodeState> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            Some(GqlNodeState {
                node_state: self_clone
                    .node_state
                    .bottom_k_by(column_cmp(&column, true), k),
            })
        })
        .await
    }

    /// Returns a view of this node state with the rows sorted by a column's values,
    /// ascending with empty cells last. `reverse` flips the whole ordering, putting
    /// empty cells first. Null if the column does not exist, is empty, or is not
    /// comparable.
    pub async fn sort_by_values(
        &self,
        #[graphql(desc = "Column name.")] column: String,
        #[graphql(desc = "Sort in descending order instead. Defaults to false.")] reverse: Option<
            bool,
        >,
    ) -> Option<GqlNodeState> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone.check_comparable(&column)?;
            let cmp = column_cmp(&column, true);
            let node_state = if reverse.unwrap_or(false) {
                self_clone
                    .node_state
                    .sort_by_values_by(|a, b| cmp(a, b).reverse())
            } else {
                self_clone.node_state.sort_by_values_by(cmp)
            };
            Some(GqlNodeState { node_state })
        })
        .await
    }

    /// Groups the nodes by their value in a column. Nodes with an empty cell form
    /// their own group. Null if the column does not exist or contains nodes.
    pub async fn group_by(
        &self,
        #[graphql(desc = "Column name.")] column: String,
    ) -> Option<Vec<GqlNodeStateGroup>> {
        let self_clone = self.clone();
        blocking_compute(move || {
            if !self_clone.is_prop_column(&column) {
                return None;
            }
            let groups = self_clone.node_state.group_by(|mut row| {
                GroupKey::new(row.swap_remove(&column).flatten().map(|value| value.0))
            });
            Some(
                groups
                    .into_iter_groups()
                    .map(|(key, nodes)| GqlNodeStateGroup {
                        value: key.value.map(GqlPropertyOutputVal),
                        nodes: GqlNodes::new(nodes),
                    })
                    .collect(),
            )
        })
        .await
    }

    /// Returns one page of this node state as a node state of its own, so that
    /// `nodes` / `rows` / `columns` on it stay row-aligned with each other.
    /// Pages past the end are empty rather than an error.
    ///
    /// For example, if page(limit: 5, offset: 1, page_index: 2) is called, a page with 5 items,
    /// offset by 11 items (2 pages of 5 + 1), will be returned.
    pub async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of rows to return on this page.")] limit: usize,
        #[graphql(desc = "Extra rows to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> Result<GqlNodeState> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.slice(start, limit)
        })
        .await)
    }

    /// Returns a view of this node state with the rows sorted by node id.
    pub async fn sort_by_id(&self) -> GqlNodeState {
        let self_clone = self.clone();
        blocking_compute(move || GqlNodeState {
            node_state: self_clone.node_state.sort_by_id(),
        })
        .await
    }

    /// The columns of the node state, one per output field of the algorithm.
    /// `values` are row-aligned with `nodes`.
    pub async fn columns(&self, ctx: &Context<'_>) -> Result<Vec<GqlNodeStateColumn>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let num_rows = self_clone.node_state.len();
            let mut columns: Vec<(String, Vec<GqlNodeStateValue>)> = self_clone
                .node_state
                .state
                .values_ref()
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().clone(), Vec::with_capacity(num_rows)))
                .collect();
            for row in self_clone.node_state.values_to_rows() {
                let mut transformed = self_clone.node_state.convert(row);
                for (name, values) in columns.iter_mut() {
                    if let Some(value) = transformed.swap_remove(name) {
                        values.push(value.into());
                    }
                }
            }
            columns
                .into_iter()
                .map(|(name, values)| GqlNodeStateColumn { name, values })
                .collect()
        })
        .await)
    }
}

#[cfg(test)]
mod graphql_test {
    use crate::test_support::setup_with_graphs;
    use dynamic_graphql::Request;
    use raphtory::{db::api::view::MaterializedGraph, prelude::*};
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_algorithm_node_state_ops() {
        let graph = Graph::new();
        // insert out of id order so sortById is meaningful
        graph.add_edge(1, "c", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "b", "a", NO_PROPS, None).unwrap();
        graph.add_edge(3, "a", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                get(node: "b") {
                  columnName
                  value {
                    __typename
                    ... on NodeStateProp { prop }
                  }
                }
                missing: get(node: "not-a-node") { columnName }
                sortById {
                  nodes { list { name } }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // in a 3-cycle all nodes have the same rank of 1/3
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "pagerank": {
                            "get": [
                                {
                                    "columnName": "pagerank_score",
                                    "value": { "__typename": "NodeStateProp", "prop": 0.3333333333333333 }
                                }
                            ],
                            "missing": null,
                            "sortById": {
                                "nodes": {
                                    "list": [
                                        { "name": "a" },
                                        { "name": "b" },
                                        { "name": "c" }
                                    ]
                                }
                            }
                        }
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_rows() {
        let graph = Graph::new();
        // asymmetric graph so every node has a distinct pagerank
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "a", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                columnNames
                rows {
                  node { name }
                  entries {
                    columnName
                    value { ... on NodeStateProp { prop } }
                  }
                }
                headlessRows {
                  node { name }
                  values { ... on NodeStateProp { prop } }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "pagerank": {
                            "columnNames": ["pagerank_score"],
                            "rows": [
                                {
                                    "node": { "name": "a" },
                                    "entries": [
                                        { "columnName": "pagerank_score", "value": { "prop": 0.197580035313204 } }
                                    ]
                                },
                                {
                                    "node": { "name": "b" },
                                    "entries": [
                                        { "columnName": "pagerank_score", "value": { "prop": 0.28155081033755053 } }
                                    ]
                                },
                                {
                                    "node": { "name": "c" },
                                    "entries": [
                                        { "columnName": "pagerank_score", "value": { "prop": 0.5208691543492454 } }
                                    ]
                                }
                            ],
                            "headlessRows": [
                                {
                                    "node": { "name": "a" },
                                    "values": [ { "prop": 0.197580035313204 } ]
                                },
                                {
                                    "node": { "name": "b" },
                                    "values": [ { "prop": 0.28155081033755053 } ]
                                },
                                {
                                    "node": { "name": "c" },
                                    "values": [ { "prop": 0.5208691543492454 } ]
                                }
                            ]
                        }
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_page() {
        let graph = Graph::new();
        // a chain a -> b -> c -> d -> e, so the state has 5 rows
        for (src, dst) in [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")] {
            graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        }
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // a page is itself a NodeState, so nodes/columns on it stay row-aligned
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              degreeCentrality {
                first: page(limit: 2) {
                  count
                  nodes { ids }
                  columns { name values { ... on NodeStateProp { prop } } }
                }
                second: page(limit: 2, pageIndex: 1) { nodes { ids } }
                withOffset: page(limit: 2, offset: 1) { nodes { ids } }
                lastPartial: page(limit: 2, pageIndex: 2) { nodes { ids } }
                pastEnd: page(limit: 2, pageIndex: 99) { count nodes { ids } }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // the first page holds the first two nodes with their values still aligned,
        // `offset` shifts by rows rather than pages, the final page is short rather
        // than padded, and paging past the end is empty rather than an error
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "degreeCentrality": {
                    "first": {
                        "count": 2,
                        "nodes": { "ids": ["a", "b"] },
                        "columns": [{
                            "name": "degree_centrality",
                            "values": [{ "prop": 0.5 }, { "prop": 1.0 }]
                        }]
                    },
                    "second": { "nodes": { "ids": ["c", "d"] } },
                    "withOffset": { "nodes": { "ids": ["b", "c"] } },
                    "lastPartial": { "nodes": { "ids": ["e"] } },
                    "pastEnd": { "count": 0, "nodes": { "ids": [] } }
                } } }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_page_composes() {
        let graph = Graph::new();
        // asymmetric graph so every node has a distinct pagerank:
        // a = 0.1976, b = 0.2816, c = 0.5209
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "a", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        // a page is a NodeState, so it chains with the other operations
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                sortedThenPaged: sortByValues(column: "pagerank_score", reverse: true) {
                  page(limit: 2) { nodes { ids } }
                }
                pagedThenAggregated: page(limit: 2) {
                  max(column: "pagerank_score") { node { id } }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "pagerank": {
                    // top two by score, descending
                    "sortedThenPaged": { "page": { "nodes": { "ids": ["c", "b"] } } },
                    // the aggregate only sees the page's rows (a, b), so b wins
                    "pagedThenAggregated": { "max": { "node": { "id": "b" } } }
                } } }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_top_k_and_sorting() {
        let graph = Graph::new();
        // asymmetric graph so every node has a distinct pagerank:
        // a = 0.1976, b = 0.2816, c = 0.5209
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "a", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                topTwo: topK(column: "pagerank_score", k: 2) { nodes { ids } }
                bottomTwo: bottomK(column: "pagerank_score", k: 2) { nodes { ids } }
                ascending: sortByValues(column: "pagerank_score") { nodes { ids } }
                descending: sortByValues(column: "pagerank_score", reverse: true) { nodes { ids } }
                missingColumn: topK(column: "nope", k: 2) { count }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": { "algorithm": { "pagerank": {
                    // largest first, smallest first
                    "topTwo": { "nodes": { "ids": ["c", "b"] } },
                    "bottomTwo": { "nodes": { "ids": ["a", "b"] } },
                    "ascending": { "nodes": { "ids": ["a", "b", "c"] } },
                    "descending": { "nodes": { "ids": ["c", "b", "a"] } },
                    "missingColumn": null
                } } }
            })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_group_by() {
        let graph = Graph::new();
        // two connected components, so wcc gives two distinct component ids
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "c", "d", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              weaklyConnectedComponents {
                groupBy(column: "component_id") {
                  value
                  nodes { ids }
                }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        // group order and node order within a group are both unordered
        let mut data = res.data.into_json().unwrap();
        let groups = data["graph"]["algorithm"]["weaklyConnectedComponents"]["groupBy"]
            .as_array_mut()
            .unwrap();
        for group in groups.iter_mut() {
            group["nodes"]["ids"]
                .as_array_mut()
                .unwrap()
                .sort_by_key(|id| id.as_str().unwrap().to_string());
        }
        groups.sort_by_key(|group| group["nodes"]["ids"][0].as_str().unwrap().to_string());
        // a-b and c-d each form their own component
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0]["nodes"]["ids"], json!(["a", "b"]));
        assert_eq!(groups[1]["nodes"]["ids"], json!(["c", "d"]));
        assert_ne!(groups[0]["value"], groups[1]["value"]);

        // grouping a node-valued column is rejected
        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              outComponents { groupBy(column: "out_components") { value } }
            }
          }
        }
        "#;
        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({ "graph": { "algorithm": { "outComponents": { "groupBy": null } } } })
        );
    }

    #[tokio::test]
    async fn test_algorithm_node_state_aggregates() {
        let graph = Graph::new();
        // asymmetric graph so every node has a distinct pagerank
        graph.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        graph.add_edge(2, "a", "c", NO_PROPS, None).unwrap();
        graph.add_edge(3, "b", "c", NO_PROPS, None).unwrap();
        let graph: MaterializedGraph = graph.into();
        let tmp_dir = tempdir().unwrap();
        let setup = setup_with_graphs(&[("g", graph)], tmp_dir.path()).await;

        let query = r#"
        {
          graph(path: "g") {
            algorithm {
              pagerank(iterCount: 20) {
                min(column: "pagerank_score") { node { name } value }
                max(column: "pagerank_score") { node { name } value }
                median(column: "pagerank_score") { node { name } value }
                sum(column: "pagerank_score")
                mean(column: "pagerank_score")
                missing: min(column: "not_a_column") { value }
              }
            }
          }
        }
        "#;

        let res = setup.schema.execute(Request::new(query)).await;
        assert_eq!(res.errors, vec![], "{:?}", res.errors);
        assert_eq!(
            res.data.into_json().unwrap(),
            json!({
                "graph": {
                    "algorithm": {
                        "pagerank": {
                            "min": {
                                "node": { "name": "a" },
                                "value": 0.197580035313204
                            },
                            "max": {
                                "node": { "name": "c" },
                                "value": 0.5208691543492454
                            },
                            "median": {
                                "node": { "name": "b" },
                                "value": 0.28155081033755053
                            },
                            "sum": 1.0,
                            "mean": 0.3333333333333333,
                            "missing": null
                        }
                    }
                }
            })
        );
    }
}
