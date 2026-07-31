use crate::{
    model::graph::{
        node::GqlNode, node_id::GqlNodeId, nodes::GqlNodes, property::GqlPropertyOutputVal,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject, Union};
use raphtory::{
    db::{
        api::{
            state::{
                NodeStateOutput, NodeStateValue, OutputTypedNodeState, PropMap, TypedNodeState,
            },
            view::{BoxableGraphView, DynamicGraph},
        },
        graph::node::NodeView,
    },
    prelude::{NodeStateOps, Prop},
};
use raphtory_api::core::entities::properties::prop::PropUnwrap;
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
pub(crate) struct GqlNodeState {
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
pub(crate) struct GqlNodeStateProp {
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
pub(crate) struct GqlNodeStateColumn {
    /// Name of the column.
    name: String,
    /// The values of this column; `values[i]` belongs to `NodeState.nodes[i]`.
    values: Vec<GqlNodeStateValue>,
}

/// One column's value for a single node.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateEntry")]
pub(crate) struct GqlNodeStateEntry {
    /// Name of the column.
    column_name: String,
    /// The node's value in this column.
    value: GqlNodeStateValue,
}

/// A `(node, value)` pair, e.g. the result of a column aggregate.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateItem")]
pub(crate) struct GqlNodeStateItem {
    /// The node.
    node: GqlNode,
    /// The node's value.
    value: GqlPropertyOutputVal,
}

/// A node's full row in the node state: one entry per column.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateRow")]
pub(crate) struct GqlNodeStateRow {
    /// The node this row belongs to.
    node: GqlNode,
    /// The row's values, one entry per column.
    entries: Vec<GqlNodeStateEntry>,
}

/// The nodes sharing one value of a column, as returned by `groupBy`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateGroup")]
pub(crate) struct GqlNodeStateGroup {
    /// The value shared by the nodes in this group; null if their cell is empty.
    value: Option<GqlPropertyOutputVal>,
    /// The nodes holding that value.
    nodes: GqlNodes,
}

/// A node's full row in the node state without the column names: `values[i]`
/// belongs to the column `NodeState.columnNames[i]`.
#[derive(SimpleObject, Clone)]
#[graphql(name = "NodeStateHeadlessRow")]
pub(crate) struct GqlNodeStateHeadlessRow {
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

// TODO: add paging: `columns`/`nodes`/`rows` currently dump every row.

// TODO: still to be implemented, blocked on the datafusion feature gate (CVE):
// `sortBy` (`GenericNodeState::sort_by`), `topK` (`GenericNodeState::top_k`),
// `groupBy` (`TypedNodeState::get_groups`).
//
// Not exposed: `merge` (takes a second NodeState, which cannot be a query argument)
// and `to_parquet`/`from_parquet` (avoid server-side filesystem access).
#[ResolvedObjectFields]
impl GqlNodeState {
    /// Returns the number of nodes with a value in this node state.
    async fn count(&self) -> usize {
        self.node_state.len()
    }

    /// The nodes with a value in this node state, in row order. Aligned with `values`.
    async fn nodes(&self) -> GqlNodes {
        GqlNodes::new(self.node_state.nodes())
    }

    /// The column names of this node state in order.
    async fn column_names(&self) -> Vec<String> {
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
    async fn rows(&self) -> Vec<GqlNodeStateRow> {
        let self_clone = self.clone();
        blocking_compute(move || {
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
        .await
    }

    /// All rows of the node state keyed by node, without the column names: the `values` of each row are
    /// in `columnNames` order.
    async fn headless_rows(&self) -> Vec<GqlNodeStateHeadlessRow> {
        let self_clone = self.clone();
        blocking_compute(move || {
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
        .await
    }

    /// Returns the values for a node, one entry per column; null if the node has no value in this NodeState.
    async fn get(
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
    async fn min(
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
    async fn max(
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
    async fn sum(
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
    async fn mean(
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
    async fn median(
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
    async fn top_k(
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
    async fn bottom_k(
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
    async fn sort_by_values(
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
    async fn group_by(
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

    /// Returns a view of this node state with the rows sorted by node id.
    async fn sort_by_id(&self) -> GqlNodeState {
        let self_clone = self.clone();
        blocking_compute(move || GqlNodeState {
            node_state: self_clone.node_state.sort_by_id(),
        })
        .await
    }

    /// The columns of the node state, one per output field of the algorithm.
    /// `values` are row-aligned with `nodes`.
    async fn columns(&self) -> Vec<GqlNodeStateColumn> {
        let self_clone = self.clone();
        blocking_compute(move || {
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
        .await
    }
}
