use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{node_state_ops::NodeStateOps, Index},
            view::{DynamicGraph, IntoDynBoxed, IntoDynamic},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};

use arrow::compute::{cast_with_options, CastOptions};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder};
use indexmap::IndexSet;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use arrow_array::{builder::UInt64Builder, UInt64Array};
use arrow_select::{concat::concat, take::take};
use raphtory_api::core::entities::properties::prop::Prop;
use rayon::{iter::Either, prelude::*};
use serde::{de::DeserializeOwned, Serialize};
use serde_arrow::{
    from_record_batch,
    schema::{SchemaLike, TracingOptions},
    to_record_batch, Deserializer,
};
use std::{
    cmp::PartialEq,
    collections::HashMap,
    fmt::{Debug, Formatter},
    fs::File,
    hash::BuildHasher,
    marker::PhantomData,
    path::Path,
    sync::Arc,
};
use tracing::field::debug;

pub trait NodeStateValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}
impl<T> NodeStateValue for T where
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}

#[derive(Clone, PartialEq)]
pub enum MergePriority {
    Left,
    Right,
    Exclude,
}

#[derive(Clone, Debug)]
pub struct GenericNodeState<'graph, G, GH = G> {
    base_graph: G,
    graph: GH,
    values: RecordBatch,
    keys: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

#[derive(Clone)]
pub struct TypedNodeState<'graph, V: NodeStateValue, G, GH = G> {
    pub state: GenericNodeState<'graph, G, GH>,
    _marker: PhantomData<V>,
}

pub struct RecordBatchIterator<'a, T> {
    deserializer: Deserializer<'a>,
    idx: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> RecordBatchIterator<'a, T>
where
    T: NodeStateValue,
{
    pub fn new(record_batch: &'a RecordBatch) -> Self {
        let deserializer: Deserializer<'a> = Deserializer::from_record_batch(record_batch).unwrap();
        let idx: usize = 0;
        Self {
            deserializer,
            idx,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, idx: usize) -> T {
        T::deserialize(self.deserializer.get(idx).unwrap()).unwrap()
    }
}

impl<'a, T> Iterator for RecordBatchIterator<'a, T>
where
    T: NodeStateValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.deserializer.get(self.idx);
        if res.is_none() {
            return None;
        }
        let item = T::deserialize(res.unwrap()).unwrap();
        self.idx += 1;
        Some(item)
    }
}

impl<'graph, G: IntoDynamic, GH: IntoDynamic> GenericNodeState<'graph, G, GH> {
    pub fn into_dyn(self) -> GenericNodeState<'graph, DynamicGraph> {
        GenericNodeState::new(
            self.base_graph.into_dynamic(),
            self.graph.into_dynamic(),
            self.values,
            self.keys,
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> GenericNodeState<'graph, G, GH> {
    pub fn new(base_graph: G, graph: GH, values: RecordBatch, keys: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }

    pub fn transform(self) -> TypedNodeState<'graph, HashMap<String, Option<Prop>>, G, GH> {
        TypedNodeState::new(self)
    }

    pub fn into_inner(self) -> (RecordBatch, Option<Index<VID>>) {
        (self.values, self.keys)
    }

    pub fn values(&self) -> &RecordBatch {
        &self.values
    }

    fn get_index_by_node<N: AsNodeRef>(&self, node: &N) -> Option<usize> {
        let id = self.graph.internalise_node(node.as_node_ref())?;
        match &self.keys {
            Some(index) => index.index(&id),
            None => Some(id.0),
        }
    }

    fn nodes(&self) -> Nodes<'graph, G, GH> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.keys.clone(),
            None,
        )
    }

    fn len(&self) -> usize {
        self.values.num_rows()
    }

    pub fn to_parquet<P: AsRef<Path>>(&self, file_path: P, id_column: Option<String>) {
        let mut batch: Option<RecordBatch> = None;
        let mut schema = self.values.schema();

        if id_column.is_some() {
            let ids: Vec<String> = self
                .nodes()
                .id()
                .iter()
                .map(|(_, gid)| gid.to_string())
                .collect();
            let ids_array = Arc::new(StringArray::from(ids)) as ArrayRef;

            let mut builder = SchemaBuilder::new();
            for field in &self.values.schema().fields().clone() {
                builder.push(field.clone())
            }
            builder.push(Arc::new(Field::new(
                id_column.unwrap(),
                DataType::Utf8,
                false,
            )));
            schema = Arc::new(Schema::new(builder.finish().fields));

            let mut columns = self.values.columns().to_vec();
            columns.push(ids_array);

            batch = Some(RecordBatch::try_new(schema.clone(), columns).unwrap());
        }

        // Now write this new_batch to Parquet
        let file = File::create(file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer
            .write(batch.as_ref().unwrap_or(&self.values))
            .expect("Writing batch");
        writer.close().unwrap();
    }

    fn merge_columns(
        col_name: &String,
        tgt_idx_set: Option<&Index<VID>>,
        tgt_idx_set_len: usize,
        lh: Option<&Index<VID>>,
        rh: Option<&Index<VID>>,
        lh_batch: &RecordBatch,
        rh_batch: &RecordBatch,
    ) -> (Arc<dyn Array>, Field) {
        let iter = if tgt_idx_set.is_some() {
            Either::Left(tgt_idx_set.as_ref().unwrap().iter())
        } else {
            Either::Right((0..tgt_idx_set_len).map(VID))
        };
        let mut idx_builder = UInt64Builder::with_capacity(tgt_idx_set_len);
        let left_col = lh_batch.column_by_name(col_name);
        let left_len = if left_col.is_some() {
            left_col.unwrap().len()
        } else {
            0
        };
        let right_col = rh_batch.column_by_name(col_name);
        let cat = match (left_col, right_col) {
            (Some(l), Some(r)) => &concat(&[l.as_ref(), r.as_ref()]).unwrap(),
            (Some(l), None) => l,
            (None, Some(r)) => r,
            (None, None) => unreachable!("at least one is guaranteed to be Some"),
        };
        for i in iter {
            if left_col.is_some() && (lh.is_none() || lh.unwrap().contains(&i)) {
                if lh.is_none() {
                    idx_builder.append_value(i.0 as u64);
                } else {
                    idx_builder.append_value(lh.unwrap().index(&i).unwrap() as u64);
                }
            } else if right_col.is_some() && (rh.is_none() || rh.unwrap().contains(&i)) {
                if rh.is_none() {
                    idx_builder.append_value((left_len + i.0) as u64);
                } else {
                    idx_builder.append_value((left_len + rh.unwrap().index(&i).unwrap()) as u64);
                }
            } else {
                idx_builder.append_null();
            }
        }
        let take_idx: UInt64Array = idx_builder.finish();
        let mut field = lh_batch.schema_ref().field_with_name(col_name);
        if field.is_err() {
            field = rh_batch.schema_ref().field_with_name(col_name);
        }
        (
            take(cat.as_ref(), &take_idx, None).unwrap(),
            field.unwrap().clone(),
        )
    }

    pub fn merge(
        &self,
        other: &GenericNodeState<'graph, G>,
        index_merge_priority: MergePriority,
        default_column_merge_priority: MergePriority,
        column_merge_priority_map: Option<HashMap<String, MergePriority>>,
    ) -> Self {
        let new_idx_set = if self.keys.is_none() || other.keys.is_none() {
            None
        } else {
            Some(
                IndexSet::union(
                    self.keys.as_ref().unwrap().index.as_ref(),
                    other.keys.as_ref().unwrap().index.as_ref(),
                )
                .clone()
                .into_iter()
                .map(|v| v.to_owned())
                .collect(),
            )
        };
        let tgt_idx_set = match index_merge_priority {
            MergePriority::Left => self.keys.as_ref(),
            MergePriority::Right => other.keys.as_ref(),
            MergePriority::Exclude => new_idx_set.as_ref(),
        };
        let tgt_index_set_len =
            tgt_idx_set.map_or(self.graph.unfiltered_num_nodes(), |idx_set| idx_set.len());

        let mut cols: Vec<Arc<dyn Array>> = vec![];
        let mut fields: Vec<Field> = vec![];

        // iterate over priority map first and merge accordingly
        if column_merge_priority_map.as_ref().is_some() {
            for (col_name, priority) in column_merge_priority_map.as_ref().unwrap() {
                let (lh, rh, lh_batch, rh_batch) = match priority {
                    MergePriority::Left => (
                        self.keys.as_ref(),
                        other.keys.as_ref(),
                        &self.values,
                        &other.values,
                    ),
                    MergePriority::Right => (
                        other.keys.as_ref(),
                        self.keys.as_ref(),
                        &other.values,
                        &self.values,
                    ),
                    MergePriority::Exclude => continue,
                };
                let (col, field) = GenericNodeState::<'graph, G>::merge_columns(
                    col_name,
                    tgt_idx_set,
                    tgt_index_set_len,
                    lh,
                    rh,
                    lh_batch,
                    rh_batch,
                );
                cols.push(col);
                fields.push(field);
            }
        }

        // if default is not exclude
        // merge remaining columns accordingly
        let (lh, rh, lh_batch, rh_batch) = match default_column_merge_priority {
            MergePriority::Left => (
                self.keys.as_ref(),
                other.keys.as_ref(),
                &self.values,
                &other.values,
            ),
            MergePriority::Right => (
                other.keys.as_ref(),
                self.keys.as_ref(),
                &other.values,
                &self.values,
            ),
            MergePriority::Exclude => {
                return GenericNodeState::new(
                    self.base_graph.clone(),
                    self.graph.clone(),
                    RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
                    tgt_idx_set.cloned(),
                )
            }
        };
        // iterate over columns in lh, if not in priority_map, merge columns
        for column in lh_batch.schema().fields().iter() {
            let col_name = column.name();
            if column_merge_priority_map
                .as_ref()
                .map_or(true, |map| map.contains_key(col_name) == false)
            {
                let (col, field) = GenericNodeState::<'graph, G>::merge_columns(
                    col_name,
                    tgt_idx_set,
                    tgt_index_set_len,
                    lh,
                    rh,
                    lh_batch,
                    rh_batch,
                );
                cols.push(col);
                fields.push(field);
            }
        }
        GenericNodeState::new(
            self.base_graph.clone(),
            self.graph.clone(),
            RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
            tgt_idx_set.cloned(),
        )
    }
}

impl<'graph, V: NodeStateValue, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    TypedNodeState<'graph, V, G, GH>
{
    pub fn new(state: GenericNodeState<'graph, G, GH>) -> Self {
        TypedNodeState {
            state: state,
            _marker: PhantomData,
        }
    }

    pub fn values_to_rows(&self) -> Vec<V> {
        let rows: Vec<V> = from_record_batch(&self.state.values).unwrap();
        rows
    }

    pub fn values_from_rows(&mut self, rows: Vec<V>) {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        self.state.values = to_record_batch(&fields, &rows).unwrap();
    }
}

impl<'a, 'graph, V: NodeStateValue + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    PartialEq<TypedNodeState<'graph, V, G, GH>> for TypedNodeState<'graph, V, G, GH>
{
    fn eq(&self, other: &TypedNodeState<'graph, V, G, GH>) -> bool {
        self.len() == other.len()
            && self
                .par_iter()
                .all(|(node, value)| other.get_by_node(node).map(|v| v == value).unwrap_or(false))
    }
}

impl<
        'graph,
        RHS: NodeStateValue + Send + Sync,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > PartialEq<Vec<RHS>> for TypedNodeState<'graph, RHS, G, GH>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values_to_rows().par_iter().eq(other)
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: NodeStateValue + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for TypedNodeState<'graph, RHS, G, GH>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|lhs| lhs == rhs).is_none() == false)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: Debug + GraphViewOps<'graph>>
    TypedNodeState<'graph, HashMap<String, Option<Prop>>, G, GH>
{
    pub fn try_eq_hashmap<K: AsNodeRef>(
        &self,
        other: HashMap<K, HashMap<String, Option<Prop>>>,
    ) -> Result<bool, &'static str> {
        if other.len() != self.len() {
            return Ok(false);
        }

        for (k, mut rhs_map) in other {
            let lhs_map = self.get_by_node(&k).ok_or("Key missing in lhs map")?;

            if lhs_map.len() != rhs_map.len() {
                return Ok(false);
            }

            for (key, lhs_val) in lhs_map {
                let rhs_val = rhs_map.remove(&key).ok_or("Key missing in rhs map")?;

                if lhs_val.is_none() {
                    if rhs_val.is_none() {
                        continue;
                    } else {
                        return Ok(false);
                    }
                } else if rhs_val.is_none() {
                    return Ok(false);
                }

                let lhs_val = lhs_val.unwrap();
                let rhs_val = rhs_val.unwrap();

                let casted_rhs = rhs_val
                    .try_cast(lhs_val.dtype())
                    .map_err(|_| "Failed to cast rhs value")?;

                if casted_rhs != lhs_val {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

impl<
        'graph,
        V: 'graph + NodeStateValue,
        G: GraphViewOps<'graph>,
        GH: Debug + GraphViewOps<'graph>,
    > Debug for TypedNodeState<'graph, V, G, GH>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'graph, V: NodeStateValue + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    IntoIterator for TypedNodeState<'graph, V, G, GH>
{
    type Item = (NodeView<'graph, G, GH>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes()
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<
        'a,
        'graph: 'a,
        V: NodeStateValue + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > NodeStateOps<'a, 'graph> for TypedNodeState<'graph, V, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;

    type Value = V;
    type OwnedValue = V;

    fn graph(&self) -> &Self::Graph {
        &self.state.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.state.base_graph
    }

    fn iter_values(&'a self) -> impl Iterator<Item = Self::Value> + 'a {
        RecordBatchIterator::new(&self.state.values)
    }

    #[allow(refining_impl_trait)]
    fn par_iter_values(&'a self) -> impl IndexedParallelIterator<Item = Self::Value> + 'a {
        let iter = RecordBatchIterator::<'a, Self::Value>::new(&self.state.values);
        (0..self.len())
            .into_par_iter()
            .map(move |i| RecordBatchIterator::get(&iter, i))
    }

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync {
        (0..self.len()).map(move |i| self.get_by_index(i).unwrap().1)
    }

    #[allow(refining_impl_trait)]
    fn into_par_iter_values(self) -> impl IndexedParallelIterator<Item = Self::OwnedValue> {
        (0..self.len())
            .into_par_iter()
            .map(move |i| self.get_by_index(i).unwrap().1)
    }

    fn iter(
        &'a self,
    ) -> impl Iterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)> + 'a
    {
        match &self.state.keys {
            Some(index) => index
                .iter()
                .zip(self.iter_values())
                .map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            n,
                        ),
                        v,
                    )
                })
                .into_dyn_boxed(),
            None => self
                .iter_values()
                .enumerate()
                .map(|(i, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            VID(i),
                        ),
                        v,
                    )
                })
                .into_dyn_boxed(),
        }
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph> {
        self.state.nodes()
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)>
    {
        match &self.state.keys {
            Some(index) => {
                Either::Left(index.par_iter().zip(self.par_iter_values()).map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            n,
                        ),
                        v,
                    )
                }))
            }
            None => Either::Right(self.par_iter_values().enumerate().map(|(i, v)| {
                (
                    NodeView::new_one_hop_filtered(
                        &self.state.base_graph,
                        &self.state.graph,
                        VID(i),
                    ),
                    v,
                )
            })),
        }
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)> {
        let vid = match &self.state.keys {
            Some(node_index) => node_index.key(index).unwrap(),
            None => VID(index),
        };
        Some((
            NodeView::new_one_hop_filtered(&self.state.base_graph, &self.state.graph, vid),
            self.get_by_node(vid).unwrap(), // &self.values[index],
        ))
    }

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let index = self.state.get_index_by_node(&node).unwrap();
        let deserializer = Deserializer::from_record_batch(&self.state.values).unwrap();
        let item = V::deserialize(
            deserializer
                .get(index)
                .ok_or_else(|| tracing::error!("Could not get item"))
                .unwrap(),
        )
        .unwrap();
        Some(item)
    }

    fn len(&self) -> usize {
        self.state.len()
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    pub fn new_from_eval<V: NodeStateValue>(graph: G, values: Vec<V>) -> Self {
        Self::new_from_eval_mapped(graph, values, |v| v)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: NodeStateValue, V: NodeStateValue>(
        graph: G,
        values: Vec<R>,
        map: impl Fn(R) -> V,
    ) -> Self {
        let index = Index::for_graph(graph.clone());
        let values: Vec<V> = match &index {
            None => values.into_iter().map(map).collect(),
            Some(index) => index
                .iter()
                .map(|vid| map(values[vid.index()].clone()))
                .collect(),
        };
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        let values = Self::convert_recordbatch(to_record_batch(&fields, &values).unwrap()).unwrap();

        Self::new(graph.clone(), graph, values, index)
    }

    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        Self::new(
            graph.clone(),
            graph,
            RecordBatch::new_empty(Schema::empty().into()),
            Some(Index::default()),
        )
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(graph: G, values: impl Into<RecordBatch>) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(
            graph.clone(),
            graph,
            Self::convert_recordbatch(values.into()).unwrap(),
            index,
        )
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R: NodeStateValue, S: BuildHasher, V: NodeStateValue>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
    ) -> Self {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new_from_values(graph, values)
        } else {
            let (index, values): (IndexSet<VID, ahash::RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new(graph.clone(), graph, values, Some(Index::new(index)))
        }
    }

    fn convert_recordbatch(
        recordbatch: RecordBatch,
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        // Cast all arrays to nullable
        let new_columns: Vec<Arc<dyn Array>> = recordbatch
            .columns()
            .iter()
            .map(|col| cast_with_options(col, &col.data_type().clone(), &CastOptions::default()))
            .collect::<arrow::error::Result<_>>()?;

        // Rebuild schema with nullable fields
        let new_fields: Vec<Field> = recordbatch
            .schema()
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect();

        let new_schema = Arc::new(Schema::new(new_fields));

        RecordBatch::try_new(new_schema.clone(), new_columns)
    }
}
