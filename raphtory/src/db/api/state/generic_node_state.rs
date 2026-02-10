use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{node_state_ops::NodeStateOps, ops::Const, Index},
            view::{DynamicGraph, IntoDynBoxed, IntoDynamic},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeViewOps},
};

use arrow::{
    array::AsArray,
    compute::{cast_with_options, CastOptions},
    datatypes::UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder};
use indexmap::{IndexMap, IndexSet};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    errors::ParquetError,
    file::properties::WriterProperties,
};

use arrow_array::{builder::UInt64Builder, UInt64Array};
use arrow_select::{concat::concat, take::take};
use raphtory_api::core::entities::properties::prop::{Prop, PropType, PropUnwrap};
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

// The bundle of traits which are useful/essential for the underlying value types
// of a GenericNodeState.
// We need serialize and deserialize to create a GenericNodeState/RecordBatch from a Vec of structs
// and to extract a Vec of structs from a GenericNodeState
// the others are useful for implementing various traits
pub trait NodeStateValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}
impl<T> NodeStateValue for T where
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}

// These I think are the minimum required to create a GenericNodeState/RecordBatch
pub trait InputNodeStateValue<V>: NodeStateValue + From<V> {}
impl<T, V> InputNodeStateValue<V> for T where T: NodeStateValue + From<V> {}

// These are the options for merging NodeState indexes/columns
#[derive(Clone, PartialEq)]
pub enum MergePriority {
    Left,
    Right,
    Exclude,
}

// This enum is very useful for interfacing with Python.
// When exposing a TypedNodeState to Python, the columns all fall into one of these variants.
#[derive(Clone, PartialEq, Debug)]
pub enum NodeStateOutput<'graph, G: GraphViewOps<'graph>> {
    Node(NodeView<'graph, G>),
    Nodes(Nodes<'graph, G, G>),
    Prop(Option<Prop>),
}

// This exists because GenericNodeStates have a data structure (node_cols) exposing which columns contain nodes.
#[derive(Clone, PartialEq, Debug)]
pub enum NodeStateOutputType {
    Node,
    Nodes,
    Prop,
}

// Rows of TypedNodeStates containing references to nodes are first deserialized into this type,
// a map of column names to generic values.
pub type PropMap = IndexMap<String, Option<Prop>>;

// Then, using node_cols, the values which are actually nodes get converted into the correct NodeStateOutput variant.
pub type TransformedPropMap<'graph, G> = IndexMap<String, NodeStateOutput<'graph, G>>;

// Shorthand for a TypedNodeState which is ideal for usage in Python.
pub type OutputTypedNodeState<'graph, G> =
    TypedNodeState<'graph, PropMap, G, TransformedPropMap<'graph, G>>;

pub trait NodeTransform {
    type Input;
    type Output;

    fn transform<'graph, G>(
        state: &GenericNodeState<'graph, G>,
        value: Self::Input,
    ) -> Self::Output
    where
        G: GraphViewOps<'graph>;
}

impl<T> NodeTransform for T
where
    T: NodeStateValue,
{
    type Input = Self;
    type Output = Self;

    fn transform<'graph, G>(
        _state: &GenericNodeState<'graph, G>,
        value: Self::Input,
    ) -> Self::Output
    where
        G: GraphViewOps<'graph>,
    {
        value
    }
}

#[derive(Clone, Debug)]
pub struct GenericNodeState<'graph, G> {
    pub base_graph: G,
    values: RecordBatch,
    pub(crate) keys: Option<Index<VID>>,
    // Data structure mapping which columns are node-containing and, if so, which graph they belong to
    // note: maybe change that Option<G> to a Option<Box<dyn GraphViewOps>> or something
    node_cols: HashMap<String, (NodeStateOutputType, Option<G>)>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G> GenericNodeState<'graph, G> {
    #[inline]
    pub fn values_ref(&self) -> &RecordBatch {
        &self.values
    }

    #[inline]
    pub fn keys_ref(&self) -> Option<&Index<VID>> {
        self.keys.as_ref()
    }
}

// This is what most code will interface with. A TypedNodeState is a wrapper around a GenericNodeState
// with a struct type, V, that values from the RecordBatch can be dumped into. If there are nodes in
// any columns, T exists as a second struct type into which V can be transformed.
// (there is no easy way to go directly from raw arrow values to nodes, hence the two struct types)
#[derive(Clone)]
pub struct TypedNodeState<'graph, V: NodeStateValue, G, T: Clone + Sync + Send = V> {
    pub state: GenericNodeState<'graph, G>,
    // function for converting primary struct type to secondary struct type in the event of node-containing columns
    pub converter: fn(&GenericNodeState<'graph, G>, V) -> T,
    _v_marker: PhantomData<V>,
    _t_marker: PhantomData<T>,
}

// Helper struct to facilitate iteration and row-fetching over RecordBatches
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

    pub fn get(&self, idx: usize) -> Option<T> {
        Some(T::deserialize(self.deserializer.get(idx)?).unwrap())
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

impl<'graph, G: IntoDynamic> GenericNodeState<'graph, G> {
    pub fn into_dyn(self) -> GenericNodeState<'graph, DynamicGraph> {
        let node_cols = Some(
            self.node_cols
                .into_iter()
                .map(|(k, (output_type, bg))| (k, (output_type, bg.map(|bg| bg.into_dynamic()))))
                .collect(),
        );
        GenericNodeState::new(
            self.base_graph.into_dynamic(),
            self.values,
            self.keys,
            node_cols,
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    pub fn new(
        base_graph: G,
        values: RecordBatch,
        keys: Option<Index<VID>>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        Self {
            base_graph,
            values,
            keys,
            node_cols: node_cols.unwrap_or(HashMap::new()),
            _marker: PhantomData,
        }
    }

    // PropMap is the primary struct type for Python-facing TypedNodeStates
    // TransformedPropMap is the secondary struct type in case any columns contain nodes
    // This function performs the conversion from primary to secondary
    pub fn get_nodes(
        state: &GenericNodeState<'graph, G>,
        value_map: PropMap,
    ) -> TransformedPropMap<'graph, G> {
        // for column in aux data structure
        value_map
            .into_iter()
            .map(|(key, value)| {
                let node_col_entry = state.node_cols.get(&key);
                let mut result = None;
                if value.is_none() || node_col_entry.is_none() {
                    return (key, NodeStateOutput::Prop(value));
                } else {
                    let (node_state_type, base_graph) = node_col_entry.unwrap();
                    if node_state_type == &NodeStateOutputType::Node {
                        if let Some(Prop::U64(vid)) = value {
                            result = Some((
                                key,
                                NodeStateOutput::Node(NodeView::new_internal(
                                    base_graph.as_ref().unwrap_or(&state.base_graph).clone(),
                                    VID(vid as usize),
                                )),
                            ));
                        }
                    } else if node_state_type == &NodeStateOutputType::Nodes {
                        if let Some(Prop::Array(vid_arr)) = value {
                            return (
                                key,
                                NodeStateOutput::Nodes(Nodes::new_filtered(
                                    base_graph.as_ref().unwrap_or(&state.base_graph).clone(),
                                    base_graph.as_ref().unwrap_or(&state.base_graph).clone(),
                                    Const(true),
                                    Some(Index::from_iter(
                                        vid_arr
                                            .iter_prop()
                                            .map(|vid| VID(vid.into_u64().unwrap() as usize)),
                                    )),
                                )),
                            );
                        } else if let Some(PropType::List(_)) =
                            value.as_ref().map(|value| value.dtype())
                        {
                            if let Some(Prop::List(vid_list)) = value {
                                if let Some(vid_list) = Arc::into_inner(vid_list) {
                                    result = Some((
                                        key,
                                        NodeStateOutput::Nodes(Nodes::new_filtered(
                                            base_graph
                                                .as_ref()
                                                .unwrap_or(&state.base_graph)
                                                .clone(),
                                            base_graph
                                                .as_ref()
                                                .unwrap_or(&state.base_graph)
                                                .clone(),
                                            Const(true),
                                            Some(Index::from_iter(vid_list.into_iter().map(
                                                |vid| {
                                                    VID(vid
                                                        .try_cast(PropType::U64)
                                                        .unwrap()
                                                        .into_u64()
                                                        .unwrap()
                                                        as usize)
                                                },
                                            ))),
                                        )),
                                    ));
                                }
                            }
                        }
                    }
                }
                result.unwrap()
            })
            .collect()
    }

    // Convert any TypedNodeState to a form suitable for Python
    pub fn to_output_nodestate(self) -> OutputTypedNodeState<'graph, G> {
        TypedNodeState::new_mapped(self, Self::get_nodes)
    }

    pub fn into_inner(self) -> (RecordBatch, Option<Index<VID>>) {
        (self.values, self.keys)
    }

    pub fn values(&self) -> &RecordBatch {
        &self.values
    }

    fn get_index_by_node<N: AsNodeRef>(&self, node: &N) -> Option<usize> {
        let id = self.base_graph.internalise_node(node.as_node_ref())?;
        match &self.keys {
            Some(index) => index.index(&id),
            None => Some(id.0),
        }
    }

    fn nodes(&self) -> Nodes<'graph, G> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.base_graph.clone(),
            Const(true),
            self.keys.clone(),
        )
    }

    fn len(&self) -> usize {
        self.values.num_rows()
    }

    pub fn from_parquet<P: AsRef<Path>>(
        &self,
        file_path: P,
        // If ID column is specified and exists, try to construct index using that column
        id_column: Option<String>,
    ) -> Result<GenericNodeState<'graph, G>, GraphError> {
        let num_nodes = self.base_graph.unfiltered_num_nodes();
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        // if id column is specified, but doesn't exist, throw error
        // we're checking here so we can potentially terminate early
        if let Some(ref col_name) = id_column {
            if schema.column_with_name(col_name).is_none() {
                return Err(GraphError::ColumnDoesNotExist(col_name.clone()));
            }
        }

        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        if batches.is_empty() {
            return Err(GraphError::ParquetError(ParquetError::ArrowError(
                "Parquet file is empty.".to_string(),
            )));
        }
        let mut batch = arrow::compute::concat_batches(&schema, &batches)?;

        if batch.num_rows() > num_nodes {
            return Err(GraphError::ParquetError(ParquetError::ArrowError(
                format!(
                    "Number of rows ({}) exceeds order of graph ({}).",
                    batch.num_rows(),
                    num_nodes,
                )
                .to_string(),
            )));
        }

        let mut index = self.keys.clone();

        // checking again so we can remove the column
        if let Some(ref col_name) = id_column {
            // reconstruct index
            if let Some(arr) = batch
                .column_by_name(col_name)
                .unwrap()
                .as_primitive_opt::<UInt64Type>()
            {
                let max_node_id = arr.iter().max().unwrap_or(Some(0)).unwrap() as usize;
                if max_node_id >= num_nodes {
                    return Err(GraphError::ParquetError(ParquetError::ArrowError(
                        format!(
                            "Max Node ID ({}) exceeds order of graph ({}).",
                            max_node_id, num_nodes,
                        )
                        .to_string(),
                    )));
                }
                index = Some(Index::from_iter(
                    arr.iter().map(|v| VID(v.unwrap_or(0) as usize)),
                ));
            } else {
                return Err(GraphError::ParquetError(ParquetError::ArrowError(
                    format!("Column {} is not unsigned integer type.", col_name).to_string(),
                )));
            }
            batch.remove_column(schema.column_with_name(col_name).unwrap().0);
        } else if batch.num_rows() < num_nodes {
            index = Some(Index::from_iter((0..batch.num_rows()).map(VID)));
        }

        Ok(GenericNodeState {
            base_graph: self.base_graph.clone(),
            values: batch,
            keys: index,
            node_cols: self.node_cols.clone(),
            _marker: PhantomData,
        })
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

    // Auxiliary function to merge columns of merging GenericNodeStates
    fn merge_columns(
        col_name: &String,
        tgt_idx_set: Option<&Index<VID>>, // index set of the merged GenricNodeState
        tgt_idx_set_len: usize,
        lh: Option<&Index<VID>>,
        rh: Option<&Index<VID>>,
        lh_batch: &RecordBatch,
        rh_batch: &RecordBatch,
    ) -> (Arc<dyn Array>, Field) {
        // clever tricks with take array_select::take to work with generic columns
        // need to construct a 'take index' to figure out which values we're grabbing
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

    // Merge with another GenericNodeState. Produces a new GenericNodeState.
    // Options for merging index sets are exclusively use left or right, or to union the sets
    // Options for merging columns are to prioritize left or right, or exclude the column
    pub fn merge(
        &self,
        other: &GenericNodeState<'graph, G>,
        index_merge_priority: MergePriority, // Exclude = Union of index sets
        default_column_merge_priority: MergePriority,
        column_merge_priority_map: Option<HashMap<String, MergePriority>>,
    ) -> Self {
        // if any columns contain nodes, you need to preserve that information in the node_cols structure
        let mut merge_node_cols: HashMap<String, (NodeStateOutputType, Option<G>)> =
            HashMap::default();
        let default_node_cols: &HashMap<String, (NodeStateOutputType, Option<G>)> =
            if default_column_merge_priority == MergePriority::Left {
                &self.node_cols
            } else {
                &other.node_cols
            };

        // handle index set merging
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
        let tgt_index_set_len = tgt_idx_set
            .map_or(self.base_graph.unfiltered_num_nodes(), |idx_set| {
                idx_set.len()
            });

        let mut cols: Vec<Arc<dyn Array>> = vec![];
        let mut fields: Vec<Field> = vec![];

        // iterate over priority map first and merge accordingly
        if column_merge_priority_map.as_ref().is_some() {
            for (col_name, priority) in column_merge_priority_map.as_ref().unwrap() {
                let (lh, rh, lh_batch, rh_batch) = match priority {
                    MergePriority::Left => {
                        if self.node_cols.contains_key(col_name) {
                            merge_node_cols.insert(
                                col_name.to_string(),
                                self.node_cols.get(col_name).unwrap().clone(),
                            );
                        }
                        (
                            self.keys.as_ref(),
                            other.keys.as_ref(),
                            &self.values,
                            &other.values,
                        )
                    }
                    MergePriority::Right => {
                        if other.node_cols.contains_key(col_name) {
                            merge_node_cols.insert(
                                col_name.to_string(),
                                other.node_cols.get(col_name).unwrap().clone(),
                            );
                        }
                        (
                            other.keys.as_ref(),
                            self.keys.as_ref(),
                            &other.values,
                            &self.values,
                        )
                    }
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
                    RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
                    tgt_idx_set.cloned(),
                    Some(merge_node_cols),
                );
            }
        };
        // iterate over columns in lh, if not in priority_map, merge columns
        for column in lh_batch.schema().fields().iter() {
            let col_name = column.name();
            if column_merge_priority_map
                .as_ref()
                .map_or(true, |map| map.contains_key(col_name) == false)
            {
                if default_node_cols.contains_key(col_name) {
                    merge_node_cols.insert(
                        col_name.to_string(),
                        default_node_cols.get(col_name).unwrap().clone(),
                    );
                }
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
            RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
            tgt_idx_set.cloned(),
            Some(merge_node_cols),
        )
    }

    // by default RecordBatch columns aren't nullable. This post-processing step makes them nullable
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

impl<'graph, V, G> TypedNodeState<'graph, V, G>
where
    V: NodeStateValue,
    G: GraphViewOps<'graph>,
{
    pub fn new(state: GenericNodeState<'graph, G>) -> Self {
        Self {
            state,
            converter: V::transform,
            _v_marker: PhantomData,
            _t_marker: PhantomData,
        }
    }
}

impl<
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
    > TypedNodeState<'graph, V, G, T>
{
    pub fn new_mapped(
        state: GenericNodeState<'graph, G>,
        converter: fn(&GenericNodeState<'graph, G>, V) -> T,
    ) -> Self {
        TypedNodeState {
            state,
            converter,
            _v_marker: PhantomData,
            _t_marker: PhantomData,
        }
    }

    pub fn to_hashmap<Func, TransformedType>(self, f: Func) -> HashMap<String, TransformedType>
    where
        Func: Fn(V) -> TransformedType,
    {
        self.into_iter()
            .map(|(node, value)| (node.name(), f(value)))
            .collect()
    }

    pub fn to_output_nodestate(self) -> OutputTypedNodeState<'graph, G> {
        TypedNodeState::new_mapped(self.state, GenericNodeState::get_nodes)
    }

    pub fn values_to_rows(&self) -> Vec<V> {
        let rows: Vec<V> = from_record_batch(&self.state.values).unwrap();
        rows
    }

    pub fn values_from_rows(&mut self, rows: Vec<V>) {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        self.state.values = to_record_batch(&fields, &rows).unwrap();
    }

    pub fn convert(&self, value: V) -> T {
        (self.converter)(&self.state, value)
    }
}

impl<
        'a,
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
    > PartialEq<TypedNodeState<'graph, V, G, T>> for TypedNodeState<'graph, V, G, T>
{
    fn eq(&self, other: &TypedNodeState<'graph, V, G, T>) -> bool {
        self.len() == other.len()
            && self
                .par_iter()
                .all(|(node, value)| other.get_by_node(node).map(|v| v == value).unwrap_or(false))
    }
}

impl<
        'graph,
        RHS: NodeStateValue + Send + Sync + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
    > PartialEq<Vec<RHS>> for TypedNodeState<'graph, RHS, G, T>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values_to_rows().par_iter().eq(other)
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for TypedNodeState<'graph, RHS, G, T>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|lhs| lhs == rhs).is_none() == false)
    }
}

impl<'graph, G: GraphViewOps<'graph>> OutputTypedNodeState<'graph, G> {
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
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
    > Debug for TypedNodeState<'graph, V, G, T>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
    > IntoIterator for TypedNodeState<'graph, V, G, T>
{
    type Item = (NodeView<'graph, G>, V);
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
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
    > NodeStateOps<'a, 'graph> for TypedNodeState<'graph, V, G, T>
{
    type Graph = G;
    type BaseGraph = G;
    type Select = Const<bool>;
    type Value = V;
    type OwnedValue = V;

    type OutputType = Self;

    fn graph(&self) -> &Self::Graph {
        &self.state.base_graph
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
            .map(move |i| RecordBatchIterator::get(&iter, i).unwrap())
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

    fn iter(&'a self) -> impl Iterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value)> + 'a {
        match &self.state.keys {
            Some(index) => index
                .iter()
                .zip(self.iter_values())
                .map(|(n, v)| (NodeView::new_internal(&self.state.base_graph, n), v))
                .into_dyn_boxed(),
            None => self
                .iter_values()
                .enumerate()
                .map(|(i, v)| (NodeView::new_internal(&self.state.base_graph, VID(i)), v))
                .into_dyn_boxed(),
        }
    }

    fn nodes(&self) -> Nodes<'graph, Self::Graph> {
        self.state.nodes()
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value)> {
        match &self.state.keys {
            Some(index) => Either::Left(
                index
                    .par_iter()
                    .zip(self.par_iter_values())
                    .map(|(n, v)| (NodeView::new_internal(&self.state.base_graph, n), v)),
            ),
            None => Either::Right(
                self.par_iter_values()
                    .enumerate()
                    .map(|(i, v)| (NodeView::new_internal(&self.state.base_graph, VID(i)), v)),
            ),
        }
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(NodeView<'a, &'a Self::Graph>, Self::Value)> {
        let vid = match &self.state.keys {
            Some(node_index) => node_index.key(index).unwrap(),
            None => VID(index),
        };
        Some((
            NodeView::new_internal(&self.state.base_graph, vid),
            self.get_by_node(vid).unwrap(), // &self.values[index],
        ))
    }

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let index = self.state.get_index_by_node(&node)?;
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

    fn construct(
        &self,
        base_graph: Self::BaseGraph,
        _graph: Self::Graph,
        keys: IndexSet<VID, ahash::RandomState>,
        values: Vec<Self::OwnedValue>,
    ) -> Self::OutputType
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        let state = GenericNodeState::new_from_eval_with_index(
            base_graph,
            values,
            Some(Index::new(keys)),
            Some(self.state.node_cols.clone()),
        );
        TypedNodeState::<'graph, V, Self::Graph, T> {
            state: state,
            converter: self.converter,
            _v_marker: PhantomData,
            _t_marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    pub fn new_from_eval_with_index<V: NodeStateValue>(
        graph: G,
        values: Vec<V>,
        index: Option<Index<VID>>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        Self::new_from_eval_with_index_mapped(graph, values, index, |v| v, node_cols)
    }

    pub fn new_from_eval_with_index_mapped<R: Clone, V: NodeStateValue>(
        graph: G,
        values: Vec<R>,
        index: Option<Index<VID>>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        let values: Vec<V> = values.into_iter().map(map).collect();
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        let values = Self::convert_recordbatch(to_record_batch(&fields, &values).unwrap()).unwrap();
        Self::new(graph, values, index, node_cols)
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        Self::new(
            graph.clone(),
            RecordBatch::new_empty(Schema::empty().into()),
            Some(Index::default()),
            None,
        )
    }

    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    pub fn new_from_eval<V: NodeStateValue>(
        graph: G,
        values: Vec<V>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        Self::new_from_eval_mapped(graph, values, |v| v, node_cols)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: Clone, V: NodeStateValue>(
        graph: G,
        values: Vec<R>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        let index = Index::for_graph(graph.clone());
        Self::new_from_eval_with_index_mapped(graph.clone(), values, index, map, node_cols)
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(
        graph: G,
        values: impl Into<RecordBatch>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(
            graph.clone(),
            Self::convert_recordbatch(values.into()).unwrap(),
            index,
            node_cols,
        )
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R: NodeStateValue, S: BuildHasher, V: NodeStateValue>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>)>>,
    ) -> Self {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new_from_values(graph, values, node_cols)
        } else {
            let (index, values): (IndexSet<VID, ahash::RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new(graph.clone(), values, Some(Index::new(index)), node_cols)
        }
    }
}
