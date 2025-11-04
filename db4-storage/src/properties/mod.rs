use crate::error::StorageError;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringViewArray, StructArray, TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DECIMAL128_MAX_PRECISION, Field, Fields};
use bigdecimal::ToPrimitive;
use raphtory_api::core::entities::properties::{
    meta::PropMapper,
    prop::{Prop, PropType, SerdeMap, SerdeProp, arrow_dtype_from_prop_type},
};
use raphtory_core::{
    entities::{
        ELID,
        properties::{props::MetadataError, tcell::TCell, tprop::TPropCell},
    },
    storage::{PropColumn, TColumns, timeindex::TimeIndexEntry},
};
use serde_arrow::ArrayBuilder;
use std::sync::Arc;

pub mod props_meta_writer;

#[derive(Debug, Default, serde::Serialize)]
pub struct Properties {
    c_properties: Vec<PropColumn>,

    additions: Vec<TCell<ELID>>,
    deletions: Vec<TCell<ELID>>,
    times_from_props: Vec<TCell<Option<usize>>>,

    t_properties: TColumns,
    earliest: Option<TimeIndexEntry>,
    latest: Option<TimeIndexEntry>,
    has_node_additions: bool,
    has_node_properties: bool,
    has_deletions: bool,
    pub additions_count: usize,
}

pub(crate) struct PropMutEntry<'a> {
    row: usize,
    properties: &'a mut Properties,
}

#[derive(Debug, Clone, Copy)]
pub struct RowEntry<'a> {
    row: usize,
    properties: &'a Properties,
}

impl Properties {
    pub fn est_size(&self) -> usize {
        self.t_properties.len() + self.c_properties.len()
    }

    pub(crate) fn get_mut_entry(&mut self, row: usize) -> PropMutEntry<'_> {
        PropMutEntry {
            row,
            properties: self,
        }
    }

    pub(crate) fn get_entry(&self, row: usize) -> RowEntry<'_> {
        RowEntry {
            row,
            properties: self,
        }
    }

    pub fn earliest(&self) -> Option<TimeIndexEntry> {
        self.earliest
    }

    pub fn latest(&self) -> Option<TimeIndexEntry> {
        self.latest
    }

    pub fn t_column(&self, prop_id: usize) -> Option<&PropColumn> {
        self.t_properties.get(prop_id)
    }

    pub fn t_column_mut(&mut self, prop_id: usize) -> Option<&mut PropColumn> {
        self.t_properties.get_mut(prop_id)
    }

    pub fn c_column(&self, prop_id: usize) -> Option<&PropColumn> {
        self.c_properties.get(prop_id)
    }

    pub fn num_t_columns(&self) -> usize {
        self.t_properties.num_columns()
    }

    pub fn num_c_columns(&self) -> usize {
        self.c_properties.len()
    }

    pub(crate) fn additions(&self, row: usize) -> Option<&TCell<ELID>> {
        self.additions.get(row)
    }

    pub(crate) fn deletions(&self, row: usize) -> Option<&TCell<ELID>> {
        self.deletions.get(row)
    }

    pub(crate) fn times_from_props(&self, row: usize) -> Option<&TCell<Option<usize>>> {
        self.times_from_props.get(row)
    }

    pub fn has_node_properties(&self) -> bool {
        self.has_node_properties
    }

    pub fn has_node_additions(&self) -> bool {
        self.has_node_additions
    }

    pub fn has_deletions(&self) -> bool {
        self.has_deletions
    }

    pub(crate) fn column_as_array(
        &self,
        column: &PropColumn,
        col_id: usize,
        meta: &PropMapper,
        indices: impl Iterator<Item = usize>,
    ) -> Option<ArrayRef> {
        match column {
            PropColumn::Empty(_) => None,
            PropColumn::U32(lazy_vec) => Some(Arc::new(UInt32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::Bool(lazy_vec) => Some(Arc::new(BooleanArray::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::U8(lazy_vec) => Some(Arc::new(UInt8Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::U16(lazy_vec) => Some(Arc::new(UInt16Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::U64(lazy_vec) => Some(Arc::new(UInt64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::I32(lazy_vec) => Some(Arc::new(Int32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::I64(lazy_vec) => Some(Arc::new(Int64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::F32(lazy_vec) => Some(Arc::new(Float32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::F64(lazy_vec) => Some(Arc::new(Float64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            ))),
            PropColumn::Str(lazy_vec) => Some(Arc::new(StringViewArray::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).map(|str| str.as_ref())),
            ))),
            PropColumn::DTime(lazy_vec) => Some(Arc::new(
                TimestampMillisecondArray::from_iter(
                    indices.map(|i| lazy_vec.get_opt(i).copied().map(|dt| dt.timestamp_millis())),
                )
                .with_timezone("UTC"),
            )),
            PropColumn::NDTime(lazy_vec) => Some(Arc::new(TimestampMillisecondArray::from_iter(
                indices.map(|i| {
                    lazy_vec
                        .get_opt(i)
                        .copied()
                        .map(|dt| dt.and_utc().timestamp_millis())
                }),
            ))),
            PropColumn::Decimal(lazy_vec) => {
                let scale = meta
                    .get_dtype(col_id)
                    .and_then(|dtype| match dtype {
                        PropType::Decimal { scale } => Some(scale as i8),
                        _ => None,
                    })
                    .unwrap();
                Some(Arc::new(
                    Decimal128Array::from_iter(indices.map(|i| {
                        lazy_vec.get_opt(i).and_then(|bd| {
                            let (num, _) = bd.as_bigint_and_scale();
                            num.to_i128()
                        })
                    }))
                    .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale)
                    .unwrap(),
                ))
            }
            // PropColumn::Array(lazy_vec) => todo!(),
            // PropColumn::List(lazy_vec) => todo!(),
            PropColumn::Map(lazy_vec) => {
                let dt = meta
                    .get_dtype(col_id)
                    .as_ref()
                    .map(arrow_dtype_from_prop_type)
                    .unwrap();
                let fields = match dt {
                    arrow::datatypes::DataType::Struct(fields) => fields,
                    _ => panic!("Expected Struct data type for Map property"),
                };
                let array_iter = indices.map(|i| lazy_vec.get_opt(i).cloned());

                let mut builder = ArrayBuilder::from_arrow(&fields).unwrap();

                for prop in array_iter {
                    builder.push(prop.as_ref().map(|m| SerdeMap(m))).unwrap();
                }

                let arrays = builder.to_arrow().unwrap();
                let struct_array = StructArray::new(fields, arrays, None);

                Some(Arc::new(struct_array))
            }
            _ => None, //todo!("Unsupported column type"),
        }
    }

    pub fn take_t_column(
        &self,
        col_id: usize,
        meta: &PropMapper,
        indices: impl ExactSizeIterator<Item = usize>,
    ) -> Option<ArrayRef> {
        let column = self.t_properties.get(col_id)?;
        self.column_as_array(column, col_id, meta, indices)
    }

    pub fn take_c_column(
        &self,
        col: usize,
        meta: &PropMapper,
        indices: impl Iterator<Item = usize>,
    ) -> Option<ArrayRef> {
        let column = self.c_properties.get(col)?;
        self.column_as_array(column, col, meta, indices)
    }

    fn update_earliest_latest(&mut self, t: TimeIndexEntry) {
        self.additions_count += 1;
        let earliest = self.earliest.get_or_insert(t);
        if t < *earliest {
            *earliest = t;
        }
        let latest = self.latest.get_or_insert(t);
        if t > *latest {
            *latest = t;
        }
    }

    pub(crate) fn t_len(&self) -> usize {
        self.t_properties.len()
    }

    pub(crate) fn t_properties_mut(&mut self) -> &mut TColumns {
        &mut self.t_properties
    }

    pub(crate) fn reset_t_len(&mut self) {
        self.t_properties.reset_len();
    }
}

impl<'a> PropMutEntry<'a> {
    pub(crate) fn append_t_props(
        &mut self,
        t: TimeIndexEntry,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) {
        let t_prop_row = if let Some(t_prop_row) = self
            .properties
            .t_properties
            .push(props)
            .expect("Internal error: properties should be validated at this point")
        {
            t_prop_row
        } else {
            self.properties.t_properties.push_null()
        };

        self.ensure_times_from_props();
        self.set_time(t, t_prop_row);

        self.properties.has_node_properties = true;
        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn ensure_times_from_props(&mut self) {
        if self.properties.times_from_props.len() <= self.row {
            self.properties
                .times_from_props
                .resize_with(self.row + 1, Default::default);
        }
    }

    pub(crate) fn set_time(&mut self, t: TimeIndexEntry, t_prop_row: usize) {
        let prop_timestamps = &mut self.properties.times_from_props[self.row];
        prop_timestamps.set(t, Some(t_prop_row));
    }

    pub(crate) fn addition_timestamp(&mut self, t: TimeIndexEntry, edge_id: ELID) {
        if self.properties.additions.len() <= self.row {
            self.properties
                .additions
                .resize_with(self.row + 1, Default::default);
        }

        self.properties.has_node_additions = true;
        let prop_timestamps = &mut self.properties.additions[self.row];
        prop_timestamps.set(t, edge_id);

        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn deletion_timestamp(&mut self, t: TimeIndexEntry, edge_id: Option<ELID>) {
        if self.properties.deletions.len() <= self.row {
            self.properties
                .deletions
                .resize_with(self.row + 1, Default::default);
        }

        self.properties.has_deletions = true;

        let prop_timestamps = &mut self.properties.deletions[self.row];
        prop_timestamps.set(t, edge_id.unwrap_or_default());
        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn append_const_props(&mut self, props: impl IntoIterator<Item = (usize, Prop)>) {
        for (prop_id, prop) in props {
            if self.properties.c_properties.len() <= prop_id {
                self.properties
                    .c_properties
                    .resize_with(prop_id + 1, Default::default);
            }
            let const_props = &mut self.properties.c_properties[prop_id];
            // property types should have been validated before!
            const_props.upsert(self.row, prop.clone()).unwrap();
        }
    }
}

impl<'a> RowEntry<'a> {
    pub(crate) fn prop(self, prop_id: usize) -> Option<TPropCell<'a>> {
        let t_cell = self.t_cell();
        Some(TPropCell::new(t_cell, self.properties.t_column(prop_id)))
    }

    pub fn metadata(self, prop_id: usize) -> Option<Prop> {
        self.properties.c_column(prop_id)?.get(self.row)
    }

    pub fn check_metadata(self, prop_id: usize, new_val: &Prop) -> Result<(), StorageError> {
        if let Some(col) = self.properties.c_column(prop_id) {
            col.check(self.row, new_val)
                .map_err(Into::<MetadataError>::into)?;
        }
        Ok(())
    }

    pub fn t_cell(self) -> &'a TCell<Option<usize>> {
        self.properties
            .times_from_props(self.row)
            .unwrap_or(&TCell::Empty)
    }

    pub fn additions(self) -> &'a TCell<ELID> {
        self.properties.additions(self.row).unwrap_or(&TCell::Empty)
    }

    pub fn deletions(self) -> &'a TCell<ELID> {
        self.properties.deletions(self.row).unwrap_or(&TCell::Empty)
    }
}
