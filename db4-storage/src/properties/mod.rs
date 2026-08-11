use crate::error::StorageError;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringViewArray, TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DECIMAL128_MAX_PRECISION;
use bigdecimal::ToPrimitive;
use raphtory_api::core::entities::properties::{
    meta::PropMapper,
    prop::{
        AsPropRef, Prop, PropRef, PropType, SerdeArrowList, SerdeArrowMap,
        arrow_dtype_from_prop_type, list_array_from_props, struct_array_from_props,
    },
};
use raphtory_core::{
    entities::{
        ELID,
        properties::{props::MetadataError, tcell::TCell, tprop::TPropCell},
    },
    storage::{PropColumn, TColumns, timeindex::EventTime},
};
use std::sync::Arc;

pub mod props_meta_writer;

#[derive(Debug, Default)]
pub struct Properties {
    c_properties: Vec<PropColumn>,

    additions: Vec<TCell<ELID>>,
    deletions: Vec<TCell<ELID>>,
    times_from_props: Vec<TCell<Option<usize>>>,

    t_properties: TColumns,
    earliest: Option<EventTime>,
    latest: Option<EventTime>,
    has_additions: bool,
    has_properties: bool,
    has_deletions: bool,
    pub additions_count: usize,
    pub deletions_count: usize,
}

pub(crate) struct PropMutEntry<'a> {
    row: usize,
    properties: &'a mut Properties,
}

#[derive(Debug, Clone, Copy)]
pub struct PropEntry<'a> {
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

    pub(crate) fn get_entry(&self, row: usize) -> PropEntry<'_> {
        PropEntry {
            row,
            properties: self,
        }
    }

    pub fn earliest(&self) -> Option<EventTime> {
        self.earliest
    }

    pub fn latest(&self) -> Option<EventTime> {
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

    pub fn has_properties(&self) -> bool {
        self.has_properties
    }

    pub fn set_has_properties(&mut self) {
        self.has_properties = true
    }

    pub fn has_additions(&self) -> bool {
        self.has_additions
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
    ) -> Result<Option<ArrayRef>, StorageError> {
        match column {
            PropColumn::Empty(_) => Ok(None),
            PropColumn::U32(lazy_vec) => Ok(Some(Arc::new(UInt32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::Bool(lazy_vec) => Ok(Some(Arc::new(BooleanArray::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::U8(lazy_vec) => Ok(Some(Arc::new(UInt8Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::U16(lazy_vec) => Ok(Some(Arc::new(UInt16Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::U64(lazy_vec) => Ok(Some(Arc::new(UInt64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::I32(lazy_vec) => Ok(Some(Arc::new(Int32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::I64(lazy_vec) => Ok(Some(Arc::new(Int64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::F32(lazy_vec) => Ok(Some(Arc::new(Float32Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::F64(lazy_vec) => Ok(Some(Arc::new(Float64Array::from_iter(
                indices.map(|i| lazy_vec.get_opt(i).copied()),
            )))),
            PropColumn::Str(lazy_vec) => Ok(Some(Arc::new(StringViewArray::from_iter(
                indices.map(|i| lazy_vec.get_opt(i)),
            )))),
            PropColumn::DTime(lazy_vec) => Ok(Some(Arc::new(
                TimestampMillisecondArray::from_iter(
                    indices.map(|i| lazy_vec.get_opt(i).copied().map(|dt| dt.timestamp_millis())),
                )
                .with_timezone("UTC"),
            ))),
            PropColumn::NDTime(lazy_vec) => Ok(Some(Arc::new(
                TimestampMillisecondArray::from_iter(indices.map(|i| {
                    lazy_vec
                        .get_opt(i)
                        .copied()
                        .map(|dt| dt.and_utc().timestamp_millis())
                })),
            ))),
            PropColumn::Decimal(lazy_vec) => {
                let prop_type = meta.get_dtype(col_id).ok_or_else(|| {
                    StorageError::GenericFailure(format!(
                        "Missing dtype for decimal column {col_id}"
                    ))
                })?;

                let PropType::Decimal { scale } = prop_type else {
                    return Err(StorageError::GenericFailure(format!(
                        "Expected Decimal dtype for decimal column {col_id}, found {prop_type:?}"
                    )));
                };

                let array = Decimal128Array::from_iter(indices.map(|i| {
                    lazy_vec.get_opt(i).and_then(|big_decimal| {
                        let (num, _) = big_decimal.as_bigint_and_scale();
                        num.to_i128()
                    })
                }))
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale as i8)
                .map_err(StorageError::ArrowRS)?;

                Ok(Some(Arc::new(array)))
            }
            PropColumn::Map(lazy_vec) => {
                let prop_type = meta.get_dtype(col_id).ok_or_else(|| {
                    StorageError::GenericFailure(format!("Missing dtype for map column {col_id}"))
                })?;

                let dt = arrow_dtype_from_prop_type(&prop_type);
                let array_iter = indices
                    .map(|i| lazy_vec.get_opt(i))
                    .map(|e| e.map(|m| SerdeArrowMap(m)));

                let struct_array = struct_array_from_props(&dt, array_iter).map_err(|e| {
                    StorageError::GenericFailure(format!(
                        "Failed to build struct array for column{col_id}: {e}"
                    ))
                })?;

                Ok(Some(Arc::new(struct_array)))
            }
            PropColumn::List(lazy_vec) => {
                let prop_type = meta.get_dtype(col_id).ok_or_else(|| {
                    StorageError::GenericFailure(format!("Missing dtype for list column {col_id}"))
                })?;

                let dt = arrow_dtype_from_prop_type(&prop_type);
                let array_iter = indices
                    .map(|i| lazy_vec.get_opt(i))
                    .map(|opt_list| opt_list.map(SerdeArrowList));

                let list_array = list_array_from_props(&dt, array_iter).map_err(|e| {
                    StorageError::GenericFailure(format!(
                        "Failed to build list array for column {col_id}: {e}"
                    ))
                })?;

                Ok(Some(Arc::new(list_array)))
            }
        }
    }

    /// Convert the temporal property column with `col_id` into an Arrow array.
    pub fn take_t_column(
        &self,
        col_id: usize,
        meta: &PropMapper,
        indices: impl ExactSizeIterator<Item = usize>,
    ) -> Result<Option<ArrayRef>, StorageError> {
        let Some(column) = self.t_properties.get(col_id) else {
            return Ok(None);
        };

        self.column_as_array(column, col_id, meta, indices)
    }

    /// Convert the constant property column with `col_id` into an Arrow array.
    pub fn take_c_column(
        &self,
        col_id: usize,
        meta: &PropMapper,
        indices: impl Iterator<Item = usize>,
    ) -> Result<Option<ArrayRef>, StorageError> {
        let Some(column) = self.c_properties.get(col_id) else {
            return Ok(None);
        };

        self.column_as_array(column, col_id, meta, indices)
    }

    fn update_earliest_latest(&mut self, t: EventTime) {
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

    pub fn t_len(&self) -> usize {
        self.t_properties.len()
    }

    pub fn deletions_count(&self) -> usize {
        self.deletions_count
    }

    pub fn num_updates(&self) -> usize {
        self.t_properties.len()
            + self
                .deletions
                .iter()
                .map(|tcell| tcell.len())
                .sum::<usize>()
    }
}

impl<'a> PropMutEntry<'a> {
    pub(crate) fn append_t_props<P: AsPropRef>(
        &mut self,
        t: EventTime,
        props: impl IntoIterator<Item = (usize, P)>,
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

        self.properties.has_properties = true;
        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn ensure_times_from_props(&mut self) {
        if self.properties.times_from_props.len() <= self.row {
            self.properties
                .times_from_props
                .resize_with(self.row + 1, Default::default);
        }
    }

    pub(crate) fn set_time(&mut self, t: EventTime, t_prop_row: usize) {
        let prop_timestamps = &mut self.properties.times_from_props[self.row];
        prop_timestamps.set(t, Some(t_prop_row));
    }

    pub(crate) fn addition_timestamp(&mut self, t: EventTime, edge_id: ELID) {
        if self.properties.additions.len() <= self.row {
            self.properties
                .additions
                .resize_with(self.row + 1, Default::default);
        }

        self.properties.has_additions = true;
        let prop_timestamps = &mut self.properties.additions[self.row];
        prop_timestamps.set(t, edge_id);

        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn deletion_timestamp(&mut self, t: EventTime, edge_id: Option<ELID>) {
        if self.properties.deletions.len() <= self.row {
            self.properties
                .deletions
                .resize_with(self.row + 1, Default::default);
        }

        self.properties.has_deletions = true;
        self.properties.deletions_count += 1;

        let prop_timestamps = &mut self.properties.deletions[self.row];
        prop_timestamps.set(t, edge_id.unwrap_or_default());
        self.properties.update_earliest_latest(t);
    }

    pub(crate) fn append_const_props<P: AsPropRef>(
        &mut self,
        props: impl IntoIterator<Item = (usize, P)>,
    ) {
        for (prop_id, prop) in props {
            if self.properties.c_properties.len() <= prop_id {
                self.properties
                    .c_properties
                    .resize_with(prop_id + 1, Default::default);
            }
            let const_props = &mut self.properties.c_properties[prop_id];
            // property types should have been validated before!
            const_props.upsert(self.row, prop.as_prop_ref()).unwrap();
        }
    }
}

impl<'a> PropEntry<'a> {
    pub(crate) fn prop(self, prop_id: usize) -> Option<TPropCell<'a>> {
        let t_cell = self.t_cell();
        Some(TPropCell::new(t_cell, self.properties.t_column(prop_id)))
    }

    pub fn metadata(self, prop_id: usize) -> Option<Prop> {
        self.properties.c_column(prop_id)?.get(self.row)
    }

    pub fn check_metadata(self, prop_id: usize, new_val: PropRef<'_>) -> Result<(), StorageError> {
        if let Some(col) = self.properties.c_column(prop_id) {
            col.check(self.row, &new_val)
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
