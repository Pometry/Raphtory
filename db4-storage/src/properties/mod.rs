use crate::error::StorageError;
use bigdecimal::ToPrimitive;
use polars_arrow::array::{Array, BooleanArray, PrimitiveArray, Utf8ViewArray};
use raphtory_api::core::entities::properties::{
    meta::PropMapper,
    prop::{Prop, PropType},
};
use raphtory_core::{
    entities::{
        ELID,
        properties::{props::MetadataError, tcell::TCell, tprop::TPropCell},
    },
    storage::{PropColumn, TColumns, timeindex::TimeIndexEntry},
};

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
    ) -> Option<Box<dyn Array>> {
        match column {
            PropColumn::Empty(_) => None,
            PropColumn::U32(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::Bool(lazy_vec) => {
                Some(BooleanArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed())
            }
            PropColumn::U8(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::U16(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::U64(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::I32(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::I64(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::F32(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::F64(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| lazy_vec.get_opt(i).copied())).boxed(),
            ),
            PropColumn::Str(lazy_vec) => {
                let vec = indices
                    .map(|i| lazy_vec.get_opt(i).map(|str| str.as_ref()))
                    .collect::<Vec<_>>();

                Some(Utf8ViewArray::from_slice(&vec).boxed())
            }
            PropColumn::DTime(lazy_vec) => Some(
                PrimitiveArray::from_iter(
                    indices.map(|i| lazy_vec.get_opt(i).copied().map(|dt| dt.timestamp_millis())),
                )
                .to(polars_arrow::datatypes::ArrowDataType::Timestamp(
                    polars_arrow::datatypes::TimeUnit::Millisecond,
                    Some("UTC".to_string()),
                ))
                .boxed(),
            ),
            PropColumn::NDTime(lazy_vec) => Some(
                PrimitiveArray::from_iter(indices.map(|i| {
                    lazy_vec
                        .get_opt(i)
                        .copied()
                        .map(|dt| dt.and_utc().timestamp_millis())
                }))
                .to(polars_arrow::datatypes::ArrowDataType::Timestamp(
                    polars_arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ))
                .boxed(),
            ),
            PropColumn::Decimal(lazy_vec) => {
                let scale = meta
                    .get_dtype(col_id)
                    .and_then(|dtype| match dtype {
                        PropType::Decimal { scale } => Some(scale as usize),
                        _ => None,
                    })
                    .unwrap();
                Some(
                    PrimitiveArray::from_iter(indices.map(|i| {
                        lazy_vec.get_opt(i).and_then(|bd| {
                            let (num, _) = bd.as_bigint_and_scale();
                            num.to_i128()
                        })
                    }))
                    .to(polars_arrow::datatypes::ArrowDataType::Decimal(38, scale))
                    .boxed(),
                )
            }
            // PropColumn::Array(lazy_vec) => todo!(),
            // PropColumn::List(lazy_vec) => todo!(),
            // PropColumn::Map(lazy_vec) => todo!(),
            // PropColumn::NDTime(lazy_vec) => todo!(),
            // PropColumn::DTime(lazy_vec) => todo!(),
            // PropColumn::Decimal(lazy_vec) => todo!(),
            _ => None, //todo!("Unsupported column type"),
        }
    }

    pub fn take_t_column(
        &self,
        col_id: usize,
        meta: &PropMapper,
        indices: impl ExactSizeIterator<Item = usize>,
    ) -> Option<Box<dyn Array>> {
        let column = self.t_properties.get(col_id)?;
        self.column_as_array(column, col_id, meta, indices)
    }

    pub fn take_c_column(
        &self,
        col: usize,
        meta: &PropMapper,
        indices: impl Iterator<Item = usize>,
    ) -> Option<Box<dyn Array>> {
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

        if self.properties.times_from_props.len() <= self.row {
            self.properties
                .times_from_props
                .resize_with(self.row + 1, Default::default);
        }

        self.set_time(t, t_prop_row);

        self.properties.has_node_properties = true;
        self.properties.update_earliest_latest(t);
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
