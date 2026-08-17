use crate::core::{
    entities::{
        properties::prop::{check_for_unification, unify_types, PropError, PropType},
        LayerId, LayerIds,
    },
    storage::{
        arc_str::ArcStr,
        dict_mapper::{DictMapper, LockedDictMapper, MaybeNew, PublicKeys, WriteLockedDictMapper},
    },
};
use itertools::Either;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

// Internal const props for node id and type
pub const NODE_ID_PROP_KEY: &str = "_raphtory_node_id";
pub const NODE_ID_IDX: usize = 0;

pub const NODE_TYPE_PROP_KEY: &str = "_raphtory_node_type";
pub const NODE_TYPE_IDX: usize = 1;

pub const STATIC_GRAPH_LAYER_NAME: &str = "_static_graph";
pub const STATIC_GRAPH_LAYER_ID: LayerId = LayerId(0);

pub const STATIC_GRAPH_LAYER: LayerIds = LayerIds::One(STATIC_GRAPH_LAYER_ID);

/// The type ID for nodes that don't have a specified type.
pub const DEFAULT_NODE_TYPE_ID: usize = 0;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Meta {
    temporal_prop_mapper: PropMapper,
    metadata_mapper: PropMapper,
    layer_mapper: DictMapper,
    node_type_mapper: DictMapper,
}

impl Meta {
    pub fn all_layer_iter(&self) -> impl Iterator<Item = (LayerId, ArcStr)> + use<'_> {
        self.layer_mapper
            .all_ids()
            .map(LayerId)
            .zip(self.layer_mapper.all_keys())
    }

    pub fn set_metadata_mapper(&mut self, meta: PropMapper) {
        self.metadata_mapper = meta;
    }

    pub fn set_temporal_prop_mapper(&mut self, meta: PropMapper) {
        self.temporal_prop_mapper = meta;
    }

    pub fn set_layer_mapper(&mut self, meta: DictMapper) {
        self.layer_mapper = meta;
    }
    pub fn metadata_mapper(&self) -> &PropMapper {
        &self.metadata_mapper
    }

    pub fn temporal_prop_mapper(&self) -> &PropMapper {
        &self.temporal_prop_mapper
    }

    pub fn layer_meta(&self) -> &DictMapper {
        &self.layer_mapper
    }

    pub fn node_type_meta(&self) -> &DictMapper {
        &self.node_type_mapper
    }

    #[inline]
    pub fn temporal_est_row_size(&self) -> usize {
        self.temporal_prop_mapper.row_size()
    }

    #[inline]
    pub fn const_est_row_size(&self) -> usize {
        self.metadata_mapper.row_size()
    }

    pub fn new_for_nodes() -> Self {
        let meta_layer = DictMapper::new_layer_mapper();
        let meta_node_type = DictMapper::default();
        meta_node_type.get_or_create_id("_default");

        Self {
            temporal_prop_mapper: PropMapper::default(),
            metadata_mapper: PropMapper::new_with_private_fields(
                [NODE_ID_PROP_KEY, NODE_TYPE_PROP_KEY],
                [PropType::Empty, PropType::U64],
            ),
            layer_mapper: meta_layer,
            node_type_mapper: meta_node_type, // type 0 is the default type for a node
        }
    }

    pub fn new_for_edges() -> Self {
        let meta_layer = DictMapper::new_layer_mapper();
        let meta_node_type = DictMapper::default();
        meta_node_type.get_or_create_id("_default");

        Self {
            temporal_prop_mapper: PropMapper::default(),
            metadata_mapper: PropMapper::default(),
            layer_mapper: meta_layer,
            node_type_mapper: meta_node_type, // type 0 is the default type for a node
        }
    }

    pub fn new_for_graph_props() -> Self {
        let meta_layer = DictMapper::new_layer_mapper();
        let meta_node_type = DictMapper::default();

        // For now, only temporal and metadata mappers are used for graph metadata.
        Self {
            temporal_prop_mapper: PropMapper::default(),
            metadata_mapper: PropMapper::default(),
            layer_mapper: meta_layer,
            node_type_mapper: meta_node_type,
        }
    }

    #[inline]
    pub fn resolve_prop_id(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, PropError> {
        if is_static {
            self.metadata_mapper.get_or_create_and_validate(prop, dtype)
        } else {
            self.temporal_prop_mapper
                .get_or_create_and_validate(prop, dtype)
        }
    }

    pub fn get_prop_id(&self, name: &str, is_static: bool) -> Option<usize> {
        if is_static {
            self.metadata_mapper.get_id(name)
        } else {
            self.temporal_prop_mapper.get_id(name)
        }
    }

    pub fn get_prop_id_and_type(&self, name: &str, is_static: bool) -> Option<(usize, PropType)> {
        if is_static {
            self.metadata_mapper.get_id_and_dtype(name)
        } else {
            self.temporal_prop_mapper.get_id_and_dtype(name)
        }
    }

    #[inline]
    pub fn get_or_create_layer_id(&self, name: Option<&str>) -> MaybeNew<LayerId> {
        self.layer_mapper
            .get_or_create_id(name.unwrap_or("_default"))
            .map(LayerId)
    }

    #[inline]
    pub fn get_default_node_type_id(&self) -> usize {
        DEFAULT_NODE_TYPE_ID
    }

    #[inline]
    pub fn get_or_create_node_type_id(&self, node_type: &str) -> MaybeNew<usize> {
        self.node_type_mapper.get_or_create_id(node_type)
    }

    #[inline]
    pub fn get_layer_id(&self, name: &str) -> Option<LayerId> {
        self.layer_mapper.get_id(name).map(LayerId)
    }

    #[inline]
    pub fn get_default_layer_id(&self) -> Option<LayerId> {
        self.layer_mapper.get_id("_default").map(LayerId)
    }

    #[inline]
    pub fn get_node_type_id(&self, node_type: &str) -> Option<usize> {
        self.node_type_mapper.get_id(node_type)
    }

    pub fn get_layer_name_by_id(&self, id: LayerId) -> ArcStr {
        self.layer_mapper.get_name(id.0)
    }

    pub fn get_node_type_name_by_id(&self, id: usize) -> Option<ArcStr> {
        if id == DEFAULT_NODE_TYPE_ID {
            None
        } else {
            Some(self.node_type_mapper.get_name(id))
        }
    }

    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.node_type_mapper
            .keys()
            .iter()
            .filter_map(|key| {
                if key != "_default" {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_all_property_names(&self, is_static: bool) -> PublicKeys<ArcStr> {
        if is_static {
            self.metadata_mapper.keys()
        } else {
            self.temporal_prop_mapper.keys()
        }
    }

    pub fn get_prop_name(&self, prop_id: usize, is_static: bool) -> ArcStr {
        if is_static {
            self.metadata_mapper.get_name(prop_id)
        } else {
            self.temporal_prop_mapper.get_name(prop_id)
        }
    }

    /// O(1) check: has temporal-prop `prop_id` been observed in `layer_id`?
    #[inline]
    pub fn temporal_layer_has(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.temporal_prop_mapper.layer_has(layer_id, prop_id)
    }

    /// O(1) check: has metadata-prop `prop_id` been observed in `layer_id`?
    #[inline]
    pub fn metadata_layer_has(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.metadata_mapper.layer_has(layer_id, prop_id)
    }
}

/// Manages the mapping of property names to their IDs and types.
#[derive(Default, Serialize, Deserialize)]
pub struct PropMapper {
    /// Maps property names to their IDs.
    id_mapper: DictMapper,

    /// Property types indexed by property ID.
    dtypes: Arc<RwLock<Vec<PropType>>>,

    /// Estimated size in bytes of a single row of properties maintained by this mapper.
    row_size: AtomicUsize,

    /// Per-layer property presence bitset; `layer_prop_presence[layer_id][prop_id]`
    /// is true iff this property has been observed in this layer
    layer_prop_presence: Arc<RwLock<Vec<Vec<bool>>>>,
}

impl Debug for PropMapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (k, (id, dtype)) in self
            .all_keys()
            .iter()
            .zip(self.all_ids().zip(self.d_types().iter()))
        {
            write!(f, "{k}: ({id}, {dtype:?}), ")?;
        }
        f.write_str("}")
    }
}

impl Deref for PropMapper {
    type Target = DictMapper;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.id_mapper
    }
}

impl PropMapper {
    pub fn new_with_private_fields(
        fields: impl IntoIterator<Item = impl Into<ArcStr>>,
        dtypes: impl IntoIterator<Item = PropType>,
    ) -> Self {
        let dtypes = Vec::from_iter(dtypes);
        let row_size = dtypes.iter().map(|dtype| dtype.est_size()).sum();

        PropMapper {
            id_mapper: DictMapper::new_with_private_fields(fields),
            row_size: AtomicUsize::new(row_size),
            dtypes: Arc::new(RwLock::new(dtypes)),
            layer_prop_presence: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// O(1) check: has property `prop_id` ever been observed in `layer_id`?
    /// `false` is authoritative; callers can safely skip column reads for
    /// this (layer, prop). `true` means at least one entity in `layer_id`
    /// has prop `prop_id`.
    #[inline]
    pub fn layer_has(&self, layer_id: LayerId, prop_id: usize) -> bool {
        self.layer_prop_presence
            .read_recursive()
            .get(layer_id.0)
            .and_then(|row| row.get(prop_id))
            .copied()
            .unwrap_or(false)
    }

    /// Mark `prop_id` as present in `layer_id`. Only takes the write lock once per (layer, prop)
    pub fn mark_prop_in_layer(&self, layer_id: LayerId, prop_id: usize) {
        if self.layer_has(layer_id, prop_id) {
            return;
        }
        let mut guard = self.layer_prop_presence.write();
        ensure_and_set(&mut guard, layer_id.0, prop_id);
    }

    /// Mark a whole set of `(layer, prop)` pairs at once, taking the write lock
    /// at most once for the entire set.
    pub fn mark_props_in_layers(
        &self,
        layers: impl IntoIterator<Item = LayerId>,
        prop_ids: &[usize],
    ) {
        if prop_ids.is_empty() {
            return;
        }
        // collect first so the common "already marked" case takes no write lock
        let missing: Vec<_> = layers
            .into_iter()
            .filter(|&layer| prop_ids.iter().any(|&p| !self.layer_has(layer, p)))
            .collect();
        if missing.is_empty() {
            return;
        }
        let mut guard = self.layer_prop_presence.write();
        for layer in missing {
            for &prop_id in prop_ids {
                ensure_and_set(&mut guard, layer.0, prop_id);
            }
        }
    }

    pub fn d_types(&self) -> impl Deref<Target = Vec<PropType>> + '_ {
        self.dtypes.read_recursive()
    }

    pub fn deep_clone(&self) -> Self {
        let dtypes = self.dtypes.read_recursive().clone();
        let layer_presence = self.layer_prop_presence.read_recursive().clone();
        Self {
            id_mapper: self.id_mapper.deep_clone(),
            row_size: AtomicUsize::new(self.row_size.load(std::sync::atomic::Ordering::Relaxed)),
            dtypes: Arc::new(RwLock::new(dtypes)),
            layer_prop_presence: Arc::new(RwLock::new(layer_presence)),
        }
    }

    #[inline]
    pub fn row_size(&self) -> usize {
        self.row_size.load(atomic::Ordering::Relaxed)
    }

    pub fn get_id_and_dtype(&self, prop: &str) -> Option<(usize, PropType)> {
        self.get_id(prop).map(|id| {
            let existing_dtype = self
                .get_dtype(id)
                .expect("Existing id should always have a dtype");
            (id, existing_dtype)
        })
    }

    pub fn get_or_create_and_validate(
        &self,
        prop: &str,
        dtype: PropType,
    ) -> Result<MaybeNew<usize>, PropError> {
        let wrapped_id = self.id_mapper.get_or_create_id(prop);
        let id = wrapped_id.inner();
        let dtype_read = self.dtypes.read_recursive();

        if let Some(old_type) = dtype_read.get(id) {
            let mut unified = false;

            if unify_types(&dtype, old_type, &mut unified).is_ok() {
                if !unified {
                    // means the types were equal, no change needed
                    return Ok(wrapped_id);
                }
            } else {
                return Err(PropError {
                    name: prop.to_owned(),
                    expected: old_type.clone(),
                    actual: dtype,
                });
            }
        }

        // Drop the read lock and grab the write lock in order to add the new
        // prop type or unify the existing prop type.
        drop(dtype_read);

        let mut dtype_write = self.dtypes.write();

        match dtype_write.get(id).cloned() {
            Some(old_type) => {
                let mut unified = false;

                if let Ok(tpe) = unify_types(&dtype, &old_type, &mut unified) {
                    if unified {
                        // The row size needs to account for the difference in sizes
                        // between the newly unified type and the old type.
                        let delta = tpe.est_size() - old_type.est_size();
                        self.row_size.fetch_add(delta, atomic::Ordering::Relaxed);
                    }

                    dtype_write[id] = tpe;
                    Ok(wrapped_id)
                } else {
                    Err(PropError {
                        name: prop.to_owned(),
                        expected: old_type,
                        actual: dtype,
                    })
                }
            }
            None => {
                // vector not resized yet; resize it, set the new dtype and return the id.
                dtype_write.resize(id + 1, PropType::Empty);

                self.row_size
                    .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);

                dtype_write[id] = dtype;
                Ok(wrapped_id)
            }
        }
    }

    pub fn set_id_and_dtype(&self, key: impl Into<ArcStr>, id: usize, dtype: PropType) {
        self.set_id(key, id);
        self.set_dtype(id, dtype);
    }

    pub fn set_dtype(&self, id: usize, dtype: PropType) {
        let mut dtypes = self.dtypes.write();
        if dtypes.len() <= id {
            dtypes.resize(id + 1, PropType::Empty);
        }
        self.row_size
            .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);
        dtypes[id] = dtype;
    }

    pub fn get_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.dtypes.read_recursive().get(prop_id).cloned()
    }

    pub fn locked(&self) -> LockedPropMapper<'_> {
        LockedPropMapper {
            dict_mapper: self.id_mapper.read(),
            d_types: self.dtypes.read_recursive(),
        }
    }

    pub fn write_locked(&self) -> WriteLockedPropMapper<'_> {
        WriteLockedPropMapper {
            dict_mapper: self.id_mapper.write(),
            d_types: self.dtypes.write(),
            row_size: &self.row_size,
            layer_presence: self.layer_prop_presence.write(),
        }
    }
}

#[inline]
fn ensure_and_set(presence: &mut Vec<Vec<bool>>, layer_idx: usize, prop_id: usize) {
    if presence.len() <= layer_idx {
        presence.resize_with(layer_idx + 1, Vec::new);
    }
    let row = &mut presence[layer_idx];
    if row.len() <= prop_id {
        row.resize(prop_id + 1, false);
    }
    row[prop_id] = true;
}

/// Write-locked view of a [`PropMapper`].
pub struct WriteLockedPropMapper<'a> {
    /// Maps property names to their IDs.
    dict_mapper: WriteLockedDictMapper<'a>,

    /// Property types indexed by property ID.
    d_types: RwLockWriteGuard<'a, Vec<PropType>>,

    /// Estimated size in bytes of a single row of properties maintained by this mapper.
    row_size: &'a AtomicUsize,

    /// Per-layer property presence bitset.
    layer_presence: RwLockWriteGuard<'a, Vec<Vec<bool>>>,
}

impl<'a> WriteLockedPropMapper<'a> {
    pub fn new_id_and_dtype(&mut self, key: impl Into<ArcStr>, dtype: PropType) -> usize {
        let id = self.dict_mapper.get_or_create_id(&key.into());
        let dtypes = self.d_types.deref_mut();

        if dtypes.len() <= id.inner() {
            dtypes.resize(id.inner() + 1, PropType::Empty);
        }

        self.row_size
            .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);

        dtypes[id.inner()] = dtype;
        id.inner()
    }

    pub fn set_id_and_dtype(&mut self, key: impl Into<ArcStr>, id: usize, dtype: PropType) {
        self.dict_mapper.set_id(key, id);
        self.set_dtype(id, dtype);
    }

    pub fn set_dtype(&mut self, id: usize, dtype: PropType) {
        let dtypes = self.d_types.deref_mut();

        if dtypes.len() <= id {
            dtypes.resize(id + 1, PropType::Empty);
        }

        self.row_size
            .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);

        dtypes[id] = dtype;
    }

    pub fn set_or_unify_id_and_dtype(
        &mut self,
        key: impl Into<ArcStr>,
        id: usize,
        dtype: PropType,
    ) -> Result<(), PropError> {
        self.dict_mapper.set_id(key, id);
        self.set_or_unify_dtype(id, dtype)
    }

    pub fn set_or_unify_dtype(&mut self, id: usize, dtype: PropType) -> Result<(), PropError> {
        let dtypes = self.d_types.deref_mut();

        match dtypes.get_mut(id) {
            None => {
                dtypes.resize(id + 1, PropType::Empty);

                self.row_size
                    .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);

                dtypes[id] = dtype;
            }
            Some(old_dtype) => {
                let mut unified = false;
                let unified_type = unify_types(old_dtype, &dtype, &mut unified)?;

                if unified {
                    // The row size needs to account for the difference in sizes
                    // between the newly unified type and the old type.
                    let delta = unified_type.est_size() - old_dtype.est_size();
                    self.row_size.fetch_add(delta, atomic::Ordering::Relaxed);
                }

                *old_dtype = unified_type;
            }
        }

        Ok(())
    }

    pub fn get_dtype(&'a self, prop_id: usize) -> Option<&'a PropType> {
        self.d_types.get(prop_id)
    }

    /// Fast check for property type without unifying the types
    /// Returns:
    /// - `Some(Either::Left(id))` if the property type can be unified
    /// - `Some(Either::Right(id))` if the property type is already set and no unification is needed
    /// - `None` if the property type is not set
    /// - `Err(PropError::PropertyTypeError)` if the property type cannot be unified
    pub fn fast_proptype_check(
        &mut self,
        prop: &str,
        dtype: PropType,
    ) -> Result<Option<Either<usize, usize>>, PropError> {
        fast_proptype_check(self.dict_mapper.map(), &self.d_types, prop, dtype)
    }

    /// Mark `prop_id` as present in `layer_id`
    pub fn mark_in_layer(&mut self, layer_id: LayerId, prop_id: usize) {
        ensure_and_set(&mut *self.layer_presence, layer_id.0, prop_id);
    }
}

pub struct LockedPropMapper<'a> {
    dict_mapper: LockedDictMapper<'a>,
    d_types: RwLockReadGuard<'a, Vec<PropType>>,
}

impl<'a> LockedPropMapper<'a> {
    pub fn get_id(&self, prop: &str) -> Option<usize> {
        self.dict_mapper.get_id(prop)
    }

    pub fn get_dtype(&'a self, prop_id: usize) -> Option<&'a PropType> {
        self.d_types.get(prop_id)
    }

    /// Fast check for property type without unifying the types
    /// Returns:
    /// - `Some(Either::Left(id))` if the property type can be unified
    /// - `Some(Either::Right(id))` if the property type is already set and no unification is needed
    /// - `None` if the property type is not set
    /// - `Err(PropError::PropertyTypeError)` if the property type cannot be unified
    pub fn fast_proptype_check(
        &self,
        prop: &str,
        dtype: PropType,
    ) -> Result<Option<Either<usize, usize>>, PropError> {
        fast_proptype_check(self.dict_mapper.map(), &self.d_types, prop, dtype)
    }

    pub fn iter_ids_and_types(&self) -> impl Iterator<Item = (usize, &ArcStr, &PropType)> {
        self.dict_mapper
            .iter_ids()
            .map(move |(id, name)| (id, name, &self.d_types[id]))
    }
}

fn fast_proptype_check(
    mapper: &FxHashMap<ArcStr, usize>,
    d_types: &[PropType],
    prop: &str,
    dtype: PropType,
) -> Result<Option<Either<usize, usize>>, PropError> {
    match mapper.get(prop) {
        Some(&id) => {
            let existing_dtype = d_types
                .get(id)
                .expect("Existing id should always have a dtype");

            let fast_check = check_for_unification(&dtype, existing_dtype);
            if fast_check.is_none() {
                // means nothing to do
                return Ok(Some(Either::Right(id)));
            }
            let can_unify = fast_check.unwrap();
            if can_unify {
                Ok(Some(Either::Left(id)))
            } else {
                Err(PropError {
                    name: prop.to_string(),
                    expected: existing_dtype.clone(),
                    actual: dtype,
                })
            }
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod prop_mapper_tests {
    use super::*;

    #[test]
    fn get_or_create_and_validate_new_property() {
        let prop_mapper = PropMapper::default();
        let result = prop_mapper.get_or_create_and_validate("new_prop", PropType::U8);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn get_or_create_and_validate_existing_property_same_type() {
        let prop_mapper = PropMapper::default();

        prop_mapper
            .get_or_create_and_validate("existing_prop", PropType::U8)
            .unwrap();

        let result = prop_mapper.get_or_create_and_validate("existing_prop", PropType::U8);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn get_or_create_and_validate_existing_property_different_type() {
        let prop_mapper = PropMapper::default();

        prop_mapper
            .get_or_create_and_validate("existing_prop", PropType::U8)
            .unwrap();

        let result = prop_mapper.get_or_create_and_validate("existing_prop", PropType::U16);

        assert!(result.is_err());

        if let Err(PropError {
            name,
            expected,
            actual,
        }) = result
        {
            assert_eq!(name, "existing_prop");
            assert_eq!(expected, PropType::U8);
            assert_eq!(actual, PropType::U16);
        } else {
            panic!("Expected PropertyTypeError");
        }
    }

    #[test]
    fn get_or_create_and_validate_unify_types() {
        let prop_mapper = PropMapper::default();

        prop_mapper
            .get_or_create_and_validate("prop", PropType::Empty)
            .unwrap();

        let result = prop_mapper.get_or_create_and_validate("prop", PropType::U8);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn get_or_create_and_validate_resize_vector() {
        let prop_mapper = PropMapper::default();

        prop_mapper.set_id_and_dtype("existing_prop", 5, PropType::U8);

        let result = prop_mapper.get_or_create_and_validate("new_prop", PropType::U16);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 6);
        assert_eq!(prop_mapper.get_dtype(6), Some(PropType::U16));
    }

    #[test]
    fn get_or_create_and_validate_two_independent_properties() {
        let prop_mapper = PropMapper::default();
        let result1 = prop_mapper.get_or_create_and_validate("prop1", PropType::U8);
        let result2 = prop_mapper.get_or_create_and_validate("prop2", PropType::U16);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap().inner(), 0);
        assert_eq!(result2.unwrap().inner(), 1);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
        assert_eq!(prop_mapper.get_dtype(1), Some(PropType::U16));
    }

    #[test]
    fn unify_types_increases_row_size() {
        let map_1 = PropType::map([("name", PropType::Str)]);
        let map_2 = PropType::map([("location", PropType::Str)]);

        let mut unified = false;
        let expected_type = unify_types(&map_1, &map_2, &mut unified).unwrap();
        let expected_delta = expected_type.est_size() - map_1.est_size();

        assert!(unified);
        assert!(expected_delta > 0, "should grow est_size on unify");

        let prop_mapper = PropMapper::default();
        prop_mapper
            .get_or_create_and_validate("attrs", map_1.clone())
            .unwrap();

        let before = prop_mapper.row_size();

        assert_eq!(before, map_1.est_size());

        prop_mapper
            .get_or_create_and_validate("attrs", map_2.clone())
            .unwrap();

        let after = prop_mapper.row_size();

        assert_eq!(after, before + expected_delta);
        assert_eq!(after, expected_type.est_size());
        assert_eq!(prop_mapper.get_dtype(0), Some(expected_type));
    }
}

#[cfg(test)]
mod write_locked_prop_mapper_tests {
    use super::*;

    #[test]
    fn new_id_and_dtype() {
        let prop_mapper = PropMapper::default();

        let id = {
            let mut locked = prop_mapper.write_locked();
            locked.new_id_and_dtype("new_prop", PropType::U8)
        };

        assert_eq!(id, 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn set_or_unify_existing_property_same_type() {
        let prop_mapper = PropMapper::default();

        let id = {
            let mut locked = prop_mapper.write_locked();
            let id = locked.new_id_and_dtype("existing_prop", PropType::U8);
            locked.set_or_unify_dtype(id, PropType::U8).unwrap();
            id
        };

        assert_eq!(id, 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn set_or_unify_existing_property_different_type() {
        let prop_mapper = PropMapper::default();

        let result = {
            let mut locked = prop_mapper.write_locked();
            let id = locked.new_id_and_dtype("existing_prop", PropType::U8);

            locked.set_or_unify_dtype(id, PropType::U16)
        };

        assert!(result.is_err());

        if let Err(PropError {
            expected, actual, ..
        }) = result
        {
            assert_eq!(expected, PropType::U8);
            assert_eq!(actual, PropType::U16);
        } else {
            panic!("Expected PropError");
        }
    }

    #[test]
    fn set_or_unify_types() {
        let prop_mapper = PropMapper::default();

        let id = {
            let mut locked = prop_mapper.write_locked();
            let id = locked.new_id_and_dtype("prop", PropType::Empty);
            locked.set_or_unify_dtype(id, PropType::U8).unwrap();
            id
        };

        assert_eq!(id, 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn new_id_and_dtype_resize_vector() {
        let prop_mapper = PropMapper::default();

        let id = {
            let mut locked = prop_mapper.write_locked();
            locked.set_id_and_dtype("existing_prop", 5, PropType::U8);
            locked.new_id_and_dtype("new_prop", PropType::U16)
        };

        assert_eq!(id, 6);
        assert_eq!(prop_mapper.get_dtype(6), Some(PropType::U16));
    }

    #[test]
    fn new_id_and_dtype_two_independent_properties() {
        let prop_mapper = PropMapper::default();

        let (id1, id2) = {
            let mut locked = prop_mapper.write_locked();
            let id1 = locked.new_id_and_dtype("prop1", PropType::U8);
            let id2 = locked.new_id_and_dtype("prop2", PropType::U16);

            (id1, id2)
        };

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
        assert_eq!(prop_mapper.get_dtype(1), Some(PropType::U16));
    }

    #[test]
    fn unify_types_increases_row_size() {
        let map_1 = PropType::map([("name", PropType::Str)]);
        let map_2 = PropType::map([("location", PropType::Str)]);

        let mut unified = false;
        let expected_type = unify_types(&map_1, &map_2, &mut unified).unwrap();
        let expected_delta = expected_type.est_size() - map_1.est_size();

        assert!(unified);
        assert!(expected_delta > 0, "should grow est_size on unify");

        let prop_mapper = PropMapper::default();
        let id = {
            let mut locked = prop_mapper.write_locked();
            locked.new_id_and_dtype("attrs", map_1.clone())
        };

        let before = prop_mapper.row_size();
        assert_eq!(before, map_1.est_size());

        {
            let mut locked = prop_mapper.write_locked();
            locked.set_or_unify_dtype(id, map_2.clone()).unwrap();
        }

        let after = prop_mapper.row_size();
        assert_eq!(after, before + expected_delta);
        assert_eq!(after, expected_type.est_size());
        assert_eq!(prop_mapper.get_dtype(0), Some(expected_type));
    }
}
