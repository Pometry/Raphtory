use crate::core::{
    entities::properties::prop::{check_for_unification, unify_types, PropError, PropType},
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

pub const STATIC_GRAPH_LAYER: &str = "_static_graph";
pub const STATIC_GRAPH_LAYER_ID: usize = 0;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Meta {
    temporal_prop_mapper: PropMapper,
    metadata_mapper: PropMapper,
    layer_mapper: DictMapper,
    node_type_mapper: DictMapper,
}

impl Meta {
    pub fn all_layer_iter(&self) -> impl Iterator<Item = (usize, ArcStr)> + use<'_> {
        self.layer_mapper
            .all_ids()
            .zip(self.layer_mapper.all_keys())
    }

    pub fn set_metadata_mapper(&mut self, meta: PropMapper) {
        self.metadata_mapper = meta;
    }
    pub fn set_temporal_prop_meta(&mut self, meta: PropMapper) {
        self.temporal_prop_mapper = meta;
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
    pub fn get_or_create_layer_id(&self, name: Option<&str>) -> MaybeNew<usize> {
        self.layer_mapper
            .get_or_create_id(name.unwrap_or("_default"))
    }

    #[inline]
    pub fn get_default_node_type_id(&self) -> usize {
        0usize
    }

    #[inline]
    pub fn get_or_create_node_type_id(&self, node_type: &str) -> MaybeNew<usize> {
        self.node_type_mapper.get_or_create_id(node_type)
    }

    #[inline]
    pub fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.layer_mapper.get_id(name)
    }

    #[inline]
    pub fn get_default_layer_id(&self) -> Option<usize> {
        self.layer_mapper.get_id("_default")
    }

    #[inline]
    pub fn get_node_type_id(&self, node_type: &str) -> Option<usize> {
        self.node_type_mapper.get_id(node_type)
    }

    pub fn get_layer_name_by_id(&self, id: usize) -> ArcStr {
        self.layer_mapper.get_name(id)
    }

    pub fn get_node_type_name_by_id(&self, id: usize) -> Option<ArcStr> {
        if id == 0 {
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
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct PropMapper {
    id_mapper: DictMapper,
    row_size: AtomicUsize,
    dtypes: Arc<RwLock<Vec<PropType>>>,
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
        }
    }

    pub fn d_types(&self) -> impl Deref<Target = Vec<PropType>> + '_ {
        self.dtypes.read_recursive()
    }

    pub fn deep_clone(&self) -> Self {
        let dtypes = self.dtypes.read().clone();
        Self {
            id_mapper: self.id_mapper.deep_clone(),
            row_size: AtomicUsize::new(self.row_size.load(std::sync::atomic::Ordering::Relaxed)),
            dtypes: Arc::new(RwLock::new(dtypes)),
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
        drop(dtype_read); // drop the read lock and wait for write lock as type did not exist yet
        let mut dtype_write = self.dtypes.write();
        match dtype_write.get(id).cloned() {
            Some(old_type) => {
                if let Ok(tpe) = unify_types(&dtype, &old_type, &mut false) {
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
                // vector not resized yet, resize it and set the dtype and return id
                dtype_write.resize(id + 1, PropType::Empty);
                self.row_size
                    .fetch_add(dtype.est_size(), atomic::Ordering::Relaxed);
                dtype_write[id] = dtype;
                Ok(wrapped_id)
            }
        }
    }

    pub fn set_id_and_dtype(&self, key: impl Into<ArcStr>, id: usize, dtype: PropType) {
        let mut dtypes = self.dtypes.write();
        self.set_id(key, id);
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
        }
    }
}

pub struct LockedPropMapper<'a> {
    dict_mapper: LockedDictMapper<'a>,
    d_types: RwLockReadGuard<'a, Vec<PropType>>,
}

pub struct WriteLockedPropMapper<'a> {
    dict_mapper: WriteLockedDictMapper<'a>,
    d_types: RwLockWriteGuard<'a, Vec<PropType>>,
}

impl<'a> WriteLockedPropMapper<'a> {
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

    pub fn set_id_and_dtype(&mut self, key: impl Into<ArcStr>, id: usize, dtype: PropType) {
        self.dict_mapper.set_id(key, id);
        let dtypes = self.d_types.deref_mut();
        if dtypes.len() <= id {
            dtypes.resize(id + 1, PropType::Empty);
        }
        dtypes[id] = dtype;
    }

    pub fn new_id_and_dtype(&mut self, key: impl Into<ArcStr>, dtype: PropType) -> usize {
        let id = self.dict_mapper.get_or_create_id(&key.into());
        let dtypes = self.d_types.deref_mut();
        if dtypes.len() <= id.inner() {
            dtypes.resize(id.inner() + 1, PropType::Empty);
        }
        dtypes[id.inner()] = dtype;
        id.inner()
    }
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
mod tests {
    use super::*;

    #[test]
    fn test_get_or_create_and_validate_new_property() {
        let prop_mapper = PropMapper::default();
        let result = prop_mapper.get_or_create_and_validate("new_prop", PropType::U8);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 0);
        assert_eq!(prop_mapper.get_dtype(0), Some(PropType::U8));
    }

    #[test]
    fn test_get_or_create_and_validate_existing_property_same_type() {
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
    fn test_get_or_create_and_validate_existing_property_different_type() {
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
    fn test_get_or_create_and_validate_unify_types() {
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
    fn test_get_or_create_and_validate_resize_vector() {
        let prop_mapper = PropMapper::default();
        prop_mapper.set_id_and_dtype("existing_prop", 5, PropType::U8);
        let result = prop_mapper.get_or_create_and_validate("new_prop", PropType::U16);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().inner(), 6);
        assert_eq!(prop_mapper.get_dtype(6), Some(PropType::U16));
    }

    #[test]
    fn test_get_or_create_and_validate_two_independent_properties() {
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
}
