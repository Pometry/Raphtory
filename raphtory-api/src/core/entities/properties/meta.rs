use std::{ops::Deref, sync::Arc};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::{
    entities::properties::prop::{unify_types, PropError, PropType},
    storage::{
        arc_str::ArcStr,
        dict_mapper::{DictMapper, MaybeNew},
        locked_vec::ArcReadLockedVec,
    },
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Meta {
    temporal_prop_mapper: PropMapper,
    metadata_mapper: PropMapper,
    layer_mapper: DictMapper,
    node_type_mapper: DictMapper,
}

impl Default for Meta {
    fn default() -> Self {
        Self::new()
    }
}

impl Meta {
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

    pub fn new() -> Self {
        let meta_layer = DictMapper::default();
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

    pub fn get_all_layers(&self) -> Vec<usize> {
        self.layer_mapper.get_values()
    }

    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.node_type_mapper
            .get_keys()
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

    pub fn get_all_property_names(&self, is_static: bool) -> ArcReadLockedVec<ArcStr> {
        if is_static {
            self.metadata_mapper.get_keys()
        } else {
            self.temporal_prop_mapper.get_keys()
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
    pub fn deep_clone(&self) -> Self {
        let dtypes = self.dtypes.read().clone();
        Self {
            id_mapper: self.id_mapper.deep_clone(),
            dtypes: Arc::new(RwLock::new(dtypes)),
        }
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
        dtypes[id] = dtype;
    }

    pub fn get_dtype(&self, prop_id: usize) -> Option<PropType> {
        self.dtypes.read_recursive().get(prop_id).cloned()
    }

    pub fn dtypes(&self) -> impl Deref<Target = Vec<PropType>> + '_ {
        self.dtypes.read_recursive()
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
