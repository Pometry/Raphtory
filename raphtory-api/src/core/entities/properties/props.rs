use std::{ops::Deref, sync::Arc};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::{
    storage::{
        arc_str::ArcStr,
        dict_mapper::{DictMapper, MaybeNew},
        locked_vec::ArcReadLockedVec,
    },
    PropType,
};

use super::PropError;

#[derive(Serialize, Deserialize, Debug)]
pub struct Meta {
    meta_prop_temporal: PropMapper,
    meta_prop_constant: PropMapper,
    meta_layer: DictMapper,
    meta_node_type: DictMapper,
}

impl Default for Meta {
    fn default() -> Self {
        Self::new()
    }
}

impl Meta {
    pub fn set_const_prop_meta(&mut self, meta: PropMapper) {
        self.meta_prop_constant = meta;
    }
    pub fn set_temporal_prop_meta(&mut self, meta: PropMapper) {
        self.meta_prop_temporal = meta;
    }
    pub fn const_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_constant
    }

    pub fn temporal_prop_meta(&self) -> &PropMapper {
        &self.meta_prop_temporal
    }

    pub fn layer_meta(&self) -> &DictMapper {
        &self.meta_layer
    }

    pub fn node_type_meta(&self) -> &DictMapper {
        &self.meta_node_type
    }

    pub fn new() -> Self {
        let meta_layer = DictMapper::default();
        meta_layer.get_or_create_id("_default");
        let meta_node_type = DictMapper::default();
        meta_node_type.get_or_create_id("_default");
        Self {
            meta_prop_temporal: PropMapper::default(),
            meta_prop_constant: PropMapper::default(),
            meta_layer,     // layer 0 is the default layer
            meta_node_type, // type 0 is the default type for a node
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
            self.meta_prop_constant
                .get_or_create_and_validate(prop, dtype)
        } else {
            self.meta_prop_temporal
                .get_or_create_and_validate(prop, dtype)
        }
    }

    #[inline]
    pub fn get_prop_id(&self, name: &str, is_static: bool) -> Option<usize> {
        if is_static {
            self.meta_prop_constant.get_id(name)
        } else {
            self.meta_prop_temporal.get_id(name)
        }
    }

    #[inline]
    pub fn get_or_create_layer_id(&self, name: &str) -> MaybeNew<usize> {
        self.meta_layer.get_or_create_id(name)
    }

    #[inline]
    pub fn get_default_node_type_id(&self) -> usize {
        0usize
    }

    #[inline]
    pub fn get_or_create_node_type_id(&self, node_type: &str) -> MaybeNew<usize> {
        self.meta_node_type.get_or_create_id(node_type)
    }

    #[inline]
    pub fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.meta_layer.get_id(name)
    }

    #[inline]
    pub fn get_node_type_id(&self, node_type: &str) -> Option<usize> {
        self.meta_node_type.get_id(node_type)
    }

    pub fn get_layer_name_by_id(&self, id: usize) -> ArcStr {
        self.meta_layer.get_name(id)
    }

    pub fn get_node_type_name_by_id(&self, id: usize) -> Option<ArcStr> {
        if id == 0 {
            None
        } else {
            Some(self.meta_node_type.get_name(id))
        }
    }

    pub fn get_all_layers(&self) -> Vec<usize> {
        self.meta_layer.get_values()
    }

    pub fn get_all_node_types(&self) -> Vec<ArcStr> {
        self.meta_node_type
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
            self.meta_prop_constant.get_keys()
        } else {
            self.meta_prop_temporal.get_keys()
        }
    }

    pub fn get_prop_name(&self, prop_id: usize, is_static: bool) -> ArcStr {
        if is_static {
            self.meta_prop_constant.get_name(prop_id)
        } else {
            self.meta_prop_temporal.get_name(prop_id)
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

    pub fn get_and_validate(
        &self,
        prop: &str,
        dtype: PropType,
    ) -> Result<Option<usize>, PropError> {
        match self.get_id(prop) {
            Some(id) => {
                let existing_dtype = self
                    .get_dtype(id)
                    .expect("Existing id should always have a dtype");
                if existing_dtype == dtype {
                    Ok(Some(id))
                } else {
                    Err(PropError::PropertyTypeError {
                        name: prop.to_string(),
                        expected: existing_dtype,
                        actual: dtype,
                    })
                }
            }
            None => Ok(None),
        }
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
            if !matches!(old_type, PropType::Empty) {
                return if *old_type == dtype {
                    Ok(wrapped_id)
                } else {
                    Err(PropError::PropertyTypeError {
                        name: prop.to_owned(),
                        expected: old_type.clone(),
                        actual: dtype,
                    })
                };
            }
        }
        drop(dtype_read); // drop the read lock and wait for write lock as type did not exist yet
        let mut dtype_write = self.dtypes.write();
        match dtype_write.get(id).cloned() {
            Some(old_type) => {
                if matches!(old_type, PropType::Empty) {
                    // vector already resized but this id is not filled yet, set the dtype and return id
                    dtype_write[id] = dtype;
                    Ok(wrapped_id)
                } else {
                    // already filled because a different thread won the race for this id, check the type matches
                    if old_type == dtype {
                        Ok(wrapped_id)
                    } else {
                        Err(PropError::PropertyTypeError {
                            name: prop.to_owned(),
                            expected: old_type,
                            actual: dtype,
                        })
                    }
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
