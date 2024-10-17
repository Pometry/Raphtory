use std::{hash::Hash, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::db::api::storage::storage::Storage;


#[repr(transparent)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Graph {
    pub(crate) inner: Arc<Storage>,
}

impl PartialEq for Graph {
    fn eq(&self, other: &Self) -> bool {
       todo!() 
    }
}

impl Hash for Graph {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!()
    }
}