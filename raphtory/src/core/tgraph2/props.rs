use serde::{Deserialize, Serialize};

use crate::core::{lazy_vec::LazyVec, tprop::TProp, Prop};

#[derive(Serialize, Deserialize)]
pub(crate) struct Props {
    // properties
    static_props: LazyVec<Option<Prop>>,
    temporal_props: LazyVec<TProp>,
}
