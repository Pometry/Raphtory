use crate::core::{lazy_vec::LazyVec, tprop::TProp, Prop};

pub(crate) struct Props {
    // properties
    static_props: LazyVec<Option<Prop>>,
    temporal_props: LazyVec<TProp>,
}
