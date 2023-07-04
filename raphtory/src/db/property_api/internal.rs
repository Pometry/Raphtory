use crate::core::locked_view::LockedView;
use crate::core::tgraph::props::DictMapper;
use crate::core::tprop::TProp;
use crate::core::Prop;
use std::ops::Deref;
use std::sync::Arc;

pub trait CorePropertiesOps {
    fn static_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop_meta(&self) -> &DictMapper<String>;
    fn temporal_prop(&self, id: usize) -> Option<&TProp>;
    fn static_prop(&self, id: usize) -> Option<&Prop>;
}
