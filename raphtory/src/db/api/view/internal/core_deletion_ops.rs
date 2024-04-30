use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds},
        storage::timeindex::{LockedLayeredIndex, TimeIndexEntry},
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait HasDeletionOps {}
