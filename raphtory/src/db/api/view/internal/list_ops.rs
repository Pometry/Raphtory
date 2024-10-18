use crate::{
    core::entities::{EID, VID},
    db::api::{state::Index, view::Base},
};
use enum_dispatch::enum_dispatch;
use rayon::{iter::Either, prelude::*};
use std::sync::Arc;

#[enum_dispatch]
pub trait ListOps {
    fn node_list(&self) -> NodeList;

    fn edge_list(&self) -> EdgeList;
}

pub trait InheritListOps: Base {}

impl<G: InheritListOps> ListOps for G
where
    <G as Base>::Base: ListOps,
{
    fn node_list(&self) -> NodeList {
        self.base().node_list()
    }

    fn edge_list(&self) -> EdgeList {
        self.base().edge_list()
    }
}

