use core::panic;

use crate::core::Direction;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::core::tgraph::vertices::structure::adjset::AdjSet;
use crate::core::tgraph::{EID, VID};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum Adj {
    #[default]
    Solo,
    List {
        // local:
        out: AdjSet<VID, EID>,
        into: AdjSet<VID, EID>,
    },
}

impl Adj {
    pub(crate) fn get_edge(&self, v: VID, dir: Direction) -> Option<EID> {
        match self {
            Adj::Solo => None,
            Adj::List { out, into } => match dir {
                Direction::OUT => out.find(v),
                Direction::IN => into.find(v),
                Direction::BOTH => self
                    .get_edge(v, Direction::OUT)
                    .or_else(|| self.get_edge(v, Direction::IN)),
            },
        }
    }

    pub(crate) fn new_out(v: VID, e: EID) -> Self {
        Adj::List {
            out: AdjSet::new(v, e),
            into: AdjSet::default(),
        }
    }

    pub(crate) fn new_into(v: VID, e: EID) -> Self {
        Adj::List {
            into: AdjSet::new(v, e),
            out: AdjSet::default(),
        }
    }

    pub(crate) fn add_edge_into(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_into(v, e),
            Adj::List { into, .. } => into.push(v, e),
        }
    }

    pub(crate) fn add_edge_out(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_out(v, e),
            Adj::List { out, .. } => out.push(v, e),
        }
    }

    pub(crate) fn iter(&self, dir: Direction) -> Box<dyn Iterator<Item = (VID, EID)> + Send + '_> {
        match self {
            Adj::Solo => Box::new(std::iter::empty()),
            Adj::List { out, into } => match dir {
                Direction::OUT => Box::new(out.iter()),
                Direction::IN => Box::new(into.iter()),
                Direction::BOTH => Box::new(out.iter().merge(into.iter())),
            },
        }
    }

    pub(crate) fn get_page_vec(
        &self,
        last: Option<VID>,
        page_size: usize,
        dir: Direction,
    ) -> Vec<(VID, EID)> {
        match self {
            Adj::Solo => Vec::new(),
            Adj::List { out, into } => match dir {
                Direction::OUT => out.get_page_vec(last, page_size),
                Direction::IN => into.get_page_vec(last, page_size),
                _ => panic!(
                    "Cannot get page vec for both direction, need to be handled by the caller"
                ),
            },
        }
    }
}
