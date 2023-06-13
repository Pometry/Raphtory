use crate::core::{tadjset::TAdjSet, Direction};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{EID, VID};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum Adj {
    #[default]
    Solo,
    List {
        // local:
        out: TAdjSet<VID>,
        into: TAdjSet<VID>,
    },
}

impl Adj {
    pub(crate) fn get_edge(&self, v: VID, dir: Direction) -> Option<EID> {
        match self {
            Adj::Solo => None,
            Adj::List { out, into } => match dir {
                Direction::OUT => out.find(v).map(|e| e.into()),
                Direction::IN => into.find(v).map(|e| e.into()),
                Direction::BOTH => self
                    .get_edge(v, Direction::OUT)
                    .or_else(|| self.get_edge(v, Direction::IN)),
            },
        }
    }

    pub(crate) fn new_out(v: VID, e: EID) -> Self {
        Adj::List {
            out: TAdjSet::new(v, e.into()),
            into: TAdjSet::default(),
        }
    }

    pub(crate) fn new_into(v: VID, e: EID) -> Self {
        Adj::List {
            into: TAdjSet::new(v, e.into()),
            out: TAdjSet::default(),
        }
    }

    pub(crate) fn add_edge_into(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_into(v, e),
            Adj::List { into, .. } => into.push(v, e.into()),
        }
    }

    pub(crate) fn add_edge_out(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_out(v, e),
            Adj::List { out, .. } => out.push(v, e.into()),
        }
    }

    pub(crate) fn iter(&self, dir: Direction) -> Box<dyn Iterator<Item = (VID, EID)> + '_> {
        match self {
            Adj::Solo => Box::new(std::iter::empty()),
            Adj::List { out, into } => match dir {
                Direction::OUT => Box::new(out.iter().map(|(v, e)| (v, e.into()))),
                Direction::IN => Box::new(into.iter().map(|(v, e)| (v, e.into()))),
                Direction::BOTH => Box::new(
                    out.iter()
                        .merge(into.iter())
                        .dedup()
                        .map(|(v, e)| (v, e.into())),
                ),
            },
        }
    }
}
