use crate::core::{tadjset::TAdjSet, Direction};
use serde::{Deserialize, Serialize};

use super::{VID, EID};

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
}
