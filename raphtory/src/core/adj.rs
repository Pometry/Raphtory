use crate::core::edge_layer::VID;
use crate::core::{tadjset::TAdjSet, Direction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum Adj {
    #[default]
    Solo,
    List {
        // local:
        out: TAdjSet<usize>,
        into: TAdjSet<usize>,
        // remote:
        remote_out: TAdjSet<u64>,
        remote_into: TAdjSet<u64>,
    },
}

impl Adj {
    pub(crate) fn get_edge(&self, v: VID, dir: Direction) -> Option<usize> {
        match self {
            Adj::Solo => None,
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match dir {
                Direction::OUT => match v {
                    VID::Remote(v) => remote_out.find(v),
                    VID::Local(v) => out.find(v),
                },
                Direction::IN => match v {
                    VID::Remote(v) => remote_into.find(v),
                    VID::Local(v) => into.find(v),
                },
                Direction::BOTH => self
                    .get_edge(v, Direction::OUT)
                    .or_else(|| self.get_edge(v, Direction::IN)),
            },
        }
    }

    pub(crate) fn new_out(v: VID, e: usize) -> Self {
        match v {
            VID::Local(v) => Adj::List {
                out: TAdjSet::new(v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            },
            VID::Remote(v) => Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(v, e),
                remote_into: TAdjSet::default(),
            },
        }
    }

    pub(crate) fn new_into(v: VID, e: usize) -> Self {
        match v {
            VID::Local(v) => Adj::List {
                into: TAdjSet::new(v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            },
            VID::Remote(v) => Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(v, e),
                remote_out: TAdjSet::default(),
            },
        }
    }
}
