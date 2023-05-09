use crate::core::{
    tadjset::{AdjEdge, TAdjSet},
    Direction,
};
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
        remote_out: TAdjSet<usize>,
        remote_into: TAdjSet<usize>,
    },
}

impl Adj {
    pub(crate) fn get_edge(&self, v: usize, dir: Direction, is_remote: bool) -> Option<AdjEdge> {
        match self {
            Adj::Solo => None,
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
            } => match dir {
                Direction::OUT => {
                    if is_remote {
                        remote_out.find(v)
                    } else {
                        out.find(v)
                    }
                }
                Direction::IN => {
                    if is_remote {
                        remote_into.find(v)
                    } else {
                        into.find(v)
                    }
                }
                Direction::BOTH => self
                    .get_edge(v, Direction::OUT, is_remote)
                    .or_else(|| self.get_edge(v, Direction::IN, is_remote)),
            },
        }
    }
    pub(crate) fn new_out(v: usize, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                out: TAdjSet::new(v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(v, e),
                remote_into: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn new_into(v: usize, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                into: TAdjSet::new(v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(v, e),
                remote_out: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        match self {
            Adj::Solo => 0,
            Adj::List {
                out, remote_out, ..
            } => out.len() + remote_out.len(),
        }
    }
}
