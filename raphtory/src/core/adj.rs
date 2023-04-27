use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::core::{
    tadjset::{AdjEdge, TAdjSet},
    Time,
};

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
    pub(crate) fn out_len_window(&self, w: &Range<Time>) -> usize {
        match self {
            Adj::Solo => 0,
            Adj::List {
                out, remote_out, ..
            } => out.len_window(w) + remote_out.len_window(w),
        }
    }

    pub(crate) fn new_out(v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                out: TAdjSet::new(t, v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(t, v, e),
                remote_into: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn new_into(v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                into: TAdjSet::new(t, v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(t, v, e),
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
