use std::{collections::BTreeSet, ops::Range};

use serde::{Deserialize, Serialize};

use crate::{
    tadjset::{AdjEdge, TAdjSet},
    Time,
};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) enum Adj {
    Solo(u64, BTreeSet<Time>),
    List {
        logical: u64,
        out: TAdjSet<usize, i64>,
        // local
        into: TAdjSet<usize, i64>,
        // local
        remote_out: TAdjSet<usize, i64>,
        // remote
        remote_into: TAdjSet<usize, i64>,
        // remote
        timestamps: BTreeSet<Time>,
    },
}

impl Adj {
    pub(crate) fn as_list(&mut self) -> Option<Self> {
        let mut swap_me_ts = BTreeSet::new();
        match self {
            Adj::Solo(logical, timestamps) => {
                std::mem::swap(&mut swap_me_ts, timestamps);
                Some(Adj::List {
                    logical: *logical,
                    out: TAdjSet::default(),
                    into: TAdjSet::default(),
                    remote_out: TAdjSet::default(),
                    remote_into: TAdjSet::default(),
                    timestamps: swap_me_ts,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn exists(&self, w: &Range<Time>) -> bool {
        match self {
            Adj::Solo(_, timestamps) | Adj::List { timestamps, .. } => {
                timestamps.range(w.clone()).next().is_some()
            }
        }
    }

    pub(crate) fn out_len_window(&self, w: &Range<Time>) -> usize {
        match self {
            Adj::Solo(_, _) => 0,
            Adj::List {
                timestamps,
                out,
                remote_out,
                ..
            } => {
                if timestamps.range(w.clone()).next().is_some() {
                    out.len_window(w) + remote_out.len_window(w)
                } else {
                    0
                }
            }
        }
    }

    pub(crate) fn register_event(&mut self, t: Time) {
        match self {
            Adj::Solo(_, timestamps) | Adj::List { timestamps, .. } => {
                timestamps.insert(t);
            }
        }
    }
    pub(crate) fn new_out(g_v_id: u64, v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::new(t, v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
                timestamps: BTreeSet::new(),
            }
        } else {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(t, v, e),
                remote_into: TAdjSet::default(),
                timestamps: BTreeSet::new(),
            }
        }
    }

    pub(crate) fn new_into(g_v_id: u64, v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                logical: g_v_id,
                into: TAdjSet::new(t, v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
                timestamps: BTreeSet::new(),
            }
        } else {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(t, v, e),
                remote_out: TAdjSet::default(),
                timestamps: BTreeSet::new(),
            }
        }
    }

    pub(crate) fn logical(&self) -> &u64 {
        match self {
            Adj::Solo(logical, _) => logical,
            Adj::List { logical, .. } => logical,
        }
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        match self {
            Adj::Solo(_, _) => 0,
            Adj::List {
                out, remote_out, ..
            } => out.len() + remote_out.len(),
        }
    }
}
