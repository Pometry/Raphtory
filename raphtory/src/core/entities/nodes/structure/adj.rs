use crate::core::{
    entities::{
        edges::edge_ref::{Dir, EdgeRef},
        nodes::structure::adjset::AdjSet,
        EID, VID,
    },
    Direction,
};
use core::panic;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub enum Adj {
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

    pub(crate) fn iter_eref(
        &self,
        dir: Direction,
        local: VID,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send + '_> {
        match self {
            Adj::Solo => Box::new(std::iter::empty()),
            Adj::List { out, into } => match dir {
                Direction::OUT => Box::new(
                    out.iter()
                        .map(move |(remote, e)| EdgeRef::new(e, local, remote, Dir::Out)),
                ),
                Direction::IN => Box::new(
                    into.iter()
                        .map(move |(remote, e)| EdgeRef::new(e, local, remote, Dir::Into)),
                ),
                Direction::BOTH => Box::new(
                    out.iter()
                        .map(move |(remote, e)| EdgeRef::new(e, local, remote, Dir::Out))
                        .merge(
                            into.iter()
                                .map(move |(remote, e)| EdgeRef::new(e, local, remote, Dir::Into)),
                        ),
                ),
            },
        }
    }

    pub(crate) fn node_iter(&self, dir: Direction) -> impl Iterator<Item = VID> + Send + '_ {
        self.iter(dir).map(|(v, _)| v)
    }

    pub(crate) fn degree(&self, dir: Direction) -> usize {
        match self {
            Adj::Solo => 0,
            Adj::List { out, into } => match dir {
                Direction::OUT => out.len(),
                Direction::IN => into.len(),
                Direction::BOTH => out
                    .iter()
                    .merge(into.iter())
                    .dedup_by(|v1, v2| v1.0 == v2.0)
                    .count(),
            },
        }
    }

    pub fn fill_page<const P: usize>(
        &self,
        last: Option<VID>,
        page: &mut [(VID, EID); P],
        dir: Dir,
    ) -> usize {
        match self {
            Adj::Solo => 0,
            Adj::List { out, into } => match dir {
                Dir::Out => out.fill_page(last, page),
                Dir::Into => into.fill_page(last, page),
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
