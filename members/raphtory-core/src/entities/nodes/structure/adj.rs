use crate::entities::{edges::edge_ref::Dir, nodes::structure::adjset::AdjSet, EID, VID};
use either::Either;
use itertools::Itertools;
use raphtory_api::{
    core::{Direction, DirectionVariants},
    iter::BoxedLIter,
};
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
    pub fn get_edge(&self, v: VID, dir: Direction) -> Option<EID> {
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

    pub fn add_edge_into(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_into(v, e),
            Adj::List { into, .. } => into.push(v, e),
        }
    }

    pub fn add_edge_out(&mut self, v: VID, e: EID) {
        match self {
            Adj::Solo => *self = Self::new_out(v, e),
            Adj::List { out, .. } => out.push(v, e),
        }
    }

    pub(crate) fn iter(&self, dir: Direction) -> BoxedLIter<(VID, EID)> {
        match self {
            Adj::Solo => Box::new(std::iter::empty()),
            Adj::List { out, into } => match dir {
                Direction::OUT => Box::new(out.iter()),
                Direction::IN => Box::new(into.iter()),
                Direction::BOTH => Box::new(out.iter().merge(into.iter())),
            },
        }
    }

    pub fn out_iter(&self) -> impl Iterator<Item = (VID, EID)> + Send + Sync + '_ {
        match self {
            Adj::Solo => Either::Left(std::iter::empty()),
            Adj::List { out, .. } => Either::Right(out.iter()),
        }
    }

    pub fn inb_iter(&self) -> impl Iterator<Item = (VID, EID)> + Send + Sync + '_ {
        match self {
            Adj::Solo => Either::Left(std::iter::empty()),
            Adj::List { into, .. } => Either::Right(into.iter()),
        }
    }

    pub fn node_iter(&self, dir: Direction) -> impl Iterator<Item = VID> + Send + '_ {
        let iter = self.iter(dir).map(|(v, _)| v);
        match dir {
            Direction::OUT => DirectionVariants::Out(iter),
            Direction::IN => DirectionVariants::In(iter),
            Direction::BOTH => DirectionVariants::Both(iter.dedup()),
        }
    }

    pub fn degree(&self, dir: Direction) -> usize {
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
}
