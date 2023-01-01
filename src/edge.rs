#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) enum OtherV {
    Local(usize),
    Remote(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) struct Edge {
    pub(crate) v: OtherV,             // physical id of the other vertex
    pub(crate) e_meta: Option<usize>, // physical id of the edge metadata
}

impl Edge {
    pub fn new(v: OtherV, e_meta: usize) -> Self {
        Edge {
            v,
            e_meta: Some(e_meta),
        }
    }

    pub fn empty(v: OtherV) -> Self {
        Edge { v, e_meta: None }
    }

    pub fn remote(v: u64, e_meta: usize) -> Self {
        Self::new(OtherV::Remote(v), e_meta)
    }

    pub fn local(v: usize, e_meta: usize) -> Self {
        Self::new(OtherV::Local(v), e_meta)
    }

    pub fn remote_empty(v: u64) -> Self {
        Self::empty(OtherV::Remote(v))
    }

    pub fn local_empty(v: usize) -> Self {
        Self::empty(OtherV::Local(v))
    }
}
