#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) struct Edge {
    pub(crate) v: usize,              // physical id of th vertex
    pub(crate) e_meta: Option<usize>, // physical id of the edge metadata
}

impl Edge {
    pub fn new(v: usize, e_meta: usize) -> Self {
        Edge {
            v,
            e_meta: Some(e_meta),
        }
    }
}
