#[cfg(feature = "arrow")]
use crate::arrow::edge::CoreArrowEdgeView;
use crate::core::entities::{edges::edge_store::EdgeStore, EID, VID};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait CoreEdgeOps {
    fn src(&self) -> VID;
    fn dst(&self) -> VID;
    fn eid(&self) -> EID;
}

impl CoreEdgeOps for EdgeStore {
    #[inline]
    fn src(&self) -> VID {
        self.src()
    }
}

#[cfg(feature = "arrow")]
impl<'a> CoreEdgeOps for CoreArrowEdgeView<'a> {
    #[inline]
    fn src(&self) -> VID {
        self.src()
    }
}

#[enum_dispatch]
pub enum CoreEdgeView<'a> {
    Mem(&'a EdgeStore),
    #[cfg(feature = "arrow")]
    Arrow(CoreArrowEdgeView<'a>),
}
