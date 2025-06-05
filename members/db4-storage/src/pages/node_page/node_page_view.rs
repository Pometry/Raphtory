use crate::pages::edge_page::edge_page_view::TProp;
use crate::pages::node_page::mem_node_page::MemNodeSegment;
use crate::pages::{
    edge_page::edge_page_view::TimeCell, node_page::disk_node_page::DiskNodeSegment,
};
use crate::LocalPOS;
use either::Either;
use raphtory_api::core::entities::{EID, VID};

#[derive(Debug, Clone, Copy)]
pub enum NodePageView<'a> {
    Mem(&'a MemNodeSegment),
    Disk(&'a DiskNodeSegment),
}

impl<'a> NodePageView<'a> {
    #[inline]
    pub fn out_nbrs(self, n: LocalPOS) -> impl Iterator<Item = VID> + 'a {
        match self {
            NodePageView::Mem(page) => Either::Left(page.out_nbrs(n)),
            NodePageView::Disk(page) => Either::Right(page.out_nbrs(n)),
        }
    }

    #[inline]
    pub fn inb_nbrs(self, n: LocalPOS) -> impl Iterator<Item = VID> + 'a {
        match self {
            NodePageView::Mem(page) => Either::Left(page.inb_nbrs(n)),
            NodePageView::Disk(page) => Either::Right(page.inb_nbrs(n)),
        }
    }

    #[inline]
    pub fn out_edges(self, n: LocalPOS) -> impl Iterator<Item = EID> + 'a {
        match self {
            NodePageView::Mem(page) => Either::Left(page.out_edges(n)),
            NodePageView::Disk(page) => Either::Right(page.out_edges(n)),
        }
    }

    #[inline]
    pub fn inb_edges(self, n: LocalPOS) -> impl Iterator<Item = EID> + 'a {
        match self {
            NodePageView::Mem(page) => Either::Left(page.inb_edges(n)),
            NodePageView::Disk(page) => Either::Right(page.inb_edges(n)),
        }
    }

    #[inline]
    pub fn contains_out(self, n: LocalPOS, dst: VID) -> bool {
        match self {
            NodePageView::Mem(page) => page.contains_out(n, dst),
            NodePageView::Disk(page) => page.contains_out(n, dst),
        }
    }

    #[inline]
    pub fn contains_in(self, n: LocalPOS, dst: VID) -> bool {
        match self {
            NodePageView::Mem(page) => page.contains_in(n, dst),
            NodePageView::Disk(page) => page.contains_in(n, dst),
        }
    }

    #[inline]
    pub fn get_out_edge(self, n: LocalPOS, dst: VID) -> Option<EID> {
        match self {
            NodePageView::Mem(page) => page.get_out_edge(n, dst),
            NodePageView::Disk(page) => page.get_out_edge(n, dst),
        }
    }

    #[inline]
    pub fn get_in_edge(self, n: LocalPOS, dst: VID) -> Option<EID> {
        match self {
            NodePageView::Mem(page) => page.get_in_edge(n, dst),
            NodePageView::Disk(page) => page.get_in_edge(n, dst),
        }
    }

    #[inline]
    pub fn has_node(self, n: LocalPOS) -> bool {
        match self {
            NodePageView::Mem(page) => page.has_node(n),
            NodePageView::Disk(page) => page.has_node(n),
        }
    }

    pub fn additions(self, n: LocalPOS) -> TimeCell<'a> {
        match self {
            NodePageView::Mem(page) => page.as_ref().additions(n),
            NodePageView::Disk(page) => TimeCell::Disk {
                props: page.prop_additions(n),
                additions: page.edge_additions(n),
            },
        }
    }

    pub(crate) fn t_prop(self, edge_pos: LocalPOS, prop_id: usize) -> TProp<'a> {
        match self {
            NodePageView::Mem(page) => page.t_prop(edge_pos, prop_id),
            NodePageView::Disk(page) => page.t_prop(edge_pos, prop_id, 0),
        }
    }
}
