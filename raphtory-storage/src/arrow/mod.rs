use polars_core::utils::arrow::{array::{MutablePrimitiveArray, MutableStructArray}, trusted_len::TrustedLen};
use raphtory::core::entities::{edges::edge_ref::EdgeRef, EID, VID};

pub(crate) mod static_graph;

type MPArr<T> = MutablePrimitiveArray<T>;

enum IngestEdgeType {
    Outbound(EdgeRef, u64),
    Inbound(EdgeRef, u64),
}

impl IngestEdgeType {
    fn pid(&self) -> EID {
        match self {
            IngestEdgeType::Outbound(e_ref, _) => e_ref.pid(),
            IngestEdgeType::Inbound(e_ref, _) => e_ref.pid(),
        }
    }

    fn pid_u64(&self) -> u64 {
        let e: usize = self.pid().into();
        e as u64
    }

    fn local(&self) -> VID {
        match self {
            IngestEdgeType::Outbound(e_ref, _) => e_ref.local(),
            IngestEdgeType::Inbound(e_ref, _) => e_ref.local(),
        }
    }

    fn vertex_u64(&self) -> u64 {
        let vid: usize = self.local().into();
        vid as u64
    }

    fn gid(&self) -> u64 {
        match self {
            IngestEdgeType::Outbound(_, gid) => *gid,
            IngestEdgeType::Inbound(_, gid) => *gid,
        }
    }

    fn remote(&self) -> VID {
        match self {
            IngestEdgeType::Outbound(e_ref, _) => e_ref.remote(),
            IngestEdgeType::Inbound(e_ref, _) => e_ref.remote(),
        }
    }

    fn remote_u64(&self) -> u64 {
        let vid: usize = self.remote().into();
        vid as u64
    }
}

#[repr(transparent)]
struct MutEdgePair<'a>(&'a mut MutableStructArray);

impl<'a> MutEdgePair<'a> {
    fn add_pair(&mut self, v: u64, e: u64) {
        self.0.value::<MPArr<u64>>(0).unwrap().push(Some(v));
        self.0.value::<MPArr<u64>>(1).unwrap().push(Some(e));
        self.0.push(true);
    }

    fn add_pair_usize(&mut self, v_id: usize, e_id: usize) {
        self.add_pair(v_id as u64, e_id as u64);
    }

    fn add_vals<I: TrustedLen<Item = u64>>(&mut self, vs: I, es: I) {
        self.0
            .value::<MPArr<u64>>(0)
            .unwrap()
            .extend_trusted_len_values(vs);
        self.0
            .value::<MPArr<u64>>(1)
            .unwrap()
            .extend_trusted_len_values(es);

        self.0.push(true);
    }
}

impl<'a> From<&'a mut MutableStructArray> for MutEdgePair<'a> {
    fn from(arr: &'a mut MutableStructArray) -> Self {
        Self(arr)
    }
}