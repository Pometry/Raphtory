use crate::core::entities::{EID, VID};
use roaring::treemap::RoaringTreemap;

#[derive(Default, Debug)]
pub struct RoaringNodeMap {
    map: RoaringTreemap,
}

impl FromIterator<VID> for RoaringNodeMap {
    fn from_iter<T: IntoIterator<Item = VID>>(iter: T) -> Self {
        let map = RoaringTreemap::from_iter(iter.into_iter().map(|v| v.as_u64()));
        Self { map }
    }
}

impl RoaringNodeMap {
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn remove(&mut self, v: VID) -> bool {
        self.map.remove(v.as_u64())
    }

    pub fn insert(&mut self, v: VID) -> bool {
        self.map.insert(v.as_u64())
    }

    pub fn len(&self) -> usize {
        self.map.len() as usize
    }

    pub fn contains(&self, v: VID) -> bool {
        self.map.contains(v.as_u64())
    }
}

#[derive(Default, Debug)]
pub struct RoaringEdgeMap {
    map: RoaringTreemap,
}

impl FromIterator<EID> for RoaringEdgeMap {
    fn from_iter<T: IntoIterator<Item = EID>>(iter: T) -> Self {
        let map = RoaringTreemap::from_iter(iter.into_iter().map(|v| v.as_u64()));
        Self { map }
    }
}

impl RoaringEdgeMap {
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn remove(&mut self, v: EID) -> bool {
        self.map.remove(v.as_u64())
    }

    pub fn insert(&mut self, v: EID) -> bool {
        self.map.insert(v.as_u64())
    }

    pub fn len(&self) -> usize {
        self.map.len() as usize
    }

    pub fn contains(&self, v: EID) -> bool {
        self.map.contains(v.as_u64())
    }
}
