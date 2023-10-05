use ahash::AHashMap;

use super::GID;

pub(crate) trait GlobalOrder: FromIterator<GID>{
    fn len(&self) -> usize;

    fn push_id<ID: Into<GID>>(&mut self, id: ID);
    fn sorted(&self) -> Box<dyn Iterator<Item = GID> + '_>;
    fn find(&self, gid: &GID) -> Option<usize>;

    fn maybe_sort(&mut self) {} // some implementations may not need to sort

    fn extend<ID: Into<GID>, I: IntoIterator<Item = ID>>(&mut self, other: I) {
        for gid in other {
            self.push_id(gid);
        }
    }
}

impl<ID: Into<GID>> FromIterator<ID> for GlobalMap {
    fn from_iter<I: IntoIterator<Item = ID>>(iter: I) -> Self {
        let mut global: GlobalMap = Default::default();
        for gid in iter {
            global.push_id(gid);
        }
        global
    }
}

impl <ID: Into<GID>, I:IntoIterator<Item = ID>> From<I> for GlobalMap {
    fn from(iter: I) -> Self {
        iter.into_iter().collect()
    }
}

#[derive(Debug, Default)]
pub(crate) struct GlobalMap {
    gids_table: AHashMap<GID, usize>,
    sorted: Vec<GID>,
}

impl GlobalOrder for GlobalMap {
    fn push_id<ID: Into<GID>>(&mut self, id: ID) {
        let id = id.into();
        if !self.gids_table.contains_key(&id) {
            self.gids_table.insert(id, self.gids_table.len());
        }
    }

    fn sorted(&self) -> Box<dyn Iterator<Item = GID> + '_> {
        Box::new(self.sorted.iter().cloned())
    }

    fn find(&self, gid: &GID) -> Option<usize> {
        self.gids_table.get(gid).cloned()
    }

    fn maybe_sort(&mut self) {
        self.sorted = self.gids_table.keys().cloned().collect();
        self.sorted.sort();
    }

    fn len(&self) -> usize {
        self.gids_table.len()
    }
}
