use lock_api::RawRwLock;

pub struct Items<'a, T: 'static, L: RawRwLock> {
    guards: Vec<lock_api::RwLockReadGuard<'a, L, Vec<Option<T>>>>,
}

// new for Items
impl<'a, T: 'static, L: RawRwLock> Items<'a, T, L> {
    pub fn new(guards: Vec<lock_api::RwLockReadGuard<'a, L, Vec<Option<T>>>>) -> Self {
        Items { guards }
    }
}

impl<'a, T: 'static, L: RawRwLock> Items<'a, T, L> {
    pub fn iter(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        let iter = self.guards.iter().flat_map(|guard| guard.iter().flatten());
        Box::new(iter)
    }
}
