use ouroboros::self_referencing;

#[self_referencing]
pub struct GenLockedIter<'a, O, OUT> {
    owner: O,
    #[borrows(owner)]
    #[covariant]
    iter: Box<dyn Iterator<Item = OUT> + Send + 'this>,
    mark: std::marker::PhantomData<&'a O>,
}

impl<'a, O, OUT> Iterator for GenLockedIter<'a, O, OUT> {
    type Item = OUT;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.with_iter(|iter| iter.size_hint())
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.nth(n))
    }
}

impl<'a, O, OUT> GenLockedIter<'a, O, OUT> {
    pub fn from<'b>(
        owner: O,
        iter_fn: impl FnOnce(&O) -> Box<dyn Iterator<Item = OUT> + Send + '_> + 'b,
    ) -> Self {
        GenLockedIterBuilder {
            owner,
            iter_builder: |owner| iter_fn(owner),
            mark: std::marker::PhantomData,
        }
        .build()
    }
}