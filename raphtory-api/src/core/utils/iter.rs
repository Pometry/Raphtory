use ouroboros::self_referencing;

struct II<'a, I, OUT>(
    I,
    std::marker::PhantomData<&'a I>,
    std::marker::PhantomData<OUT>,
);

impl<'a, OUT, I: Iterator<Item = OUT>> Iterator for II<'a, I, OUT> {
    type Item = OUT;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[self_referencing]
pub struct GenLockedIter2<'a, O, OUT, I: 'a> {
    owner: O,
    #[borrows(owner)]
    #[covariant]
    iter: II<'this, I, OUT>,
    mark: std::marker::PhantomData<&'a O>,
}

impl<'a, O, OUT, I: Iterator<Item = OUT>> Iterator for GenLockedIter2<'a, O, OUT, I> {
    type Item = OUT;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.0.next())
    }
}

impl<'a, O, OUT, I: Iterator<Item = OUT>> GenLockedIter2<'a, O, OUT, I> {
    pub fn from<'b>(owner: O, iter_fn: impl FnOnce(&O) -> I + 'b) -> Self {
        GenLockedIter2Builder {
            owner,
            iter_builder: |owner| {
                II(
                    iter_fn(owner),
                    std::marker::PhantomData,
                    std::marker::PhantomData,
                )
            },
            mark: std::marker::PhantomData,
        }
        .build()
    }
}

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
