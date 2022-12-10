use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
};

use roaring::RoaringTreemap;

trait TVec<A> {
    /**
     * Append the item at the end of the TVec
     *  */
    fn push(&mut self, t: u64, a: A);

    /**
     * Append the item at the end of the TVec
     *  */
    fn insert(&mut self, t: u64, a: A, i: usize);

    /**
     *  Iterate all the items irrespective of time
     *  */
    fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_>;

    /**
     *  Iterate the items in the time window
     *  */
    fn iter_window<R: RangeBounds<u64>>(&self, r: R) -> Box<dyn Iterator<Item = &A> + '_>;

    /**
     *  Iterate the items in the time window and return the time with them
     *  */
    fn iter_window_timed<R: RangeBounds<u64>>(
        &self,
        r: R,
    ) -> Box<dyn Iterator<Item = (&A, u64)> + '_>;

    /**
     *  get me the oldest value at i
     *  */
    fn get_oldest_at(&self, i: usize) -> Option<&A>;

    /**
     *  get me the youngest value at i
     *  */
    fn get_youngest_at(&self, i: usize) -> Option<&A>;

    /**
     * get all the items at index i
     *  */
    fn get_window<R: RangeBounds<u64>>(
        &self,
        i: usize,
        r: R,
    ) -> Box<dyn Iterator<Item = (&A, u64)> + '_>;

    /**
     *  sort the values and rebuild the index
     *  */
    fn sort(&mut self)
    where
        A: Ord;
}

#[derive(Debug)]
enum TCell<A> {
    Cell1(A),
    Cell2(A, A),
    CellN(Vec<A>),
}

impl<A> TCell<A> {
    fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::Cell1(a) => {
                let x: Box<std::iter::Once<&A>> = Box::new(std::iter::once(a));
                x
            }
            TCell::Cell2(a1, a2) => Box::new(std::iter::once(a1).chain(std::iter::once(a2))),
            TCell::CellN(v) => Box::new(v.iter()),
        }
    }
}

#[derive(Debug, Default)]
struct DefaultTVec<A> {
    vs: Vec<A>,
    t_index: BTreeMap<u64, RoaringTreemap>,
}

impl<A> TVec<A> for DefaultTVec<A> {
    fn push(&mut self, t: u64, a: A) {
        let i = self.vs.len();
        self.vs.push(a);

        self.t_index
            .entry(t)
            .and_modify(|set| {
                set.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
            })
            .or_insert_with(|| {
                let mut bs = RoaringTreemap::default();
                bs.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
                bs
            });
    }

    fn insert(&mut self, t: u64, a: A, i: usize) {
        self.vs.insert(i, a);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        Box::new(self.vs.iter())
    }

    fn iter_window<R: RangeBounds<u64>>(&self, r: R) -> Box<dyn Iterator<Item = &A> + '_> {
        let iter = self
            .t_index
            .range(r)
            .flat_map(|(_, vs)| vs.iter())
            .map(|id| {
                let i: usize = id.try_into().unwrap();
                &self.vs[i]
            });
        Box::new(iter)
    }

    fn iter_window_timed<R: RangeBounds<u64>>(
        &self,
        r: R,
    ) -> Box<dyn Iterator<Item = (&A, u64)> + '_> {
        todo!()
    }

    fn get_oldest_at(&self, i: usize) -> Option<&A> {
        todo!()
    }

    fn get_youngest_at(&self, i: usize) -> Option<&A> {
        todo!()
    }

    fn get_window<R: RangeBounds<u64>>(
        &self,
        i: usize,
        r: R,
    ) -> Box<dyn Iterator<Item = (&A, u64)> + '_> {
        todo!()
    }

    fn sort(&mut self)
    where
        A: Ord,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12);
        tvec.push(9, 3);
        tvec.push(1, 2);

        assert_eq!(tvec.iter().collect::<Vec<_>>(), vec![&12, &3, &2]);
    }

    #[test]
    fn timed_iter() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12);
        tvec.push(9, 3);
        tvec.push(1, 2);

        println!("{:?}", tvec);
        assert_eq!(tvec.iter_window(0..5).collect::<Vec<_>>(), vec![&2, &12]);
    }

    #[test]
    fn insert() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12);
        tvec.push(9, 3);
        tvec.push(1, 2);

        tvec.insert(3, 19, 2);

        println!("{:?}", tvec);
        assert_eq!(
            tvec.iter_window(0..5).collect::<Vec<_>>(),
            vec![&2, &19, &12]
        );
    }
}
