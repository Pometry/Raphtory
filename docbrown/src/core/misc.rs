pub trait MinMax<A> {
    fn min() -> A;
    fn max() -> A;
    fn one() -> A;
}

impl MinMax<u64> for u64 {
    fn min() -> u64 {
        u64::MIN
    }

    fn max() -> u64 {
        u64::MAX
    }

    fn one() -> u64 {
        1
    }
}

impl MinMax<i32> for i32 {
    fn min() -> i32 {
        i32::MIN
    }

    fn max() -> i32 {
        i32::MAX
    }

    fn one() -> i32 {
        1
    }
}
