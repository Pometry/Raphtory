use raphtory_api_macros::box_on_debug_lifetime;

// Mock types to match your examples
struct Direction;
struct EdgeRef;
enum LayerIds {
    None,
    All,
    One(usize),
    Multiple(Vec<usize>),
}

// Mock Iter4 type similar to your usage
enum Iter4<A, B = A, C = A, D = A> {
    I(A),
    J(B),
    K(C),
    L(D),
}

impl<A, B, C, D> Iterator for Iter4<A, B, C, D>
where
    A: Iterator<Item = EdgeRef>,
    B: Iterator<Item = EdgeRef>,
    C: Iterator<Item = EdgeRef>,
    D: Iterator<Item = EdgeRef>,
{
    type Item = EdgeRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter4::I(iter) => iter.next(),
            Iter4::J(iter) => iter.next(),
            Iter4::K(iter) => iter.next(),
            Iter4::L(iter) => iter.next(),
        }
    }
}

// Mock trait with methods like yours
trait TestSelfTrait<'a> {
    type EntryRef;

    fn edge_iter_layer(&self, layer: usize) -> impl Iterator<Item = Self::EntryRef>;

    // Your first example function - should work with #[box_on_debug_lifetime]
    #[box_on_debug_lifetime]
    fn edge_iter<'b, 'c: 'b>(
        &'b self,
        _layer_ids: &'c LayerIds,
    ) -> impl Iterator<Item = Self::EntryRef> + Send + Sync + 'b
    where
        'a: 'b,
    {
        // Simplified version of your complex matching logic
        std::iter::empty()
    }

    fn edges_dir(&self, layer_id: usize, dir: Direction) -> impl Iterator<Item = EdgeRef>;
}

// Test trait for the second example
trait TestEdgesTrait<'a>
where
    Self: Sized,
{
    // Your second example function - should work with #[box_on_debug_lifetime]
    #[box_on_debug_lifetime]
    fn edges_iter<'b>(
        self,
        _layers_ids: &'b LayerIds,
        _dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        // Simplified version of your complex matching logic
        std::iter::empty()
    }
}

// Concrete implementation for testing
struct TestStruct;

impl<'a> TestSelfTrait<'a> for &'a TestStruct {
    type EntryRef = EdgeRef;

    fn edge_iter_layer(&self, _layer: usize) -> impl Iterator<Item = Self::EntryRef> {
        std::iter::empty()
    }

    fn edges_dir(&self, _layer_id: usize, _dir: Direction) -> impl Iterator<Item = EdgeRef> {
        std::iter::empty()
    }
}

impl<'a> TestEdgesTrait<'a> for &'a TestStruct {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_iter_compiles() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds::All;
        let test_ref = &test_struct;

        // Test that the first example function compiles and works
        let iter = test_ref.edge_iter(&layer_ids);
        let _results: Vec<EdgeRef> = iter.collect();
    }

    #[test]
    fn test_edges_iter_compiles() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds::All;
        let direction = Direction;
        let test_ref = &test_struct;

        // Test that the second example function compiles and works
        let iter = test_ref.edges_iter(&layer_ids, direction);
        let _results: Vec<EdgeRef> = iter.collect();
    }

    #[cfg(debug_assertions)]
    #[test]
    fn test_debug_returns_boxed() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds::All;
        let test_ref = &test_struct;

        // In debug builds, verify we get boxed iterators
        let iter = test_ref.edge_iter(&layer_ids);
        let _boxed: Box<dyn Iterator<Item = EdgeRef> + Send + Sync + '_> = iter;
    }
}
