use raphtory_api_macros::{box_on_debug, box_on_debug_lifetime};

// Mock types for testing
struct LayerIds;
struct Direction;
struct EdgeRef;
struct Iter4<T>(T);

impl<T> Iterator for Iter4<T>
where
    T: Iterator,
{
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl Iter4<std::iter::Empty<EdgeRef>> {
    fn I(iter: std::iter::Empty<EdgeRef>) -> Self {
        Iter4(iter)
    }
}

impl<T: Iterator<Item = EdgeRef>> Iter4<T> {
    fn J(iter: T) -> Self {
        Iter4(iter)
    }
    
    fn K(iter: T) -> Self {
        Iter4(iter)
    }
    
    fn L(iter: T) -> Self {
        Iter4(iter)
    }
}

// Test struct with methods similar to your examples
struct TestStruct;

impl TestStruct {
    fn edge_iter_layer(&self, _layer: usize) -> impl Iterator<Item = EdgeRef> {
        std::iter::empty()
    }

    fn edges_dir(&self, _layer_id: usize, _dir: Direction) -> impl Iterator<Item = EdgeRef> {
        std::iter::empty()
    }
}

impl TestStruct {
    // This should work with #[box_on_debug_lifetime]
    #[box_on_debug_lifetime]
    fn edge_iter<'a, 'b: 'a>(
        &'a self,
        layer_ids: &'b LayerIds,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a {
        // Simplified version of your complex matching logic
        std::iter::empty()
    }
}

// Test trait with lifetime bounds
trait TestTrait<'a> {
    type EntryRef;
    
    fn edges_iter<'b>(
        self,
        layers_ids: &'b LayerIds,
        dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized;
}

// Test implementation
impl<'a> TestTrait<'a> for &'a TestStruct {
    type EntryRef = EdgeRef;

    #[box_on_debug_lifetime]
    fn edges_iter<'b>(
        self,
        layers_ids: &'b LayerIds,
        _dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        // Simplified version of your example
        std::iter::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_iter() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds;
        let iter = test_struct.edge_iter(&layer_ids);
        let collected: Vec<EdgeRef> = iter.collect();
        assert_eq!(collected.len(), 0); // Empty iterator as expected
    }

    #[test]
    fn test_edges_iter() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds;
        let direction = Direction;
        let iter = (&test_struct).edges_iter(&layer_ids, direction);
        let collected: Vec<EdgeRef> = iter.collect();
        assert_eq!(collected.len(), 0); // Empty iterator as expected
    }
}