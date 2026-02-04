use raphtory_api_macros::box_on_debug_lifetime;

struct LayerIds;
struct Direction;
struct EdgeRef;

struct TestStruct;

impl TestStruct {
    #[box_on_debug_lifetime]
    fn edge_iter<'a, 'b: 'a>(
        &'a self,
        _layer_ids: &'b LayerIds,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a {
        // Simplified version of your complex matching logic
        std::iter::empty()
    }
}

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

impl<'a> TestTrait<'a> for &'a TestStruct {
    type EntryRef = EdgeRef;

    #[box_on_debug_lifetime]
    fn edges_iter<'b>(
        self,
        _layers_ids: &'b LayerIds,
        _dir: Direction,
    ) -> impl Iterator<Item = EdgeRef> + Send + Sync + 'a
    where
        Self: Sized,
    {
        std::iter::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn can_send_and_sync<T: Send + Sync>(_t: &T) {}

    #[test]
    fn test_edge_iter() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds;
        let iter = test_struct.edge_iter(&layer_ids);
        can_send_and_sync(&iter);
        let collected: Vec<EdgeRef> = iter.collect();
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn test_edges_iter() {
        let test_struct = TestStruct;
        let layer_ids = LayerIds;
        let direction = Direction;
        let iter = (&test_struct).edges_iter(&layer_ids, direction);
        can_send_and_sync(&iter);
        let collected: Vec<EdgeRef> = iter.collect();
        assert_eq!(collected.len(), 0);
    }
}
