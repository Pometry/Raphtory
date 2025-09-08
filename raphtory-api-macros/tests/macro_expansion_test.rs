use raphtory_api_macros::box_on_debug_lifetime;

struct TestItem;

#[box_on_debug_lifetime]
fn test_function<'a>() -> impl Iterator<Item = TestItem> + Send + Sync + 'a {
    std::iter::empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_vs_release_types() {
        let iter = test_function();
        
        // This test verifies that the function returns the correct type
        // In debug builds: Box<dyn Iterator<Item = TestItem> + Send + Sync + 'a>
        // In release builds: impl Iterator<Item = TestItem> + Send + Sync + 'a
        
        let _collected: Vec<TestItem> = iter.collect();
    }
    
    #[test]
    #[cfg(debug_assertions)]
    fn test_debug_build_returns_box() {
        let iter = test_function();
        // In debug builds, we should get a boxed iterator
        // This is a compile-time check - if the return type is wrong, this won't compile
        let _boxed: Box<dyn Iterator<Item = TestItem> + Send + Sync> = iter;
    }
}