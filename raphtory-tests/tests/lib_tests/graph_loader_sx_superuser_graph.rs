#[cfg(test)]
mod sx_superuser_test {
    use raphtory::graph_loader::sx_superuser_graph::{sx_superuser_file, sx_superuser_graph};

    #[test]
    #[ignore] // don't hit SNAP by default
    fn test_download_works() {
        let file = sx_superuser_file().unwrap();
        assert!(file.is_file())
    }

    #[test]
    #[ignore] // don't hit SNAP by default  FIXME: add a truncated test file for this one?
    fn test_graph_loading_works() {
        sx_superuser_graph().unwrap();
    }
}
