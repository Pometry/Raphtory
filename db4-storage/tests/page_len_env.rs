//! Lives in its own test binary because it sets process-wide environment variables.

use db4_storage::persist::{
    args::{ArgsOps, BaseArgs},
    config::{BaseConfig, ConfigOps},
};

#[test]
fn page_len_env_vars_are_ignored_on_load() {
    let config = BaseConfig::default()
        .with_max_node_page_len(10)
        .with_max_edge_page_len(20);
    let stored = config.clone();

    // SAFETY: the only test in this binary, so nothing else reads the environment.
    unsafe {
        std::env::set_var("RAPHTORY_MAX_NODE_PAGE_LEN", "1024");
        std::env::set_var("RAPHTORY_MAX_EDGE_PAGE_LEN", "2048");
    }

    // The env vars set the page sizes of new graphs.
    let created = BaseConfig::from(BaseArgs::default());
    assert_eq!(created.max_node_page_len(), 1024);
    assert_eq!(created.max_edge_page_len(), 2048);

    // Args that only carry the env values keep the stored page sizes.
    let loaded = BaseArgs::default().apply_to_config(stored).unwrap();
    assert_eq!(loaded.max_node_page_len(), 10);
    assert_eq!(loaded.max_edge_page_len(), 20);

    // A page size set by the caller is still refused.
    for args in [
        BaseArgs::default().with_max_node_page_len(123),
        BaseArgs::default().with_max_edge_page_len(123),
    ] {
        let err = args.apply_to_config(stored).unwrap_err();
        assert!(err.to_string().contains("Page sizes cannot be overridden"));
    }
}
