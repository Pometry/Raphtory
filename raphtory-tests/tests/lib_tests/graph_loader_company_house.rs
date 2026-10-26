#[cfg(test)]
mod company_house_graph_test {
    use raphtory::{
        db::api::view::{NodeViewOps, TimeOps},
        prelude::*,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::info;

    use raphtory::graph_loader::company_house::*;

    #[test]
    #[ignore]
    fn test_ch_load() {
        global_info_logger();
        let g = company_house_graph(None);
        assert_eq!(g.start().unwrap(), 1000);
        assert_eq!(g.end().unwrap(), 1001);
        g.window(1000, 1001)
            .nodes()
            .into_iter()
            .for_each(|v| info!("nodeid = {}", v.id()));
    }
}
