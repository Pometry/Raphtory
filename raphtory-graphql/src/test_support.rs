//! Shared GraphQL test scaffolding: schema/fixture builders and single-mutation
//! runners used by the integration-style tests in `lib.rs`. Gated by `#[cfg(test)]`
//! and only intended for in-crate consumers.

#![allow(dead_code)]

use crate::{
    auth::Access, auth_policy::AuthorizationPolicy, config::app_config::AppConfig, data::Data,
    model::App,
};
use async_graphql::dynamic::Schema;
use dynamic_graphql::Request;
use raphtory::db::api::{storage::storage::Config, view::MaterializedGraph};
use raphtory_api::core::storage::graph_folder::{DIRTY_PATH, ROOT_META_PATH};
use std::{path::Path, sync::Arc};

pub(crate) struct TestSetup {
    pub(crate) data: Data,
    pub(crate) schema: Schema,
}

pub(crate) async fn setup_with_graphs(
    graphs: &[(&str, MaterializedGraph)],
    work_dir: &Path,
) -> TestSetup {
    let data = Data::new(work_dir, &AppConfig::default(), Config::default());
    for (path, graph) in graphs {
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { data, schema }
}

pub(crate) async fn setup_with_policy(
    graphs: &[(&str, MaterializedGraph)],
    work_dir: &Path,
    policy: Arc<dyn AuthorizationPolicy>,
) -> TestSetup {
    let mut data = Data::new(work_dir, &AppConfig::default(), Config::default());
    for (path, graph) in graphs {
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    data.set_auth_policy(policy);
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { data, schema }
}

pub(crate) async fn run_mutation(schema: &Schema, query: &str) -> async_graphql::Response {
    let req = Request::new(query).data(Access::Rw);
    schema.execute(req).await
}

pub(crate) async fn run_mutation_as_user(schema: &Schema, query: &str) -> async_graphql::Response {
    // No `Access::Rw` injected, so the policy decides allow/deny. `FakePolicy` resolves
    // permissions by path alone, so no identity needs to travel in the request.
    let req = Request::new(query);
    schema.execute(req).await
}

/// Assert that `path` is an existing directory that is NOT a graph folder
/// (no `ROOT_META_PATH`) and contains no leftover `DIRTY_PATH` marker.
pub(crate) fn assert_is_namespace_dir(path: &Path) {
    assert!(path.is_dir(), "expected directory at {:?}", path);
    assert!(
        !path.join(ROOT_META_PATH).exists(),
        "{:?} contains a graph metadata file ({}); expected a plain namespace directory",
        path,
        ROOT_META_PATH,
    );
    assert!(
        !path.join(DIRTY_PATH).exists(),
        "{:?} contains a dirty marker ({}); expected a clean namespace directory",
        path,
        DIRTY_PATH,
    );
}
