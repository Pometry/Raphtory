//! Shared GraphQL test scaffolding: schema/fixture builders and single-mutation
//! runners used by the integration-style tests in `lib.rs`. Gated by `#[cfg(test)]`
//! and only intended for in-crate consumers.

#![allow(dead_code)]

use crate::{
    auth::Access,
    auth_policy::AuthorizationPolicy,
    config::app_config::AppConfig,
    data::{Data, DIRTY_PATH},
    model::App,
};
use async_graphql::dynamic::Schema;
use dynamic_graphql::Request;
use raphtory::{
    db::api::{storage::storage::Config, view::MaterializedGraph},
    serialise::ROOT_META_PATH,
};
use std::{path::Path, sync::Arc};
use tempfile::{tempdir, TempDir};

pub(crate) struct TestSetup {
    pub(crate) tmp: TempDir,
    pub(crate) data: Data,
    pub(crate) schema: Schema,
}

pub(crate) async fn setup_with_graphs(graphs: &[(&str, MaterializedGraph)]) -> TestSetup {
    let tmp = tempdir().unwrap();
    let data = Data::new(tmp.path(), &AppConfig::default(), Config::default());
    for (path, graph) in graphs {
        let folder = data.validate_path_for_insert(path, false).unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { tmp, data, schema }
}

pub(crate) async fn setup_with_policy(
    graphs: &[(&str, MaterializedGraph)],
    policy: Arc<dyn AuthorizationPolicy>,
) -> TestSetup {
    let tmp = tempdir().unwrap();
    let mut data = Data::new(tmp.path(), &AppConfig::default(), Config::default());
    for (path, graph) in graphs {
        let folder = data.validate_path_for_insert(path, false).unwrap();
        data.insert_graph(folder, graph.clone()).await.unwrap();
    }
    data.set_auth_policy(policy);
    let schema = App::create_schema().data(data.clone()).finish().unwrap();
    TestSetup { tmp, data, schema }
}

pub(crate) async fn run_mutation(schema: &Schema, query: &str) -> async_graphql::Response {
    let req = Request::new(query).data(Access::Rw);
    schema.execute(req).await
}

pub(crate) async fn run_mutation_as_user(schema: &Schema, query: &str) -> async_graphql::Response {
    // No `Access::Rw` injected, so the policy decides allow/deny.
    // A role (`Option<String>`) is injected because `write_denied` in
    // `model/mod.rs` returns the specific policy error message only when a
    // role is present; with `None` it returns the generic
    // `AuthError::RequireWrite` string, which would fail tests that assert on
    // the policy-specific text.
    let req = Request::new(query).data(Some("test-user".to_string()));
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
