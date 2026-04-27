use proptest::prelude::*;

const MAX_GRANTS_PER_CASE: usize = 50;

#[derive(Debug, Clone, Copy)]
pub enum PermissionTarget {
    Graph,
    Namespace,
}

#[derive(Debug, Clone, Copy)]
pub enum GraphPermission {
    Read,
    Write,
}

impl GraphPermission {
    pub(crate) fn as_gql(self) -> &'static str {
        match self {
            GraphPermission::Read => "READ",
            GraphPermission::Write => "WRITE",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NamespacePermission {
    Discover,
    Introspect,
    Read,
    Write,
}

impl NamespacePermission {
    pub(crate) fn as_gql(self) -> &'static str {
        match self {
            NamespacePermission::Discover => "DISCOVER",
            NamespacePermission::Introspect => "INTROSPECT",
            NamespacePermission::Read => "READ",
            NamespacePermission::Write => "WRITE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PermissionGrant {
    pub user_idx: u64,
    pub target: PermissionTarget,
    pub path_idx: usize,
    pub graph_permission: GraphPermission,
    pub namespace_permission: NamespacePermission,
}

pub fn permissions_strategy(
    num_users: u64,
    num_graph_paths: usize,
    num_namespace_paths: usize,
) -> impl Strategy<Value = Vec<PermissionGrant>> {
    let max_user = num_users.saturating_sub(1);
    let max_graph = num_graph_paths.saturating_sub(1);
    let max_namespace = num_namespace_paths.saturating_sub(1);
    let namespace_available = num_namespace_paths > 0;

    prop::collection::vec(
        (
            0..=max_user,
            any::<bool>(),
            prop_oneof![Just(GraphPermission::Read), Just(GraphPermission::Write)],
            prop_oneof![
                Just(NamespacePermission::Discover),
                Just(NamespacePermission::Introspect),
                Just(NamespacePermission::Read),
                Just(NamespacePermission::Write),
            ],
            0usize..=max_graph,
            0usize..=max_namespace,
        )
            .prop_map(
                move |(
                    user_idx,
                    choose_namespace,
                    graph_permission,
                    namespace_permission,
                    graph_idx,
                    namespace_idx,
                )| {
                    let target = if choose_namespace && namespace_available {
                        PermissionTarget::Namespace
                    } else {
                        PermissionTarget::Graph
                    };
                    let path_idx = match target {
                        PermissionTarget::Graph => graph_idx,
                        PermissionTarget::Namespace => namespace_idx,
                    };

                    PermissionGrant {
                        user_idx,
                        target,
                        path_idx,
                        graph_permission,
                        namespace_permission,
                    }
                },
            ),
        1..=MAX_GRANTS_PER_CASE,
    )
}
