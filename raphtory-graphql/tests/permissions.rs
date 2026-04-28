mod utils;

use std::ops::RangeInclusive;
use std::sync::LazyLock;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use proptest::prelude::*;
use serde_json::json;
use url::Url;

use utils::graphql::{
    create_graphs, create_role, get_client,
    grant_graph, grant_namespace, start_server,
};
use utils::strategy::{permissions_strategy, PermissionGrant};

const PORT: u16 = 43871;

// Borrowed from test_permissions.py.
const PUB_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

pub static ADMIN_JWT: LazyLock<String> = LazyLock::new(|| {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes())
        .expect("decode Ed25519 private key for test JWTs");

    encode(
        &Header::new(Algorithm::EdDSA),
        &json!({ "access": "rw", "role": "admin" }),
        &key,
    )
    .expect("encode admin JWT")
});


#[test]
fn permissions_proptest() {
    const PROPTEST_CASES: u32 = 10;
    const NAMESPACE_SIZE: RangeInclusive<usize> = 1..=20;
    const NUM_USERS: RangeInclusive<usize> = 1..=10;

    proptest!(
        ProptestConfig::with_cases(PROPTEST_CASES),
        |(case in permissions_strategy(NAMESPACE_SIZE, NUM_USERS))| {
            let (_server, _tempdir) = start_server(PORT, PUB_KEY);

            let url = Url::parse(&format!("http://127.0.0.1:{PORT}")).unwrap();
            let client = get_client(url, ADMIN_JWT.to_string());

            // Create nested namespaces and graphs on the server.
            create_graphs(&client, &case.namespace_tree);

            // Create roles for each user.
            for i in 0..case.num_users {
                let role = format!("user_{i}");
                create_role(&client, &role);
            }

            for grant in &case.grants {
                match grant {
                    PermissionGrant::Graph {
                        user_id,
                        path,
                        permission,
                    } => {
                        let role = format!("user_{user_id}");
                        grant_graph(&client, &role, path, *permission)
                    }
                    PermissionGrant::Namespace {
                        user_id,
                        path,
                        permission,
                    } => {
                        let role = format!("user_{user_id}");
                        grant_namespace(&client, &role, path, *permission)
                    }
                }
            }
        }
    );
}
