use raphtory_graphql::GQLError;
use std::io;

/// An I/O failure is about the server, so `?` must carry it to the generic answer, never
/// to the client with its own text (which can name server paths).
#[test]
fn an_io_error_reaches_the_client_as_an_internal_error() {
    let error: dynamic_graphql::Error =
        GQLError::from(io::Error::other("/srv/graphs/secret: permission denied")).into();
    assert_eq!(error.message, "Internal server error");
}
