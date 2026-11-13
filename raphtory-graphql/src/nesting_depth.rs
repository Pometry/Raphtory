use async_graphql::{
    async_trait,
    extensions::{Extension, ExtensionContext, ExtensionFactory, NextParseQuery},
    parser::types::ExecutableDocument,
    ServerError, ServerResult, Variables,
};
use std::sync::Arc;

/// Refuses a query whose `{`, `[` and `(` nest deeper than its limit, before it is parsed. The
/// parser recurses once per level of an argument value with no limit of its own, so a deep enough
/// value overflows the stack; `limit_recursive_depth` only counts selection sets.
#[derive(Clone, Copy)]
pub(crate) struct NestingDepthLimit(pub(crate) usize);

impl ExtensionFactory for NestingDepthLimit {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(*self)
    }
}

#[async_trait::async_trait]
impl Extension for NestingDepthLimit {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        if nesting_exceeds(query.as_bytes(), self.0) {
            return Err(ServerError::new(
                format!("the query is nested deeper than {} levels", self.0),
                None,
            ));
        }
        next.run(ctx, query, variables).await
    }
}

/// Whether `{`, `[` and `(` nest deeper than `limit` in `query`, outside strings, block
/// strings and comments. One pass, no recursion.
fn nesting_exceeds(query: &[u8], limit: usize) -> bool {
    let mut depth = 0usize;
    let mut i = 0;
    while i < query.len() {
        let rest = &query[i..];
        i += if rest.starts_with(b"\"\"\"") {
            3 + block_string_len(&rest[3..])
        } else {
            match rest[0] {
                b'"' => 1 + string_len(&rest[1..]),
                b'#' => comment_len(rest),
                b'{' | b'[' | b'(' => {
                    depth += 1;
                    if depth > limit {
                        return true;
                    }
                    1
                }
                b'}' | b']' | b')' => {
                    depth = depth.saturating_sub(1);
                    1
                }
                _ => 1,
            }
        };
    }
    false
}

/// The length of a string's body and closing quote, after its opening quote.
fn string_len(body: &[u8]) -> usize {
    let mut i = 0;
    while i < body.len() {
        match body[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    body.len()
}

/// The length of a block string's body and closing `"""`, after its opening `"""`.
fn block_string_len(body: &[u8]) -> usize {
    let mut i = 0;
    while i < body.len() {
        if body[i..].starts_with(b"\\\"\"\"") {
            i += 4;
        } else if body[i..].starts_with(b"\"\"\"") {
            return i + 3;
        } else {
            i += 1;
        }
    }
    body.len()
}

/// The length of a comment, up to the end of its line.
fn comment_len(comment: &[u8]) -> usize {
    comment
        .iter()
        .position(|&b| b == b'\n' || b == b'\r')
        .unwrap_or(comment.len())
}
