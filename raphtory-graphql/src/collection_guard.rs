use crate::config::concurrency_config::ConcurrencyConfig;
use async_graphql::{
    async_trait,
    extensions::{Extension, ExtensionContext, ExtensionFactory, NextParseQuery},
    parser::types::{ExecutableDocument, Field, Selection, SelectionSet, VariableDefinition},
    Name, Positioned, ServerError, ServerResult, Variables,
};
use async_graphql_value::{ConstValue, Value};
use std::{collections::HashSet, sync::Arc};

const LIST_DISABLED_ERROR: &str =
    "Bulk list endpoints are disabled on this server. Use `page` instead.";

/// Enforces `concurrency.disable_lists` and `concurrency.max_page_size` at parse time
/// by walking the `ExecutableDocument` and rejecting any `list`/`listRev` field (when
/// lists are disabled) or any `page`/`pageRev` field whose `limit` argument exceeds
/// the configured maximum.
pub struct CollectionGuard {
    disable_lists: bool,
    max_page_size: Option<usize>,
}

impl CollectionGuard {
    /// Returns `None` when neither guard is active — avoids installing an extension
    /// that would only no-op.
    pub fn from_config(config: &ConcurrencyConfig) -> Option<Self> {
        if !config.disable_lists && config.max_page_size.is_none() {
            return None;
        }
        Some(Self {
            disable_lists: config.disable_lists,
            max_page_size: config.max_page_size,
        })
    }
}

impl ExtensionFactory for CollectionGuard {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(CollectionGuardExtension {
            disable_lists: self.disable_lists,
            max_page_size: self.max_page_size,
        })
    }
}

struct CollectionGuardExtension {
    disable_lists: bool,
    max_page_size: Option<usize>,
}

#[async_trait::async_trait]
impl Extension for CollectionGuardExtension {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let doc = next.run(ctx, query, variables).await?;
        for (_, op) in doc.operations.iter() {
            let resolver = VariableResolver::new(&op.node.variable_definitions, variables);
            let mut visited = HashSet::new();
            self.walk(&op.node.selection_set.node, &doc, &resolver, &mut visited)?;
        }
        Ok(doc)
    }
}

impl CollectionGuardExtension {
    fn walk<'a>(
        &self,
        set: &'a SelectionSet,
        doc: &'a ExecutableDocument,
        resolver: &VariableResolver<'_>,
        visited: &mut HashSet<&'a str>,
    ) -> ServerResult<()> {
        for item in &set.items {
            match &item.node {
                Selection::Field(field) => {
                    let field_node = &field.node;
                    let name = field_node.name.node.as_str();
                    let pos = field.pos;
                    match name {
                        "list" | "listRev" if self.disable_lists => {
                            return Err(ServerError::new(LIST_DISABLED_ERROR, Some(pos)));
                        }
                        "page" | "pageRev" => {
                            if let Some(max) = self.max_page_size {
                                if let Some(limit) = field_limit(field_node, resolver) {
                                    if limit > max {
                                        return Err(ServerError::new(
                                            format!(
                                                "page limit {limit} exceeds the maximum allowed page size {max}"
                                            ),
                                            Some(pos),
                                        ));
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    self.walk(&field_node.selection_set.node, doc, resolver, visited)?;
                }
                Selection::InlineFragment(frag) => {
                    self.walk(&frag.node.selection_set.node, doc, resolver, visited)?;
                }
                Selection::FragmentSpread(spread) => {
                    let fragment_name = spread.node.fragment_name.node.as_str();
                    if !visited.insert(fragment_name) {
                        continue;
                    }
                    if let Some(def) = doc.fragments.get(&spread.node.fragment_name.node) {
                        self.walk(&def.node.selection_set.node, doc, resolver, visited)?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn field_limit(field: &Field, resolver: &VariableResolver<'_>) -> Option<usize> {
    let (_, value) = field
        .arguments
        .iter()
        .find(|(n, _)| n.node.as_str() == "limit")?;
    match &value.node {
        Value::Number(n) => n.as_u64().map(|v| v as usize),
        Value::Variable(name) => match resolver.resolve(name)? {
            ConstValue::Number(n) => n.as_u64().map(|v| v as usize),
            _ => None,
        },
        _ => None,
    }
}

/// Resolves a variable by name, falling back to the operation's declared default value
/// when the client omitted it. Stays scoped to a single operation because defaults are
/// per-operation.
struct VariableResolver<'a> {
    variables: &'a Variables,
    defaults: Vec<(&'a Name, &'a ConstValue)>,
}

impl<'a> VariableResolver<'a> {
    fn new(definitions: &'a [Positioned<VariableDefinition>], variables: &'a Variables) -> Self {
        let defaults = definitions
            .iter()
            .filter_map(|def| {
                def.node
                    .default_value
                    .as_ref()
                    .map(|v| (&def.node.name.node, &v.node))
            })
            .collect();
        Self {
            variables,
            defaults,
        }
    }

    fn resolve(&self, name: &Name) -> Option<&ConstValue> {
        if let Some(value) = self.variables.get(name) {
            return Some(value);
        }
        self.defaults
            .iter()
            .find_map(|(n, v)| (*n == name).then_some(*v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::parser::parse_query;

    fn run(
        disable_lists: bool,
        max_page_size: Option<usize>,
        query: &str,
        variables: Variables,
    ) -> Result<(), String> {
        let ext = CollectionGuardExtension {
            disable_lists,
            max_page_size,
        };
        let doc = parse_query(query).map_err(|e| e.to_string())?;
        for (_, op) in doc.operations.iter() {
            let resolver = VariableResolver::new(&op.node.variable_definitions, &variables);
            let mut visited = HashSet::new();
            ext.walk(&op.node.selection_set.node, &doc, &resolver, &mut visited)
                .map_err(|e| e.message)?;
        }
        Ok(())
    }

    #[test]
    fn rejects_list_when_disabled() {
        let err = run(true, None, "{ foo { list { bar } } }", Variables::default()).unwrap_err();
        assert!(err.contains("Bulk list endpoints are disabled"));
    }

    #[test]
    fn rejects_list_rev_when_disabled() {
        let err = run(
            true,
            None,
            "{ foo { listRev { bar } } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("Bulk list endpoints are disabled"));
    }

    #[test]
    fn allows_list_when_not_disabled() {
        run(
            false,
            None,
            "{ foo { list { bar } } }",
            Variables::default(),
        )
        .unwrap();
    }

    #[test]
    fn rejects_page_over_max() {
        let err = run(
            false,
            Some(10),
            "{ foo { page(limit: 50) { bar } } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("page limit 50 exceeds the maximum allowed page size 10"));
    }

    #[test]
    fn allows_page_under_max() {
        run(
            false,
            Some(50),
            "{ foo { page(limit: 10) { bar } } }",
            Variables::default(),
        )
        .unwrap();
    }

    #[test]
    fn resolves_limit_from_provided_variable() {
        let vars = Variables::from_json(serde_json::json!({ "n": 100 }));
        let err = run(
            false,
            Some(10),
            "query ($n: Int!) { foo { page(limit: $n) { bar } } }",
            vars,
        )
        .unwrap_err();
        assert!(err.contains("page limit 100 exceeds"));
    }

    #[test]
    fn resolves_limit_from_variable_default() {
        let err = run(
            false,
            Some(10),
            "query ($n: Int = 100) { foo { page(limit: $n) { bar } } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("page limit 100 exceeds"));
    }

    #[test]
    fn walks_into_inline_fragments() {
        let err = run(
            true,
            None,
            "{ foo { ... on Foo { list { bar } } } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("Bulk list endpoints are disabled"));
    }

    #[test]
    fn walks_into_fragment_spreads() {
        let err = run(
            true,
            None,
            "fragment F on Foo { list { bar } } { foo { ...F } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("Bulk list endpoints are disabled"));
    }

    #[test]
    fn handles_cyclic_fragments_without_looping() {
        // Cycle is spec-invalid but arrives here before async-graphql's validation; the
        // visited set must prevent infinite recursion.
        let err = run(
            true,
            None,
            "fragment A on T { ...B list { x } } fragment B on T { ...A } { root { ...A } }",
            Variables::default(),
        )
        .unwrap_err();
        assert!(err.contains("Bulk list endpoints are disabled"));
    }
}
