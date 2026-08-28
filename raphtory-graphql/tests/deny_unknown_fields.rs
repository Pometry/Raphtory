//! Lint: every type that is both a GraphQL input and serde-deserialized must reject unknown fields.
//!
//! A type deriving `InputObject` reaches the server over the wire; one deriving `Deserialize` also
//! arrives through serde — a permissions file, a token, a policy document. serde's default is to
//! *silently ignore* a field it does not recognise, so a struct on that path without
//! `#[serde(deny_unknown_fields)]` turns a typo (`filtr` for `filter`) into an empty value rather
//! than an error. For an access filter that meant an unfiltered grant served as if scoped.
//!
//! So: any struct deriving both `InputObject` and `Deserialize` must carry
//! `#[serde(deny_unknown_fields)]`. The one structural exception is a struct with a
//! `#[serde(flatten)]` field — serde cannot combine `flatten` with `deny_unknown_fields` — which is
//! skipped here. (`AppConfig`/`TokenClaims` use `flatten`, but they are `Deserialize`-only, not
//! `InputObject`, so they never reach this check anyway.)
//!
//! This scans the crate's own `src/` with syn; it is not a runtime test.

use quote::ToTokens;
use syn::{Attribute, Item, ItemStruct};

/// The rendered tokens of every `#[<name>(...)]` attribute on `attrs`, joined. Substring matching
/// over this is robust to spacing and path qualification (`dynamic_graphql::InputObject`) in a way
/// that hand-parsing the meta is not, and the identifiers we look for are unambiguous.
fn attr_tokens(attrs: &[Attribute], name: &str) -> String {
    attrs
        .iter()
        .filter(|a| a.path().is_ident(name))
        .map(|a| a.to_token_stream().to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether this item is behind `#[cfg(test)]`, so test-only fixtures are not linted.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().is_ident("cfg") && a.to_token_stream().to_string().contains("test"))
}

fn check_struct(s: &ItemStruct, file: &str, offenders: &mut Vec<String>) {
    let derives = attr_tokens(&s.attrs, "derive");
    if !(derives.contains("InputObject") && derives.contains("Deserialize")) {
        return;
    }
    // A flattened field is incompatible with deny_unknown_fields; such a struct is exempt.
    let has_flatten = s
        .fields
        .iter()
        .any(|f| attr_tokens(&f.attrs, "serde").contains("flatten"));
    if has_flatten {
        return;
    }
    if !attr_tokens(&s.attrs, "serde").contains("deny_unknown_fields") {
        offenders.push(format!("{file}: struct {}", s.ident));
    }
}

fn check_items(items: &[Item], file: &str, offenders: &mut Vec<String>) {
    for item in items {
        match item {
            Item::Struct(s) if !is_test_only(&s.attrs) => check_struct(s, file, offenders),
            // Recurse into inline modules (external `mod foo;` files are visited on their own).
            Item::Mod(m) if !is_test_only(&m.attrs) => {
                if let Some((_, inner)) = &m.content {
                    check_items(inner, file, offenders);
                }
            }
            _ => {}
        }
    }
}

#[test]
fn input_objects_reject_unknown_fields() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for entry in walkdir::WalkDir::new(&src)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let text = std::fs::read_to_string(path).expect("read source file");
        let rel = path
            .strip_prefix(&src)
            .unwrap_or(path)
            .display()
            .to_string();
        if let Ok(ast) = syn::parse_file(&text) {
            check_items(&ast.items, &rel, &mut offenders);
        }
    }

    assert!(
        offenders.is_empty(),
        "these types derive both `InputObject` and `Deserialize` but do not \
         `#[serde(deny_unknown_fields)]`, so an unrecognised field is silently dropped instead of \
         rejected (add the attribute, or `#[serde(flatten)]` a field if that is intended):\n  {}",
        offenders.join("\n  ")
    );
}
