use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Error, ItemFn, Path, Result, ReturnType, Type, TypeParamBound};

/// A procedural macro that boxes iterators in debug builds for better debugging experience.
/// In release builds, the function remains unchanged for optimal performance.
///
/// This macro transforms iterator-returning functions to return boxed iterators
/// only when debug assertions are enabled (`cfg(debug_assertions)`). This provides
/// better debugging experience by making iterator types concrete while maintaining
/// optimal performance in release builds.
///
/// # Usage
///
/// Simply annotate your iterator-returning function with `#[box_on_debug]`:
///
/// ```rust
/// use raphtory_api_macros::box_on_debug;
///
/// #[box_on_debug]
/// pub fn simple_iter(count: usize) -> impl Iterator<Item = i32> {
///     (0..count as i32).filter(|x| x % 2 == 0)
/// }
///
/// // Test the function works
/// let result: Vec<i32> = simple_iter(10).collect();
/// assert_eq!(result, vec![0, 2, 4, 6, 8]);
/// ```
///
/// ### Method with `&self` parameter:
/// ```rust
/// use raphtory_api_macros::box_on_debug;
///
/// struct Graph {
///     node_count: usize,
/// }
///
/// impl Graph {
///     #[box_on_debug]
///     pub fn iter_node_ids(&self) -> impl Iterator<Item = usize> {
///         0..self.node_count
///     }
/// }
///
/// // Test the method works
/// let graph = Graph { node_count: 5 };
/// let ids: Vec<usize> = graph.iter_node_ids().collect();
/// assert_eq!(ids, vec![0, 1, 2, 3, 4]);
/// ```
///
/// ### Function with additional bounds:
/// ```rust
/// use raphtory_api_macros::box_on_debug;
///
/// #[box_on_debug]
/// pub fn concurrent_iter(count: usize) -> impl Iterator<Item = i32> + Send + Sync {
///     (0..count as i32).filter(|x| x % 3 == 0)
/// }
///
/// // Test the function works
/// let result: Vec<i32> = concurrent_iter(10).collect();
/// assert_eq!(result, vec![0, 3, 6, 9]);
/// ```
///
#[proc_macro_attribute]
pub fn box_on_debug(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    match generate_box_on_debug_impl(&input_fn) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// A specialized procedural macro for functions with complex lifetime parameters.
/// This macro handles functions that have explicit lifetime parameters and complex bounds.
///
/// # Usage
///
/// Simply annotate your iterator-returning function with `#[box_on_debug_lifetime]`:
///
/// ## Method with complex lifetime bounds:
/// ```rust
/// use raphtory_api_macros::box_on_debug_lifetime;
///
/// struct Graph;
/// struct LayerIds;
/// struct EntryRef<'a>(&'a str);
///
/// impl Graph {
///     #[box_on_debug_lifetime]
///     fn edge_iter<'a, 'b: 'a>(
///         &'a self,
///         layer_ids: &'b LayerIds,
///     ) -> impl Iterator<Item = EntryRef<'a>> + Send + Sync + 'a {
///         std::iter::once(EntryRef("test"))
///     }
/// }
///
/// // Test the method works
/// let graph = Graph;
/// let layer_ids = LayerIds;
/// let entries: Vec<EntryRef> = graph.edge_iter(&layer_ids).collect();
/// assert_eq!(entries.len(), 1);
/// assert_eq!(entries[0].0, "test");
/// ```
///
/// ## Function consuming self with lifetime parameter:
/// ```rust
/// use raphtory_api_macros::box_on_debug_lifetime;
///
/// struct EdgeStorage;
/// struct LayerIds;
/// struct EdgeStorageEntry<'a>(&'a str);
///
/// impl EdgeStorage {
///     #[box_on_debug_lifetime]
///     pub fn iter<'a>(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
///         std::iter::once(EdgeStorageEntry("test"))
///     }
/// }
///
/// // Test the function works
/// let storage = EdgeStorage;
/// let layer_ids = LayerIds;
/// let entries: Vec<EdgeStorageEntry> = storage.iter(&layer_ids).collect();
/// assert_eq!(entries.len(), 1);
/// assert_eq!(entries[0].0, "test");
/// ```
///
/// ## Function with where clause:
/// ```rust
/// use raphtory_api_macros::box_on_debug_lifetime;
///
/// struct Data<T> {
///     items: Vec<T>,
/// }
///
/// impl<T> Data<T>
/// where
///     T: Clone + Send + Sync,
/// {
///     #[box_on_debug_lifetime]
///     pub fn iter_cloned<'a>(&'a self) -> impl Iterator<Item = T> + 'a
///     where
///         T: Clone,
///     {
///         self.items.iter().cloned()
///     }
/// }
///
/// // Test the function works
/// let data = Data { items: vec![1, 2, 3, 4, 5] };
/// let cloned: Vec<i32> = data.iter_cloned().collect();
/// assert_eq!(cloned, vec![1, 2, 3, 4, 5]);
/// ```
///
#[proc_macro_attribute]
pub fn box_on_debug_lifetime(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    match generate_box_on_debug_lifetime_impl(&input_fn) {
        Ok(output) => output.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_box_on_debug_impl(input_fn: &ItemFn) -> Result<TokenStream2> {
    let attrs = &input_fn.attrs;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;
    let fn_name = &sig.ident;

    // Parse the return type to extract iterator information
    let (item_type, bounds) = parse_iterator_return_type(&sig.output)?;

    // Generate the debug version (boxed)
    let debug_return_type = generate_boxed_return_type(&item_type, &bounds);

    // Generate the release version (original)
    let release_return_type = &sig.output;

    let generics = &sig.generics;
    let inputs = &sig.inputs;
    let where_clause = &sig.generics.where_clause;

    Ok(quote! {
        #[cfg(has_debug_symbols)]
        #(#attrs)*
        #vis fn #fn_name #generics(#inputs) #debug_return_type #where_clause {
            let iter = #block;
            Box::new(iter)
        }

        #[cfg(not(has_debug_symbols))]
        #(#attrs)*
        #vis fn #fn_name #generics(#inputs) #release_return_type #where_clause {
            #block
        }
    })
}

fn generate_box_on_debug_lifetime_impl(input_fn: &ItemFn) -> Result<TokenStream2> {
    let attrs = &input_fn.attrs;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;
    let fn_name = &sig.ident;

    // Parse the return type to extract iterator information
    let (item_type, bounds) = parse_iterator_return_type(&sig.output)?;

    // For lifetime version, we preserve all bounds including lifetimes
    let debug_return_type = generate_boxed_return_type_with_lifetimes(&item_type, &bounds);

    // Generate the release version (original)
    let release_return_type = &sig.output;

    let generics = &sig.generics;
    let inputs = &sig.inputs;
    let where_clause = &sig.generics.where_clause;

    Ok(quote! {
        #[cfg(has_debug_symbols)]
        #(#attrs)*
        #vis fn #fn_name #generics(#inputs) #debug_return_type #where_clause {
            let iter = #block;
            Box::new(iter)
        }

        #[cfg(not(has_debug_symbols))]
        #(#attrs)*
        #vis fn #fn_name #generics(#inputs) #release_return_type #where_clause {
            #block
        }
    })
}

fn parse_iterator_return_type(
    return_type: &ReturnType,
) -> Result<(TokenStream2, Vec<TokenStream2>)> {
    match return_type {
        ReturnType::Type(_, ty) => {
            if let Type::ImplTrait(impl_trait) = ty.as_ref() {
                let mut item_type = None;
                let mut bounds = Vec::new();

                for bound in &impl_trait.bounds {
                    match bound {
                        TypeParamBound::Trait(trait_bound) => {
                            let path = &trait_bound.path;

                            // Check if this is an Iterator trait
                            if is_iterator_trait(path) {
                                // Extract the Item type from Iterator<Item = ...>
                                if let Some(seg) = path.segments.last() {
                                    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments
                                    {
                                        for arg in &args.args {
                                            if let syn::GenericArgument::AssocType(binding) = arg {
                                                if binding.ident == "Item" {
                                                    item_type = Some(binding.ty.to_token_stream());
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                // This is another bound like Send, Sync, or lifetime
                                bounds.push(bound.to_token_stream());
                            }
                        }
                        TypeParamBound::Lifetime(_) => {
                            bounds.push(bound.to_token_stream());
                        }
                        _ => {
                            // Handle any other bounds (e.g. Verbatim)
                            bounds.push(bound.to_token_stream());
                        }
                    }
                }

                if let Some(item) = item_type {
                    Ok((item, bounds))
                } else {
                    Err(Error::new_spanned(
                        return_type,
                        "Expected Iterator<Item = ...> in return type",
                    ))
                }
            } else {
                Err(Error::new_spanned(
                    return_type,
                    "Expected impl Iterator<...> return type",
                ))
            }
        }
        _ => Err(Error::new_spanned(
            return_type,
            "Expected -> impl Iterator<...> return type",
        )),
    }
}

fn is_iterator_trait(path: &Path) -> bool {
    path.segments
        .last()
        .map(|seg| seg.ident == "Iterator")
        .unwrap_or(false)
}

fn generate_boxed_return_type(item_type: &TokenStream2, bounds: &[TokenStream2]) -> TokenStream2 {
    if bounds.is_empty() {
        quote! { -> Box<dyn Iterator<Item = #item_type> + Send + Sync> }
    } else {
        quote! { -> Box<dyn Iterator<Item = #item_type> + #(#bounds)+*> }
    }
}

fn generate_boxed_return_type_with_lifetimes(
    item_type: &TokenStream2,
    bounds: &[TokenStream2],
) -> TokenStream2 {
    if bounds.is_empty() {
        quote! { -> Box<dyn Iterator<Item = #item_type>> }
    } else {
        quote! { -> Box<dyn Iterator<Item = #item_type> + #(#bounds)+*> }
    }
}
