use dynamic_graphql::Result;

pub(crate) fn path_prefix(path: String) -> Result<String> {
    let elements: Vec<&str> = path.split('/').collect();
    let size = elements.len();
    return if size > 2 {
        let delimiter = "/";
        let joined_string = elements
            .iter()
            .take(size - 1)
            .copied()
            .collect::<Vec<_>>()
            .join(delimiter);
        Ok(joined_string)
    } else {
        Err("Invalid graph path".into())
    };
}
