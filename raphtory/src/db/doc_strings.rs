#[macro_export]
macro_rules! default_layer_doc_string {
    () => {
        "
Create a view including all the edges in the default layer

Returns:
    a view including all the edges in the default layer"
    };
}

#[macro_export]
macro_rules! layer_doc_string {
    () => {
        "
Create a view including all the edges in the layer `name`

Arguments:
    name (str) : the name of the layer

Returns:
    a view including all the edges in the layer `name`"
    };
}
