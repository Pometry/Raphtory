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
macro_rules! layers_doc_string {
    () => {
        "
Create a view including all the edges in the layers `names`

Arguments:
    names (str) : the names of the layers to include

Returns:
    a view including all the edges in the layers `names`"
    };
}

#[macro_export]
macro_rules! window_size_doc_string {
    () => {
        "
Returns the size of the window covered by this view

Returns:
    int: the size of the window"
    };
}
