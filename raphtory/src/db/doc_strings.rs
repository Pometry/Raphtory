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

#[macro_export]
macro_rules! window_size_doc_string {
    () => {
        "
Returns the size of the window covered by this view

Returns:
    int: the size of the window"
    };
}

#[macro_export]
macro_rules! time_index_doc_string {
    () => {
        "
Returns the time index of this window set

It uses the last time of each window as the reference or the center of each if `center` is set to
`True`

Arguments:
    center (bool): if True time indexes are centered. Defaults to False

Returns:
    Iterable: the time index"
    };
}
