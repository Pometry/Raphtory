# Filtering

The `filter` module provides a variety of functions prefixed with 'filter' that take a [filter expression][raphtory.filter.FilterExpr] and return a corresponding view.

Filter expressions allow you to create complex logical queries to select a narrower set of your data based on multiple criteria. This is useful when 

The following functions can be called on a `graph` or `node`:

- [filter_edges][raphtory.GraphView.filter_edges]
- [filter_exploded_edges][raphtory.GraphView.filter_exploded_edges]
- [filter_nodes][raphtory.GraphView.filter_nodes]

