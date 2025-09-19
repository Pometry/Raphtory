# Filtering

The `filter` module provides a variety of functions prefixed with 'filter' that take a [filter expression][raphtory.filter.FilterExpr] and return a corresponding view.

Filter expressions allow you to create complex logical queries to select a narrower set of your data based on multiple criteria. This is useful when you already have some knowledge of the subset you want to isolate. For example, a cybersecurity team investigating the impact of a CVE on your companies servers might filter for nodes with a 'server' type that have a specific 'OS' and 'version' in their metadata. This would give the security team a view that contains only nodes that might be vulnerable.

The following functions can be called on a `graph` or `node`:

- [filter_edges][raphtory.GraphView.filter_edges]
- [filter_exploded_edges][raphtory.GraphView.filter_exploded_edges]
- [filter_nodes][raphtory.GraphView.filter_nodes]

