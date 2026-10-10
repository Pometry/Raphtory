"""
Validates a graph against SHACL shapes now, as of the start of each year of its history, and on its
whole history (every triple ever asserted or retracted).

    python scripts/validate.py GRAPH_PATH SHAPES
"""

import collections
import datetime
import sys

from raphtory import RdfGraph


def summary(report):
    if report["conforms"]:
        return "conforms"
    by_constraint = collections.Counter(
        (result["path"] or "-", result["constraint_component"].rsplit("#", 1)[-1])
        for result in report["results"]
    )
    details = ", ".join(f"{n} x {path} {constraint}" for (path, constraint), n in by_constraint.most_common(3))
    return f"{len(report['results'])} violations ({details})"


def main(graph_path, shapes):
    graph = RdfGraph.load(graph_path)
    print(f"now:          {summary(graph.valid().validate_shacl(shapes))}")
    first, last = graph.graph.earliest_time.dt.year, graph.graph.latest_time.dt.year
    years = [datetime.datetime(year, 1, 1, tzinfo=datetime.timezone.utc) for year in range(first + 1, last + 1)]
    for time, report in graph.validate_shacl_at(shapes, years):
        print(f"as of {time.dt:%Y-%m-%d}: {summary(report)}")
    print(f"ever:         {summary(graph.validate_shacl(shapes))}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
