"""
Builds a temporal RDF graph of a GitHub repository's history from the JSON lines written by fetch.sh.

    python scripts/build_github_graph.py DATA_DIR GRAPH_PATH

Every event is written at the time it happened, so the graph can be queried as of any date:

    pull request opened   <pr> a gh:PullRequest ; gh:title ; gh:author ; gh:state gh:Open ;
                               gh:additions ; gh:deletions ; gh:label ; gh:closes <issue>
    review submitted      <developer> gh:reviewed <pr>  (and gh:approved <pr> if it approves)
    pull request merged   retract <pr> gh:state gh:Open ; assert gh:state gh:Merged ; gh:mergedBy
    pull request closed   retract <pr> gh:state gh:Open ; assert gh:state gh:Closed
    issue opened          <issue> a gh:Issue ; gh:title ; gh:author ; gh:state gh:Open ;
                                  gh:label ; gh:assignee
    issue closed          retract <issue> gh:state gh:Open ; assert gh:state gh:Closed

Developers and labels are typed (gh:Developer, gh:Label) and named with rdfs:label when first seen.
See schema/github.ttl for the shapes of the data.
"""

import collections
import json
import os
import shutil
import sys
import time

from raphtory import PersistentGraph

GH = "http://example.org/gh/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
# GitHub reports the author of content whose account was deleted as null
DELETED_USER = "ghost"


def iri(value):
    return f"<{value}>"


def literal(text, datatype=None):
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
    return f'"{escaped}"' + (f"^^<{datatype}>" if datatype else "")


def gh(name):
    return iri(GH + name)


def developer(login):
    return iri("https://github.com/" + login)


def login_of(actor):
    return (actor or {}).get("login") or DELETED_USER


def read_lines(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


class History:
    """Triples to assert and retract, grouped by the time they happened."""

    def __init__(self):
        self.events = collections.defaultdict(lambda: {"retract": [], "assert": []})
        self.first_seen = {}

    def add(self, time, s, p, o, action="assert"):
        self.events[time][action].append(f"{s} {p} {o} .")

    def node(self, time, subject, cls, name):
        """Types and names `subject` at the earliest time it appears."""
        if subject not in self.first_seen or time < self.first_seen[subject][0]:
            self.first_seen[subject] = (time, cls, name)

    def write(self, graph):
        for subject, (time, cls, name) in self.first_seen.items():
            self.add(time, subject, iri(RDF_TYPE), gh(cls))
            self.add(time, subject, iri(RDFS_LABEL), literal(name))
        written = 0
        for time in sorted(self.events):
            for action in ("retract", "assert"):
                triples = self.events[time][action]
                if triples:
                    document = "\n".join(triples).encode()
                    load = graph.load_rdf if action == "assert" else graph.retract_rdf
                    written += load(time, document, format="nt")
        return written


def add_labels(history, time, item, labels):
    for label in labels["nodes"]:
        history.add(time, item, gh("label"), iri(label["url"]))
        history.node(time, iri(label["url"]), "Label", label["name"])


def add_pull_request(history, pr):
    item, opened = iri(pr["url"]), pr["createdAt"]
    author = login_of(pr["author"])
    history.node(opened, developer(author), "Developer", author)
    history.add(opened, item, iri(RDF_TYPE), gh("PullRequest"))
    history.add(opened, item, gh("title"), literal(pr["title"]))
    history.add(opened, item, gh("author"), developer(author))
    history.add(opened, item, gh("state"), gh("Open"))
    history.add(opened, item, gh("additions"), literal(str(pr["additions"]), XSD_INTEGER))
    history.add(opened, item, gh("deletions"), literal(str(pr["deletions"]), XSD_INTEGER))
    add_labels(history, opened, item, pr["labels"])
    for issue in pr["closingIssuesReferences"]["nodes"]:
        history.add(opened, item, gh("closes"), iri(issue["url"]))
    for review in pr["reviews"]["nodes"]:
        reviewer = login_of(review["author"])
        if review["submittedAt"] is None or reviewer == author:
            continue
        history.node(review["submittedAt"], developer(reviewer), "Developer", reviewer)
        history.add(review["submittedAt"], developer(reviewer), gh("reviewed"), item)
        if review["state"] == "APPROVED":
            history.add(review["submittedAt"], developer(reviewer), gh("approved"), item)
    if pr["mergedAt"]:
        merged = pr["mergedAt"]
        history.add(merged, item, gh("state"), gh("Open"), "retract")
        history.add(merged, item, gh("state"), gh("Merged"))
        if pr["mergedBy"]:
            merger = login_of(pr["mergedBy"])
            history.node(merged, developer(merger), "Developer", merger)
            history.add(merged, item, gh("mergedBy"), developer(merger))
    elif pr["closedAt"]:
        history.add(pr["closedAt"], item, gh("state"), gh("Open"), "retract")
        history.add(pr["closedAt"], item, gh("state"), gh("Closed"))


def add_issue(history, issue):
    item, opened = iri(issue["url"]), issue["createdAt"]
    author = login_of(issue["author"])
    history.node(opened, developer(author), "Developer", author)
    history.add(opened, item, iri(RDF_TYPE), gh("Issue"))
    history.add(opened, item, gh("title"), literal(issue["title"]))
    history.add(opened, item, gh("author"), developer(author))
    history.add(opened, item, gh("state"), gh("Open"))
    add_labels(history, opened, item, issue["labels"])
    for assignee in issue["assignees"]["nodes"]:
        history.node(opened, developer(assignee["login"]), "Developer", assignee["login"])
        history.add(opened, item, gh("assignee"), developer(assignee["login"]))
    if issue["closedAt"]:
        history.add(issue["closedAt"], item, gh("state"), gh("Open"), "retract")
        history.add(issue["closedAt"], item, gh("state"), gh("Closed"))


def main(data_dir, graph_path):
    prs = read_lines(os.path.join(data_dir, "prs.jsonl"))
    issues = read_lines(os.path.join(data_dir, "issues.jsonl"))
    history = History()
    for pr in prs:
        add_pull_request(history, pr)
    for issue in issues:
        add_issue(history, issue)

    graph = PersistentGraph()
    start = time.time()
    written = history.write(graph)
    shutil.rmtree(graph_path, ignore_errors=True)
    os.makedirs(os.path.dirname(os.path.abspath(graph_path)), exist_ok=True)
    graph.save_to_file(graph_path)
    developers = sum(1 for _, cls, _ in history.first_seen.values() if cls == "Developer")
    print(
        f"{len(prs)} pull requests, {len(issues)} issues, {developers} developers: "
        f"{written} triple events at {len(history.events)} times in {time.time() - start:.1f}s -> {graph_path}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
