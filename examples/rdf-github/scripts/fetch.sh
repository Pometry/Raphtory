#!/usr/bin/env bash
# Downloads every pull request (with its reviews) and issue of a GitHub repository as JSON lines.
#   scripts/fetch.sh OWNER/NAME OUT_DIR
# Needs the GitHub CLI (`gh`), logged in (`gh auth login`).
set -euo pipefail

REPO="${1:?usage: fetch.sh OWNER/NAME OUT_DIR}"
OUT="${2:?usage: fetch.sh OWNER/NAME OUT_DIR}"
OWNER="${REPO%%/*}"
NAME="${REPO#*/}"
mkdir -p "$OUT"

# 40 pull requests per page: with their reviews, larger pages time out on GitHub's side.
gh api graphql --paginate --jq '.data.repository.pullRequests.nodes[]' \
  -f owner="$OWNER" -f name="$NAME" -f query='
query($owner: String!, $name: String!, $endCursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 40, after: $endCursor, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        url number title createdAt mergedAt closedAt state additions deletions
        author { login } mergedBy { login }
        labels(first: 20) { nodes { name url } }
        reviews(first: 50) { nodes { author { login } submittedAt state } }
        closingIssuesReferences(first: 10) { nodes { url } }
      }
    }
  }
}' > "$OUT/prs.jsonl.tmp"
mv "$OUT/prs.jsonl.tmp" "$OUT/prs.jsonl"

gh api graphql --paginate --jq '.data.repository.issues.nodes[]' \
  -f owner="$OWNER" -f name="$NAME" -f query='
query($owner: String!, $name: String!, $endCursor: String) {
  repository(owner: $owner, name: $name) {
    issues(first: 100, after: $endCursor, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        url number title createdAt closedAt state
        author { login }
        labels(first: 20) { nodes { name url } }
        assignees(first: 10) { nodes { login } }
      }
    }
  }
}' > "$OUT/issues.jsonl.tmp"
mv "$OUT/issues.jsonl.tmp" "$OUT/issues.jsonl"

echo "$REPO: $(wc -l < "$OUT/prs.jsonl" | tr -d ' ') pull requests, $(wc -l < "$OUT/issues.jsonl" | tr -d ' ') issues in $OUT"
