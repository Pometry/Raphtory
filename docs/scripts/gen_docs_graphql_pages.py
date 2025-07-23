import subprocess
from pathlib import Path

root = Path(__file__).parent.parent.parent
graphql_dir = root /"docs" / "reference" / "graphql"
schema_dir = root / "raphtory-graphql"

frontmatter = """---
hide:
  - navigation
---

<!-- START graphql-markdown -->

<!-- END graphql-markdown -->
"""

# Create graphql_API.md if it does not already exist
# Write frontmatter to file

graphql_file_path = graphql_dir / "graphql_API.md"
graphql_file_path.parent.mkdir(parents=True, exist_ok=True)

with open(graphql_file_path, 'w', encoding='utf8') as f:
    f.write(frontmatter)


result = subprocess.run(
    [
        "npx", "graphql-markdown",
        "--no-toc",
        "--update-file", (graphql_dir / "graphql_API.md").as_posix(),
        (schema_dir / "schema.graphql").as_posix()
    ],
    capture_output=True,
    text=True
)
print(result)
