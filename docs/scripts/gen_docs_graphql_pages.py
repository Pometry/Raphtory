import subprocess

result = subprocess.run(
    [
        "npx", "graphql-markdown",
        "--no-toc",
        "--update-file", "../reference/graphql/graphql_API.md",
        "../../raphtory-graphql/schema.graphql"
    ],
    capture_output=True,
    text=True
)
print(result)
