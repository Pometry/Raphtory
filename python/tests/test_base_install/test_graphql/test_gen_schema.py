import subprocess
from pathlib import Path

root = Path(__file__).parent
#graphql_dir = root /"docs" / "reference" / "graphql"
graphql_file_path = (root / "schema.graphql").as_posix()

result = subprocess.run(
    [
        "raphtory", "schema"
    ],
    capture_output=True,
    text=True
)

with open(graphql_file_path, 'w', encoding='utf8') as f:
    f.write(result)