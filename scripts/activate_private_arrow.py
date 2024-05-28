#!/usr/bin/env python3
import subprocess
from pathlib import Path
import re

directory = "./raphtory-arrow"
root_dir = Path(__file__).parent.parent
toml_file = root_dir / "Cargo.toml"

with open(toml_file, "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines[:-1]):
    if "#[private-arrow]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("#") and "raphtory-arrow" in next_line:
            lines[i + 1] = re.sub(r"#\s*", "", next_line, 1)
    if "#[public-arrow]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("raphtory-arrow"):
            lines[i + 1] = next_line.replace("raphtory-arrow", "# raphtory-arrow", 1)

with open(toml_file, "w") as f:
    f.writelines(lines)
