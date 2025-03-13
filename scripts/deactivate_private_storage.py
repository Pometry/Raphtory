#!/usr/bin/env python3
import subprocess
from pathlib import Path
import re

directory = "./pometry-storage-private"
root_dir = Path(__file__).parent.parent
toml_file = root_dir / "Cargo.toml"

with open(toml_file, "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines[:-1]):
    if "#[public-storage]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("#") and "pometry-storage-private" in next_line:
            lines[i + 1] = re.sub(r"#\s*", "", next_line, 1)
    if "#[private-storage]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("pometry-storage-private"):
            lines[i + 1] = next_line.replace("pometry-storage-private", "# pometry-storage-private", 1)

with open(toml_file, "w") as f:
    f.writelines(lines)
