#!/usr/bin/env python3
import subprocess
from pathlib import Path
import re

directory = "./pometry-storage"
root_dir = Path(__file__).parent.parent
toml_file = root_dir / "Cargo.toml"

with open(toml_file, "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines[:-1]):
    if "#[private-storage]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("#") and "pometry-storage" in next_line:
            lines[i + 1] = re.sub(r"#\s*", "", next_line, 1)
    if "#[public-storage]" in line:
        next_line = lines[i + 1]
        if next_line.strip().startswith("pometry-storage"):
            lines[i + 1] = next_line.replace("pometry-storage", "# pometry-storage", 1)

with open(toml_file, "w") as f:
    f.writelines(lines)
