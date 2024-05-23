#!/usr/bin/env python3
import sys
import tempfile
import shutil

tmp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

if len(sys.argv) != 2:
    print("Usage: flip_ra.py <file>")
    sys.exit(1)

file = sys.argv[1]

with open(file, 'r') as f:
    with open(tmp_file.name, 'w') as tmp:
        for line in f:
            if line.startswith("#flip raphtory-arrow"):
                line = line.replace("#flip raphtory-arrow", "raphtory-arrow", 1)
            elif line.startswith("raphtory-arrow"):
                line = line.replace("raphtory-arrow", "#flip raphtory-arrow", 1)
            tmp.write(line)

shutil.move(tmp_file.name, file)
