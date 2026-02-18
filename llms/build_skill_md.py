#!/usr/bin/env python3
"""
Build SKILL.md from raphtory_python_examples.py by executing the script
line-by-line and interleaving code with captured output.

Usage: python3 scripts/build_skill_md.py
"""

import ast
import io
import contextlib
import subprocess
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).parent / "raphtory_python_examples.py"
OUTPUT_PATH = Path(__file__).parent / "SKILL.md"

FRONTMATTER = """\
---
name: raphtory-python-api
description: >
  Use this skill when the user asks to write Python code using raphtory,
  create temporal graphs, run graph algorithms, query graph properties,
  use time views or layers, load graph data, or work with the raphtory
  Python library in any way. Also use when discussing raphtory API usage,
  graph analytics, or temporal network analysis with raphtory.
version: {version}
---

# Raphtory Python API Examples

"""


def get_version():
    result = subprocess.run(
        [sys.executable, "-c", "import raphtory; print(raphtory.__version__)"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def build_skill():
    version = get_version()
    source = SCRIPT_PATH.read_text()
    lines = source.splitlines()

    # Parse the script into blocks: comments, blank lines, and code chunks.
    # A code chunk is one or more consecutive non-comment, non-blank lines
    # that form a complete statement (or group of statements).
    blocks = []  # each block is ("comment", text) | ("blank",) | ("code", lines_text)
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip() == "":
            blocks.append(("blank",))
            i += 1
        elif line.lstrip().startswith("#"):
            blocks.append(("comment", line))
            i += 1
        else:
            # Collect consecutive code lines until we hit a blank or comment
            code_lines = []
            while i < len(lines) and lines[i].strip() != "" and not (
                lines[i].lstrip().startswith("#") and not code_lines_need_continuation(code_lines)
            ):
                code_lines.append(lines[i])
                i += 1
            blocks.append(("code", "\n".join(code_lines)))

    # Now execute code blocks incrementally and capture output
    namespace = {}
    md_lines = [FRONTMATTER.format(version=version)]
    in_code_block = False

    for block in blocks:
        if block[0] == "blank":
            if in_code_block:
                md_lines.append("```\n")
                in_code_block = False
            md_lines.append("")
        elif block[0] == "comment":
            if in_code_block:
                md_lines.append("```\n")
                in_code_block = False
            text = block[1]
            if text.startswith("## "):
                # Section header
                md_lines.append(text)
            else:
                md_lines.append(text)
        elif block[0] == "code":
            code_text = block[1]
            if not in_code_block:
                md_lines.append("```python")
                in_code_block = True

            # Capture stdout from executing this code block
            stdout_capture = io.StringIO()
            try:
                with contextlib.redirect_stdout(stdout_capture):
                    exec(compile(code_text, SCRIPT_PATH, "exec"), namespace)
            except Exception as e:
                print(f"Error executing:\n{code_text}\n\nException: {e}", file=sys.stderr)
                sys.exit(1)

            captured = stdout_capture.getvalue()

            # Add code lines to markdown
            for code_line in code_text.splitlines():
                md_lines.append(code_line)

            # Add captured output as comments
            if captured.strip():
                for out_line in captured.strip().splitlines():
                    md_lines.append(f"# => {out_line}")

    if in_code_block:
        md_lines.append("```\n")

    OUTPUT_PATH.write_text("\n".join(md_lines))
    print(f"Written {OUTPUT_PATH} ({len(md_lines)} lines, version {version})")


def code_lines_need_continuation(code_lines):
    """Check if the accumulated code lines are an incomplete statement."""
    if not code_lines:
        return False
    text = "\n".join(code_lines)
    try:
        ast.parse(text)
        return False
    except SyntaxError:
        return True


if __name__ == "__main__":
    build_skill()
