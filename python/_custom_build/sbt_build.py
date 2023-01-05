from __future__ import annotations


import subprocess
from pathlib import Path


jars = ("arrow-core.jar", "arrow-messaging.jar", "core.jar")


def make_python_build(lib_folder: Path | str):
    lib_folder = Path(lib_folder)
    if not all((lib_folder / jar).is_file() for jar in jars):
        subprocess.check_call(["make", "sbt-build"])
