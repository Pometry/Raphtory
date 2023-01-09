from __future__ import annotations


import subprocess
from pathlib import Path

from paths import lib_folder, ivy_folder, root_folder


ivy_files = (
    "arrow-core_2.13.jar",
    "arrow-core_2.13_ivy.xml",
    "arrow-messaging_2.13.jar",
    "arrow_messaging_2.13_ivy.xml",
    "core_2.13_ivy.xml",
    "core_2.13.jar",
)


def check_folder(folder: Path, files) -> bool:
    return all((folder / file).is_file() for file in files)


def check_build():
    build_done = check_folder(ivy_folder, ivy_files)
    return build_done


def make_python_build(rebuild=False):
    if rebuild or not check_build():
        subprocess.check_call(["make", "sbt-build"], cwd=root_folder)
