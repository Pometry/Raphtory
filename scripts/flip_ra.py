#!/usr/bin/env python3
import shutil
import os, stat
import subprocess

directory = "./raphtory-arrow"

def remove_readonly(func, path, _):
    "Clear the readonly bit and reattempt the removal"
    os.chmod(path, stat.S_IWRITE)
    func(path)

shutil.rmtree(directory, onerror=remove_readonly)

subprocess.run(
    [
        "git",
        "clone",
        "--depth",
        "1",
        "git@github.com:Pometry/raphtory-arrow.git",
        "raphtory-arrow",
    ]
)
