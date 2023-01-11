from __future__ import annotations

import os
import tempfile
from download import safe_download_file, Link
from pathlib import Path
import shutil
import logging
import subprocess


from paths import ivy_folder, lib_folder, ivy_bin, build_folder, root_folder

IVY_BIN = Link(
    link='https://github.com/Raphtory/Data/raw/main/apache-ivy-2.5.0-bin.zip',
    checksum='7c6f467e33c28d82f4f8c3c10575bb461498ad8dcabf57770f481bfea59b1e59'
)


def version() -> str:
    with open(root_folder / "version") as f:
        return f.readline()


def download_ivy(download_dir):
    ivy_file_location = safe_download_file(download_dir, IVY_BIN)
    shutil.unpack_archive(ivy_file_location, extract_dir=download_dir)
    return download_dir / "apache-ivy-2.5.0" / "ivy-2.5.0.jar"


def get_and_run_ivy(java: str | Path) -> None:
        ivy_bin.mkdir(exist_ok=True, parents=True)
        ivy_jar = ivy_bin / "ivy-2.5.0.jar"
        if not ivy_jar.is_file():
            with tempfile.TemporaryDirectory() as download_dir:
                download_dir = Path(download_dir)
                shutil.copyfile(download_ivy(download_dir), ivy_jar)

                logging.info(
                    f"IVY downloaded: {download_dir}, input dir: {ivy_folder}, lib dir: {lib_folder}")
        retrieve = str(lib_folder) + "/[organisation].[artifact]-[revision](-[classifier]).[ext]"
        settings = str(build_folder / "ivysettings.xml")
        # retrieve = "."
        with tempfile.TemporaryDirectory() as cache_dir:
            subprocess.check_call(
                [str(java), f"-Divy_dir={ivy_folder}", f"-Divy_tmp_cache={cache_dir}", "-jar", ivy_jar,
                 "-settings", settings, "-dependency", "com.raphtory", "core_2.13", version(),
                 "-retrieve", retrieve, "-sync", "-refresh", "-confs", "runtime"])

