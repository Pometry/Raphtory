from __future__ import annotations

import os
import tempfile
from download import safe_download_file, Link
from pathlib import Path
import shutil
import logging
import subprocess


IVY_BIN = Link(
    link='https://github.com/Raphtory/Data/raw/main/apache-ivy-2.5.0-bin.zip',
    checksum='7c6f467e33c28d82f4f8c3c10575bb461498ad8dcabf57770f481bfea59b1e59'
)


def download_ivy(download_dir):
    ivy_file_location = safe_download_file(download_dir, IVY_BIN)
    shutil.unpack_archive(ivy_file_location, extract_dir=download_dir)
    return download_dir / "apache-ivy-2.5.0" / "ivy-2.5.0.jar"


def get_and_run_ivy(java: str | Path, ivy_folder: str | Path, lib_folder: str | Path, ivy_bin: str | Path = None) -> None:
    ivy_folder = Path(ivy_folder)
    lib_folder = Path(lib_folder)
    with tempfile.TemporaryDirectory() as download_dir:
        # cleans up ivy after we are done
        download_dir = Path(download_dir)
        if ivy_bin is not None:
            ivy_bin = Path(ivy_bin)
            ivy_bin.mkdir(exist_ok=True, parents=True)
            ivy_jar = ivy_bin / "ivy-2.5.0.jar"
            if not ivy_jar.is_file():
                shutil.copyfile(download_ivy(download_dir), ivy_jar)
        else:
            ivy_jar = download_ivy(download_dir)

        logging.info(
            f"IVY dl dir: {download_dir}, input dir: {ivy_folder}, lib dir: {lib_folder}")
        retrieve = str(lib_folder) + "/[artifact]-[type]-[revision].[ext]"
        settings = str(ivy_folder.parent / "ivysettings.xml")
        # retrieve = "."
        subprocess.check_call(
            [str(java), f"-Divy_dir={ivy_folder}", "-jar", ivy_jar,
             "-settings", settings, "-ivy", str(ivy_folder / "core_ivy.xml"),
             "-retrieve", retrieve, "-confs", "runtime"])
