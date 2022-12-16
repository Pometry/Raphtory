from __future__ import annotations
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


def get_and_run_ivy(java: str | Path, ivy_folder: str | Path, lib_folder: str | Path) -> None:
    ivy_folder = Path(ivy_folder)
    lib_folder = Path(lib_folder)
    with tempfile.TemporaryDirectory() as download_dir:
        # cleans up ivy after we are done
        download_dir = Path(download_dir)
        ivy_file_location = safe_download_file(download_dir, IVY_BIN)
        shutil.unpack_archive(ivy_file_location, extract_dir=download_dir)
        logging.info(
            f"IVY dl dir: {download_dir}, input dir: {ivy_folder}, lib dir: {lib_folder}")
        retrieve = str(lib_folder) + "/[conf]/[artifact]-[type]-[revision].[ext]"
        ivy_jar = str(download_dir / "apache-ivy-2.5.0" / "ivy-2.5.0.jar")
        for fname in ivy_folder.glob("*.xml"):
            subprocess.check_call(
                [str(java), "-jar", ivy_jar, "-ivy", str(fname), "-retrieve", retrieve, "-confs", "runtime"])
