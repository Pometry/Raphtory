from __future__ import annotations
from check_platform import OS_MAC, OS_LINUX, OS_AARCH64, OS_X64, getOS, getArch
from download import Link, safe_download_file, empty_folder
import logging
from pathlib import Path
import tempfile
import shutil
import os


from paths import jre_folder

SOURCES = {
    OS_MAC: {
        OS_X64: Link(
            link='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_x64_mac_hotspot_11.0.16.1_1.tar.gz',
            checksum='10be61a8dd3766f7c12e2e823a6eca48cc6361d97e1b76310c752bd39770c7fe'
        ),
        OS_AARCH64: Link(
            link='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_aarch64_mac_hotspot_11.0.16.1_1.tar.gz',
            checksum='c84f38a7d87d50649ffc1f625facb4398fa54885371336a2cbf6ae2b435cbd10'
        )
    },
    OS_LINUX: {
        OS_X64: Link(
            link='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_linux_hotspot_11.0.17_8.tar.gz',
            checksum='752616097e09d7f60a3ad8bd312f90eaf50ac72577e55df229fe6e8091148f79'
        ),
        OS_AARCH64: Link(
            link='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_aarch64_linux_hotspot_11.0.17_8.tar.gz',
            checksum='bd6efe3290c8b5a42f695a55a26f3e3c9c284288574879d4b7089f31f5114177'
        )
    },
    # 'Windows': {
    #     OS_X64:
    #         {
    #             LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_windows_hotspot_11.0.17_8.zip',
    #             CHECKSUM_SHA256: '814a731f92dd67ad6cfb11a8b06dfad5f629f67be88ae5ae37d34e6eea6be6f4'
    #         }
    # }
}


def download_java(system_os: str, architecture: str, download_dir: str | Path) -> Path:
    # get the version and sha which is stored locally
    if system_os not in SOURCES:
        msg = f'Error: System OS {system_os} not supported.\nNot downloading.'
        logging.info(msg)
        raise SystemExit(msg)
    link = SOURCES[system_os][architecture]
    # open a session and download
    logging.info(f"Downloading JAVA JRE {system_os}-{architecture} from {link.link}. Please wait...")
    file_location = safe_download_file(download_dir, link)
    return file_location


def unpack_jre(filename: str | Path, jre_loc: str | Path):
    filename = Path(filename)
    jre_loc = Path(jre_loc)
    with tempfile.TemporaryDirectory() as unpack_dir:
        unpack_dir = Path(unpack_dir)
        system_os = getOS()
        logging.info(f'Unpacking JRE for {system_os}...')
        shutil.unpack_archive(filename, unpack_dir)
        try:
            result_dir = next(unpack_dir.iterdir())
        except StopIteration:
            raise Exception('Error: JRE unpacking failed.')
        # Empty jre folder
        empty_folder(jre_loc)
        if system_os == OS_MAC:
            move_dir = 'Contents/Home'
        else:  # if system_os == OS_LINUX:
            move_dir = '.'
        for file_name in (result_dir / move_dir).iterdir():
            shutil.move(file_name, jre_loc)
        logging.info('Cleaning up...')


def check_system_dl_java() -> Path:
    java = jre_folder / "bin" / 'java'
    if not java.is_file():
        with tempfile.TemporaryDirectory() as download_dir:
            download_dir = Path(download_dir)
            logging.info("Downloading java...")
            system_os = getOS()
            logging.info(f"- Operating system: {system_os} ")
            architecture = getArch()
            logging.info(f"- Architecture: {architecture}")
            logging.info(f"Downloading to {download_dir}")
            jre_folder.mkdir(exist_ok=True, parents=True)
            download_loc = download_java(system_os, architecture, download_dir)
            unpack_jre(download_loc, jre_folder)
    return java
