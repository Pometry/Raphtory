from __future__ import annotations
from pathlib import Path
import hashlib
import requests
from requests.adapters import HTTPAdapter, Retry
from dataclasses import dataclass
import logging
import shutil


@dataclass
class Link:
    link: str
    checksum: str


def checksum(filepath: Path | str, expected_sha_hash: str) -> bool:
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    if sha256_hash.hexdigest() == expected_sha_hash:
        logging.info(f"{filepath} hash correct")
        return True
    else:
        logging.info(f"{filepath} hash invalid, got {sha256_hash.hexdigest()} expected {expected_sha_hash}")
        return False


def delete_source(filename: Path | str) -> None:
    logging.info(f"Deleting source file {filename}...")
    filename = Path(filename)
    filename.unlink(missing_ok=True)


def empty_folder(folder: str | Path):
    folder = Path(folder)
    for filename in folder.iterdir():
        try:
            if filename.is_file() or filename.is_symlink():
                filename.unlink()
            elif filename.is_dir():
                shutil.rmtree(filename, ignore_errors=True)
        except Exception:
            # silent continue
            continue


def safe_download_file(download_dir: Path | str, link: Link) -> Path:
    url = link.link
    expected_sha = link.checksum
    download_dir = Path(download_dir)
    logging.info(f"Downloading {url} to {download_dir}")
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    filename = url.split('/')[-1]
    file_location = download_dir / filename
    # stream file to reduce mem footprint
    try:
        r = req_session.get(url, stream=True)
        # raise an exception if http issue occurs
        r.raise_for_status()
        download_dir.mkdir(parents=True, exist_ok=True)
        with open(file_location, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
        status = checksum(file_location, expected_sha)
        r.close()
        if not status:
            delete_source(file_location)
            logging.info(f"Downloaded Jar {file_location} has incorrect checksum")
            raise SystemExit()
    except requests.exceptions.RequestException as err:
        logging.info(f"Major exception: {err}")
        delete_source(file_location)
        raise SystemExit(err)
    return file_location