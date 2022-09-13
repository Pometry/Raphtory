import requests
from requests.adapters import HTTPAdapter, Retry
import shutil
import os.path
import hashlib

_URL = 'url'
_SHA256 = 'sha256'

RAPHTORY_JARS_HASH = {
    '0.2.0a0': {
        _URL: 'https://github.com/Haaroon/Raphtory/releases/download/v0.2.0a0/0.2.0a0.jar',
        _SHA256:  'e3f85e40825916c3f0f7395204c95869f65b6330d12abcc9f2f5f8d8b83a3f26'
    }
}

def checksum(filepath, expected_sha_hash):
    sha256_hash = hashlib.sha256()
    with open(filepath,"rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
    if sha256_hash.hexdigest() == expected_sha_hash:
        print(f"{filepath} hash correct")
        return True
    else:
        print(f"{filepath} hash invalid, got {sha256_hash.hexdigest()} expected {expected_sha_hash}")
        return False

def delete_jar(filename):
    if os.path.exists(filename):
        shutil.rmtree(filename)

def download_file(url: str, download_dir: str, expected_sha: str):
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    print(f"Downloading file from {url}")
    filename = url.split('/')[-1]
    file_location = str(download_dir + '/' + filename)
    # stream file to reduce mem footprint
    try:
        with req_session.get(url, stream=True) as r:
            # raise an exception if http issue occurs
            r.raise_for_status()
            if not os.path.exists(download_dir):
                os.mkdir(download_dir)
            with open(file_location, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        status = checksum(file_location, expected_sha)
        if not status:
            delete_jar(file_location)
            raise SystemExit(f"Downloaded Jar {file_location} has incorrect checksum")
    except requests.exceptions.TooManyRedirects as err:
        print(f"Bad URL, Too Many Redirects: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    except requests.exceptions.RequestException as err:
        print(f"Major exception: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    return file_location

def download_raphtory(version, download_dir):
    if version not in RAPHTORY_JARS_HASH:
        msg = f'Error: Version {version} not found in Python dictionary.\nNot downloading.'
        print(msg)
        raise SystemExit(msg)
    file_url = RAPHTORY_JARS_HASH[version][_URL]
    expected_sha = RAPHTORY_JARS_HASH[version][_SHA256]
    download_file(file_url, download_dir, expected_sha)

